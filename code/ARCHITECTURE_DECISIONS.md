# code/ build — architecture decisions

ADR-style entries for decisions made *during* implementation. Purpose: stop a later phase (or a
fresh session) from re-litigating or accidentally reversing an already-settled call. Grep this
before re-deriving reasoning.

## ADR-1 — Config-driven pivot

**Context:** initial plan was one hand-written loader + one hand-authored DAG per named table.
**Decision:** one declarative YAML schema per table, generic 3-layer engine dispatching on it,
single `dag_factory.py`, tests parametrized over config.
**Alternatives considered:** per-table hand-written modules (rejected — user explicitly
required config-driven + auto-generated DAGs + per-table tests generated from config).
**Consequences:** `layer3` must be a list (not a single target) to support `dim_client`/
`client_profile`'s two-source-file, two-target shape. Two DAGs (`bootstrap_warehouse`,
`cdc_historical_reload`) stay hand-authored for stated reasons (cross-config topological
ordering; runtime-parameterized client iteration) — guarded by
`test_dags_dir_contains_only_factory_and_two_named_files` so a third never sneaks in.

## ADR-2 — G2 baseline-seed ordering and idempotency

**Context:** `risk_snapshot_key NOT NULL` on both fact tables; all 62 real fact rows predate
the earliest CDC event, so `resolve_risk_snapshot_key()` returns NULL for all of them on first
load.
**Decision:** seed one baseline `dim_client_risk_snapshot` row per client at `source_lsn=0`,
`valid_from='1970-01-01'`, via `INSERT ... ON CONFLICT ON CONSTRAINT uq_client_lsn DO NOTHING`.
Exclude CL030 (its only CDC event is `op='insert'` — seeding it would fabricate a
pre-existing state that never existed). Orphan clients CL099/CL031 get sentinel values
(`risk_category='unknown'`, `account_balance_usd=0.00`, `account_status='unknown'`).
Sequenced as an explicit `bootstrap_warehouse` DAG dependency, strictly before any
`apply_cdc_event` call.
**Alternatives considered:** plain re-run without ON CONFLICT guard (rejected — `make verify`
runs the seed twice, would violate the constraint on the second run); seeding CL030 like every
other client (rejected — fabricates false history, makes the dataset's only `insert` op
untestable); relying on DAG task ordering alone without documenting the invariant (rejected —
`resolve_risk_snapshot_key()` has no `ORDER BY` before `LIMIT 1`, so overlapping
`is_current=true` rows would make results nondeterministic if seed-before-CDC were ever
violated).
**Consequences:** e2e test suite must include an explicit invariant test ("≤1 current row per
client, no overlapping validity windows"), not just a happy-path count check.

## ADR-3 — `scd2_apply` / `scd2_baseline_seed` naming unification

**Context:** early plan drafts used `cdc_scd2` and `scd2_apply` interchangeably for the same
concept, and briefly listed `scd2_apply` in both the layer2 and layer3 strategy enums.
**Decision:** one name, `scd2_apply`, layer3-only. `scd2_baseline_seed` is `client_profile`'s
second layer3 target (implements ADR-2). Both are documented, explicit exceptions to "every
layer3 strategy is a generic upsert" — named in the enum, not hidden in code.
**Consequences:** any code or test referencing `cdc_scd2` is wrong; grep for the literal string
`scd2_apply` when in doubt about which config triggers `warehouse.apply_cdc_event()`.

## ADR-4 — Great Expectations scoped down

**Context:** `airflow-provider-great-expectations` operator + GE's version coupling to Airflow
identified by dual review as the single most likely cause of a broken `docker compose up`,
while satisfying none of the six hard requirements on its own.
**Decision:** exactly one real GE suite (`staging_vendor_deposits`, per `part1_pipeline.md`
§5's own example), executed via a plain `PythonOperator` calling `ge_runner.run_suite()`
against a `SqlAlchemyExecutionEngine` — not the GE Airflow provider operator. Every other
table's DQ runs via config-declared SQL assertions through the identical
severity/on_failure → quarantine/`dq_check_results` routing path.
**Alternatives considered:** GE suite per table via the official operator (rejected — highest
dependency-breakage risk for a deliverable whose primary requirement is "runs cleanly on a
laptop with nothing but Docker installed"); dropping GE entirely (rejected — would silently
drop a `CLAUDE.md`-locked platform decision without disclosure).
**Consequences:** `ge==0.18.22` pinned (0.18.x keeps the YAML suite format; 1.x dropped it).
Any new table's DQ checks are SQL assertions in its config unless there's a specific reason to
add it as a second real GE suite.

## ADR-5 — Step 1 harness fixes (postgres healthcheck race, advisory lock, GE driver)

**Context:** dual review (Opus + Sonnet) against the built harness surfaced three issues that
would silently corrupt or block a clean `docker compose up` despite tests passing locally.
**Decision:**
1. Postgres healthcheck must run an actual query against the target DB (`psql ... select 1`),
   not `pg_isready` with no host — `pg_isready` goes green against the Unix socket that's live
   *during* `docker-entrypoint-initdb.d`, before `00_create_databases.sh` has created `deriv`/
   `airflow`, so dependents could start against a not-yet-ready database.
2. `migrate.py::run_migrations()` takes `pg_advisory_xact_lock` before checking/applying the
   manifest, so two concurrent callers (e.g. a retried `airflow-init` alongside a local
   `pytest` run) serialize instead of racing on the same `CREATE TABLE`/`CREATE FUNCTION` DDL.
3. `great_expectations[postgresql]` (not bare `great_expectations`), because Airflow 2.10.5
   pins `SQLAlchemy<2.0`, which has no native psycopg3 dialect — GE's planned
   `SqlAlchemyExecutionEngine` (ADR-4) needs `psycopg2-binary` to talk to Postgres at all. The
   Airflow 2.10.5 constraints file is passed to the image's `pip install` so this doesn't
   silently downgrade Airflow's own pinned deps.
**Alternatives considered:** leaving `pg_isready` and relying on `depends_on: service_healthy`
timing to mask the race (rejected — passed locally by luck, not by construction, per Opus's
finding); skipping the advisory lock since the harness has no current concurrent caller
(rejected — cheap to add now, becomes load-bearing once Airflow retries or parallel test runs
exist).
**Consequences:** any future service added to `docker-compose.yml` that depends on Postgres
being fully initialized (not just accepting connections) must declare `postgres:
condition: service_healthy` explicitly — YAML anchors do not deep-merge, so an overridden
`depends_on` block silently drops it if omitted (this exact bug hit 3 services in Step 1).

## ADR-6 — Step 2 config-loader fail-loud invariants

**Context:** dual review (Opus + Sonnet) independently converged on the same core risk in
`deriv_pipeline/config.py`: a config-driven pipeline's single biggest failure mode is a
broken/misspelled config file failing *silently* — passing `--validate-all`, then simply
producing no DAG or corrupting data downstream — since the whole point of the design is that
these YAML files are the only per-table code most future changes will touch.
**Decision — three invariants, enforced from Step 2 onward and binding on every future config
loader:**
1. Every field documented as a YAML list (`natural_key`, `expected_columns`,
   `update_columns`, `layer3[].columns`) must be validated as an actual list of strings, not
   merely truthy — a bare `if not value` accepts a scalar string and silently produces
   per-character iteration downstream.
2. Every raised error must include the source file's path. A raw `TypeError`/`KeyError` from
   unpacking a YAML dict into a dataclass is caught and re-raised as `ValueError(f"{path}:
   ...")` — `airflow-init`'s one-shot container log is the *only* diagnostic surface on a
   broken deploy, so an error without a filename is not actionable.
3. "Zero configs found" and "an unrecognized/typo'd `kind`" must be load-time errors, never
   silently-empty success — `validate_all()` raises if `config/tables/*.yml` yields nothing,
   and the single `_load_all_table_dir()` dispatch raises on any file whose `kind` isn't in
   the known set (previously, three independent per-kind filters simply skipped a file with a
   bad `kind`, which is a config-driven pipeline's worst possible failure mode: green
   validation, missing pipeline).
**Alternatives considered:** leaving type-checking to whatever consumes `TableConfig` later
(rejected — pushes a load-time bug to a runtime failure deep inside the layer engine, with a
much worse error message); logging a warning on an unknown `kind` instead of raising (rejected
— warnings in a one-shot init container's scrollback are not a reliable gate, per the same
"only diagnostic signal" reasoning as #2).
**Consequences:** every future config kind (the CDC/`scd2_apply` table, reconciliation
configs) must route through the same `_load_yaml`/`_require_str_list` helpers rather than
re-implementing ad hoc parsing, so these three invariants stay enforced repo-wide rather than
per-loader.

## ADR-7 — Step 3 risk-snapshot stopgap-seed safety rules

**Context:** Step 3's walking skeleton needs `_resolve_risk_snapshot_key()` to resolve a
`risk_snapshot_key` for every vendor_deposits row, but ADR-2's real, properly-ordered G2
baseline seed doesn't exist until Step 4's `bootstrap_warehouse` DAG. Every real fact row in
Step 3 predates any CDC event, so every single lookup misses on first load. Dual review (Opus +
Sonnet) independently flagged the first implementation's naive fallback — insert one wide-open
`valid_from='1970-01-01', valid_to='9999-12-31', is_current=true` sentinel row per client on
any miss — as unsafe on two counts: it can be silently shadowed by or collide with ADR-2's real
seed once Step 4 lands, and it can violate `resolve_risk_snapshot_key()`'s own un-ordered
`LIMIT 1` invariant (at most one row may cover any instant) *within Step 3 itself*, since one
`fact_upsert()` run processes a client's deposits across multiple distinct `deposit_date`s with
no real baseline for any of them.
**Decision — the Step 3 stopgap seed (not a substitute for ADR-2, superseded by it in Step 4)
must follow three rules:**
1. Only seed when the client has zero rows with `source_lsn >= 0` (a "real" CDC/baseline lsn).
   A miss against a client who already has real history means `event_ts` predates their
   earliest real `valid_from` — a genuine backfill/reconciliation question, not something a
   blind seed should paper over — so this case raises instead of guessing.
2. Every stopgap window is the narrowest possible: `[event_ts, event_ts + 1 microsecond)`.
   Since `event_ts` is always a `deposit_date` at midnight, distinct dates for the same client
   can never overlap, and a repeat lookup for the same date idempotently resolves to the
   already-seeded row instead of attempting a second insert.
3. Every stopgap row uses a strictly negative `source_lsn` (`COALESCE(MIN(source_lsn), 0) - 1`
   per client, so multiple stopgap windows per client get distinct values and never collide on
   `uq_client_lsn`), and `is_current=false` — both so Step 4's real bootstrap can identify and
   supersede every `source_lsn < 0` row per client, and so `source_lsn >= 0` remains a reliable
   "this client has real history" signal for rule 1 above.
**Alternatives considered:** a single 1970–9999 sentinel per client (rejected — both
correctness failures above, confirmed independently by both review agents); deferring FK
resolution entirely until Step 4 (rejected — blocks the whole point of Step 3, an actually-
running walking skeleton); blocking on any existing row regardless of `source_lsn` sign
(rejected during fix verification — real Step 3 data has clients with multiple deposit dates,
so this raised spuriously on the second date for the same client; only *real* history should
block the fallback).
**Consequences:** Step 4's `bootstrap_warehouse` DAG must explicitly find and replace every
`dim_client_risk_snapshot` row with `source_lsn < 0` per client as part of applying the real,
properly-ordered baseline seed — it cannot assume the table starts empty, since this is a
persistent live DB shared across steps, not a fresh fixture per phase.

## ADR-8 — Step 4: dimensions properly, real G2 baseline seed, and orphan-client handling

**Context:** Step 4 onboards `client_signup`/`client_profile` as full config-driven tables and
adds two new dimension-config kinds (`derived_dimension`: `dim_manager`, `dim_instrument;
`generated_dimension`: `dim_date`), plus the real ADR-2 G2 baseline seed for
`dim_client_risk_snapshot` that Step 3's stopgap (ADR-7) exists to tide over until.

**Decisions:**
1. **Derived-dimension source, per dimension.** `dim_manager` reads distinct `assigned_manager`
   values from the already-staged `staging.client_signup` table (`source_table`). `dim_instrument`
   instead reads directly from the raw `data/client_trades.json` file (`raw_source_glob`),
   because `client_trades.yml` has no `kind: table` config until Step 5 — reading from a
   not-yet-onboarded staging table would create a forward dependency `bootstrap_warehouse` can't
   express. `DerivedDimensionConfig` requires exactly one of the two at config-load time.
2. **Owned-column `dimension_upsert` into a shared dimension.** `client_signup` and
   `client_profile` both write into `warehouse.dim_client`, but each config's `layer3[].columns`
   lists only the columns that source owns (e.g. `client_signup` never touches
   `full_name`/`risk_category`). `fk_resolution` resolves a column (e.g. `manager_key`) via
   another dimension's natural key instead of copying a raw value.
3. **Orphan-client baseline handling (generic, not hardcoded).** ADR-2's text calls out
   CL099/CL031-shaped clients — referenced by fact tables but absent from `client_profile`
   entirely — as needing a sentinel baseline (`risk_category='unknown'`,
   `account_balance_usd=0.00`, `account_status='unknown'`). Implemented generically:
   `scd2_baseline_seed` finds orphans as "has a Step 3 `source_lsn < 0` stopgap row in
   `dim_client_risk_snapshot` AND never appeared in this run's `client_profile` rows AND wasn't
   excluded as insert-first," not by hardcoded client_id. This was a dual-review finding
   (Sonnet #3 / Opus #4, independently convergent) — the first implementation only seeded
   baselines for clients present in `client_profile`, silently leaving orphans stuck on a
   Step 3 stopgap forever, which ADR-7 explicitly frames as temporary.
4. **Guard against overlapping real history.** `scd2_baseline_seed` now skips seeding (rather
   than inserting) for any client who already has a `source_lsn > 0` row — a wide-open
   1970–9999 baseline landing after real CDC history exists would create two overlapping
   `is_current=true` rows, the exact failure mode ADR-7 rule 1 exists to prevent on the Step 3
   side (Opus dual-review finding).
5. **`bootstrap_warehouse` DAG edge, not two independent branches.** `client_signup`'s and
   `client_profile`'s upserts both write into `warehouse.dim_client`; the DAG now sequences
   `upsert_client_signup_dimension >> stage_client_profile` instead of running the two branches
   independently, so a real scheduler can't run them concurrently against the same row
   (Opus dual-review finding).
6. **`client_signup`/`client_profile` orchestration schedule is `null`, not `@daily`.** Both are
   static one-time snapshot files; `dag_factory.py` auto-generates a standalone `table__*` DAG
   for every `kind: table` config regardless of schedule, and a `@daily`-scheduled instance of
   that auto-generated DAG would run concurrently with (and independently of)
   `bootstrap_warehouse`'s hand-ordered sequencing — wrong semantically and a source of
   deadlock/ordering risk (Opus dual-review finding). `verify.sh`'s `airflow dags test
   bootstrap_warehouse` runs (twice, for idempotency) before the loop over all `table__*` DAGs
   specifically so a fresh environment's standalone `table__client_signup`/`table__client_profile`
   test run doesn't fail resolving `dim_manager`/`dim_client` FKs that only `bootstrap_warehouse`
   populates first.
7. **Rowcount-based, not unconditional, counters.** Both `derived.py`'s `load()` and
   `scd2_baseline_seed`'s orphan/normal seeding paths count actual writes via `cur.rowcount`
   (or a dedicated newly-inserted check before the stopgap repoint), not `+= 1` per candidate
   considered — a rerun's `ON CONFLICT DO NOTHING` no-ops must not inflate the reported count.

**Dual review (Opus + Sonnet), fixes applied:** NULL FK value crash in `_resolve_dimension_fk`
(both reviewers, independently) — now returns `None` immediately for a `None` input instead of
issuing a `WHERE col = NULL` lookup that always misses and then raising; missing
`cdc_source_glob` validation for `scd2_baseline_seed` at config-load time (Sonnet) — now raises
`ValueError` in `TableConfig.load()`; TOCTOU gap in `_repoint_and_clear_stopgap_snapshots`
(Opus) — the final DELETE now uses the exact `stopgap_keys` list captured by the initial SELECT,
not a re-evaluated predicate; fact-table FK discovery via `information_schema.columns` matching
column name could match views or the dimension's own PK column (Opus) — replaced with a
`pg_constraint`/`pg_attribute` walk for true FKs into `dim_client_risk_snapshot`; orphan-client
baseline gap (Sonnet/Opus, convergent) — see decision 3 above; missing DAG edge (Opus) — see
decision 5; `@daily` schedule mismatch (Opus) — see decision 6; unconditional counters (Opus,
partial) — see decision 7.

**Deferred (accepted limitations, not fixed this phase):** Sonnet #4 (no dedup guard if a
config's `columns:` list repeats an entry — malformed config, not exercised by any shipped
config); Sonnet #5 (JSON floats parsed as Python `float` rather than `Decimal` — acceptable at
this prototype's scale, revisit if real currency-precision requirements surface); Opus #10
(multi-column natural keys / column-name collisions across dimensions — no shipped config needs
this yet); Opus #11 (`derived.py` hardcodes the `staging.` schema prefix rather than resolving
the referenced table config's actual `layer2.target` — every shipped `source_table` case
happens to target `staging.*`, so this is a latent forward-compat gap, not a live bug); Opus #12
(`derived_dimension`'s hard-fail on an unmapped `derived_columns` value is deliberate fail-loud
behavior per this project's established convention, not something to soften).

**Consequences:** Step 6's CDC-apply DAG must depend on `bootstrap_complete` (ADR-2's ordering
invariant, restated from ADR-7) — the baseline seed, including the orphan-client sentinel path,
must exist before any `apply_cdc_event` call. The `source_lsn > 0` guard added in decision 4
means `scd2_baseline_seed` becomes a safe no-op once Step 6 lands, rather than needing to be
disabled or special-cased.
