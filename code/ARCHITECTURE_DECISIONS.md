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

## ADR-9 — Step 5: generic `fact_upsert`, shared `fact_deposits`, and cross-DAG stopgap safety

**Context:** Step 5 onboards `client_deposit`/`client_trades` as the second and third callers of
`fact_upsert`, which Step 3's dual review flagged (and deliberately deferred) as hardcoded to
`vendor_deposits`' exact column shape. Generalizing it surfaces real interactions between
independently-scheduled `@daily` DAGs that a single-caller function never exercised.

**Decisions:**
1. **Generalize via two new `Layer3Target` fields, not a second strategy.** `event_date_column`
   names the staging column driving both `dim_date` resolution and the risk-snapshot lookup
   timestamp; `literals` writes a fixed per-row value (e.g. `source_system`) instead of copying
   one from staging. `fk_resolution` entries may now be a dict shaped like `dimension_upsert`'s
   (`from_column`/`dim_target`/`dim_natural_key`/`dim_surrogate_key`), reusing
   `_resolve_dimension_fk`, alongside the two existing special strings
   (`inferred_member_on_miss`/`snapshotted_fk`). One generic function, config expresses the
   variation — consistent with ADR-1's overall config-driven design.
2. **Shared `warehouse.fact_deposits`, disjoint natural keys, not per-source fact tables.**
   `client_deposit.yml` and `vendor_deposits.yml` both target `fact_deposits`, distinguished by a
   `literals: {source_system: ...}` entry and disjoint `deposit_id` prefixes (`DEP*` vs `VDEP*`).
   Mirrors Step 4's shared-`dim_client` pattern, but here each source owns whole rows rather than
   disjoint columns of the same row, so (unlike client_signup/client_profile) no DAG-ordering
   edge is needed between the two — verified no shipped id collides (decision 6 covers the
   unenforced general case).
3. **`_resolve_dimension_fk` gains an explicit `nullable` keyword, default `True`.** The
   function's original `None → None` short-circuit was written for `dim_client.manager_key`
   (genuinely nullable); reusing it unguarded for `fact_trades.instrument_key` (`NOT NULL`) let a
   trade with a missing instrument stage as a silent NULL and crash later on a bare
   `NotNullViolation` instead of a clear diagnostic (Step 5 dual review finding, both reviewers).
   Default `True` preserves `dimension_upsert`'s existing behavior with no config change;
   `client_trades.yml`'s `instrument_key` rule sets `nullable: false`.
4. **Cross-DAG stopgap-lsn allocation is now lock-guarded, not just single-writer-assumed.**
   `_resolve_risk_snapshot_key`'s stopgap path (ADR-7) was written when `fact_upsert` had exactly
   one caller, each DAG run serializing its own rows through one connection — no concurrent
   writer was reachable. Step 5 adds two more independent `@daily` DAGs with overlapping
   `client_id`s and differing event dates between feeds, making the read-then-insert allocation
   a real, independently-confirmed race (both reviewers — see fix below). Chose
   `pg_advisory_xact_lock(hashtext(client_id))` over restructuring the DAG graph: it fixes the
   race unconditionally regardless of scheduling, is transaction-scoped (auto-released, no
   explicit unlock), and doesn't require synchronizing `client_deposit`/`client_trades` against
   each other the way client_signup/client_profile's *column*-level race required in Step 4 —
   this is a *row*-level allocation race, orthogonal to schedule/DAG topology.
5. **`client_trades` gets an explicit `requires_dim_instrument` orchestration flag, honored by a
   `PythonSensor`, not an `ExternalTaskSensor` on `bootstrap_warehouse`.** Unlike
   `vendor_deposits`/`client_deposit` (fully self-sufficient: `dim_client`/`dim_date` rows are
   created on demand), `client_trades`' `instrument_key` fk_resolution is a hard,
   non-creatable-on-demand prerequisite on `warehouse.dim_instrument`, populated only by
   `bootstrap_warehouse` — which is `schedule=None` (manual/run-once) and has no comparable
   `execution_date` for an `ExternalTaskSensor` to match against this DAG's own `@daily` runs.
   Instead, `dag_factory.py` reads a new `orchestration.requires_dim_instrument: true` flag and
   prepends a `PythonSensor` (`mode="reschedule"`) polling `SELECT EXISTS(SELECT 1 FROM
   warehouse.dim_instrument)` before `land_layer1`. This was the concrete gap behind an
   initially-wrong premise: the plan going into this step was "these tables only read
   already-bootstrapped dims, so `bootstrap_warehouse` needs no changes" — true for `dim_client`
   (idempotent `ON CONFLICT DO NOTHING`), but false in the sense that mattered: `dim_instrument`
   isn't creatable on demand at all (Opus dual-review finding).
6. **Config-load-time validation added for all three new `fact_upsert` shapes.** Mirroring the
   existing `scd2_baseline_seed`/`cdc_source_glob` pattern: `event_date_column` is now required
   (a missing one previously crashed with a bare `TypeError` deep in `fact_upsert` at DAG
   runtime); `literals` must be a YAML mapping (a scalar previously crashed with
   `AttributeError`); each dict-shaped `fk_resolution` rule must carry all four required keys (a
   missing one previously crashed with `KeyError`); and a non-dict `fk_resolution` value must be
   one of the two known sentinel strings (a typo, e.g. `inferred_member` instead of
   `inferred_member_on_miss`, previously silently flipped behavior with no error at all — the
   most severe of the four, since it fails silently rather than loudly) (both reviewers).

**Dual review (Opus + Sonnet) process note:** the first attempt (both agents) hit a
session-wide rate limit before producing findings and was retried after reset. The retry used
`isolation: worktree` for both agents, which silently handed them a checkout missing the
uncommitted Step 5 diff entirely (and, for the Sonnet agent, no `code/` tree at all — a worktree
built from a state that predates this repo's own Step 1) — the Sonnet agent correctly refused to
fabricate findings against files it couldn't read rather than rubber-stamp the diff; had it not,
this would have been a false "nothing found" clean bill. Retried a third time without worktree
isolation, both agents then reviewed the actual diff and converged independently on the same
central findings (decisions 3-5 above) — noted here as a process lesson: `isolation: worktree`
is unsafe for reviewing *uncommitted* changes and should not be used for that again.

**Fixes applied:** see decisions 3-6 above (nullable FK semantics, advisory-lock stopgap
allocation, `requires_dim_instrument` sensor, config-load-time validation); `insert_cols` in
`fact_upsert` was built by flat list concatenation while `source_cols` was a deduped set — any
config overlap (e.g. a `columns` entry colliding with a `literals` key) would emit a column
twice in the generated INSERT and be rejected by postgres (Opus) — fixed with
`list(dict.fromkeys(...))`, the same idiom `layer2_staging.py` already uses for the identical
reason; `_resolve_dimension_fk`'s "no such row" error unconditionally said `"dimension_upsert:
..."`, misleading when raised from `fact_upsert` (Opus) — fixed to a caller-agnostic message.

**Deferred (accepted limitations, not fixed this phase):** a NULL value for a fact column with a
`DEFAULT` (e.g. `fact_deposits.fee_usd DEFAULT 0`) is still inserted as an explicit NULL rather
than letting the DEFAULT fire, since `fact_upsert` always lists every configured column — no
shipped source data exercises this (Opus); `event_date_column`'s dual use for both `dim_date`
and the risk-snapshot timestamp silently truncates time-of-day via `datetime.combine` if ever
pointed at a `timestamptz` column instead of a `date` column, and the same coupling becomes a
systematic one-day-staleness risk once Step 6's real CDC can change `valid_from` intraday (Opus)
— not reachable by any current config, revisit if a future table needs the two to diverge;
`vendor_deposits`/`client_deposit`'s shared-`fact_deposits` disjoint-`deposit_id` assumption is
unenforced — a colliding id between the two feeds would silently flip that row's `source_system`
and overwrite its measures via the generated `ON CONFLICT DO UPDATE` (Opus) — `Reconciliation
Config` already models this exact pair but isn't wired to enforce anything until Step 9.

**Consequences:** any future `fact_upsert` caller with a hard, non-creatable dimension
prerequisite (like `dim_instrument`) should follow decision 5's `requires_dim_instrument`-style
pattern rather than assuming `bootstrap_warehouse` ordering happens to work out; any future
`fact_upsert` caller with a NOT NULL dict-shaped FK should set `nullable: false` on that rule.

## ADR-10 — Step 6: `scd2_apply` CDC, the two-shape raw-table generalization, and file-order replay

**Context:** `client_profile_changes.yml` is the second and last config-declared exception to a
generic layer3 upsert (ADR-1/ADR-3): `strategy: scd2_apply` calls the locked
`warehouse.apply_cdc_event()` (`sql/03`) once per staged row. Two design questions had to be
answered before any code: (1) `client_profile_changes` doesn't fit the `natural_key + jsonb
payload` shape every other raw table uses — `sql/04`'s reload driver (Step 7) needs to read it by
real column name, so both its raw and staging tables were already built typed (see migrations
005/006, anticipated ahead of this step); (2) `apply_cdc_event`'s watermark check is the *sole*
staleness guard by design (no batch sort inside the function) — so whatever replays staging rows
into it must itself preserve true file-arrival order, not just any order.

**Decision:**
- **Two raw-table shapes, detected generically.** `layer1_raw.py`/`layer2_staging.py` branch on
  `"payload" in column_types(conn, schema, table)` (an `information_schema` introspection, never a
  hardcoded table name) rather than special-casing `client_profile_changes` by name anywhere —
  consistent with ADR-1's "no per-table special-casing" rule. The existing jsonb-payload path is
  byte-identical for every other source.
- **`raw_seq`/`staging_seq` bigserial ordering columns** (migration 010) give a real, gap-tolerant
  total order matching insertion order exactly — a plain `SELECT` with no `ORDER BY` gives no
  ordering guarantee, and a timestamptz column (`ingested_at`/`staged_at`) can tie under fast
  sequential inserts within one layer run. `ON CONFLICT DO UPDATE` on re-land never touches these
  columns in its SET clause, so a rerun preserves the original ordinal. Verified against real
  data: `client_profile_changes.jsonl`'s file order for CL001 is lsn 1005, then 1004 (stale —
  arrives after 1005 in file order), then 1006 (applied last, wins) — proof this ordering
  requirement isn't a constructed edge case.
- **`requires_scd2_baseline` + `wait_for_scd2_baseline` sensor**, the same cross-DAG shape as
  ADR-9's `requires_dim_instrument`: gates the `@daily` `client_profile_changes` DAG behind
  `bootstrap_warehouse`'s (`schedule=None`) baseline-seed step having run. This still matters even
  though `apply_cdc_event` itself tolerates either order without crashing: if CDC-apply for a
  client runs first, `scd2_baseline_seed`'s "already has real (`source_lsn > 0`) history" check
  then skips seeding a baseline for that client entirely, leaving their pre-CDC-era fact rows
  (deposits/trades dated before the earliest CDC event) with no `resolve_risk_snapshot_key` window
  to resolve into.

**Dual review (Opus + Sonnet)**, both directly against the repo (`isolation: worktree` explicitly
avoided per ADR-9's own lesson: it silently hands a review agent a checkout with no uncommitted
diff). Both independently converged on the same most-severe finding; Opus's pass caught two
further real bugs. **All 6 confirmed findings were fixed before this phase was considered done —
nothing deferred:**

1. **Unbounded quarantine growth on every rerun (both reviewers, empirically reproduced: run1
   quarantine=2 rows, run2=14).** `scd2_apply`'s replay `SELECT` was unconditional — it re-scanned
   the *entire* staging table every call. `apply_cdc_event`'s stale-lsn branch is not idempotent
   (a fresh `quarantine.rejected_rows` row on every call with an already-applied lsn), so an
   `@daily` schedule would re-quarantine a client's whole history every single day, burying the one
   real signal (a genuine out-of-order event) in permanently growing replay noise. Fixed: the
   replay `SELECT` is filtered by a semi-join against `warehouse.cdc_watermark`
   (`WHERE NOT EXISTS (SELECT 1 FROM warehouse.cdc_watermark w WHERE w.client_id = s.client_id
   AND s.lsn <= w.last_applied_lsn)`), making an already-applied row a true no-op at the Python
   level without touching the locked `sql/03` function. The replay-idempotency test now also
   asserts on `quarantine.rejected_rows` count, not just the snapshot table — the original version
   of that test would have passed while this bug happened.
2. **`wait_for_scd2_baseline`'s check was a one-time global existence test, but the invariant is
   per-client and ongoing (Opus).** `EXISTS(...WHERE source_lsn = 0)` is satisfied forever after
   `bootstrap_warehouse`'s *first* run, even for a client onboarded later with no baseline of
   their own; combined with `scd2_baseline_seed`'s skip-if-already-has-real-history guard, the
   miss would be permanent and unrecoverable through normal operation. Fixed: the sensor now also
   checks, set-based, that every client with a staged CDC event (other than one whose *only* event
   is an `insert`) already has a `source_lsn = 0` row — kept alongside the original global check
   as a floor, since `staging.client_profile_changes` is still empty the first time this sensor
   ever runs (this DAG's own `land_layer1`/`stage_layer2` execute *after* the sensor), so the
   per-client check alone would pass vacuously on a fresh deploy.
3. **Concurrent DAG runs could crash on the `cdc_watermark` insert (Opus).** `sql/03`'s
   `SELECT ... FOR UPDATE` takes no lock at all when no watermark row exists yet for a client —
   two concurrent first-applies for the same client could both see `NULL` and collide on the
   watermark `INSERT`'s PK, rolling back the whole run with a raw `UniqueViolation`. Fixed in
   `scd2_apply` (since `sql/03` is locked): a per-client `pg_advisory_xact_lock(hashtext(...))`,
   the same pattern ADR-9 already used for the analogous risk-snapshot stopgap race.
4. **A `delete` for a client with zero existing snapshot rows was a silent triple no-op (Opus).**
   `apply_cdc_event`'s tombstone `SELECT` finds nothing to copy for such a client, so no dimension
   row is written; no quarantine row either; yet the watermark still advances — contradicting
   `scd2_apply`'s own "a layer3 row *or* a quarantine row per layer2 row" invariant and making the
   gap unrecoverable by a plain rerun. Fixed: `scd2_apply` checks explicitly for an existing
   snapshot row before calling `apply_cdc_event` on a `delete`, and quarantines by name
   (`delete_with_no_baseline`) instead of proceeding.
5. **The typed-passthrough branch used `row[c]`, not `.get(c)` (both reviewers).** A CDC source
   that omits a key entirely (rather than emitting explicit `null` — common for WAL decoders on
   insert) would raise an uncaught `KeyError` and drop the whole batch, with nothing landing in
   quarantine (this path has no drift/quarantine handling of its own, unlike the jsonb-payload
   path which defers to layer2's `resolve_header`). Fixed: `.get(c)`.
6. **No config-load-time validation tied `scd2_apply`'s hardcoded column list, target, or
   orchestration flag to the actual config (Opus).** `scd2_apply` hardcodes
   `client_id, lsn, commit_ts, op, after` and always writes to
   `warehouse.dim_client_risk_snapshot`, but nothing checked `expected_columns` actually included
   those names, that `target` was that table, or that `requires_scd2_baseline` was set — any one
   typo would silently NULL the dimension, silently ignore a misconfigured target, or silently
   reintroduce decision 4's ordering race. Fixed: `config.py` validates all three for
   `strategy: scd2_apply` entries at config-load time, matching the existing
   `fact_upsert`/`scd2_baseline_seed` pattern.

**Consequences:** any future CDC-style config should reuse the `raw_seq`/`staging_seq` ordering
mechanism rather than trusting an unordered `SELECT`; any future `requires_*` cross-DAG sensor
should be re-examined for whether its precondition is really global or actually per-entity (the
same class of bug as finding 2 here); a strategy function that calls a locked, non-idempotent SQL
function should filter its own replay set rather than relying on the function's internal checks
to make reruns safe.
