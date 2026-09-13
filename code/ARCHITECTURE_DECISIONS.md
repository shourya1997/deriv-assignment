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
