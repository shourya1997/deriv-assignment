# code/ — runnable prototype

A config-driven, dockerized, TDD-built implementation of the pipeline designed in
[../part1_pipeline.md](../part1_pipeline.md) and [../part2_data_model.md](../part2_data_model.md):
Postgres warehouse, Airflow orchestration, one generated DAG per table config, layer 1 (raw) →
layer 2 (staging) → layer 3 (warehouse). See [PROGRESS.md](PROGRESS.md) and
[ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md) for the full phase-by-phase build log and
every dual-reviewed design decision (ADR-1 through ADR-14).

## Run it

```bash
docker compose up -d --build
make verify
```

`make verify` (= `scripts/verify.sh`) validates every config, runs the full pytest suite
(unit/integration/dags), waits for every expected DAG to appear, runs each generated and
hand-authored DAG twice via `airflow dags test` (proving idempotency), then runs the `e2e` suite
against the live database re-deriving every assertion from `data/` at test time — never a bare
literal. Exits non-zero on any failure.

## Architecture

- **Config-driven**: one YAML per table (`config/tables/*.yml`) or reconciliation
  (`config/reconciliations/*.yml`) drives the layer engine, `dags/dag_factory.py`, and the test
  suite identically — adding a new source table needs only a new config file, no new DAG file and
  no new hand-written per-table test (ADR-1).
- **Generic layer engine** (`deriv_pipeline/layers/`): `land_layer1` → `stage_layer2` (schema-drift
  and late-arrival detection) → `run_dq_checks` → `load_layer3`, dispatched by
  `layer3.strategy` (`fact_upsert`, `dimension_upsert`, `generated_dimension`, and the two named
  exceptions `scd2_apply` / `scd2_baseline_seed`, which call locked plpgsql functions instead of a
  generic upsert — ADR-3).
- **`dag_factory.py`** generates one `table__{name}` DAG per `kind: table` config and one
  `reconcile_{name}` DAG per reconciliation config; only two DAGs are hand-authored
  (`bootstrap_warehouse`, for the cross-config ordering a per-table loop can't express, and
  `cdc_historical_reload`, a manual-only operator-triggered repair job) — enforced by
  `test_dags_dir_contains_only_factory_and_named_hand_authored_files` and the Step 10 completeness
  guardrails (`test_dag_factory_generates_one_dag_per_*_config`,
  `test_generated_task_dependencies_are_layer1_2_3_order`; ADR-14).
- **Data quality**: one real Great Expectations suite (`staging_vendor_deposits`); every other
  table's checks are config-declared SQL assertions. Both paths log to
  `data_quality.dq_check_results` / the `dq_table_health` regression view, and quarantine CRITICAL
  failures — but a failed CRITICAL check does not block `load_layer3` (ADR-12; see Caveats below).
- **Reconciliation**: `reconcile_vendor_feed` diffs vendor vs. internal deposit key sets
  (config-declared SQL, same convention as DQ checks) and logs discrepancies through the same
  `dq_table_health` path (ADR-13).

## Caveats (documented trade-offs, not bugs)

- **Watermark-only CDC ordering can permanently drop an out-of-order event from the live stream.**
  `scd2_apply` (Step 6) processes CDC events in file-arrival order and advances a per-client
  watermark; an event whose `lsn` arrives *after* a newer one has already advanced the watermark is
  quarantined as `stale_lsn` and never applied to `dim_client_risk_snapshot` by the live DAG. This
  is real in the shipped data (client CL001's `lsn 1004` arrives after `lsn 1005`/`1006` and is
  dropped by the streaming path). The version is not lost — `cdc_historical_reload` (Step 7)
  re-replays a client's full raw history in `lsn` order and repairs it — but between the drop and
  the next reload run, that dimension version is simply absent from `dim_client_risk_snapshot`.
  This is a deliberate trade-off (a real-time streaming apply cannot re-sort an unbounded future
  window without unbounded buffering), not an oversight.
- **CRITICAL data-quality failures are logged and quarantined but don't block load.** The real
  `vendor_deposits` data has a permanent negative-amount row; raising on CRITICAL would fail
  `run_dq_checks` on every run forever and, under `verify.sh`'s `set -e`, permanently break the
  project's own definition of "green" (ADR-12).
- **`resolve_risk_snapshot_key()` has no `ORDER BY` before `LIMIT 1`** — safe only because the G2
  baseline seed (ADR-2/ADR-8) guarantees non-overlapping SCD2 validity windows per client, not
  because the query itself enforces it.
- **Tombstoned rows keep `is_current = true`.** Every current-state read must also filter
  `is_deleted = false`.

## Tests

```bash
docker compose --profile test run --rm tests            # full suite
docker compose --profile test run --rm --entrypoint /bin/bash tests \
  -c "pytest /opt/deriv/code/tests/dags/test_dag_factory.py -v"   # targeted run
```

Airflow-dependent tests (`tests/dags/`) `importorskip("airflow")` and only run inside the
container; `tests/unit/` and `tests/integration/` also run on a bare host with a reachable
Postgres. See [TASK.md](TASK.md) for the phase checklist.
