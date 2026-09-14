# code/ — runnable prototype

A config-driven, dockerized, TDD-built implementation of the pipeline designed in
[../part1_pipeline.md](../part1_pipeline.md) and [../part2_data_model.md](../part2_data_model.md):
Postgres warehouse, Airflow orchestration, one generated DAG per table config, layer 1 (raw) →
layer 2 (staging) → layer 3 (warehouse). See [PROGRESS.md](PROGRESS.md) and
[ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md) for the full phase-by-phase build log and
every dual-reviewed design decision (ADR-1 through ADR-14).

## Contents

- [Run it locally](#run-it-locally)
- [Directory layout](#directory-layout)
- [High-level architecture](#high-level-architecture)
- [How a config becomes a DAG](#how-a-config-becomes-a-dag)
- [Code flow: one row's journey through the pipeline](#code-flow-one-rows-journey-through-the-pipeline)
- [Writing a table config](#writing-a-table-config)
- [Writing a dimension config](#writing-a-dimension-config)
- [Writing data quality checks](#writing-data-quality-checks)
- [Writing a reconciliation config](#writing-a-reconciliation-config)
- [Caveats](#caveats-documented-trade-offs-not-bugs)
- [Tests](#tests)

## Run it locally

**Prerequisites:** Docker Desktop (or a Docker Engine + Compose v2 install) — nothing else. No
local Python/Postgres/Airflow install is required or used.

```bash
git clone <this repo>
cd deriv-assignment/code
docker compose up -d --build
```

This builds the Airflow image (`docker/airflow/Dockerfile`) and starts three long-running
services:

| Service | What it does | Port |
|---|---|---|
| `postgres` | Postgres 16, owns both the `airflow` metadata DB and the `deriv` warehouse DB | `localhost:5432` |
| `airflow-scheduler` | Parses `dags/`, schedules/executes tasks | — |
| `airflow-webserver` | Airflow UI | `localhost:8080` |

A one-shot `airflow-init` service runs first (via `depends_on: service_completed_successfully`)
and does everything a fresh deploy needs before the scheduler/webserver start: `airflow db
migrate`, creates the `admin`/`admin` UI user, runs `deriv_pipeline/migrate.py` (applies every
`sql/*.sql` + gap-fill DDL in dependency order, idempotently), then `python -m
deriv_pipeline.config --validate-all` (fails the whole `docker compose up` loudly if any YAML
config is broken, rather than surfacing as a silent DAG-import error later).

Once containers are up (`docker compose ps` — wait for `airflow-webserver` to report
`healthy`, usually 30–60s):

- **Airflow UI**: open **http://localhost:8080** in a browser, log in with `admin` / `admin`.
  You'll see 9 DAGs: `bootstrap_warehouse`, `cdc_historical_reload`, `reconcile_vendor_feed`, and
  6 `table__*` DAGs (one per `config/tables/*.yml` with `kind: table`).
- **Postgres**: connect directly with `psql postgresql://deriv:deriv@localhost:5432/deriv` (or any
  client) — override `POSTGRES_USER`/`POSTGRES_PASSWORD` via a `.env` file if you want different
  credentials; `docker-compose.yml` reads them with `deriv`/`deriv` defaults.

**Run the pipeline for real** (in dependency order, either by hand from the UI or all at once):

```bash
make verify
```

`make verify` (= `bash scripts/verify.sh`) is the single command that proves the whole thing
works end-to-end. In order, it: validates every config; runs the full pytest suite
(unit/integration/dags); polls `airflow dags list` until every expected DAG has been parsed; runs
every DAG **twice** via `airflow dags test` in dependency order (`bootstrap_warehouse` → every
`table__*` DAG → `reconcile_vendor_feed` → `cdc_historical_reload`) to prove idempotency; then runs
the `e2e` pytest suite against the live `deriv` database, re-deriving every count assertion from
[../data/](../data/) at test time rather than a hardcoded literal. Exits non-zero on the first
failure and prints a summary table on success.

**Other everyday commands** (see [Makefile](Makefile)):

```bash
make test    # full pytest suite only (docker compose --profile test run --rm tests)
make logs    # tail scheduler/webserver logs
make down    # docker compose down -v — stops everything and drops the Postgres volume
```

**Troubleshooting:**
- `airflow-webserver` stuck at `starting`/unhealthy for a long time → check `docker compose logs
  airflow-init`; a config validation failure there is the most common cause and prints the exact
  file and reason.
- Editing a file under `config/`, `dags/`, or `deriv_pipeline/` takes effect without a rebuild —
  they're bind-mounted (see the `volumes:` block in [docker-compose.yml](docker-compose.yml)); the
  scheduler picks up DAG changes within ~30s.
- `docker compose down -v` before re-running `up` if you want a fully clean Postgres volume (e.g.
  after editing `sql/*.sql` or a migration file).

## Directory layout

```
code/
├── config/
│   ├── tables/            # one YAML per table/derived-dimension/generated-dimension
│   └── reconciliations/   # one YAML per cross-source reconciliation
├── dags/
│   ├── dag_factory.py     # the only per-table DAG code — generates every table__*/reconcile_* DAG
│   ├── bootstrap_warehouse.py    # hand-authored: one-time dimension + baseline seed ordering
│   └── cdc_historical_reload.py  # hand-authored: manual-only CDC repair job
├── deriv_pipeline/
│   ├── config.py          # config schema + validation (this is what --validate-all runs)
│   ├── db.py               # connection helper (DERIV_DSN)
│   ├── migrate.py          # applies sql/*.sql + gap-fill migrations, tracked in schema_migrations
│   ├── dq.py                # Step 8: GE suite + SQL-assertion dispatch
│   ├── layers/
│   │   ├── layer1_raw.py       # land source files as-is into raw.*
│   │   ├── layer2_staging.py   # drift/late-arrival detection, cast, upsert into staging.*
│   │   └── layer3_warehouse.py # strategy dispatch into warehouse.* (facts/dimensions/SCD2)
│   ├── recon/reconcile.py  # Step 9: config-declared-SQL key-set diff
│   ├── reload.py            # Step 7: lsn-order historical replay
│   └── dims/dim_date.py     # on-demand dim_date row creation
├── migrations/              # gap-fill DDL not in ../sql/ (schemas, quarantine, data_quality, …)
├── scripts/verify.sh        # `make verify`
└── tests/{unit,integration,dags,e2e}/
```

## High-level architecture

```mermaid
flowchart LR
    subgraph Sources["data/ (assignment input files)"]
        CSV["Vendor CSVs\ndeposits_vendor_*.csv"]
        JSON["client_trades.json\nclient_signup.json\nclient_profile.json"]
        JSONL["client_profile_changes.jsonl\n(CDC stream)"]
    end

    subgraph L1["Layer 1 — raw.*"]
        R1["land as-is\n(layer1_raw.py)"]
    end

    subgraph L2["Layer 2 — staging.*"]
        S1["schema-drift + late-arrival\ndetection, typed cast\n(layer2_staging.py)"]
        DQ["run_dq_checks\n(dq.py: GE suite or\nSQL assertions)"]
    end

    subgraph L3["Layer 3 — warehouse.* (Kimball star)"]
        FACT["fact_deposits\nfact_trades\n(fact_upsert)"]
        DIM["dim_client, dim_manager,\ndim_instrument, dim_date\n(dimension_upsert /\nderived / generated)"]
        SCD2["dim_client_risk_snapshot\n(scd2_apply / scd2_baseline_seed)"]
    end

    subgraph Quality["data_quality schema"]
        DQR[("dq_check_results\n+ dq_table_health view")]
        QUAR[("quarantine.rejected_rows")]
        RECON[("reconciliation_discrepancies")]
    end

    CSV --> R1
    JSON --> R1
    JSONL --> R1
    R1 --> S1
    S1 --> DQ
    DQ --> FACT
    DQ --> DIM
    DQ --> SCD2
    DQ -.logs.-> DQR
    DQ -.CRITICAL fail.-> QUAR
    FACT -.reads.-> DIM
    FACT -.snapshotted FK.-> SCD2
    RECONCILE["reconcile_vendor_feed\n(reconcile.py)"] -.diffs.-> FACT
    RECONCILE -.logs.-> DQR
    RECONCILE -.logs.-> RECON

    ORCH["Airflow\n(dag_factory.py generates\none DAG per config)"] -.orchestrates.-> R1
    ORCH -.orchestrates.-> S1
    ORCH -.orchestrates.-> DQ
    ORCH -.orchestrates.-> FACT
```

Every arrow from `data/` to `warehouse.*` is driven by a YAML file, not hand-written per-table
code (ADR-1) — see [Writing a table config](#writing-a-table-config) below.

## How a config becomes a DAG

```mermaid
flowchart TD
    YML["config/tables/*.yml\n(kind: table)"] -->|"TableConfig.load_all(kind='table')"| CFG["list[TableConfig]"]
    RYML["config/reconciliations/*.yml"] -->|"ReconciliationConfig.load_all()"| RCFG["list[ReconciliationConfig]"]

    CFG -->|"for cfg in configs:\n_make_dag(cfg)"| LOOP["dag_factory.py\nmodule-level loop"]
    RCFG -->|"for cfg in configs:\n_make_reconciliation_dag(cfg)"| LOOP

    LOOP -->|"globals()['table__' + cfg.name] = dag"| REG["Airflow's DagBag\n(scans dags/ folder,\nimports this module)"]
    LOOP -->|"globals()['reconcile_' + cfg.name] = dag"| REG

    REG --> UI["Airflow UI /\nairflow dags list"]

    style YML fill:#e8f4ea,stroke:#2f8a4c
    style RYML fill:#e8f4ea,stroke:#2f8a4c
    style LOOP fill:#fdf3d8,stroke:#a06d10
```

`dag_factory.py` is imported fresh by the scheduler every DAG-parsing cycle (~30s), so editing or
adding a `config/tables/*.yml` file produces a new/changed DAG without touching `dags/` at all —
this is the mechanism [`test_dag_factory_generates_one_dag_per_table_config`](tests/dags/test_dag_factory.py)
guards (ADR-14).

## Code flow: one row's journey through the pipeline

Walking through `table__vendor_deposits`'s daily run, task by task (every generated DAG follows
this identical shape — see [`_make_dag`](dags/dag_factory.py)):

```mermaid
flowchart LR
    A["land_layer1"] --> B["stage_layer2"] --> C["run_dq_checks"] --> D["load_layer3"]
```

1. **`land_layer1`** (`layers/layer1_raw.py`) — reads every file matching `source.glob` under
   `data/`, parses per `source.format` (csv/json/jsonl), and `INSERT ... ON CONFLICT DO UPDATE`s
   the row as-is (no aliasing, no typing) into `raw.<name>` keyed by `source.natural_key`. Re-running
   against the same files is a no-op content-wise.
2. **`stage_layer2`** (`layers/layer2_staging.py`) — reads back from `raw.<name>`, resolves each
   row's header against `source.expected_columns`/`source.aliases` (a per-`source_file` cache,
   since drift is a per-file-header fact — a schema-drift file gets its aliased column populated
   plus a `schema_drift_detected` WARNING), casts to the real column types of
   `layer2.target` (introspected via `information_schema`, no type map in config), applies
   `source.late_arrival` if declared, and upserts per `layer2.conflict_strategy`.
3. **`run_dq_checks`** (`deriv_pipeline/dq.py`) — if `layer2.ge_suite` is set, runs that Great
   Expectations suite against `layer2.target`; always also runs every `layer2.dq_checks` SQL
   assertion. Every check logs one row to `data_quality.dq_check_results` (shared `run_id` =
   Airflow's own DAG-run id); a failed `CRITICAL` check additionally writes a summary row to
   `quarantine.rejected_rows`. Does **not** raise or block the next task (see
   [Caveats](#caveats-documented-trade-offs-not-bugs)).
4. **`load_layer3`** (`layers/layer3_warehouse.py`) — for each entry in `layer3`, dispatches on
   `strategy`:
   - `fact_upsert` — resolves every `fk_resolution` entry (a plain dimension lookup, the
     `inferred_member_on_miss` late-arriving-dimension pattern, or `snapshotted_fk` against
     `dim_client_risk_snapshot` at the row's `event_date_column`), then
     `INSERT ... ON CONFLICT DO UPDATE`s into the fact target using `columns` + `literals`.
   - `dimension_upsert` — owned-column upsert into a shared dimension table (e.g. `client_signup`
     and `client_profile` both write different columns of `dim_client` without clobbering each
     other).
   - `scd2_apply` / `scd2_baseline_seed` — the two named exceptions (ADR-3): call the locked
     plpgsql `warehouse.apply_cdc_event()` / baseline-seed insert directly, once per staged CDC
     row in arrival order, rather than a generic upsert.

A `wait_for_dim_instrument` or `wait_for_scd2_baseline` `PythonSensor` is prepended before
`land_layer1` when `orchestration.requires_dim_instrument` / `requires_scd2_baseline` is set (see
[Writing a table config](#writing-a-table-config)) — see [`_make_dag`](dags/dag_factory.py) and
ADR-9/ADR-10.

## Writing a table config

Add a new file under `config/tables/your_table.yml` — no other file needs to change; the next DAG
parse (or `make verify`) picks it up automatically. Full shape:

```yaml
kind: table                 # table | derived_dimension | generated_dimension
name: your_table             # DAG becomes table__your_table; must be unique across
                              # config/tables/ AND config/reconciliations/ (shared dq_check_results namespace)

source:
  format: csv                 # csv | json | jsonl
  glob: "your_table_*.csv"    # glob under ../data/, resolved by layer1_raw.py
  natural_key: [id_column]    # REQUIRED, non-empty list — used for ON CONFLICT in raw.* and staging.*
  expected_columns: [id_column, other_col, ...]   # source columns beyond natural_key
  aliases: {legacy_name: expected_name}           # optional: source header -> expected_columns name
  late_arrival:                                    # optional
    delivery_date: {from: filename, pattern: 'your_table_(\d{8})\.csv'}
    event_date_col: some_date_column
    threshold_days: 2         # (delivery_date - event_date) > threshold_days => flagged late

layer1:
  target: raw.your_table

layer2:
  target: staging.your_table
  conflict_strategy: upsert_do_update   # currently the only supported value
  update_columns: [status]              # columns refreshed on conflict (natural_key never re-updates itself)
  ge_suite: staging_vendor_deposits     # optional — see "Writing data quality checks"
  dq_checks: [...]                      # optional — see "Writing data quality checks"

layer3:
  - target: warehouse.fact_your_table
    strategy: fact_upsert     # dimension_upsert | fact_upsert | scd2_apply | scd2_baseline_seed
    columns: [col_a, col_b]                # non-key columns copied from staging
    event_date_column: some_date_column     # REQUIRED for fact_upsert — drives dim_date + snapshotted_fk lookup
    literals: {source_system: your_source}  # optional: target_column -> fixed value
    fk_resolution:
      dim_client: inferred_member_on_miss   # sentinel: create a late-arriving dim_client stub on miss
      risk_snapshot: snapshotted_fk          # sentinel: FK into dim_client_risk_snapshot at event_date_column
      some_fk_column:                        # or: a full dict rule for a plain dimension lookup
        from_column: staging_column_name
        dim_target: warehouse.dim_something
        dim_natural_key: natural_key_col_on_dim
        dim_surrogate_key: surrogate_key_col_on_dim
        nullable: false        # optional, default true — set false to raise loudly instead of
                                # inserting NULL into a NOT NULL FK column

orchestration:
  schedule: "@daily"           # any Airflow schedule string
  start_date: "2024-01-01"
  catchup: false
  tags: [your_tag]
  requires_dim_instrument: true   # REQUIRED if any fk_resolution rule targets warehouse.dim_instrument
  requires_scd2_baseline: true    # REQUIRED if any layer3 entry has strategy: scd2_apply
```

**`layer3.strategy` reference** (see real examples in [config/tables/](config/tables/)):

| Strategy | Use for | Notable fields |
|---|---|---|
| `fact_upsert` | A fact table row-per-event | `event_date_column` (required), `fk_resolution`, `literals` |
| `dimension_upsert` | A dimension populated from a staging table (owned-column upsert, shares a target with other tables if needed) | see `client_signup.yml`/`client_profile.yml` |
| `scd2_apply` | CDC apply into `dim_client_risk_snapshot` | `target` must be `warehouse.dim_client_risk_snapshot`; `orchestration.requires_scd2_baseline: true` mandatory; `source.expected_columns` must include `client_id, lsn, commit_ts, op, after` |
| `scd2_baseline_seed` | One-time baseline seed (used by `bootstrap_warehouse` only) | `cdc_source_glob` mandatory (excludes insert-first clients from a fabricated baseline — ADR-2) |

`config.py`'s `TableConfig.load()` validates all of this at load time (not at DAG-run time) —
a missing `event_date_column`, an unset `requires_dim_instrument`/`requires_scd2_baseline` flag, an
`fk_resolution` rule missing a required key, or an unknown `strategy` string all raise a
path-annotated `ValueError` immediately, either from `python -m deriv_pipeline.config
--validate-all` (what `airflow-init` runs) or from `make verify`'s first stage. Run it directly
after editing a config:

```bash
docker compose --profile test run --rm --entrypoint /bin/bash tests \
  -c "python -m deriv_pipeline.config --validate-all"
```

## Writing a dimension config

Two `kind`s besides `table` live in `config/tables/*.yml` too — same directory, same load pass,
same `--validate-all` gate:

**`derived_dimension`** — populate a dimension from distinct values of a column, either in an
already-staged table (`source_table`) or straight from a raw `data/` file (`raw_source_glob`, for
a dimension needed before its source table is itself onboarded — exactly one of the two is
required):

```yaml
kind: derived_dimension
name: dim_instrument
raw_source_glob: "client_trades.json"   # OR: source_table: client_trades
source_column: instrument
target: warehouse.dim_instrument
target_key_column: instrument_name       # the dimension's natural key column
derived_columns:                          # optional: column -> {natural_key_value: derived_value}
  asset_class:
    EUR/USD: FX
    Gold: Commodity
```

**`generated_dimension`** — a pure date-range dimension, no source data at all:

```yaml
kind: generated_dimension
name: dim_date
target: warehouse.dim_date
from: "2024-01-01"
to: "2024-12-31"
```

## Writing data quality checks

Two mechanisms, both logging to the same `data_quality.dq_check_results` table (and therefore the
same `dq_table_health` regression view) — pick one per table, or neither:

**SQL assertions** (`layer2.dq_checks`, works for any table) — each `sql` must return a single
row/column: the count of rows failing the assertion (`0` = pass):

```yaml
layer2:
  target: staging.your_table
  dq_checks:
    - name: amount_is_positive
      sql: "SELECT count(*) FROM staging.your_table WHERE amount_usd <= 0"
      severity: CRITICAL       # INFO | WARNING | CRITICAL
```

- `severity: CRITICAL` failures additionally write one summary row to `quarantine.rejected_rows`
  (table-level, not per-offending-row — see [Caveats](#caveats-documented-trade-offs-not-bugs)).
- `name` must be unique within that table's `dq_checks` list; `dq_checks` is only read from
  `layer2` — declaring it anywhere else raises at config-load time rather than silently running
  zero checks.

**A real Great Expectations suite** (`layer2.ge_suite`, currently wired for exactly one table —
`staging_vendor_deposits`, per ADR-4/ADR-12's deliberate GE-scope-down) — the suite's expectations
are a hardcoded dict in [`deriv_pipeline/dq.py`](deriv_pipeline/dq.py)'s `_GE_SUITES`, keyed by
suite name:

```python
_GE_SUITES = {
    "staging_vendor_deposits": {
        "expect_column_values_to_be_between": ({"column": "amount_usd", "min_value": 0.01}, "CRITICAL"),
        "expect_column_values_to_not_be_null": ({"column": "deposit_id"}, "CRITICAL"),
        "expect_column_values_to_be_in_set": (
            {"column": "payment_method", "value_set": [...]}, "WARNING",
        ),
    },
}
```

To add a second real GE suite: add an entry here (expectation_type → (kwargs, severity)) and set
`layer2.ge_suite: your_suite_name` on the table's config — `dq.run_ge_suite` builds an ephemeral
`SqlAlchemyExecutionEngine` DataContext against `layer2.target` and validates it, no other wiring
required. Both a GE suite and `dq_checks` can be declared on the same table — both run.

## Writing a reconciliation config

Add a file under `config/reconciliations/your_recon.yml`; it generates a `reconcile_your_recon`
DAG the same way a table config generates `table__*`:

```yaml
name: your_recon
key: [client_id, event_date, amount_usd]   # the natural key columns both sides must agree on
left: >
  SELECT c.client_id, f.event_date, f.amount_usd
  FROM warehouse.fact_deposits f JOIN warehouse.dim_client c USING (client_key)
  WHERE f.source_system = 'vendor'
right: >
  SELECT c.client_id, f.event_date, f.amount_usd
  FROM warehouse.fact_deposits f JOIN warehouse.dim_client c USING (client_key)
  WHERE f.source_system = 'internal'
```

`left`/`right` are each a complete SQL `SELECT` returning the `key` columns, in that order, for
one source's rows (config-declares-the-SQL, same convention as `dq_checks` above, rather than the
engine building joins generically for a design with exactly one real instance — ADR-13). A key
tuple returned by one side and not the other is logged as a `missing_left`/`missing_right`
discrepancy to `data_quality.reconciliation_discrepancies`, and the run's total discrepancy count
is also logged through `dq.log_check` so it shows up in `dq_table_health` for free. `name` shares
its uniqueness namespace with every `config/tables/*.yml` name (`validate_all()` raises on a
collision).

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
