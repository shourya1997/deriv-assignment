# code/ build — progress log

Append-only. One entry per phase, never rewritten retroactively. Answers "what state is the
repo actually in" independent of git history.

## Step 0 — bootstrap

- Confirmed git root: `git rev-parse --show-toplevel` → `/Users/shouryasengupta/Vibe/projects/deriv-assignment`.
  cwd for this session started at parent `Vibe/`, now corrected — all commits target this repo.
- Logged the architecture pivot (config-driven/auto-generated override) and the G1/G2 gaps in
  root `PROMPTS.md`, per `CLAUDE.md`'s required AI-prompt-log format.
- Created this file, `TASK.md`, `ARCHITECTURE_DECISIONS.md`.
- No code written yet. `code/` dir exists but is otherwise empty.
- Open issues carried into Step 1: none yet — nothing has run.

## Step 1 — harness

- Built: `docker-compose.yml` (postgres/airflow-init/airflow-scheduler/airflow-webserver/tests),
  `docker/airflow/Dockerfile`+`requirements.txt`, `docker/postgres/initdb/00_create_databases.sh`,
  `deriv_pipeline/db.py`+`migrate.py`, gap-fill migrations `000_schemas` → `005_raw` →
  `006_staging` → `007_quarantine` → `008_data_quality` → `009_reconciliation` (G1 fix),
  `pyproject.toml`, `scripts/verify.sh` (partial — steps 3-5 land with later phases), `Makefile`.
- Tests green: 30/30 (`tests/unit/test_harness.py`, `tests/integration/test_harness_db.py`) —
  run both via a local venv against a bare `docker compose up -d postgres`, AND via the
  documented `docker compose --profile test run --rm tests` path after fixing the dual-review
  findings below. `docker compose up -d --build` also verified end-to-end: `airflow-init`
  applies all 10 manifest migrations against the real `deriv` DB, `airflow-scheduler`/
  `airflow-webserver` come up healthy.
- Dual review (Opus + Sonnet) caught 1 shared blocker and several real gaps, all fixed:
  - **Blocker (both reviewers):** `airflow-init` called `python -m deriv_pipeline.config
    --validate-all`, a module that doesn't exist until Step 2 — `docker compose up -d` could
    never succeed. Removed the line from `airflow-init`'s command; will be re-added in Step 2.
  - **Blocker (Opus):** `airflow-init`'s multi-line `bash -c` had no `set -e`, so a failed
    `airflow db migrate` would be masked by the next command's exit code. Added
    `set -euo pipefail`.
  - **Major (Opus):** Postgres healthcheck (`pg_isready`, no host) went green during
    `initdb.d` bootstrap, before `00_create_databases.sh` had actually created the databases —
    a cold-start race. Changed to `psql -d airflow -c 'select 1' -h 127.0.0.1`, and re-added
    the `postgres: service_healthy` condition to the 3 services whose `depends_on` override had
    silently dropped it (YAML merge keys don't deep-merge).
  - **Major (both):** no `.dockerignore` — the 564MB local `.venv`, `.pytest_cache`, `.env`
    (with the Postgres password) were all going into the build context/image. Added
    `code/.dockerignore`.
  - **Major (Opus):** `AIRFLOW_UID=50000` hardcoded default only works on macOS/Windows
    (Docker Desktop remaps ownership) — breaks bind-mount writes on Linux. Documented
    `AIRFLOW_UID=$(id -u)` in `.env.example`.
  - **Major (Sonnet):** Airflow 2.10.5 pins `SQLAlchemy<2.0`, which has no native psycopg3
    dialect — GE's planned `SqlAlchemyExecutionEngine` (ADR-4, Step 8) would have had no
    working Postgres driver. Switched to `great_expectations[postgresql]` (pulls
    `psycopg2-binary`).
  - **Major (Opus, verified by actually building the image):** GE install was unconstrained
    against Airflow's own pins. Added `--constraint
    .../constraints-2.10.5/constraints-3.11.txt` to the Dockerfile's pip install — image now
    builds clean with no downgrade warnings.
  - **Minor (Opus):** `migrate.py` had no advisory lock — two concurrent `run_migrations()`
    calls would race on the same DDL. Added `pg_advisory_xact_lock`.
  - **Minor (Opus):** `staging.client_deposit` was missing `schema_drift_detected`/
    `is_late_arrival` even though `client_deposit.json` has the same `payment_method`/`method`
    drift as the vendor feed (e.g. `DEP012`). Added both columns.
  - **Minor (Opus):** added `test_apply_cdc_event_stale_lsn_quarantines` so the manifest's
    `007_quarantine`-before-`sql/03` ordering is load-bearing, not just asserted in a comment.
  - **Minor (Sonnet):** `tests` service redundantly `pip install`ed the package over the
    network at container start even though the image already has it baked in — dropped.
  - Stale docstring/comment wording fixed in `migrate.py` and `verify.sh`.
- Open issues carried into Step 2: `airflow-init` needs `python -m deriv_pipeline.config
  --validate-all` added back once `deriv_pipeline/config.py` exists.

## Step 2 — config schema

- Built `deriv_pipeline/config.py`: `TableConfig`/`DerivedDimensionConfig`/
  `GeneratedDimensionConfig`/`ReconciliationConfig` dataclasses + `.load()`/`.load_all()`,
  a single enumerate-and-dispatch pass over `config/tables/*.yml` (`_load_all_table_dir`),
  `validate_all()`, and a `python -m deriv_pipeline.config --validate-all` CLI re-added to
  `airflow-init`'s command in `docker-compose.yml` (removed in Step 1 since the module didn't
  exist yet). First real config shipped: `config/tables/vendor_deposits.yml`.
- Tests green: 42/42, locally (venv against a bare `docker compose up -d postgres`) and via
  the containerized `docker compose --profile test run --rm tests` path. `docker compose up
  -d --build` end-to-end: `airflow-init` prints `config validation OK: 1 config(s) —
  vendor_deposits`, scheduler/webserver come up healthy.
- Dual review (Opus + Sonnet) converged independently on the same 3 real bugs, plus Opus
  found a 4th; all fixed:
  - **Major (both reviewers, independently):** `source.natural_key` (and
    `expected_columns`/`update_columns`/`layer3[].columns`) only checked truthiness, not
    type — `natural_key: deposit_id` (missing YAML brackets) is a truthy non-empty string,
    silently accepted, and would later be iterated character-by-character instead of as one
    column name. Added `_require_str_list()`: rejects anything that isn't a YAML list of
    strings, with the file path in the error.
  - **Major (both reviewers, independently):** `LayerTarget(**raw["layer1"])` /
    `Layer3Target(**entry)` unpacked raw YAML dicts straight into dataclass constructors —
    a typo'd key (`conflict_stratgey`) or missing required key raised a bare `TypeError`/
    `KeyError` with no file path, and the CLI's `except Exception: print(str(exc))` discarded
    the traceback entirely, so `airflow-init`'s one-shot log gave no way to find the broken
    file. Wrapped every dataclass construction to re-raise `ValueError(f"{path}: ...")`, and
    the CLI now also prints the full traceback.
  - **Major (Opus):** an unrecognized/typo'd `kind` (`kind: tabel`, or a `.yaml` extension)
    was silently skipped by every one of the three per-kind `load_all()` filters — a broken
    config file produced a green `--validate-all` and a silently-missing DAG, the worst
    failure mode for a config-driven design. Replaced the three independent filter-and-load
    passes with one `_load_all_table_dir()` that reads each file's `kind` once and raises on
    anything outside `{table, derived_dimension, generated_dimension}` — this also fixed
    Sonnet's separately-flagged double-YAML-parse inefficiency as a side effect.
  - **Major (Opus):** `validate_all()` treated zero configs found (missing/misconfigured
    `CONFIG_DIR`, wrong `DERIV_CONFIG_DIR`, empty directory) as success — `airflow-init` would
    print `config validation OK: 0 config(s)` and exit 0. Now raises `ValueError` if
    `config/tables/*.yml` is empty. Also fixed: a missing/misspelled `--validate-all` flag
    used to fall through and exit 0 silently — now exits 2 with a usage message.
  - **Minor (Opus):** `config.py` imported `REPO_ROOT` from `db.py`, which does `import
    psycopg` at module scope — a pure-YAML validation pass needlessly depended on the DB
    driver. `config.py` now computes `REPO_ROOT` itself.
  - **Minor (Opus):** empty/comment-only YAML files raised an opaque `AttributeError`
    (`None.get(...)`). `_load_yaml()` now raises a clear `ValueError` naming the path.
  - **Minor (Opus):** `GeneratedDimensionConfig` didn't check `from_date <= to_date` — an
    inverted range would silently produce an empty `dim_date`. Now raises.
  - New tests added for all of the above:
    `test_rejects_natural_key_as_scalar`, `test_layer_target_typo_raises_value_error_with_path`,
    `test_rejects_unknown_kind_in_table_dir`, `test_validate_all_raises_on_empty_config_dir`,
    `test_rejects_generated_dimension_from_after_to`.
- Open issue noted, not yet fixed (Sonnet, minor, low priority): `config/reconciliations/`
  is currently empty and untracked by git (git doesn't track empty dirs) — a fresh clone will
  simply lack the directory until `vendor_feed.yml` lands in Step 9.
  `ReconciliationConfig.load_all()` already treats a missing directory as "zero results", not
  an error, so this is a documented gap rather than a bug — revisit if Step 9 needs an earlier
  placeholder.

## Step 3 — walking skeleton: vendor_deposits end-to-end

- Built the generic three-layer engine (`deriv_pipeline/layers/{common,layer1_raw,
  layer2_staging,layer3_warehouse}.py`), pure transforms (`deriv_pipeline/transforms.py`:
  `resolve_header` for schema-drift detection, `compute_late_arrival` for the
  filename-derived delivery-date rule), the on-demand `dims/dim_date.py` (reused as-is by
  Step 4's `generated_dimension` engine), the Airflow dynamic-DAG factory
  (`dags/dag_factory.py`, one DAG shape: `land_layer1 >> stage_layer2 >> run_dq_checks >>
  load_layer3`, `run_dq_checks` a no-op placeholder until Step 8), and `scripts/verify.sh`'s
  remaining steps (config validate → pytest → `airflow dags list` → `airflow dags test` x2
  per DAG for idempotency).
- Tests green: 60/60 (added: 6 `test_transforms.py`, 2 `test_dim_date.py`, 7
  `test_vendor_deposits_pipeline.py` integration tests against the real
  `config/tables/vendor_deposits.yml` + real `data/deposits_vendor_*.csv` files, 3
  `test_dag_factory.py` airflow-only tests) — run both locally (venv) and via
  `docker compose --profile test run --rm tests`. Full `scripts/verify.sh` run end-to-end:
  config validation OK, 60/60 pytest, `airflow dags list` shows `table__vendor_deposits`,
  `airflow dags test` run twice both reached `state=success`.
- Dual review (Opus + Sonnet) found 10 + 2 issues (1 overlapping), all fixed:
  - **Most severe (both reviewers, independently):** the on-demand sentinel seed in
    `layer3_warehouse.py::_resolve_risk_snapshot_key` (Step 3's deliberate stand-in for
    ADR-2's real, properly-ordered G2 baseline seed, which lands in Step 4) originally
    blindly inserted a wide-open `1970-9999, is_current=true` row on ANY resolution miss.
    This could (a) collide with/be silently shadowed by Step 4's real ordered baseline seed,
    and (b) even self-violate the "at most one matching row per instant" invariant
    `resolve_risk_snapshot_key`'s un-ordered `LIMIT 1` depends on, since a single Step 3 run
    can see the same client at multiple distinct `deposit_date`s with no real history for
    any of them yet. Fixed: only seed when the client has zero *real* (`source_lsn >= 0`)
    snapshot rows — a miss against real history now raises, signaling "needs a real
    backfill decision" instead of guessing; each stopgap window is the narrowest possible
    (`[event_ts, event_ts + 1us)`, since `event_ts` is always a deposit_date at midnight,
    so distinct dates never overlap and the same date is idempotently reused); each gets its
    own strictly-negative `source_lsn` (`MIN(source_lsn) - 1` per client) so multiple windows
    per client don't collide on `uq_client_lsn`, and Step 4's real bootstrap can find/replace
    every `source_lsn < 0` row per client once it lands. Documented in-line since this is a
    load-bearing design decision, not just a bug fix — see ADR-7.
  - **Opus:** unchecked `None` after the retry `fetchone()` in both `_resolve_client_key` and
    `_resolve_risk_snapshot_key` — added explicit raises instead of a bare `NoneType` crash.
  - **Opus:** `layer2_staging.py`'s upsert overwrote `schema_drift_detected`/`is_late_arrival`
    on conflict instead of OR-combining — a clean redelivery could un-flag a natural key that
    a dirtier earlier delivery had correctly flagged. Now `{table}.{col} OR EXCLUDED.{col}`.
  - **Opus:** `layer2_staging.py`'s `update_columns + [flag columns]` wasn't deduplicated —
    a config listing a flag column in `update_columns` would emit "multiple assignments to
    same column" and fail at the SQL level. Deduplicated via `dict.fromkeys`.
  - **Opus:** `layer1_raw.py`'s `ON CONFLICT DO UPDATE SET payload = EXCLUDED.payload` never
    refreshed `source_file` — harmless today (source_file is part of vendor_deposits' PK) but
    a latent bug for any future single-column-PK raw table. Added
    `source_file = EXCLUDED.source_file`.
  - **Opus:** `staged_at = now()` applied unconditionally, contradicting `part1_pipeline.md`'s
    documented true-no-op-on-redelivery design. Now gated behind an `IS DISTINCT FROM` check
    across every tracked column; a no-op redelivery leaves `staged_at` untouched.
  - **Opus:** `layers/common.py::primary_key_columns()`'s join from
    `information_schema.table_constraints` to `key_column_usage` was missing the
    `table_schema`/`table_name` condition (joined on `constraint_name`/`constraint_schema`
    only) — a real, if currently dormant, cross-table bug. Added the missing join condition.
  - **Opus:** `scripts/verify.sh`'s DAG-test loop could pass vacuously if `airflow dags list`
    errored or returned nothing (`2>/dev/null` plus an empty `for`-loop doesn't trip
    `set -e`). Now captures the dag-id list into a variable and explicitly fails with a clear
    message if it's empty; dropped the `2>/dev/null` swallowing.
  - **Opus:** `test_layer3_inferred_member_pattern_for_orphan_client` only ever exercised the
    "miss → stub" branch of `_resolve_client_key`, never proving the "already exists" branch
    is distinguishable. Now also seeds a real (`is_inferred=false`) `dim_client` row for CL001
    before running layer3 and asserts it's reused as-is (not re-stubbed, not duplicated).
  - **Opus:** `test_full_pipeline_is_idempotent_end_to_end` only compared `count(*)` across
    two runs, which can't catch a second run silently reshuffling FK keys or re-seeding
    sentinels while leaving the row count flat. Now compares a full-content snapshot (every
    fact column, plus `dim_client`/`dim_client_risk_snapshot` counts) across both runs.
  - **Minor:** the hardcoded `date_key == 20240301` literal assertion is now derived from the
    fetched row's own `deposit_date` at test time.
  - **Sonnet (acceptable as scoped per both reviewers, documented not generalized):**
    `fact_upsert()`'s column lists are hardcoded to vendor_deposits'/fact_deposits' shape;
    `Layer3Target.columns` is defined but unused. Added an explicit in-code note on
    `fact_upsert()` itself (not just the module docstring) that this needs to read from
    `layer3_target.columns` before a second `fact_upsert`-strategy table can reuse it.
- Re-ran the full suite (60/60) and `scripts/verify.sh` end-to-end after applying every fix
  above — all green, both `airflow dags test` runs still `state=success`.
- Open issues carried into Step 4: the Step 3 sentinel-seed stopgap leaves `source_lsn < 0`
  rows in `warehouse.dim_client_risk_snapshot` for any client whose deposits had no real
  baseline yet — Step 4's real, ordered ADR-2 `bootstrap_warehouse` baseline seed must find
  and supersede/replace these (by `source_lsn < 0`) rather than assume a clean table, since
  this is a persistent live DB shared across steps.
