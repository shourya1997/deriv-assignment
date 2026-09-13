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
