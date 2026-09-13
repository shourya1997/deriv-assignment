#!/usr/bin/env bash
# Full verification sequence (grows as later phases land — see plan's
# "Verification (make verify)" section for the complete target shape):
#   1. config validation gate
#   2. full pytest (unit/integration/dags)
#   3. poll `airflow dags list` for full DAG set
#   4. run every DAG twice via `airflow dags test`
#   5. `pytest -m e2e` against the live deriv DB
# Steps 1 (config validation) and 3-5 are added in later phases once
# config.py/DAGs exist; Step 1 (harness) only has step 2 (pytest) wired.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== running test suite =="
docker compose --profile test run --rm tests

echo "== verify: PASS (harness-level checks only so far) =="
