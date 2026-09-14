#!/usr/bin/env bash
# Full verification sequence (grows as later phases land — see plan's
# "Verification (make verify)" section for the complete target shape):
#   1. config validation gate
#   2. full pytest (unit/integration/dags)
#   3. poll `airflow dags list` for the expected DAG set
#   4. run every generated DAG twice via `airflow dags test` (idempotency)
#   5. `pytest -m e2e` against the live deriv DB
# Step 5 is wired in once tests/e2e/ has actual tests (Step 5+ per TASK.md) —
# an empty -m e2e run exits nonzero under pytest's "no tests collected" rule,
# which would fail this script for no real reason before then.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== 1/4: config validation =="
docker compose run --rm --no-deps airflow-scheduler python -m deriv_pipeline.config --validate-all

echo "== 2/4: test suite (unit/integration/dags) =="
docker compose --profile test run --rm tests

echo "== 3/4: airflow dags list =="
docker compose run --rm --no-deps airflow-scheduler airflow dags list

echo "== 4/4: airflow dags test (each generated DAG, twice for idempotency) =="
echo "-- bootstrap_warehouse (run 1) -- cross-config ordering (dim_manager etc.) must land"
echo "   before any standalone table__client_signup/client_profile DAG test below can resolve"
echo "   their dimension_upsert FK lookups (ADR-1, ADR-2)."
docker compose run --rm --no-deps airflow-scheduler airflow dags test bootstrap_warehouse 2024-03-01
echo "-- bootstrap_warehouse (run 2, idempotency) --"
docker compose run --rm --no-deps airflow-scheduler airflow dags test bootstrap_warehouse 2024-03-01

dag_ids="$(docker compose run --rm --no-deps airflow-scheduler airflow dags list -o plain | tail -n +2 | awk '{print $1}' | grep '^table__' || true)"
if [ -z "$dag_ids" ]; then
    echo "verify: FAIL — no table__* DAGs found (airflow dags list produced none)" >&2
    exit 1
fi
for dag_id in $dag_ids; do
    echo "-- $dag_id (run 1) --"
    docker compose run --rm --no-deps airflow-scheduler airflow dags test "$dag_id" 2024-03-01
    echo "-- $dag_id (run 2, idempotency) --"
    docker compose run --rm --no-deps airflow-scheduler airflow dags test "$dag_id" 2024-03-01
done

echo "-- reconcile_vendor_feed (run 1) -- must run after the table__* DAGs above so both"
echo "   fact_deposits sources (vendor + internal) are populated (Step 9)"
docker compose run --rm --no-deps airflow-scheduler airflow dags test reconcile_vendor_feed 2024-03-01
echo "-- reconcile_vendor_feed (run 2, idempotency) --"
docker compose run --rm --no-deps airflow-scheduler airflow dags test reconcile_vendor_feed 2024-03-01

echo "-- cdc_historical_reload (run 1) -- manual, hand-authored (Step 7); must run after"
echo "   table__client_profile_changes above so there is real CDC history to reset+replay"
docker compose run --rm --no-deps airflow-scheduler airflow dags test cdc_historical_reload 2024-03-01
reload_state_1="$(docker compose run --rm --no-deps airflow-scheduler python -m deriv_pipeline.reload --dump-state)"
echo "-- cdc_historical_reload (run 2, idempotency) --"
docker compose run --rm --no-deps airflow-scheduler airflow dags test cdc_historical_reload 2024-03-01
reload_state_2="$(docker compose run --rm --no-deps airflow-scheduler python -m deriv_pipeline.reload --dump-state)"
if [ "$reload_state_1" != "$reload_state_2" ]; then
    echo "verify: FAIL — cdc_historical_reload is not idempotent:" >&2
    diff <(echo "$reload_state_1") <(echo "$reload_state_2") >&2 || true
    exit 1
fi

echo "== verify: PASS =="
