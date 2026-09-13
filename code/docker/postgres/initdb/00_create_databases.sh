#!/usr/bin/env bash
# Creates the deriv/deriv_test/airflow databases only — schema DDL is never run
# here (see ARCHITECTURE_DECISIONS.md ADR on docker-entrypoint-initdb.d being
# rejected: not re-runnable, can't interleave gap-fill DDL, unusable from pytest).
set -euo pipefail

for db in deriv deriv_test airflow; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
    SELECT 'CREATE DATABASE $db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')\gexec
EOSQL
done
