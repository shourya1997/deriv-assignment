"""Hand-authored (ADR-1): `cdc_historical_reload` re-issues sql/04's driver
query with bound `from_date`/`to_date` runtime params and iterates *clients*,
not tables — there is no `kind: table` config row a reload could be generated
from, so this can't be produced by `dag_factory.py`'s per-config loop.

Manual-only (`schedule=None`): a reload is an operator-triggered repair
action, not a recurring ingestion. `deriv_pipeline.reload.historical_reload`
does the actual reset (sql/04's `reset_client_for_reload`) + lsn-order replay
(through `apply_cdc_event`, sql/03) per affected client.

`max_active_runs=1`: `historical_reload` holds every affected client's
per-client advisory lock (the same `cdc_apply:<client_id>` key `scd2_apply`
takes) until this task's single final commit, and its driver query now
orders by `client_id` for a deterministic acquisition order — but two
*concurrent reload runs* would still each try to lock the same clients in
that same order, which is fine (they just serialize), except a second run
starting mid-way through the first could still interleave lock acquisitions
across runs in a way that isn't provably deadlock-free. Capping this DAG to
one active run removes that residual risk entirely rather than requiring an
operator runbook caveat (Opus dual-review finding F3, Step 7)."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from deriv_pipeline.db import get_connection
from deriv_pipeline.reload import historical_reload


def _run_reload(**context) -> dict:
    params = context["params"]
    conn = get_connection()
    try:
        result = historical_reload(conn, params["from_date"], params["to_date"])
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


with DAG(
    dag_id="cdc_historical_reload",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["client", "cdc", "reload"],
    params={
        "from_date": Param("2024-11-01", type="string", format="date"),
        "to_date": Param("2024-12-01", type="string", format="date"),
    },
) as dag:
    run_reload = PythonOperator(task_id="run_reload", python_callable=_run_reload)
