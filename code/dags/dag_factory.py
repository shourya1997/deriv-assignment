"""The only per-table DAG code in the repo (ADR-1): loops
`TableConfig.load_all(kind="table")` and generates one DAG per entry via
Airflow's standard dynamic-DAG pattern (loop at module scope, register into
`globals()`). Every generated DAG has the identical
`land_layer1 >> stage_layer2 >> run_dq_checks >> load_layer3` shape — no
per-table divergence lives here. `run_dq_checks` is a no-op placeholder until
Step 8 wires real checks in; the shape doesn't change when it does.

Two DAGs stay hand-authored (bootstrap_warehouse, cdc_historical_reload) for
reasons documented in ARCHITECTURE_DECISIONS.md ADR-1 — this file never
generates them."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from deriv_pipeline.config import TableConfig
from deriv_pipeline.db import get_connection
from deriv_pipeline.layers import layer1_raw, layer2_staging, layer3_warehouse


def _run(fn, cfg):
    conn = get_connection()
    try:
        fn(cfg, conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _noop_dq_checks(cfg):
    """Placeholder until Step 8 (Great Expectations + SQL-assertion DQ)."""


def _dim_instrument_populated() -> bool:
    """Polled by wait_for_dim_instrument below — a data-condition check
    rather than an ExternalTaskSensor, since bootstrap_warehouse is
    schedule=None (manual, run-once) and has no comparable execution_date to
    match against this DAG's own @daily schedule."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS(SELECT 1 FROM warehouse.dim_instrument)")
            return cur.fetchone()[0]
    finally:
        conn.close()


def _make_dag(cfg: TableConfig) -> DAG:
    orch = cfg.orchestration
    with DAG(
        dag_id=f"table__{cfg.name}",
        schedule=orch.get("schedule", "@daily"),
        start_date=datetime.fromisoformat(orch.get("start_date", "2024-01-01")),
        catchup=orch.get("catchup", False),
        tags=orch.get("tags", []),
    ) as dag:
        land_layer1 = PythonOperator(
            task_id="land_layer1", python_callable=lambda: _run(layer1_raw.load, cfg)
        )
        stage_layer2 = PythonOperator(
            task_id="stage_layer2", python_callable=lambda: _run(layer2_staging.stage, cfg)
        )
        run_dq_checks = PythonOperator(
            task_id="run_dq_checks", python_callable=lambda: _noop_dq_checks(cfg)
        )
        load_layer3 = PythonOperator(
            task_id="load_layer3", python_callable=lambda: _run(layer3_warehouse.load, cfg)
        )
        # This table's fact_upsert has a hard (non-creatable-on-demand) FK
        # prerequisite on warehouse.dim_instrument, which only
        # bootstrap_warehouse populates — without this, a fresh deploy's
        # first @daily run races bootstrap_warehouse and can fail (Step 5
        # dual review finding, confirmed independently by both reviewers).
        if orch.get("requires_dim_instrument"):
            wait_for_dim_instrument = PythonSensor(
                task_id="wait_for_dim_instrument",
                python_callable=_dim_instrument_populated,
                poke_interval=30,
                timeout=3600,
                mode="reschedule",
            )
            wait_for_dim_instrument >> land_layer1
        land_layer1 >> stage_layer2 >> run_dq_checks >> load_layer3
    return dag


for _cfg in TableConfig.load_all(kind="table"):
    globals()[f"table__{_cfg.name}"] = _make_dag(_cfg)
