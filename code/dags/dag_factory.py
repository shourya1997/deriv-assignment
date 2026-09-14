"""The only per-table DAG code in the repo (ADR-1): loops
`TableConfig.load_all(kind="table")` and generates one DAG per entry via
Airflow's standard dynamic-DAG pattern (loop at module scope, register into
`globals()`). Every generated DAG has the identical
`land_layer1 >> stage_layer2 >> run_dq_checks >> load_layer3` shape — no
per-table divergence lives here. `run_dq_checks` dispatches to
`deriv_pipeline.dq.run_dq_checks` (Step 8): the one real GE suite for
vendor_deposits, config-declared SQL assertions for every other table.
`ReconciliationConfig.load_all()` generates one further DAG per entry (Step
9), a single task calling `deriv_pipeline.recon.reconcile.run_reconciliation`.

Two DAGs stay hand-authored (bootstrap_warehouse, cdc_historical_reload) for
reasons documented in ARCHITECTURE_DECISIONS.md ADR-1 — this file never
generates them."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from deriv_pipeline import dq
from deriv_pipeline.config import ReconciliationConfig, TableConfig
from deriv_pipeline.db import get_connection
from deriv_pipeline.layers import layer1_raw, layer2_staging, layer3_warehouse
from deriv_pipeline.recon import reconcile


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


def _run_dq_checks(cfg, **context):
    # context["run_id"] is Airflow's own per-DAG-run id — shared by every
    # task in this run with no extra XCom plumbing, which is exactly the
    # shared `run_id` dq_table_health's regression comparison needs.
    _run(lambda c, conn: dq.run_dq_checks(c, conn, context["run_id"]), cfg)


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


def _scd2_baseline_seeded() -> bool:
    """Polled by wait_for_scd2_baseline below — same data-condition-poll
    shape as _dim_instrument_populated, for client_profile_changes' own hard
    prerequisite (ADR-2/ADR-10): every client with a staged CDC event (other
    than one whose *only* event is an 'insert', which legitimately has no
    baseline — see scd2_baseline_seed's own exclusion logic) must already
    have a source_lsn=0 baseline row.

    A bare `EXISTS(...WHERE source_lsn = 0)` (the original version of this
    check) is satisfied forever after bootstrap_warehouse's *first* run, even
    for a client onboarded afterwards with no baseline of their own — that
    client's CDC-derived history would then apply with no ADR-2 window for
    their pre-CDC-era fact rows to resolve into, and bootstrap_warehouse
    can't retroactively fix it (scd2_baseline_seed skips any client who
    already has real, source_lsn > 0, history). This set-based check is
    per-client and re-evaluated on every poke, so it stays correct even if
    bootstrap_warehouse is later re-triggered incrementally (Step 6 dual
    review finding, Opus).

    The global EXISTS is kept as a floor alongside the per-client check: on a
    completely fresh deploy `staging.client_profile_changes` is still empty
    (this DAG's own land_layer1/stage_layer2 run *after* this sensor), so the
    per-client check alone would pass vacuously — nothing staged yet means
    nothing to fail on — even though bootstrap_warehouse has never run."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    EXISTS(SELECT 1 FROM warehouse.dim_client_risk_snapshot WHERE source_lsn = 0)
                    AND NOT EXISTS (
                        SELECT 1
                        FROM (
                            SELECT client_id, (array_agg(op ORDER BY staging_seq))[1] AS first_op
                            FROM staging.client_profile_changes
                            GROUP BY client_id
                        ) AS first_events
                        WHERE first_events.first_op != 'insert'
                          AND NOT EXISTS (
                              SELECT 1 FROM warehouse.dim_client_risk_snapshot s
                              WHERE s.client_id = first_events.client_id AND s.source_lsn = 0
                          )
                    )
                """
            )
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
            task_id="run_dq_checks", python_callable=_run_dq_checks, op_kwargs={"cfg": cfg}
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
        # client_profile_changes' scd2_apply has the same cross-DAG shape as
        # requires_dim_instrument above: bootstrap_warehouse's baseline seed
        # must exist first (ADR-2/ADR-10), and nothing else orders this
        # @daily DAG after that schedule=None, manual DAG on a fresh deploy.
        if orch.get("requires_scd2_baseline"):
            wait_for_scd2_baseline = PythonSensor(
                task_id="wait_for_scd2_baseline",
                python_callable=_scd2_baseline_seeded,
                poke_interval=30,
                timeout=3600,
                mode="reschedule",
            )
            wait_for_scd2_baseline >> land_layer1
        land_layer1 >> stage_layer2 >> run_dq_checks >> load_layer3
    return dag


def _run_reconciliation(cfg, **context):
    _run(lambda c, conn: reconcile.run_reconciliation(c, conn, context["run_id"]), cfg)


def _make_reconciliation_dag(cfg: ReconciliationConfig) -> DAG:
    with DAG(
        dag_id=f"reconcile_{cfg.name}",
        schedule="@daily",
        start_date=datetime.fromisoformat("2024-01-01"),
        catchup=False,
        tags=["reconciliation"],
    ) as dag:
        PythonOperator(
            task_id="run_reconciliation",
            python_callable=_run_reconciliation,
            op_kwargs={"cfg": cfg},
        )
    return dag


for _cfg in TableConfig.load_all(kind="table"):
    globals()[f"table__{_cfg.name}"] = _make_dag(_cfg)

for _rcfg in ReconciliationConfig.load_all():
    globals()[f"reconcile_{_rcfg.name}"] = _make_reconciliation_dag(_rcfg)
