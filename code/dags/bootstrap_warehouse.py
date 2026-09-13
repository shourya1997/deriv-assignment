"""Hand-authored (ADR-1): the one DAG responsible for cross-config
topological order that `dag_factory.py`'s per-table loop cannot express —
dim_manager/dim_instrument/dim_date have no `kind: table` config of their
own, and client_signup's dimension_upsert needs dim_manager already
populated (FK resolution on assigned_manager), which in turn needs
client_signup staged first.

Sequence: stage client_signup -> build dim_manager (+ dim_instrument/dim_date,
independent) -> upsert client_signup's owned dim_client columns -> stage +
upsert client_profile's owned dim_client columns and seed the real G2
baseline (ADR-2) into dim_client_risk_snapshot. client_profile's branch is
made to depend on client_signup's upsert completing first (rather than
running as an independent branch) so the two owned-column upserts into the
same warehouse.dim_client row never race in a real scheduler.

`bootstrap_complete` is a no-op sentinel: Step 6+'s CDC-apply DAG must depend
on it (ADR-2's ordering invariant — the baseline seed must exist before any
`apply_cdc_event` call), documented here since that dependency can't be wired
until the CDC DAG exists."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from deriv_pipeline.config import CONFIG_DIR, DerivedDimensionConfig, GeneratedDimensionConfig, TableConfig
from deriv_pipeline.db import get_connection
from deriv_pipeline.dims import derived as derived_dim
from deriv_pipeline.dims import generated as generated_dim
from deriv_pipeline.layers import layer1_raw, layer2_staging, layer3_warehouse

_TABLES_DIR = CONFIG_DIR / "tables"

_client_signup_cfg = TableConfig.load(_TABLES_DIR / "client_signup.yml")
_client_profile_cfg = TableConfig.load(_TABLES_DIR / "client_profile.yml")
_dim_manager_cfg = DerivedDimensionConfig.load(_TABLES_DIR / "dim_manager.yml")
_dim_instrument_cfg = DerivedDimensionConfig.load(_TABLES_DIR / "dim_instrument.yml")
_dim_date_cfg = GeneratedDimensionConfig.load(_TABLES_DIR / "dim_date.yml")


def _run(*fns_and_cfgs) -> None:
    conn = get_connection()
    try:
        for fn, cfg in fns_and_cfgs:
            fn(cfg, conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


with DAG(
    dag_id="bootstrap_warehouse",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["warehouse", "bootstrap"],
) as dag:
    stage_client_signup = PythonOperator(
        task_id="stage_client_signup",
        python_callable=lambda: _run(
            (layer1_raw.load, _client_signup_cfg), (layer2_staging.stage, _client_signup_cfg)
        ),
    )
    build_dim_manager = PythonOperator(
        task_id="build_dim_manager",
        python_callable=lambda: _run((derived_dim.load, _dim_manager_cfg)),
    )
    build_dim_instrument = PythonOperator(
        task_id="build_dim_instrument",
        python_callable=lambda: _run((derived_dim.load, _dim_instrument_cfg)),
    )
    build_dim_date = PythonOperator(
        task_id="build_dim_date",
        python_callable=lambda: _run((generated_dim.load, _dim_date_cfg)),
    )
    upsert_client_signup_dimension = PythonOperator(
        task_id="upsert_client_signup_dimension",
        python_callable=lambda: _run((layer3_warehouse.load, _client_signup_cfg)),
    )
    stage_client_profile = PythonOperator(
        task_id="stage_client_profile",
        python_callable=lambda: _run(
            (layer1_raw.load, _client_profile_cfg), (layer2_staging.stage, _client_profile_cfg)
        ),
    )
    upsert_client_profile_dimension_and_baseline_seed = PythonOperator(
        task_id="upsert_client_profile_dimension_and_baseline_seed",
        python_callable=lambda: _run((layer3_warehouse.load, _client_profile_cfg)),
    )
    bootstrap_complete = EmptyOperator(task_id="bootstrap_complete")

    stage_client_signup >> build_dim_manager >> upsert_client_signup_dimension
    # Both branches write owned columns into the same warehouse.dim_client row;
    # without this edge they could race in a real scheduler (Opus dual-review
    # finding — Step 4).
    upsert_client_signup_dimension >> stage_client_profile
    stage_client_profile >> upsert_client_profile_dimension_and_baseline_seed
    [
        build_dim_instrument,
        build_dim_date,
        upsert_client_signup_dimension,
        upsert_client_profile_dimension_and_baseline_seed,
    ] >> bootstrap_complete
