"""Airflow-dependent — only runs inside the airflow image (the `tests`
compose service), never from a plain host `pytest` run (pyproject.toml
deliberately doesn't depend on apache-airflow, see its own comment)."""
import pytest

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="/opt/airflow/dags", include_examples=False)


def test_no_dag_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_vendor_deposits_dag_exists_with_expected_task_shape(dagbag):
    dag = dagbag.get_dag("table__vendor_deposits")
    assert dag is not None
    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == ["land_layer1", "stage_layer2", "run_dq_checks", "load_layer3"]
    land, stage, dq, load = dag.tasks
    assert list(land.downstream_task_ids) == ["stage_layer2"]
    assert list(stage.downstream_task_ids) == ["run_dq_checks"]
    assert list(dq.downstream_task_ids) == ["load_layer3"]


def test_client_trades_dag_waits_for_dim_instrument(dagbag):
    """client_trades' fact_upsert has a hard FK prerequisite on
    warehouse.dim_instrument (only bootstrap_warehouse populates it) — the
    generated DAG must gate load_layer3 behind a sensor for that condition,
    not just rely on bootstrap_warehouse having already run by chance
    (Step 5 dual review finding)."""
    dag = dagbag.get_dag("table__client_trades")
    assert dag is not None
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {
        "wait_for_dim_instrument", "land_layer1", "stage_layer2", "run_dq_checks", "load_layer3",
    }
    wait = dag.get_task("wait_for_dim_instrument")
    assert list(wait.downstream_task_ids) == ["land_layer1"]


def test_client_deposit_dag_has_no_dim_instrument_wait(dagbag):
    """client_deposit has no such prerequisite (dim_client/dim_date are both
    created on demand) — the sensor must not appear on unrelated DAGs."""
    dag = dagbag.get_dag("table__client_deposit")
    assert dag is not None
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {"land_layer1", "stage_layer2", "run_dq_checks", "load_layer3"}


def test_dags_dir_contains_only_factory_and_named_hand_authored_files():
    """Guards against the design silently degrading back to hand-authored
    per-table DAGs during later steps (Step 4-9 each add a config, never a
    new dags/*.py file) — see ARCHITECTURE_DECISIONS.md ADR-1."""
    from pathlib import Path

    dag_files = {p.name for p in Path("/opt/airflow/dags").glob("*.py")}
    # bootstrap_warehouse.py / cdc_historical_reload.py land in Step 4/7;
    # only dag_factory.py exists as of Step 3.
    assert dag_files <= {"dag_factory.py", "bootstrap_warehouse.py", "cdc_historical_reload.py"}
