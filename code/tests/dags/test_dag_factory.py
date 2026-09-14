"""Airflow-dependent — only runs inside the airflow image (the `tests`
compose service), never from a plain host `pytest` run (pyproject.toml
deliberately doesn't depend on apache-airflow, see its own comment)."""
import pytest

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

from deriv_pipeline.config import ReconciliationConfig, TableConfig  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="/opt/airflow/dags", include_examples=False)


def test_no_dag_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_dag_factory_generates_one_dag_per_table_config(dagbag):
    """Step 10 completeness guardrail: every `kind: table` config must have
    produced exactly one `table__{name}` DAG — not just the handful spot-
    checked by name in the tests below — and nothing else. Guards against a
    config silently failing to generate a DAG (e.g. an exception swallowed by
    a future refactor of the factory's loop)."""
    table_cfgs = TableConfig.load_all(kind="table")
    expected = {f"table__{cfg.name}" for cfg in table_cfgs}
    # A set alone can't distinguish "6 configs, 6 DAGs" from "a missing/empty
    # tables dir" or "two configs sharing a name collided in the factory's
    # globals() registration" — both would still satisfy generated == expected
    # as bare sets. Pin the count against the raw config list too.
    assert len(table_cfgs) == len(expected) > 0
    generated = {dag_id for dag_id in dagbag.dag_ids if dag_id.startswith("table__")}
    assert generated == expected


def test_dag_factory_generates_one_dag_per_reconciliation_config(dagbag):
    recon_cfgs = ReconciliationConfig.load_all()
    expected = {f"reconcile_{cfg.name}" for cfg in recon_cfgs}
    assert len(recon_cfgs) == len(expected) > 0
    generated = {dag_id for dag_id in dagbag.dag_ids if dag_id.startswith("reconcile_")}
    assert generated == expected


def test_generated_task_dependencies_are_layer1_2_3_order(dagbag):
    """Every generated table__* DAG must run land_layer1 >> stage_layer2 >>
    run_dq_checks >> load_layer3 in that order, regardless of whether it also
    has a leading sensor — the four-task shape and its ordering is the one
    thing every generated DAG must never diverge on (ADR-1)."""
    table_cfgs = TableConfig.load_all(kind="table")
    assert table_cfgs  # a zero-iteration loop below would vacuously "pass"
    for cfg in table_cfgs:
        dag = dagbag.get_dag(f"table__{cfg.name}")
        assert dag is not None, f"table__{cfg.name} was not generated"
        land = dag.get_task("land_layer1")
        stage = dag.get_task("stage_layer2")
        dq = dag.get_task("run_dq_checks")
        load = dag.get_task("load_layer3")
        assert set(land.downstream_task_ids) == {"stage_layer2"}
        assert set(stage.downstream_task_ids) == {"run_dq_checks"}
        assert set(dq.downstream_task_ids) == {"load_layer3"}
        assert set(load.downstream_task_ids) == set()
        # Any extra task (a sensor) must actually be wired upstream of
        # land_layer1, not just constructed and left dangling — a config
        # whose orchestration flag is set but whose sensor >> land_layer1
        # edge got dropped in a refactor would otherwise still pass.
        extra_tasks = set(dag.task_ids) - {"land_layer1", "stage_layer2", "run_dq_checks", "load_layer3"}
        assert set(land.upstream_task_ids) == extra_tasks


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


def test_client_profile_changes_dag_waits_for_scd2_baseline(dagbag):
    """client_profile_changes' scd2_apply has the same cross-DAG shape as
    client_trades' dim_instrument dependency: bootstrap_warehouse's baseline
    seed must exist first (ADR-2/ADR-10), and nothing else orders this
    @daily DAG after that schedule=None, manual DAG (Step 6 dual review
    finding, by analogy with Step 5)."""
    dag = dagbag.get_dag("table__client_profile_changes")
    assert dag is not None
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {
        "wait_for_scd2_baseline", "land_layer1", "stage_layer2", "run_dq_checks", "load_layer3",
    }
    wait = dag.get_task("wait_for_scd2_baseline")
    assert list(wait.downstream_task_ids) == ["land_layer1"]


def test_cdc_historical_reload_dag_exists_manual_only(dagbag):
    """Step 7: hand-authored, not config-generated (no `kind: table` row a
    reload could come from) — schedule=None, operator-triggered only."""
    dag = dagbag.get_dag("cdc_historical_reload")
    assert dag is not None
    assert dag.schedule_interval is None
    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == ["run_reload"]


def test_dags_dir_contains_only_factory_and_named_hand_authored_files():
    """Guards against the design silently degrading back to hand-authored
    per-table DAGs during later steps (Step 4-9 each add a config, never a
    new dags/*.py file) — see ARCHITECTURE_DECISIONS.md ADR-1."""
    from pathlib import Path

    dag_files = {p.name for p in Path("/opt/airflow/dags").glob("*.py")}
    # bootstrap_warehouse.py / cdc_historical_reload.py land in Step 4/7;
    # only dag_factory.py exists as of Step 3.
    assert dag_files <= {"dag_factory.py", "bootstrap_warehouse.py", "cdc_historical_reload.py"}
