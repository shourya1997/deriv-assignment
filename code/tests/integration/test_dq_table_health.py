"""Step 8: dq_table_health (migrations/008_data_quality.sql) flags a
regression when a table's failed-check count is worse than the immediately
preceding run for that table — tested here with synthetic run pairs, per the
plan's explicit instruction, since dq.py itself never reads this view."""
from deriv_pipeline import dq


def _log(db_conn, run_id, table_name, check_name, unexpected_count):
    from deriv_pipeline.config import DqCheck

    check = DqCheck(name=check_name, sql=f"SELECT {unexpected_count}", severity="WARNING")
    dq.run_sql_assertion(db_conn, table_name, run_id, check)
    # Postgres now() (executed_at's default) is fixed for the life of a
    # transaction — dq_table_health's LAG(...ORDER BY run_at) needs each
    # synthetic run to land in its own transaction so run_at actually
    # advances between runs, exactly as separate DAG runs' own commits do.
    db_conn.commit()


def _purge(db_conn, table_name):
    """Each _log() call above commits, so db_conn's rollback-on-teardown
    can't clean these synthetic rows up — delete them explicitly, scoped to
    this test's own synthetic table_name so real pipeline data is untouched."""
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM data_quality.dq_check_results WHERE table_name = %s", (table_name,))
    db_conn.commit()


def _health_row(db_conn, table_name, run_id):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT is_regression FROM data_quality.dq_table_health"
            " WHERE table_name = %s AND run_id = %s",
            (table_name, run_id),
        )
        return cur.fetchone()[0]


def test_dq_table_health_flags_a_run_with_more_failures_than_the_previous_one(db_conn):
    table_name = "staging.synthetic"
    try:
        _log(db_conn, "run-1", table_name, "check_a", 0)
        _log(db_conn, "run-2", table_name, "check_a", 2)
        assert _health_row(db_conn, table_name, "run-1") is False
        assert _health_row(db_conn, table_name, "run-2") is True
    finally:
        _purge(db_conn, table_name)


def test_dq_table_health_does_not_flag_an_improved_or_equal_run(db_conn):
    table_name = "staging.synthetic2"
    try:
        _log(db_conn, "run-1", table_name, "check_a", 2)
        _log(db_conn, "run-2", table_name, "check_a", 2)
        _log(db_conn, "run-3", table_name, "check_a", 0)
        assert _health_row(db_conn, table_name, "run-2") is False
        assert _health_row(db_conn, table_name, "run-3") is False
    finally:
        _purge(db_conn, table_name)


def test_dq_table_health_first_run_is_never_a_regression(db_conn):
    table_name = "staging.synthetic3"
    try:
        _log(db_conn, "run-1", table_name, "check_a", 5)
        assert _health_row(db_conn, table_name, "run-1") is False
    finally:
        _purge(db_conn, table_name)
