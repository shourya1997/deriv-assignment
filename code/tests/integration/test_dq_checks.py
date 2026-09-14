"""Step 8: config-declared SQL assertions + the one real GE suite, both
routed through data_quality.dq_check_results / quarantine.rejected_rows."""
from deriv_pipeline.config import CONFIG_DIR, DqCheck, TableConfig
from deriv_pipeline.layers import layer1_raw, layer2_staging

from deriv_pipeline import dq


def _check_results(db_conn, run_id, table_name):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT check_name, severity, passed, unexpected_count"
            " FROM data_quality.dq_check_results WHERE run_id = %s AND table_name = %s",
            (run_id, table_name),
        )
        return {row[0]: row[1:] for row in cur.fetchall()}


def _purge_run(db_conn, run_id, table_name):
    """run_ge_suite opens its own connection/transaction for GE's validator,
    independent of db_conn — this test commits db_conn once (below) so GE can
    see the staged rows, which also means db_conn's rollback-on-teardown won't
    clean up what either connection wrote. Delete it explicitly, scoped
    tightly by run_id so real pipeline data (the idempotent staging/raw rows)
    is left alone."""
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM data_quality.dq_check_results WHERE run_id = %s", (run_id,))
        cur.execute(
            "DELETE FROM quarantine.rejected_rows WHERE table_name = %s AND raw_payload->>'run_id' = %s",
            (table_name, run_id),
        )
    db_conn.commit()


def test_run_sql_assertion_logs_a_passing_check(db_conn):
    check = DqCheck(name="always_passes", sql="SELECT 0", severity="WARNING")
    passed = dq.run_sql_assertion(db_conn, "staging.example", "run-pass-1", check)
    assert passed is True
    results = _check_results(db_conn, "run-pass-1", "staging.example")
    assert results["always_passes"] == ("WARNING", True, 0)

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM quarantine.rejected_rows WHERE reason_code = 'always_passes'")
        assert cur.fetchone()[0] == 0


def test_run_sql_assertion_quarantines_a_failing_critical_check(db_conn):
    check = DqCheck(name="always_fails", sql="SELECT 3", severity="CRITICAL")
    passed = dq.run_sql_assertion(db_conn, "staging.example", "run-fail-1", check)
    assert passed is False
    results = _check_results(db_conn, "run-fail-1", "staging.example")
    assert results["always_fails"] == ("CRITICAL", False, 3)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT severity, raw_payload FROM quarantine.rejected_rows"
            " WHERE table_name = 'staging.example' AND reason_code = 'always_fails'"
        )
        row = cur.fetchone()
        assert row[0] == "CRITICAL"
        assert row[1] == {"run_id": "run-fail-1", "unexpected_count": 3}


def test_run_sql_assertion_does_not_quarantine_a_failing_warning_check(db_conn):
    check = DqCheck(name="warn_only", sql="SELECT 1", severity="WARNING")
    dq.run_sql_assertion(db_conn, "staging.example", "run-warn-1", check)
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM quarantine.rejected_rows WHERE reason_code = 'warn_only'")
        assert cur.fetchone()[0] == 0


def test_run_dq_checks_runs_the_one_real_ge_suite_and_quarantines_critical_failures(db_conn):
    cfg = TableConfig.load(CONFIG_DIR / "tables" / "vendor_deposits.yml")
    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)
    # GE opens its own SQLAlchemy connection (run_ge_suite), so it can't see
    # db_conn's uncommitted staging rows without this — in production this is
    # a non-issue, since dag_factory's _run() commits after every task.
    db_conn.commit()

    run_id = "run-ge-1"
    try:
        # part1_pipeline.md's own edge case: at least one real deposit row has
        # a negative amount_usd, so this CRITICAL expectation must fail — it's
        # logged and quarantined but does not raise (see dq.py's docstring for
        # why this doesn't block load_layer3).
        failed_critical = dq.run_dq_checks(cfg, db_conn, run_id)
        assert "expect_column_values_to_be_between:amount_usd" in failed_critical

        results = _check_results(db_conn, run_id, "staging.vendor_deposits")
        assert set(results) == {
            "expect_column_values_to_be_between:amount_usd",
            "expect_column_values_to_not_be_null:deposit_id",
            "expect_column_values_to_be_in_set:payment_method",
        }
        severity, passed, unexpected_count = results["expect_column_values_to_be_between:amount_usd"]
        assert severity == "CRITICAL"
        assert passed is False
        assert unexpected_count >= 1

        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM quarantine.rejected_rows"
                " WHERE table_name = 'staging.vendor_deposits'"
                "   AND reason_code = 'expect_column_values_to_be_between:amount_usd'"
                "   AND raw_payload->>'run_id' = %s",
                (run_id,),
            )
            assert cur.fetchone()[0] >= 1
    finally:
        _purge_run(db_conn, run_id, "staging.vendor_deposits")
