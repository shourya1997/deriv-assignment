import pytest

EXPECTED_SCHEMAS = {"warehouse", "raw", "staging", "quarantine", "data_quality"}

EXPECTED_TABLES = {
    ("warehouse", "dim_manager"),
    ("warehouse", "dim_instrument"),
    ("warehouse", "dim_date"),
    ("warehouse", "dim_client"),
    ("warehouse", "dim_client_risk_snapshot"),
    ("warehouse", "cdc_watermark"),
    ("warehouse", "fact_deposits"),
    ("warehouse", "fact_trades"),
    ("raw", "client_signup"),
    ("raw", "client_profile"),
    ("raw", "client_deposit"),
    ("raw", "client_trades"),
    ("raw", "vendor_deposits"),
    ("raw", "client_profile_changes"),
    ("staging", "client_signup"),
    ("staging", "client_profile"),
    ("staging", "client_deposit"),
    ("staging", "client_trades"),
    ("staging", "vendor_deposits"),
    ("staging", "client_profile_changes"),
    ("quarantine", "rejected_rows"),
    ("data_quality", "dq_check_results"),
    ("data_quality", "reconciliation_discrepancies"),
}

EXPECTED_FUNCTIONS = {
    ("warehouse", "apply_cdc_event"),
    ("warehouse", "resolve_risk_snapshot_key"),
    ("warehouse", "reset_client_for_reload"),
}


def test_expected_schemas_exist(db_conn):
    with db_conn.cursor() as cur:
        cur.execute("SELECT schema_name FROM information_schema.schemata")
        found = {row[0] for row in cur.fetchall()}
    assert EXPECTED_SCHEMAS <= found


@pytest.mark.parametrize("schema,table", sorted(EXPECTED_TABLES))
def test_expected_table_exists(db_conn, schema, table):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (schema, table),
        )
        assert cur.fetchone() is not None, f"{schema}.{table} missing"


@pytest.mark.parametrize("schema,func", sorted(EXPECTED_FUNCTIONS))
def test_expected_function_exists(db_conn, schema, func):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.routines WHERE routine_schema = %s AND routine_name = %s",
            (schema, func),
        )
        assert cur.fetchone() is not None, f"{schema}.{func}() missing"


def test_rerunning_migrations_is_a_noop():
    from deriv_pipeline.migrate import run_migrations

    assert run_migrations() == []


def test_apply_cdc_event_stale_lsn_quarantines(db_conn):
    """Makes the migration manifest's 007_quarantine-before-sql/03 ordering
    load-bearing: a fresh client's watermark starts at 0, so lsn=0 is stale on
    first call and must land in quarantine.rejected_rows, not raise (the table
    this depends on wouldn't exist if the manifest order regressed)."""
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT warehouse.apply_cdc_event(%s, %s, now(), 'update', '{}'::jsonb)",
            ("CL_HARNESS_TEST", 0),
        )
        cur.execute(
            "SELECT reason_code FROM quarantine.rejected_rows"
            " WHERE table_name = 'dim_client_risk_snapshot'"
            " AND raw_payload->>'client_id' = 'CL_HARNESS_TEST'"
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "stale_lsn"
