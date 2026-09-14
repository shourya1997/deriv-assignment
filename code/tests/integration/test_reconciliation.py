"""Step 9: reconcile_vendor_feed's diff engine against the real shipped
vendor_deposits + client_deposit data. part1_pipeline.md's own note: these
two sources share zero (client_id, deposit_date, amount_usd) matches in this
dataset, so "discrepancies is non-empty" alone would pass even for a broken
diff engine (e.g. one that always reports "everything missing" regardless of
the other side) — the real assertion is the exact symmetric difference, plus
a positive control proving a genuine match is correctly excluded."""
from deriv_pipeline.config import CONFIG_DIR, ReconciliationConfig, TableConfig
from deriv_pipeline.layers import layer1_raw, layer2_staging, layer3_warehouse
from deriv_pipeline.recon import reconcile


def _load_fully(cfg, conn):
    layer1_raw.load(cfg, conn)
    layer2_staging.stage(cfg, conn)
    layer3_warehouse.load(cfg, conn)


def _keys(conn, source_system):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.client_id, f.deposit_date, f.amount_usd"
            " FROM warehouse.fact_deposits f JOIN warehouse.dim_client c USING (client_key)"
            " WHERE f.source_system = %s",
            (source_system,),
        )
        return {tuple(row) for row in cur.fetchall()}


def test_reconciliation_matches_exact_symmetric_difference_and_excludes_a_true_match(db_conn):
    _load_fully(TableConfig.load(CONFIG_DIR / "tables" / "vendor_deposits.yml"), db_conn)
    _load_fully(TableConfig.load(CONFIG_DIR / "tables" / "client_deposit.yml"), db_conn)

    vendor_keys = _keys(db_conn, "vendor")
    internal_keys = _keys(db_conn, "internal")
    assert vendor_keys & internal_keys == set()

    # Positive control: clone one internal row into a matching vendor row
    # (identical client/date/amount) — the diff engine must NOT report it.
    # This also *removes* one of the pre-existing discrepancies (the cloned
    # internal row was previously "missing" on the vendor side), so the
    # expected count is the symmetric difference recomputed *after* the
    # clone, not the pre-clone vendor+internal sum.
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT client_key, risk_snapshot_key, date_key, deposit_date, amount_usd"
            " FROM warehouse.fact_deposits WHERE source_system = 'internal' LIMIT 1"
        )
        client_key, risk_snapshot_key, date_key, deposit_date, amount_usd = cur.fetchone()
        cur.execute(
            "INSERT INTO warehouse.fact_deposits"
            " (deposit_id, client_key, risk_snapshot_key, date_key, deposit_date, amount_usd, source_system)"
            " VALUES ('RECON_TEST_MATCH', %s, %s, %s, %s, %s, 'vendor')",
            (client_key, risk_snapshot_key, date_key, deposit_date, amount_usd),
        )

    vendor_keys = _keys(db_conn, "vendor")
    expected_discrepancies = len(vendor_keys ^ internal_keys)

    cfg = ReconciliationConfig.load(CONFIG_DIR / "reconciliations" / "vendor_feed.yml")
    run_id = "run-recon-1"
    count = reconcile.run_reconciliation(cfg, db_conn, run_id)
    assert count == expected_discrepancies

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM data_quality.reconciliation_discrepancies WHERE run_id = %s",
            (run_id,),
        )
        assert cur.fetchone()[0] == expected_discrepancies

        cur.execute(
            "SELECT discrepancy_type, count(*) FROM data_quality.reconciliation_discrepancies"
            " WHERE run_id = %s GROUP BY discrepancy_type",
            (run_id,),
        )
        by_type = dict(cur.fetchall())
        assert by_type["missing_right"] == len(vendor_keys - internal_keys)
        assert by_type["missing_left"] == len(internal_keys - vendor_keys)

        cur.execute(
            "SELECT passed, unexpected_count FROM data_quality.dq_check_results"
            " WHERE run_id = %s AND check_name = 'reconciliation:vendor_feed'",
            (run_id,),
        )
        passed, unexpected_count = cur.fetchone()
        assert passed is False
        assert unexpected_count == expected_discrepancies


def test_reconciliation_reports_nothing_when_both_sides_are_empty(db_conn):
    cfg = ReconciliationConfig.load(CONFIG_DIR / "reconciliations" / "vendor_feed.yml")
    run_id = "run-recon-empty"
    count = reconcile.run_reconciliation(cfg, db_conn, run_id)
    assert count == 0

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT passed FROM data_quality.dq_check_results"
            " WHERE run_id = %s AND check_name = 'reconciliation:vendor_feed'",
            (run_id,),
        )
        assert cur.fetchone()[0] is True
