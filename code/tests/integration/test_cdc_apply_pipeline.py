"""Step 6: client_profile_changes.yml (scd2_apply) against the real shipped
config and data/client_profile_changes.jsonl — proving the locked,
watermark-only `warehouse.apply_cdc_event()` mechanism (sql/03) against a
genuinely out-of-order real event (CL001: file order is lsn 1005, then 1004
— stale — then 1006), a real delete (CL012, tombstoned not hard-deleted), and
a real insert with no pre-existing baseline (CL030, excluded from the G2
seed per ADR-2)."""
from __future__ import annotations

from deriv_pipeline.config import CONFIG_DIR, TableConfig
from deriv_pipeline.layers import layer1_raw, layer2_staging, layer3_warehouse

_TABLES_DIR = CONFIG_DIR / "tables"


def _client_profile_cfg() -> TableConfig:
    return TableConfig.load(_TABLES_DIR / "client_profile.yml")


def _cdc_cfg() -> TableConfig:
    return TableConfig.load(_TABLES_DIR / "client_profile_changes.yml")


def _seed_baseline(db_conn) -> None:
    """bootstrap_warehouse's job in the real DAG graph (ADR-2/ADR-10: the
    baseline seed must complete before any apply_cdc_event call) — done
    directly here rather than running the whole DAG, matching Step 5's
    _build_dim_instrument test helper."""
    cfg = _client_profile_cfg()
    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)
    layer3_warehouse.load(cfg, db_conn)


def _load_cdc(db_conn):
    cfg = _cdc_cfg()
    layer1_raw.load(cfg, db_conn)
    staged = layer2_staging.stage(cfg, db_conn)
    applied = layer3_warehouse.load(cfg, db_conn)
    return staged, applied


def test_client_profile_changes_quarantines_out_of_order_lsn_and_applies_in_file_order(db_conn):
    """CL001's real file-order events are lsn 1005, then 1004, then 1006 —
    1004 arrives strictly after 1005 in file order and must be rejected as
    stale by the watermark check (sql/03), not silently reordered and
    applied as if it were newest."""
    _seed_baseline(db_conn)
    staged, applied = _load_cdc(db_conn)
    assert staged == applied == 12  # every layer2 row is "processed" (applied or quarantined)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT reason_code, raw_payload FROM quarantine.rejected_rows"
            " WHERE table_name = 'dim_client_risk_snapshot'"
            "   AND raw_payload->>'client_id' = 'CL001' AND (raw_payload->>'lsn')::bigint = 1004"
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "stale_lsn"

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT risk_category, account_balance_usd, account_status, source_lsn"
            " FROM warehouse.dim_client_risk_snapshot WHERE client_id = 'CL001' AND is_current = true"
        )
        risk_category, balance, status, source_lsn = cur.fetchone()
    # lsn 1006 (the real newest event) won, not the stale lsn 1004 or an
    # accidentally-applied-out-of-order intermediate state.
    assert (risk_category, str(balance), status, source_lsn) == ("high", "1850.00", "under_review", 1006)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND source_lsn = 1004"
        )
        assert cur.fetchone()[0] == 0  # the stale event never got applied at all


def test_client_profile_changes_delete_appends_tombstone_not_hard_delete(db_conn):
    _seed_baseline(db_conn)
    _load_cdc(db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT risk_category, account_status, is_current, is_deleted, source_lsn"
            " FROM warehouse.dim_client_risk_snapshot WHERE client_id = 'CL012' AND is_current = true"
        )
        risk_category, status, is_current, is_deleted, source_lsn = cur.fetchone()
    assert (status, is_current, is_deleted, source_lsn) == ("deleted", True, True, 1010)
    assert risk_category == "low"  # carried over from the pre-delete row, per sql/03

    with db_conn.cursor() as cur:
        # the pre-delete version (source_lsn=0 baseline) must still exist,
        # just no longer current — nothing hard-deleted.
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL012' AND source_lsn = 0 AND is_current = false"
        )
        assert cur.fetchone()[0] == 1


def test_client_profile_changes_insert_op_with_no_baseline_becomes_first_current_row(db_conn):
    """CL030 is excluded from the G2 baseline seed (its only event, lsn 1001,
    is the 'insert' itself) — apply_cdc_event must still succeed with no
    pre-existing current row to end-date, making this insert the first and
    only snapshot."""
    _seed_baseline(db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot WHERE client_id = 'CL030'"
        )
        assert cur.fetchone()[0] == 0  # confirmed excluded from the baseline

    _load_cdc(db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT risk_category, account_balance_usd, account_status, is_current, source_lsn"
            " FROM warehouse.dim_client_risk_snapshot WHERE client_id = 'CL030'"
        )
        rows = cur.fetchall()
    assert len(rows) == 1
    risk_category, balance, status, is_current, source_lsn = rows[0]
    assert (risk_category, str(balance), status, is_current, source_lsn) == (
        "medium", "1420.00", "active", True, 1001,
    )


def test_client_profile_changes_delete_with_no_prior_snapshot_is_quarantined(db_conn):
    """A delete for a client with zero existing dim_client_risk_snapshot rows
    is a silent triple no-op in apply_cdc_event itself (its tombstone SELECT
    finds nothing to copy, so nothing is inserted) — yet the watermark would
    still advance, leaving neither a dimension row nor a quarantine row for
    that layer2 row (Step 6 dual review finding, Opus). scd2_apply must
    check for this explicitly and quarantine it by name instead."""
    _seed_baseline(db_conn)  # CL030 is excluded (insert-first) — has zero rows

    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO staging.client_profile_changes"
            " (client_id, lsn, commit_ts, op, before, after)"
            " VALUES ('CL030', 9999, now(), 'delete', NULL, NULL)"
        )

    layer3_warehouse.load(_cdc_cfg(), db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot WHERE client_id = 'CL030'"
        )
        assert cur.fetchone()[0] == 0  # still no dimension row — no phantom tombstone

        cur.execute(
            "SELECT reason_code FROM quarantine.rejected_rows"
            " WHERE table_name = 'dim_client_risk_snapshot'"
            "   AND raw_payload->>'client_id' = 'CL030' AND (raw_payload->>'lsn')::bigint = 9999"
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "delete_with_no_baseline"


def test_client_profile_changes_replay_is_idempotent(db_conn):
    """Also asserts on quarantine.rejected_rows, not just the snapshot table
    (Step 6 dual review finding, both reviewers independently confirmed):
    scd2_apply's replay SELECT used to be unfiltered, so every rerun replayed
    the whole staging table and apply_cdc_event's non-idempotent stale-lsn
    branch wrote a fresh quarantine row for every already-applied event —
    unbounded growth that a snapshot-count-only assertion would never catch
    (empirically: run1 quarantine=2 rows, run2=14 before the fix)."""
    _seed_baseline(db_conn)
    staged1, applied1 = _load_cdc(db_conn)

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM warehouse.dim_client_risk_snapshot")
        first_snapshot_count = cur.fetchone()[0]
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND is_current = true"
        )
        first_current_key = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM quarantine.rejected_rows WHERE table_name = 'dim_client_risk_snapshot'"
        )
        first_quarantine_count = cur.fetchone()[0]

    staged2, applied2 = _load_cdc(db_conn)  # replay the same file again: watermark already past every lsn

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM warehouse.dim_client_risk_snapshot")
        second_snapshot_count = cur.fetchone()[0]
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND is_current = true"
        )
        second_current_key = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM quarantine.rejected_rows WHERE table_name = 'dim_client_risk_snapshot'"
        )
        second_quarantine_count = cur.fetchone()[0]

    assert second_snapshot_count == first_snapshot_count
    assert second_current_key == first_current_key
    assert second_quarantine_count == first_quarantine_count  # no re-quarantining on replay
    assert staged1 == staged2 == 12
    assert applied2 == 0  # every row already reflected in the watermark: nothing left to replay
