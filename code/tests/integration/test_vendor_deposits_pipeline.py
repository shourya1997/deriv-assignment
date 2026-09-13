"""Step 3 walking skeleton: vendor_deposits end-to-end, layer1 -> layer2 ->
layer3, against the real shipped config/tables/vendor_deposits.yml and the
real data/deposits_vendor_*.csv files — not fixtures, so this is the
milestone proving the architecture actually runs, not just parses."""
import csv

from deriv_pipeline.config import CONFIG_DIR, TableConfig
from deriv_pipeline.db import REPO_ROOT
from deriv_pipeline.layers import layer1_raw, layer2_staging, layer3_warehouse


def _cfg() -> TableConfig:
    return TableConfig.load(CONFIG_DIR / "tables" / "vendor_deposits.yml")


def _real_rows_by_file():
    data_dir = REPO_ROOT / "data"
    out = {}
    for path in sorted(data_dir.glob("deposits_vendor_*.csv")):
        with path.open(newline="") as f:
            out[path.name] = list(csv.DictReader(f))
    return out


def test_layer1_lands_every_row_from_every_matched_file(db_conn):
    cfg = _cfg()
    landed = layer1_raw.load(cfg, db_conn)
    total_rows = sum(len(rows) for rows in _real_rows_by_file().values())
    assert landed == total_rows

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw.vendor_deposits")
        assert cur.fetchone()[0] == total_rows


def test_layer1_is_rerun_safe(db_conn):
    cfg = _cfg()
    first = layer1_raw.load(cfg, db_conn)
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw.vendor_deposits")
        after_first = cur.fetchone()[0]
    layer1_raw.load(cfg, db_conn)
    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw.vendor_deposits")
        after_second = cur.fetchone()[0]
    assert after_first == after_second == first


def test_layer2_dedups_by_natural_key_and_flags_drift(db_conn):
    cfg = _cfg()
    layer1_raw.load(cfg, db_conn)
    staged = layer2_staging.stage(cfg, db_conn)

    by_file = _real_rows_by_file()
    distinct_ids = {row["deposit_id"] for rows in by_file.values() for row in rows}
    assert staged == len(distinct_ids)

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM staging.vendor_deposits")
        assert cur.fetchone()[0] == len(distinct_ids)

    # deposits_vendor_20240302.csv uses `method`, not `payment_method` — every
    # deposit_id unique to that file must be flagged as drift.
    ids_only_in_0302 = {r["deposit_id"] for r in by_file["deposits_vendor_20240302.csv"]} - {
        r["deposit_id"] for r in by_file["deposits_vendor_20240301.csv"]
    }
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT deposit_id, schema_drift_detected, payment_method FROM staging.vendor_deposits"
            " WHERE deposit_id = ANY(%s)",
            (list(ids_only_in_0302),),
        )
        rows = cur.fetchall()
    assert len(rows) == len(ids_only_in_0302)
    for deposit_id, drift, payment_method in rows:
        assert drift is True
        assert payment_method is not None  # alias resolved, not dropped


def test_layer2_flags_late_arrival_from_filename_not_ingested_at(db_conn):
    cfg = _cfg()
    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)

    with db_conn.cursor() as cur:
        # VDEP017: delivered in deposits_vendor_20240303.csv, deposit_date
        # 2024-02-26 -> 6 days late, over the 2-day threshold.
        cur.execute("SELECT is_late_arrival FROM staging.vendor_deposits WHERE deposit_id = 'VDEP017'")
        assert cur.fetchone()[0] is True
        # VDEP001: delivered same-day in its own file -> not late.
        cur.execute("SELECT is_late_arrival FROM staging.vendor_deposits WHERE deposit_id = 'VDEP001'")
        assert cur.fetchone()[0] is False


def test_layer3_fact_upsert_loads_one_row_per_staged_deposit_with_resolved_fks(db_conn):
    cfg = _cfg()
    layer1_raw.load(cfg, db_conn)
    staged = layer2_staging.stage(cfg, db_conn)
    loaded = layer3_warehouse.load(cfg, db_conn)
    assert loaded == staged

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM warehouse.fact_deposits WHERE source_system = 'vendor'")
        assert cur.fetchone()[0] == staged
        cur.execute(
            "SELECT client_key, risk_snapshot_key, date_key, deposit_date FROM warehouse.fact_deposits"
            " WHERE deposit_id = 'VDEP002'"
        )
        client_key, risk_snapshot_key, date_key, deposit_date = cur.fetchone()
        expected_date_key = int(deposit_date.strftime("%Y%m%d"))
        assert client_key is not None and risk_snapshot_key is not None and date_key == expected_date_key


def test_layer3_inferred_member_pattern_for_orphan_client(db_conn):
    """VDEP020 belongs to CL099, which has no client_signup/client_profile
    row in this dataset (part2_data_model.md "Late-arriving dimension
    members") — must get a stub dim_client row, not a quarantined fact.

    Also seeds a real (non-inferred) dim_client row for a client who DOES
    appear in the deposits data (CL001), so the test actually distinguishes
    the "hit" branch from the "miss -> stub" branch instead of only ever
    exercising the miss path — without this, _resolve_client_key's `if row
    is not None: return row[0]` branch is never proven."""
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO warehouse.dim_client (client_id, is_inferred) VALUES ('CL001', false)"
        )
        cur.execute("SELECT client_key FROM warehouse.dim_client WHERE client_id = 'CL001'")
        real_client_key = cur.fetchone()[0]

    cfg = _cfg()
    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)
    layer3_warehouse.load(cfg, db_conn)

    with db_conn.cursor() as cur:
        cur.execute("SELECT is_inferred FROM warehouse.dim_client WHERE client_id = 'CL099'")
        row = cur.fetchone()
        assert row is not None and row[0] is True
        cur.execute("SELECT count(*) FROM warehouse.fact_deposits WHERE deposit_id = 'VDEP020'")
        assert cur.fetchone()[0] == 1

        # the pre-seeded real CL001 row must be reused as-is (not re-stubbed,
        # not duplicated) by every fact row for CL001.
        cur.execute("SELECT is_inferred FROM warehouse.dim_client WHERE client_id = 'CL001'")
        assert cur.fetchone()[0] is False
        cur.execute("SELECT count(*) FROM warehouse.dim_client WHERE client_id = 'CL001'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT deposit_id FROM staging.vendor_deposits WHERE client_id = 'CL001'")
        cl001_deposit_ids = [r[0] for r in cur.fetchall()]
        assert cl001_deposit_ids  # CL001 must actually appear in this dataset
        cur.execute(
            "SELECT DISTINCT client_key FROM warehouse.fact_deposits WHERE deposit_id = ANY(%s)",
            (cl001_deposit_ids,),
        )
        assert [r[0] for r in cur.fetchall()] == [real_client_key]


def _fact_deposits_snapshot(db_conn):
    """Full-content snapshot (not just count(*)): every column of every
    vendor-sourced fact row, plus the dim_client/dim_client_risk_snapshot
    row counts a second run could silently inflate even if fact count(*)
    stayed flat (e.g. by re-seeding sentinels or re-stubbing clients)."""
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT deposit_id, client_key, risk_snapshot_key, date_key, deposit_date,"
            " amount_usd, exchange_rate, fee_usd, processing_days, payment_method,"
            " currency_original, status, is_late_arrival"
            " FROM warehouse.fact_deposits WHERE source_system = 'vendor' ORDER BY deposit_id"
        )
        facts = cur.fetchall()
        cur.execute("SELECT count(*) FROM warehouse.dim_client")
        dim_client_count = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM warehouse.dim_client_risk_snapshot")
        risk_snapshot_count = cur.fetchone()[0]
    return facts, dim_client_count, risk_snapshot_count


def test_full_pipeline_is_idempotent_end_to_end(db_conn):
    cfg = _cfg()
    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)
    layer3_warehouse.load(cfg, db_conn)
    first = _fact_deposits_snapshot(db_conn)
    assert first[0]  # sanity: the run actually loaded fact rows

    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)
    layer3_warehouse.load(cfg, db_conn)
    second = _fact_deposits_snapshot(db_conn)

    assert first == second
