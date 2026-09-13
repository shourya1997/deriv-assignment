"""Step 4: dim_manager/dim_instrument/dim_date + client_signup/client_profile
owned-column dimension_upsert + the real G2 baseline seed (ADR-2), against
the real shipped configs and real data/ files — not fixtures."""
from __future__ import annotations

from deriv_pipeline.config import CONFIG_DIR, DerivedDimensionConfig, GeneratedDimensionConfig, TableConfig
from deriv_pipeline.dims import derived as derived_dim
from deriv_pipeline.dims import generated as generated_dim
from deriv_pipeline.layers import layer1_raw, layer2_staging, layer3_warehouse

_TABLES_DIR = CONFIG_DIR / "tables"


def _client_signup_cfg() -> TableConfig:
    return TableConfig.load(_TABLES_DIR / "client_signup.yml")


def _client_profile_cfg() -> TableConfig:
    return TableConfig.load(_TABLES_DIR / "client_profile.yml")


def _dim_manager_cfg() -> DerivedDimensionConfig:
    return DerivedDimensionConfig.load(_TABLES_DIR / "dim_manager.yml")


def _dim_instrument_cfg() -> DerivedDimensionConfig:
    return DerivedDimensionConfig.load(_TABLES_DIR / "dim_instrument.yml")


def _dim_date_cfg() -> GeneratedDimensionConfig:
    return GeneratedDimensionConfig.load(_TABLES_DIR / "dim_date.yml")


def _vendor_deposits_cfg() -> TableConfig:
    return TableConfig.load(_TABLES_DIR / "vendor_deposits.yml")


def test_dim_manager_derived_dimension_has_one_row_per_distinct_assigned_manager(db_conn):
    cfg = _client_signup_cfg()
    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)

    loaded = derived_dim.load(_dim_manager_cfg(), db_conn)
    assert loaded == 4

    with db_conn.cursor() as cur:
        cur.execute("SELECT manager_id FROM warehouse.dim_manager ORDER BY manager_id")
        assert [r[0] for r in cur.fetchall()] == ["MGR01", "MGR02", "MGR03", "MGR04"]


def test_dim_instrument_derived_dimension_reads_raw_client_trades_json_directly(db_conn):
    """No client_trades.yml table config exists until Step 5 — dim_instrument
    must derive straight from the raw JSON file, not a staged table."""
    loaded = derived_dim.load(_dim_instrument_cfg(), db_conn)
    assert loaded == 5

    with db_conn.cursor() as cur:
        cur.execute("SELECT instrument_name, asset_class FROM warehouse.dim_instrument")
        by_name = dict(cur.fetchall())
    assert by_name == {
        "EUR/USD": "FX", "USD/JPY": "FX", "Gold": "Commodity",
        "BTC/USD": "Crypto", "S&P500": "Index",
    }


def test_dim_date_generated_dimension_covers_full_2024_range(db_conn):
    loaded = generated_dim.load(_dim_date_cfg(), db_conn)
    assert loaded == 366  # 2024 is a leap year

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM warehouse.dim_date")
        assert cur.fetchone()[0] == 366
        cur.execute("SELECT date_key FROM warehouse.dim_date WHERE full_date = '2024-01-01'")
        assert cur.fetchone()[0] == 20240101
        cur.execute("SELECT date_key FROM warehouse.dim_date WHERE full_date = '2024-12-31'")
        assert cur.fetchone()[0] == 20241231


def test_client_signup_dimension_upsert_resolves_manager_key(db_conn):
    cfg = _client_signup_cfg()
    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)
    derived_dim.load(_dim_manager_cfg(), db_conn)
    layer3_warehouse.load(cfg, db_conn)

    with db_conn.cursor() as cur:
        cur.execute("SELECT manager_key FROM warehouse.dim_manager WHERE manager_id = 'MGR01'")
        mgr01_key = cur.fetchone()[0]
        cur.execute(
            "SELECT country, kyc_status, manager_key, is_inferred FROM warehouse.dim_client"
            " WHERE client_id = 'CL001'"
        )
        country, kyc_status, manager_key, is_inferred = cur.fetchone()
    assert (country, kyc_status, manager_key, is_inferred) == ("Malaysia", "approved", mgr01_key, False)


def test_client_signup_dimension_upsert_never_touches_columns_it_does_not_own(db_conn):
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO warehouse.dim_client (client_id, full_name, is_inferred)"
            " VALUES ('CL001', 'Pre-existing Name', true)"
        )

    cfg = _client_signup_cfg()
    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)
    derived_dim.load(_dim_manager_cfg(), db_conn)
    layer3_warehouse.load(cfg, db_conn)

    with db_conn.cursor() as cur:
        cur.execute("SELECT full_name, country FROM warehouse.dim_client WHERE client_id = 'CL001'")
        full_name, country = cur.fetchone()
    assert full_name == "Pre-existing Name"  # client_profile's column, untouched
    assert country == "Malaysia"  # client_signup's own column, updated


def test_client_profile_dimension_upsert_owns_disjoint_columns(db_conn):
    cfg = _client_profile_cfg()
    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)
    layer3_warehouse.load(cfg, db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT full_name, nationality, currency, preferred_language, country"
            " FROM warehouse.dim_client WHERE client_id = 'CL001'"
        )
        full_name, nationality, currency, preferred_language, country = cur.fetchone()
    assert (full_name, nationality, currency, preferred_language) == (
        "Aisha Tan", "Malaysian", "USD", "English",
    )
    assert country is None  # client_signup's column, never staged in this test


def test_scd2_baseline_seed_excludes_cl030(db_conn):
    """ADR-2: CL030's only CDC event is an 'insert' at lsn 1001 — its `after`
    values are the client's first-ever state, not a pre-existing state a
    baseline should represent, so it must get no source_lsn=0 row."""
    cfg = _client_profile_cfg()
    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)
    layer3_warehouse.load(cfg, db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL030' AND source_lsn = 0"
        )
        assert cur.fetchone()[0] == 0

        cur.execute(
            "SELECT risk_category, account_balance_usd, account_status, valid_from,"
            " valid_to, is_current FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND source_lsn = 0"
        )
        risk_category, balance, status, valid_from, valid_to, is_current = cur.fetchone()
    assert (risk_category, str(balance), status, is_current) == ("medium", "1250.00", "active", True)
    assert valid_from.date().isoformat() == "1970-01-01"
    assert valid_to.date().isoformat() == "9999-12-31"


def test_scd2_baseline_seed_is_idempotent_via_on_conflict(db_conn):
    cfg = _client_profile_cfg()
    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)
    layer3_warehouse.load(cfg, db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND source_lsn = 0"
        )
        first_key = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM warehouse.dim_client_risk_snapshot")
        first_count = cur.fetchone()[0]

    layer3_warehouse.load(cfg, db_conn)  # re-run: must be a no-op, not a duplicate/error

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND source_lsn = 0"
        )
        second_key = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM warehouse.dim_client_risk_snapshot")
        second_count = cur.fetchone()[0]

    assert second_key == first_key
    assert second_count == first_count


def test_scd2_baseline_seed_repoints_and_clears_step3_stopgap_snapshots(db_conn):
    """ADR-7's consequence: Step 3's on-demand stopgap (negative source_lsn)
    must be superseded once the real, properly-ordered baseline lands — any
    fact row FK'd into a stopgap gets repointed at the real baseline key, and
    the stopgap row is deleted, not left as an orphaned duplicate."""
    vendor_cfg = _vendor_deposits_cfg()
    layer1_raw.load(vendor_cfg, db_conn)
    layer2_staging.stage(vendor_cfg, db_conn)
    layer3_warehouse.load(vendor_cfg, db_conn)  # creates CL001 stopgap row(s), no real baseline yet

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND source_lsn < 0"
        )
        assert cur.fetchone()[0] > 0  # sanity: the stopgap path actually fired
        cur.execute(
            "SELECT DISTINCT risk_snapshot_key FROM warehouse.fact_deposits WHERE client_key IN"
            " (SELECT client_key FROM warehouse.dim_client WHERE client_id = 'CL001')"
        )
        stopgap_keys_in_facts = {r[0] for r in cur.fetchall()}
    assert stopgap_keys_in_facts  # CL001 has fact rows referencing the stopgap

    profile_cfg = _client_profile_cfg()
    layer1_raw.load(profile_cfg, db_conn)
    layer2_staging.stage(profile_cfg, db_conn)
    layer3_warehouse.load(profile_cfg, db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND source_lsn < 0"
        )
        assert cur.fetchone()[0] == 0  # every stopgap row for CL001 is gone

        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND source_lsn = 0"
        )
        real_key = cur.fetchone()[0]
        cur.execute(
            "SELECT DISTINCT risk_snapshot_key FROM warehouse.fact_deposits WHERE client_key IN"
            " (SELECT client_key FROM warehouse.dim_client WHERE client_id = 'CL001')"
        )
        fact_keys_after = {r[0] for r in cur.fetchall()}
    assert fact_keys_after == {real_key}  # repointed, not left dangling on a deleted row


def test_scd2_baseline_seed_gives_orphan_client_sentinel_baseline_and_clears_stopgaps(db_conn):
    """CL099 has no client_profile row at all (part2_data_model.md
    late-arriving dimension members) — ADR-2 says it must get a sentinel
    baseline (risk_category='unknown', account_balance_usd=0.00,
    account_status='unknown') rather than being left permanently stuck on a
    Step 3 stopgap, since a stopgap row is not meant to be the answer forever."""
    vendor_cfg = _vendor_deposits_cfg()
    layer1_raw.load(vendor_cfg, db_conn)
    layer2_staging.stage(vendor_cfg, db_conn)
    layer3_warehouse.load(vendor_cfg, db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL099' AND source_lsn < 0"
        )
        before = cur.fetchone()[0]
    assert before > 0

    profile_cfg = _client_profile_cfg()
    layer1_raw.load(profile_cfg, db_conn)
    layer2_staging.stage(profile_cfg, db_conn)
    layer3_warehouse.load(profile_cfg, db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL099' AND source_lsn < 0"
        )
        after = cur.fetchone()[0]
        cur.execute(
            "SELECT risk_category, account_balance_usd, account_status FROM"
            " warehouse.dim_client_risk_snapshot WHERE client_id = 'CL099' AND source_lsn = 0"
        )
        real_row = cur.fetchone()
    assert after == 0  # stopgap cleared, repointed onto the sentinel baseline
    assert real_row == ("unknown", 0.00, "unknown")
