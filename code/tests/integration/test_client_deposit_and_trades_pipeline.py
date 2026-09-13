"""Step 5: client_deposit.yml and client_trades.yml against the real shipped
configs and data/*.json — the second and third users of the generic
fact_upsert (after vendor_deposits), proving it was actually generalized off
its Step 3 hardcoded shape rather than merely documented as needing it."""
import json

from deriv_pipeline.config import CONFIG_DIR, DerivedDimensionConfig, TableConfig
from deriv_pipeline.db import REPO_ROOT
from deriv_pipeline.dims import derived as derived_dim
from deriv_pipeline.layers import layer1_raw, layer2_staging, layer3_warehouse


def _client_deposit_cfg() -> TableConfig:
    return TableConfig.load(CONFIG_DIR / "tables" / "client_deposit.yml")


def _client_trades_cfg() -> TableConfig:
    return TableConfig.load(CONFIG_DIR / "tables" / "client_trades.yml")


def _build_dim_instrument(conn):
    """client_trades' instrument_key fk_resolution needs dim_instrument
    already populated — in the real DAG graph this is bootstrap_warehouse's
    job (ADR-1); tests do it directly rather than running the whole DAG."""
    cfg = DerivedDimensionConfig.load(CONFIG_DIR / "tables" / "dim_instrument.yml")
    derived_dim.load(cfg, conn)


def _real_records(filename):
    return json.loads((REPO_ROOT / "data" / filename).read_text())


def test_client_deposit_loads_into_shared_fact_deposits_as_internal(db_conn):
    cfg = _client_deposit_cfg()
    layer1_raw.load(cfg, db_conn)
    staged = layer2_staging.stage(cfg, db_conn)
    loaded = layer3_warehouse.load(cfg, db_conn)
    assert loaded == staged == len(_real_records("client_deposit.json"))

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM warehouse.fact_deposits WHERE source_system = 'internal'")
        assert cur.fetchone()[0] == staged
        # DEP012 has the credit_card/payment_method alias drift (like vendor's
        # `method`) — must resolve, not silently drop the column.
        cur.execute(
            "SELECT payment_method FROM warehouse.fact_deposits WHERE deposit_id = 'DEP012'"
        )
        assert cur.fetchone()[0] is not None
        cur.execute(
            "SELECT client_key, risk_snapshot_key, date_key, deposit_date"
            " FROM warehouse.fact_deposits WHERE deposit_id = 'DEP001'"
        )
        client_key, risk_snapshot_key, date_key, deposit_date = cur.fetchone()
        assert client_key is not None and risk_snapshot_key is not None
        assert date_key == int(deposit_date.strftime("%Y%m%d"))


def test_client_deposit_and_vendor_deposits_coexist_in_shared_fact_table(db_conn):
    """Both tables' fact_upsert targets warehouse.fact_deposits with distinct
    deposit_id natural keys — loading one must never touch the other's rows,
    proving the config-driven column lists (not a shared hardcoded literal)
    are what distinguishes 'vendor' from 'internal' per source."""
    vendor_cfg = TableConfig.load(CONFIG_DIR / "tables" / "vendor_deposits.yml")
    layer1_raw.load(vendor_cfg, db_conn)
    layer2_staging.stage(vendor_cfg, db_conn)
    vendor_loaded = layer3_warehouse.load(vendor_cfg, db_conn)

    deposit_cfg = _client_deposit_cfg()
    layer1_raw.load(deposit_cfg, db_conn)
    layer2_staging.stage(deposit_cfg, db_conn)
    internal_loaded = layer3_warehouse.load(deposit_cfg, db_conn)

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM warehouse.fact_deposits WHERE source_system = 'vendor'")
        assert cur.fetchone()[0] == vendor_loaded
        cur.execute("SELECT count(*) FROM warehouse.fact_deposits WHERE source_system = 'internal'")
        assert cur.fetchone()[0] == internal_loaded


def test_client_trades_resolves_instrument_and_risk_snapshot_fks(db_conn):
    _build_dim_instrument(db_conn)
    cfg = _client_trades_cfg()
    layer1_raw.load(cfg, db_conn)
    staged = layer2_staging.stage(cfg, db_conn)
    loaded = layer3_warehouse.load(cfg, db_conn)
    assert loaded == staged == len(_real_records("client_trades.json"))

    with db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM warehouse.fact_trades")
        assert cur.fetchone()[0] == staged
        cur.execute(
            "SELECT t.client_key, t.risk_snapshot_key, t.date_key, t.trade_date, i.instrument_name, i.asset_class"
            " FROM warehouse.fact_trades t JOIN warehouse.dim_instrument i USING (instrument_key)"
            " WHERE t.trade_id = 'TRD001'"
        )
        client_key, risk_snapshot_key, date_key, trade_date, instrument_name, asset_class = cur.fetchone()
        assert client_key is not None and risk_snapshot_key is not None
        assert date_key == int(trade_date.strftime("%Y%m%d"))
        assert instrument_name == "EUR/USD"
        assert asset_class == "FX"


def test_client_trades_with_null_instrument_raises_clear_error(db_conn):
    """instrument_key is NOT NULL on fact_trades — a trade with a missing
    instrument must raise a clear, diagnostic error from fact_upsert, not
    silently insert NULL and crash later on a constraint violation (Step 5
    dual review finding)."""
    import pytest

    _build_dim_instrument(db_conn)
    cfg = _client_trades_cfg()
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO staging.client_trades"
            " (trade_id, client_id, trade_date, instrument, direction, volume_lots,"
            "  open_price, close_price, pnl_usd, trade_status)"
            " VALUES ('TRDNULL', 'CL001', '2024-01-10', NULL, 'buy', 1.0, 1.1, 1.2, 10.0, 'closed')"
        )
    with pytest.raises(ValueError, match="instrument"):
        layer3_warehouse.load(cfg, db_conn)


def test_client_trades_is_idempotent(db_conn):
    _build_dim_instrument(db_conn)
    cfg = _client_trades_cfg()
    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)
    layer3_warehouse.load(cfg, db_conn)
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT trade_id, client_key, risk_snapshot_key, instrument_key, date_key,"
            " direction, volume_lots, open_price, close_price, pnl_usd, trade_status"
            " FROM warehouse.fact_trades ORDER BY trade_id"
        )
        first = cur.fetchall()

    layer1_raw.load(cfg, db_conn)
    layer2_staging.stage(cfg, db_conn)
    layer3_warehouse.load(cfg, db_conn)
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT trade_id, client_key, risk_snapshot_key, instrument_key, date_key,"
            " direction, volume_lots, open_price, close_price, pnl_usd, trade_status"
            " FROM warehouse.fact_trades ORDER BY trade_id"
        )
        second = cur.fetchall()

    assert first == second
