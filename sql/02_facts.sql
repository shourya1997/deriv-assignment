-- ============================================================================
-- Part 2 — Fact tables (warehouse schema)
-- fact_deposits grain: one row per deposit (deposit_id), from both
--   client_deposit.json (source_system='internal') and the vendor CSV feed
--   (source_system='vendor'). Both share this table so the Part 1
--   reconciliation job can diff them by (client_id, deposit_date, amount_usd).
-- fact_trades grain: one row per trade (trade_id), from client_trades.json.
--
-- Low-cardinality attributes (payment_method, currency_original, status,
-- direction, trade_status) are kept as degenerate dimensions — plain columns
-- on the fact row — per the Part 2a decision; only assigned_manager got a
-- real dimension table (dim_manager), reached via dim_client.manager_key.
-- ============================================================================

CREATE TABLE warehouse.fact_deposits (
    deposit_id          text PRIMARY KEY,               -- natural key doubles as PK; grain is already unique
    client_key          bigint NOT NULL REFERENCES warehouse.dim_client(client_key),
    risk_snapshot_key   bigint NOT NULL REFERENCES warehouse.dim_client_risk_snapshot(risk_snapshot_key),
    date_key            int    NOT NULL REFERENCES warehouse.dim_date(date_key),
    deposit_date        date   NOT NULL,
    amount_usd          numeric(14,2) NOT NULL,
    exchange_rate       numeric(12,6),
    fee_usd             numeric(10,2) NOT NULL DEFAULT 0,
    processing_days     int,
    payment_method      text,          -- degenerate dimension
    currency_original   text,          -- degenerate dimension
    status              text,          -- degenerate dimension
    source_system       text NOT NULL CHECK (source_system IN ('internal','vendor')),
    is_late_arrival     boolean NOT NULL DEFAULT false
);
CREATE INDEX ix_fact_deposits_client ON warehouse.fact_deposits (client_key);
CREATE INDEX ix_fact_deposits_date ON warehouse.fact_deposits (date_key);

CREATE TABLE warehouse.fact_trades (
    trade_id            text PRIMARY KEY,
    client_key          bigint NOT NULL REFERENCES warehouse.dim_client(client_key),
    risk_snapshot_key   bigint NOT NULL REFERENCES warehouse.dim_client_risk_snapshot(risk_snapshot_key),
    instrument_key      bigint NOT NULL REFERENCES warehouse.dim_instrument(instrument_key),
    date_key            int    NOT NULL REFERENCES warehouse.dim_date(date_key),
    trade_date          date   NOT NULL,
    direction            text,         -- degenerate dimension: 'buy' | 'sell'
    volume_lots          numeric(10,2) NOT NULL,
    open_price            numeric(14,5) NOT NULL,
    close_price           numeric(14,5) NOT NULL,
    pnl_usd               numeric(14,2) NOT NULL,
    trade_status          text          -- degenerate dimension: 'closed' | 'open' | ...
);
CREATE INDEX ix_fact_trades_client ON warehouse.fact_trades (client_key);
CREATE INDEX ix_fact_trades_instrument ON warehouse.fact_trades (instrument_key);
CREATE INDEX ix_fact_trades_date ON warehouse.fact_trades (date_key);
