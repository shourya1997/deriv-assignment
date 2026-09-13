-- ============================================================================
-- Part 2 — Dimension tables (warehouse schema)
-- See part2_data_model.md for the design rationale behind each choice below.
-- Order matters: dim_manager must exist before dim_client (FK dependency).
-- ============================================================================

-- dim_manager: thin dimension for assigned_manager (MGR01..MGR04 in source
-- data). Kept separate from dim_client because it's a real, independently
-- reportable business entity, not a fixed-value tag.
CREATE TABLE warehouse.dim_manager (
    manager_key   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    manager_id    text NOT NULL UNIQUE,     -- natural key, e.g. 'MGR01'
    manager_name  text                      -- not present in source data today; nullable until available
);

-- dim_instrument: one row per traded instrument, with asset_class for
-- exposure/PNL rollups without string-parsing instrument names per query.
CREATE TABLE warehouse.dim_instrument (
    instrument_key  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instrument_name text NOT NULL UNIQUE,   -- e.g. 'EUR/USD', 'Gold', 'BTC/USD'
    asset_class     text NOT NULL           -- 'FX' | 'Commodity' | 'Crypto' | 'Index'
);

-- dim_date: standard generated date dimension, not sourced from input files.
CREATE TABLE warehouse.dim_date (
    date_key    int PRIMARY KEY,            -- YYYYMMDD
    full_date   date NOT NULL UNIQUE,
    day_of_week int  NOT NULL,
    day_name    text NOT NULL,
    month       int  NOT NULL,
    month_name  text NOT NULL,
    quarter     int  NOT NULL,
    year        int  NOT NULL,
    is_weekend  boolean NOT NULL
);

-- dim_client: SCD Type 1 (overwrite, no history) — static/rarely-changing
-- attributes only. The 3 volatile CDC-tracked attributes live in the separate
-- dim_client_risk_snapshot mini-dimension (SCD Type 4) below, not here.
CREATE TABLE warehouse.dim_client (
    client_key      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    client_id       text        NOT NULL UNIQUE,          -- natural key, e.g. 'CL001'
    full_name       text,
    date_of_birth   date,
    nationality     text,
    country         text,
    currency        text,
    preferred_language text,
    kyc_status      text,
    account_type    text,
    referral_source text,
    signup_platform text,
    promo_code      text,
    signup_date     date,
    manager_key     bigint      REFERENCES warehouse.dim_manager(manager_key),
    is_inferred     boolean     NOT NULL DEFAULT false,   -- true = late-arriving stub (see "late-arriving dimensions" in part2_data_model.md)
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- dim_client_risk_snapshot: SCD Type 4 mini-dimension, versioned SCD Type 2.
-- One row per (client, attribute-version). Source: client_profile_changes.jsonl.
CREATE TABLE warehouse.dim_client_risk_snapshot (
    risk_snapshot_key   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    client_id           text        NOT NULL,             -- durable reference to dim_client.client_id, not versioned
    risk_category       text        NOT NULL,
    account_balance_usd numeric(14,2) NOT NULL,
    account_status      text        NOT NULL,
    valid_from          timestamptz NOT NULL,
    valid_to            timestamptz NOT NULL DEFAULT '9999-12-31',
    is_current          boolean     NOT NULL DEFAULT true,
    is_deleted          boolean     NOT NULL DEFAULT false,  -- true = tombstone row (source CDC delete)
    source_lsn          bigint      NOT NULL,
    CONSTRAINT uq_client_lsn UNIQUE (client_id, source_lsn)   -- idempotency guard, see part1_pipeline.md §2
);
CREATE INDEX ix_risk_snapshot_current ON warehouse.dim_client_risk_snapshot (client_id) WHERE is_current;
CREATE INDEX ix_risk_snapshot_asof ON warehouse.dim_client_risk_snapshot (client_id, valid_from, valid_to);

-- cdc_watermark: per-entity high-watermark, the sole correctness guarantee for
-- CDC apply order (no batch-level ORDER BY — see part1_pipeline.md §1 for why).
CREATE TABLE warehouse.cdc_watermark (
    client_id           text PRIMARY KEY,
    last_applied_lsn    bigint NOT NULL DEFAULT 0
);
