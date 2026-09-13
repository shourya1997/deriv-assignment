-- G1 fix — typed staging tables (layer2). Layer2 applies expected_columns/aliases
-- (schema-drift detection), casts types, applies the optional late_arrival rule,
-- and dedups per each config's declared conflict_strategy. One table per source;
-- `client_profile_changes` stays typed+deduped here too even though its layer3 is
-- a named function call (scd2_apply), not a generic upsert.

CREATE TABLE staging.client_signup (
    client_id        text PRIMARY KEY,
    signup_date      date,
    country          text,
    email            text,
    referral_source  text,
    account_type     text,
    kyc_status       text,
    signup_platform  text,
    promo_code       text,
    assigned_manager text,
    staged_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE staging.client_profile (
    client_id           text PRIMARY KEY,
    full_name           text,
    date_of_birth       date,
    nationality         text,
    risk_category       text,
    account_balance_usd numeric(14,2),
    account_status      text,
    currency            text,
    last_login_date     date,
    preferred_language  text,
    staged_at           timestamptz NOT NULL DEFAULT now()
);

-- client_deposit.json has the same payment_method/method drift as the vendor
-- feed (e.g. DEP012), so it needs the same drift/late-arrival flags as
-- staging.vendor_deposits below, not just a subset (Step 1 dual review finding).
CREATE TABLE staging.client_deposit (
    deposit_id            text PRIMARY KEY,
    client_id             text,
    deposit_date          date,
    amount_usd            numeric(14,2),
    payment_method        text,
    currency_original     text,
    exchange_rate         numeric(12,6),
    status                text,
    processing_days       int,
    fee_usd               numeric(10,2),
    schema_drift_detected boolean NOT NULL DEFAULT false,
    is_late_arrival       boolean NOT NULL DEFAULT false,
    staged_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE staging.client_trades (
    trade_id     text PRIMARY KEY,
    client_id    text,
    trade_date   date,
    instrument   text,
    direction    text,
    volume_lots  numeric(10,2),
    open_price   numeric(14,5),
    close_price  numeric(14,5),
    pnl_usd      numeric(14,2),
    trade_status text,
    staged_at    timestamptz NOT NULL DEFAULT now()
);

-- Dedup target for the vendor deposit_id duplicates: `conflict_strategy:
-- upsert_do_update` in vendor_deposits.yml keys off this PK.
CREATE TABLE staging.vendor_deposits (
    deposit_id            text PRIMARY KEY,
    client_id             text,
    deposit_date          date,
    amount_usd            numeric(14,2),
    payment_method        text,
    currency_original     text,
    exchange_rate         numeric(12,6),
    status                text,
    processing_days       int,
    fee_usd               numeric(10,2),
    schema_drift_detected boolean NOT NULL DEFAULT false,
    is_late_arrival       boolean NOT NULL DEFAULT false,
    staged_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE staging.client_profile_changes (
    client_id text NOT NULL,
    lsn       bigint NOT NULL,
    commit_ts timestamptz NOT NULL,
    op        text NOT NULL,
    before    jsonb,
    after     jsonb,
    staged_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (client_id, lsn)
);
