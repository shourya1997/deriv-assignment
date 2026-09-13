-- G1 fix — landing tables for layer1. Generic shape (natural key + jsonb payload
-- + provenance) for every plain-source table: layer1 lands rows as-is, typing and
-- dedup happen at layer2. `client_profile_changes` is the one exception (typed
-- columns, not jsonb) because sql/03/04's plpgsql functions and sql/04's driver
-- query read it directly by column name.

CREATE TABLE raw.client_signup (
    client_id   text NOT NULL,
    payload     jsonb NOT NULL,
    source_file text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (client_id)
);

CREATE TABLE raw.client_profile (
    client_id   text NOT NULL,
    payload     jsonb NOT NULL,
    source_file text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (client_id)
);

CREATE TABLE raw.client_deposit (
    deposit_id  text NOT NULL,
    payload     jsonb NOT NULL,
    source_file text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (deposit_id)
);

CREATE TABLE raw.client_trades (
    trade_id    text NOT NULL,
    payload     jsonb NOT NULL,
    source_file text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_id)
);

-- Vendor deposits arrive across multiple daily files and legitimately repeat a
-- deposit_id across files (the duplicate-row edge case) — PK is scoped to
-- (deposit_id, source_file) so raw stays rerun-safe per file without collapsing
-- real cross-file duplicates before layer2 gets a chance to dedup them.
CREATE TABLE raw.vendor_deposits (
    deposit_id  text NOT NULL,
    source_file text NOT NULL,
    payload     jsonb NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (deposit_id, source_file)
);

-- CDC log — typed, not jsonb, because warehouse.apply_cdc_event() (sql/03) and
-- reset_client_for_reload()'s driver query (sql/04) both read named columns from
-- raw.client_profile_changes directly.
CREATE TABLE raw.client_profile_changes (
    client_id   text NOT NULL,
    lsn         bigint NOT NULL,
    commit_ts   timestamptz NOT NULL,
    op          text NOT NULL CHECK (op IN ('insert', 'update', 'delete')),
    before      jsonb,
    after       jsonb,
    source_file text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (client_id, lsn)
);
