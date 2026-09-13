-- G1 fix — sql/03_cdc_apply.sql writes quarantine.rejected_rows on every stale-lsn
-- event; the table never existed. Generic across every table's quarantine path
-- (config-declared DQ assertions route here too, not just CDC).
CREATE TABLE quarantine.rejected_rows (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name   text NOT NULL,
    reason_code  text NOT NULL,
    severity     text NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    raw_payload  jsonb NOT NULL,
    rejected_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_rejected_rows_table ON quarantine.rejected_rows (table_name, rejected_at);
