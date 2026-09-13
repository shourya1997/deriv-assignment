-- G1 fix — reconciliation output table. One row per discrepancy found by a
-- reconciliation config's diff engine (config/reconciliations/*.yml), keyed by
-- the exact natural-key tuple the config declares, not a fixed schema per pair.
CREATE TABLE data_quality.reconciliation_discrepancies (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reconciliation_name text NOT NULL,
    run_id              text NOT NULL,
    natural_key         jsonb NOT NULL,
    discrepancy_type    text NOT NULL CHECK (discrepancy_type IN ('missing_left', 'missing_right')),
    detected_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_reconciliation_discrepancies_name ON data_quality.reconciliation_discrepancies (reconciliation_name, detected_at);
