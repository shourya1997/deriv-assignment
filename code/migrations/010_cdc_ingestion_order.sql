-- Step 6 — scd2_apply must replay client_profile_changes in true file-arrival
-- order: apply_cdc_event()'s watermark check (sql/03) is the SOLE staleness
-- guard by design (no batch sort), so if the replaying SELECT can silently
-- reorder rows, a real out-of-order event (verified: CL001 is lsn 1005, then
-- 1004, then 1006 in the actual file) would apply in the wrong order and
-- either drop the wrong version or never quarantine the stale one at all.
-- A plain `SELECT ... FROM raw/staging_table` gives no such guarantee without
-- an ORDER BY key — and `ingested_at`/`staged_at` (timestamptz) can tie under
-- fast sequential inserts within a single layer run. A bigserial column gives
-- a real, gap-tolerant total order that exactly matches insertion order.
ALTER TABLE raw.client_profile_changes ADD COLUMN raw_seq bigserial;
ALTER TABLE staging.client_profile_changes ADD COLUMN staging_seq bigserial;
