-- ============================================================================
-- Part 2b — Historical reload / backfill for dim_client_risk_snapshot
--
-- Plain Airflow backfill (re-triggering DAG runs for a date range) is NOT
-- sufficient on its own: the CDC apply function is watermark-gated and uses
-- ON CONFLICT ... DO NOTHING, both deliberately chosen for idempotency
-- (part1_pipeline.md §2-3). That means a naive re-run of "November" is a
-- guaranteed no-op — every lsn in that range is already <= the current
-- watermark, so every event gets rejected as stale before it can correct
-- anything. This file is the explicit reset step that makes reprocessing
-- actually possible, scoped per-client so it never touches other clients'
-- already-correct history.
-- ============================================================================

CREATE OR REPLACE FUNCTION warehouse.reset_client_for_reload(
    p_client_id text,
    p_from_lsn  bigint   -- reset and replay everything from this lsn onward (inclusive)
) RETURNS void AS $$
DECLARE
    v_restore_lsn bigint;
BEGIN
    -- Remove every version row this client accumulated from p_from_lsn onward
    -- (updates AND the delete tombstone, if any fall in this range — replaying
    -- the full event sequence, not just updates, is what prevents a reload
    -- from silently "undeleting" a client like CL012).
    DELETE FROM warehouse.dim_client_risk_snapshot
    WHERE client_id = p_client_id AND source_lsn >= p_from_lsn;

    -- Reactivate whichever version was current immediately before p_from_lsn,
    -- preserving its own is_deleted flag as-is (don't force it back to false —
    -- if the client was already deleted before this window, it should stay so).
    SELECT MAX(source_lsn) INTO v_restore_lsn
    FROM warehouse.dim_client_risk_snapshot
    WHERE client_id = p_client_id AND source_lsn < p_from_lsn;

    IF v_restore_lsn IS NOT NULL THEN
        UPDATE warehouse.dim_client_risk_snapshot
        SET is_current = true, valid_to = '9999-12-31'
        WHERE client_id = p_client_id AND source_lsn = v_restore_lsn;
    END IF;

    -- Roll the watermark back so replayed events are accepted, not rejected
    -- as stale. Scoped to this one client only — a blanket reset-everyone
    -- would also discard already-correct history for every other client
    -- touched anywhere in the affected date range.
    UPDATE warehouse.cdc_watermark
    SET last_applied_lsn = p_from_lsn - 1
    WHERE client_id = p_client_id;
END;
$$ LANGUAGE plpgsql;


-- ----------------------------------------------------------------------------
-- Driver: identify which clients need a reset for "reload November 2024",
-- and the lsn each one should be rolled back to. Airflow's backfill task
-- runs this first, calls reset_client_for_reload() for each row returned,
-- then replays raw.client_profile_changes for those clients in lsn order
-- through warehouse.apply_cdc_event() (03_cdc_apply.sql).
-- ----------------------------------------------------------------------------
SELECT client_id, MIN(lsn) AS reset_from_lsn
FROM raw.client_profile_changes
WHERE commit_ts >= '2024-11-01' AND commit_ts < '2024-12-01'
GROUP BY client_id;
