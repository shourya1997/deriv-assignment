-- ============================================================================
-- Part 2b — CDC apply logic for dim_client_risk_snapshot (SCD Type 2/4)
-- Called once per CDC event, by Airflow, after the staging layer has already
-- typed the row (see part1_pipeline.md §1 for the raw -> staging boundary).
-- ============================================================================

CREATE OR REPLACE FUNCTION warehouse.apply_cdc_event(
    p_client_id  text,
    p_lsn        bigint,
    p_commit_ts  timestamptz,
    p_op         text,          -- 'insert' | 'update' | 'delete'
    p_after      jsonb          -- NULL on delete
) RETURNS void AS $$
DECLARE
    v_last_lsn bigint;
BEGIN
    -- 1. Watermark check: the sole ordering guarantee. Reject (quarantine)
    --    anything not strictly newer than what's already been applied for
    --    this client, regardless of arrival order or batch boundary.
    SELECT last_applied_lsn INTO v_last_lsn
    FROM warehouse.cdc_watermark
    WHERE client_id = p_client_id
    FOR UPDATE;

    IF v_last_lsn IS NULL THEN
        INSERT INTO warehouse.cdc_watermark (client_id, last_applied_lsn) VALUES (p_client_id, 0);
        v_last_lsn := 0;
    END IF;

    IF p_lsn <= v_last_lsn THEN
        INSERT INTO quarantine.rejected_rows (table_name, reason_code, severity, raw_payload)
        VALUES ('dim_client_risk_snapshot', 'stale_lsn', 'INFO',
                jsonb_build_object('client_id', p_client_id, 'lsn', p_lsn, 'op', p_op));
        RETURN;
    END IF;

    -- 2. End-date whatever version is currently active for this client.
    UPDATE warehouse.dim_client_risk_snapshot
    SET valid_to = p_commit_ts, is_current = false
    WHERE client_id = p_client_id AND is_current = true;

    -- 3a. insert/update: append the new version.
    IF p_op IN ('insert', 'update') THEN
        INSERT INTO warehouse.dim_client_risk_snapshot
            (client_id, risk_category, account_balance_usd, account_status,
             valid_from, valid_to, is_current, is_deleted, source_lsn)
        VALUES
            (p_client_id, p_after->>'risk_category',
             (p_after->>'account_balance_usd')::numeric, p_after->>'account_status',
             p_commit_ts, '9999-12-31', true, false, p_lsn)
        ON CONFLICT (client_id, source_lsn) DO NOTHING;

    -- 3b. delete: append a terminal tombstone row instead of removing anything.
    ELSIF p_op = 'delete' THEN
        INSERT INTO warehouse.dim_client_risk_snapshot
            (client_id, risk_category, account_balance_usd, account_status,
             valid_from, valid_to, is_current, is_deleted, source_lsn)
        SELECT client_id, risk_category, account_balance_usd, 'deleted',
               p_commit_ts, '9999-12-31', true, true, p_lsn
        FROM warehouse.dim_client_risk_snapshot
        WHERE client_id = p_client_id
        ORDER BY source_lsn DESC
        LIMIT 1
        ON CONFLICT (client_id, source_lsn) DO NOTHING;
    END IF;

    -- 4. Advance the watermark last, so a failure partway through this
    --    function never advances the watermark past an event that wasn't
    --    actually applied.
    UPDATE warehouse.cdc_watermark SET last_applied_lsn = p_lsn WHERE client_id = p_client_id;
END;
$$ LANGUAGE plpgsql;


-- ----------------------------------------------------------------------------
-- Snapshotted FK resolution: called at fact-load time (fact_deposits /
-- fact_trades) to find which risk_snapshot_key was in effect at the moment
-- of the transaction. This is what makes the fact-to-dimension join a plain
-- equi-join at query time — no BETWEEN clause, no risk of a query forgetting
-- the point-in-time condition (see part2_data_model.md, "fact join strategy").
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION warehouse.resolve_risk_snapshot_key(
    p_client_id text,
    p_event_ts  timestamptz
) RETURNS bigint AS $$
    SELECT risk_snapshot_key
    FROM warehouse.dim_client_risk_snapshot
    WHERE client_id = p_client_id
      AND valid_from <= p_event_ts
      AND valid_to   >  p_event_ts
    LIMIT 1;
$$ LANGUAGE sql STABLE;
