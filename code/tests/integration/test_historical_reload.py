"""Step 7: cdc_historical_reload's driver (deriv_pipeline.reload) against the
real shipped data/client_profile_changes.jsonl — proving sql/04's reset +
lsn-order replay actually repairs the version scd2_apply's watermark-only,
no-sort design permanently drops (CL001's real lsn 1004), while leaving an
already-correct delete (CL012) exactly as deleted. Also covers the Opus
dual-review F1 finding: a fact row FK'd into a snapshot version the reset is
about to delete must survive (get repointed, then re-resolved), never raise
ForeignKeyViolation — including the deep case where a client's *entire*
snapshot history falls inside the reload window."""
from __future__ import annotations

from datetime import date, datetime, timezone

from psycopg.types.json import Jsonb

from deriv_pipeline.config import CONFIG_DIR, TableConfig
from deriv_pipeline.dims.dim_date import ensure_date
from deriv_pipeline.layers import layer1_raw, layer2_staging, layer3_warehouse
from deriv_pipeline.reload import dump_state, historical_reload

_TABLES_DIR = CONFIG_DIR / "tables"


def _client_profile_cfg() -> TableConfig:
    return TableConfig.load(_TABLES_DIR / "client_profile.yml")


def _cdc_cfg() -> TableConfig:
    return TableConfig.load(_TABLES_DIR / "client_profile_changes.yml")


def _seed_and_apply(db_conn) -> None:
    profile_cfg = _client_profile_cfg()
    layer1_raw.load(profile_cfg, db_conn)
    layer2_staging.stage(profile_cfg, db_conn)
    layer3_warehouse.load(profile_cfg, db_conn)

    cdc_cfg = _cdc_cfg()
    layer1_raw.load(cdc_cfg, db_conn)
    layer2_staging.stage(cdc_cfg, db_conn)
    layer3_warehouse.load(cdc_cfg, db_conn)


def _commit_ts_bounds(db_conn, client_id: str) -> tuple[str, str]:
    """Recomputed from the real raw table at test time (PROMPTS.md's
    numeric-claims policy), not a hardcoded 'November 2024' literal."""
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT date_trunc('day', min(commit_ts)),"
            " date_trunc('day', max(commit_ts)) + interval '1 day'"
            " FROM raw.client_profile_changes WHERE client_id = %s",
            (client_id,),
        )
        return cur.fetchone()


def test_historical_reload_repairs_the_dropped_stale_lsn_version(db_conn):
    """Streaming scd2_apply quarantines CL001's real lsn 1004 as stale
    (verified in test_cdc_apply_pipeline.py) because 1005 arrives first in
    file order and the watermark is the sole staleness guard — that version
    is otherwise gone from the live stream forever. A historical reload over
    CL001's own commit_ts window resets and replays in lsn order, so 1004 is
    no longer stale and must land as a real (non-current) snapshot row."""
    _seed_and_apply(db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND source_lsn = 1004"
        )
        assert cur.fetchone()[0] == 0  # confirmed dropped by the streaming apply

    from_date, to_date = _commit_ts_bounds(db_conn, "CL001")
    result = historical_reload(db_conn, from_date, to_date)
    assert "CL001" in result["clients_reset"]

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT is_current FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND source_lsn = 1004"
        )
        row = cur.fetchone()
    assert row is not None  # the previously-dropped version is now present
    assert row[0] is False  # superseded by 1005 then 1006, so not current

    # the final current state is unchanged by the reload — same real newest
    # event (lsn 1006) wins either way, just with the gap now repaired behind it.
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT risk_category, account_balance_usd, account_status, source_lsn"
            " FROM warehouse.dim_client_risk_snapshot WHERE client_id = 'CL001' AND is_current = true"
        )
        risk_category, balance, status, source_lsn = cur.fetchone()
    assert (risk_category, str(balance), status, source_lsn) == ("high", "1850.00", "under_review", 1006)


def test_historical_reload_keeps_an_already_deleted_client_deleted(db_conn):
    """CL012's real event is a delete (lsn 1010) — a reload over its window
    must reset and replay that same delete, leaving it tombstoned exactly as
    before, never accidentally resurrected (plan's own stated invariant). The
    key itself must actually change (delete-then-reinsert happened, not a
    no-op that merely left the old row alone) — Opus flagged that the
    original version of this test could not distinguish those two cases."""
    _seed_and_apply(db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL012' AND is_current = true"
        )
        key_before = cur.fetchone()[0]

    from_date, to_date = _commit_ts_bounds(db_conn, "CL012")
    result = historical_reload(db_conn, from_date, to_date)
    assert "CL012" in result["clients_reset"]

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT risk_snapshot_key, is_current, is_deleted, source_lsn"
            " FROM warehouse.dim_client_risk_snapshot WHERE client_id = 'CL012' AND is_current = true"
        )
        key_after, is_current, is_deleted, source_lsn = cur.fetchone()
    assert (is_current, is_deleted, source_lsn) == (True, True, 1010)
    assert key_after != key_before  # proves reset really deleted+reinserted, not a no-op


def test_historical_reload_only_touches_clients_with_events_in_the_window(db_conn):
    """A window with zero commit_ts overlap for a given client must not
    appear in clients_reset at all — reset_client_for_reload is scoped
    per-client and must never run against a client outside the driver query's
    own result (sql/04's own stated scope). The whole warehouse snapshot
    state (not just the return value) must be byte-identical, since Opus
    noted the original assertion couldn't catch a reload that silently
    touched an out-of-window client's rows without reporting it."""
    _seed_and_apply(db_conn)
    state_before = dump_state(db_conn)

    result = historical_reload(db_conn, "2099-01-01", "2099-02-01")
    assert result == {"clients_reset": [], "events_replayed": 0}
    assert dump_state(db_conn) == state_before


def test_historical_reload_is_idempotent(db_conn):
    """Running the same reload twice must land on the same final state —
    the reset+replay is a full reconstruction from raw each time, not an
    incremental step, so a rerun changes nothing observable."""
    _seed_and_apply(db_conn)
    from_date, to_date = _commit_ts_bounds(db_conn, "CL001")

    historical_reload(db_conn, from_date, to_date)
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot WHERE client_id = 'CL001'"
        )
        first_count = cur.fetchone()[0]
        cur.execute(
            "SELECT source_lsn FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND is_current = true"
        )
        first_current_lsn = cur.fetchone()[0]

    historical_reload(db_conn, from_date, to_date)
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot WHERE client_id = 'CL001'"
        )
        second_count = cur.fetchone()[0]
        cur.execute(
            "SELECT source_lsn FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND is_current = true"
        )
        second_current_lsn = cur.fetchone()[0]

    assert second_count == first_count
    assert second_current_lsn == first_current_lsn


def _insert_fact_deposit(db_conn, deposit_id: str, client_id: str, risk_snapshot_key: int, deposit_date: date) -> None:
    with db_conn.cursor() as cur:
        cur.execute("SELECT client_key FROM warehouse.dim_client WHERE client_id = %s", (client_id,))
        client_key = cur.fetchone()[0]
    date_key = ensure_date(db_conn, deposit_date)
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO warehouse.fact_deposits"
            " (deposit_id, client_key, risk_snapshot_key, date_key, deposit_date,"
            "  amount_usd, fee_usd, source_system)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (deposit_id, client_key, risk_snapshot_key, date_key, deposit_date, 100.00, 0, "internal"),
        )


def test_historical_reload_repoints_fact_row_referencing_a_doomed_snapshot_key(db_conn):
    """Opus F1: fact_deposits.risk_snapshot_key FKs into dim_client_risk_
    snapshot with no ON DELETE clause (sql/02_facts.sql) — reset_client_for_
    reload's unconditional DELETE would raise ForeignKeyViolation if any fact
    row still points at a version the reset is about to remove. CL001's real
    current version (source_lsn=1006) is exactly such a version once its
    window is reloaded. A fact dated after all of CL001's CDC activity
    resolves to that version, so wiring a synthetic fact row to it and then
    reloading must not crash, and the fact row must come out repointed at
    whatever key the *rebuilt* current version now has — not left dangling on
    the deleted key, and not silently left on a stale one."""
    _seed_and_apply(db_conn)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND is_current = true"
        )
        doomed_key = cur.fetchone()[0]

    deposit_date = date(2024, 12, 1)  # after all of CL001's real CDC activity
    _insert_fact_deposit(db_conn, "TEST-DEP-1", "CL001", doomed_key, deposit_date)

    from_date, to_date = _commit_ts_bounds(db_conn, "CL001")
    result = historical_reload(db_conn, from_date, to_date)  # must not raise ForeignKeyViolation
    assert "CL001" in result["clients_reset"]

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.fact_deposits WHERE deposit_id = 'TEST-DEP-1'"
        )
        fact_key_after = cur.fetchone()[0]
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = 'CL001' AND is_current = true"
        )
        current_key_after = cur.fetchone()[0]
        event_ts = datetime.combine(deposit_date, datetime.min.time(), tzinfo=timezone.utc)
        cur.execute("SELECT warehouse.resolve_risk_snapshot_key(%s, %s)", ("CL001", event_ts))
        resolved_key = cur.fetchone()[0]

    assert fact_key_after != doomed_key  # not left dangling on the deleted key
    assert fact_key_after == current_key_after == resolved_key  # correctly re-resolved, not stale

    # the reset's own IDENTITY column guarantees a brand-new key even though
    # the rebuilt row's values are identical — proves this is a real
    # delete+reinsert repoint, not a lucky no-op.
    assert current_key_after != doomed_key


def test_historical_reload_handles_full_history_reset_with_a_fact_reference(db_conn):
    """Deepest Opus-flagged edge case (F1 + F2 combined): a client whose
    *entire* dim_client_risk_snapshot history — including its baseline row —
    falls inside the reload window has nothing left for reset_client_for_
    reload to reactivate. If a fact row still references that sole version,
    there is no existing safe repoint target at all, forcing the temporary
    sentinel-row path (the negative-source_lsn/is_current=false convention
    from ADR-7) rather than the simpler "reactivated row" path exercised by
    the test above."""
    _seed_and_apply(db_conn)

    client_id = "CL997"
    with db_conn.cursor() as cur:
        # dim_client row so the synthetic fact row below has a valid client_key
        # to FK into (is_inferred=true: a late-arriving stub, not a real signup
        # — this client only ever existed via CDC, no client_profile baseline).
        cur.execute(
            "INSERT INTO warehouse.dim_client (client_id, is_inferred) VALUES (%s, true)",
            (client_id,),
        )
        # A synthetic client that only ever existed via CDC — no client_profile
        # baseline row at all — so its one-and-only snapshot version (built by
        # this single insert+replay) is the sole thing left once the window is
        # reloaded, reproducing "no row survives below reset_from_lsn".
        after = {"risk_category": "medium", "account_balance_usd": 500.00, "account_status": "active"}
        cur.execute(
            "INSERT INTO raw.client_profile_changes"
            " (client_id, lsn, commit_ts, op, before, after, source_file, ingested_at)"
            " VALUES (%s, 9001, '2024-11-10T09:00:00Z', 'insert', NULL, %s, 'test', now())",
            (client_id, Jsonb(after)),
        )
        cur.execute(
            "SELECT warehouse.apply_cdc_event(%s, %s, %s, %s, %s)",
            (client_id, 9001, "2024-11-10T09:00:00Z", "insert", Jsonb(after)),
        )
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = %s AND is_current = true",
            (client_id,),
        )
        sole_key = cur.fetchone()[0]

    deposit_date = date(2024, 12, 1)  # after CL997's only event, so it resolves to sole_key
    _insert_fact_deposit(db_conn, "TEST-DEP-2", client_id, sole_key, deposit_date)

    result = historical_reload(db_conn, "2024-11-10", "2024-11-11")  # must not raise ForeignKeyViolation
    assert client_id in result["clients_reset"]

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.fact_deposits WHERE deposit_id = 'TEST-DEP-2'"
        )
        fact_key_after = cur.fetchone()[0]
        cur.execute(
            "SELECT risk_snapshot_key, is_current, is_deleted FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = %s AND is_current = true",
            (client_id,),
        )
        current_key_after, is_current, is_deleted = cur.fetchone()
        # the throwaway sentinel row (negative source_lsn) must have been
        # cleaned up once the fact row no longer needs it
        cur.execute(
            "SELECT count(*) FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = %s AND source_lsn < 0",
            (client_id,),
        )
        leftover_sentinels = cur.fetchone()[0]

    assert (is_current, is_deleted) == (True, False)
    assert fact_key_after == current_key_after  # repointed onto the rebuilt real row, not the sentinel
    assert fact_key_after != sole_key  # a genuinely new key (delete+reinsert), not the original
    assert leftover_sentinels == 0
