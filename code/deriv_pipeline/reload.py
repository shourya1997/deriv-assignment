"""Step 7 — cdc_historical_reload's driver.

`warehouse.reset_client_for_reload()` (sql/04, locked) is the reset primitive:
a plain Airflow backfill re-run is not enough on its own because
`apply_cdc_event`'s watermark gate + `ON CONFLICT DO NOTHING` (sql/03) makes a
naive replay of an already-applied date range a guaranteed no-op. This module
re-issues sql/04's own driver query with bound `from_date`/`to_date` params
(never hardcoded to November, per PROMPTS.md's numeric-claims policy), then
for each affected client calls `reset_client_for_reload()` followed by a full
replay of that client's `raw.client_profile_changes` history from the reset
point onward, in **lsn order** — the reload-time replay rule from sql/04's own
driver comment, distinct from (not a contradiction of) the streaming
file-arrival-order rule `scd2_apply` uses (ADR-10). Replaying by lsn is what
lets a reload repair a version `scd2_apply`'s watermark-only, no-sort design
permanently dropped (the real CL001 case: file order is lsn 1005, then 1004 —
quarantined as stale — then 1006).

The advisory lock matches the one `scd2_apply` already takes
(`layer3_warehouse.scd2_apply`) so a reload can never interleave with a
concurrent streaming apply for the same client.

`reset_client_for_reload`'s own DELETE removes every `dim_client_risk_snapshot`
row for a client from the reset point onward — but `fact_deposits`/
`fact_trades` FK into that table with no `ON DELETE` clause (sql/02_facts.sql),
so any fact row whose snapshotted FK (part2_data_model.md's "fact join
strategy") already resolved into one of those rows would make the DELETE raise
`ForeignKeyViolation`. `_repoint_facts_for_reset`/`_reresolve_facts_after_reload`
below repoint any such fact row onto a safe placeholder before the reset and
back onto its correctly-resolved key once the client's history is rebuilt —
the same repoint-before-delete shape `layer3_warehouse._repoint_and_clear_
stopgap_snapshots` already uses for ADR-7's stopgap rows (Opus dual-review
finding, Step 7)."""
from __future__ import annotations

import sys
from datetime import date, datetime, timezone

from psycopg.types.json import Jsonb

from deriv_pipeline.config import fact_event_date_columns
from deriv_pipeline.layers.common import fk_columns_into, primary_key_columns, split_target


def _repoint_facts_for_reset(conn, client_id: str, reset_from_lsn: int, fk_columns, event_date_cols):
    """Finds every fact row FK'd into a `dim_client_risk_snapshot` row this
    reset is about to delete (`source_lsn >= reset_from_lsn`) and repoints it
    onto a safe key so the delete can't raise `ForeignKeyViolation`. Returns
    `(rows_to_reresolve, created_temp_key)`: the former is replayed against
    the rebuilt history by `_reresolve_facts_after_reload` once replay
    finishes; the latter, if not None, is a throwaway placeholder row (same
    negative-source_lsn convention as the Step 3/ADR-7 stopgap) that must be
    deleted once nothing references it any more."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = %s AND source_lsn >= %s",
            (client_id, reset_from_lsn),
        )
        doomed_keys = [r[0] for r in cur.fetchall()]
    if not doomed_keys:
        return [], None

    to_repoint: list[tuple[str, str, str, object, date]] = []
    with conn.cursor() as cur:
        for fact_table, fk_column in fk_columns:
            event_date_col = event_date_cols.get(fact_table)
            if event_date_col is None:
                continue  # an FK into this table from something other than a fact_upsert target
            schema, table = split_target(fact_table)
            pk_col = primary_key_columns(conn, schema, table)[0]
            cur.execute(
                f"SELECT {pk_col}, {event_date_col} FROM {fact_table} WHERE {fk_column} = ANY(%s)",
                (doomed_keys,),
            )
            for pk_value, event_date_val in cur.fetchall():
                to_repoint.append((fact_table, fk_column, pk_col, pk_value, event_date_val))

    if not to_repoint:
        return [], None

    with conn.cursor() as cur:
        # Reuse whichever version reset_client_for_reload will itself
        # reactivate (source_lsn immediately below reset_from_lsn) if one
        # exists for this client.
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.dim_client_risk_snapshot"
            " WHERE client_id = %s AND source_lsn < %s ORDER BY source_lsn DESC LIMIT 1",
            (client_id, reset_from_lsn),
        )
        row = cur.fetchone()

    created_temp_key = None
    if row is not None:
        safe_key = row[0]
    else:
        # This client has zero surviving history below reset_from_lsn (its
        # entire lifetime falls inside the reload window) — there is no
        # existing row left to repoint onto. Insert a throwaway placeholder,
        # deleted below once every repointed row has its real key back.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT risk_category, account_balance_usd, account_status"
                " FROM warehouse.dim_client_risk_snapshot WHERE risk_snapshot_key = %s",
                (doomed_keys[0],),
            )
            risk_category, account_balance_usd, account_status = cur.fetchone()
            cur.execute(
                "INSERT INTO warehouse.dim_client_risk_snapshot"
                " (client_id, risk_category, account_balance_usd, account_status,"
                "  valid_from, valid_to, is_current, source_lsn)"
                " VALUES (%s, %s, %s, %s, '1970-01-01', '9999-12-31', false, %s)"
                " RETURNING risk_snapshot_key",
                (client_id, risk_category, account_balance_usd, account_status, reset_from_lsn - 1),
            )
            safe_key = cur.fetchone()[0]
            created_temp_key = safe_key

    with conn.cursor() as cur:
        for fact_table, fk_column in {(t, c) for t, c, *_ in to_repoint}:
            cur.execute(
                f"UPDATE {fact_table} SET {fk_column} = %s WHERE {fk_column} = ANY(%s)",
                (safe_key, doomed_keys),
            )

    return to_repoint, created_temp_key


def _reresolve_facts_after_reload(conn, client_id: str, to_repoint, created_temp_key) -> None:
    with conn.cursor() as cur:
        for fact_table, fk_column, pk_col, pk_value, event_date_val in to_repoint:
            event_ts = datetime.combine(event_date_val, datetime.min.time(), tzinfo=timezone.utc)
            cur.execute("SELECT warehouse.resolve_risk_snapshot_key(%s, %s)", (client_id, event_ts))
            new_key = cur.fetchone()[0]
            if new_key is None:
                raise ValueError(
                    f"historical_reload: no risk snapshot resolves for {fact_table}"
                    f" {pk_col}={pk_value!r} at {event_ts!r} after replay — history rebuild left a gap"
                )
            cur.execute(f"UPDATE {fact_table} SET {fk_column} = %s WHERE {pk_col} = %s", (new_key, pk_value))
        if created_temp_key is not None:
            cur.execute(
                "DELETE FROM warehouse.dim_client_risk_snapshot WHERE risk_snapshot_key = %s",
                (created_temp_key,),
            )


def historical_reload(conn, from_date: str | date, to_date: str | date) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT client_id, MIN(lsn) AS reset_from_lsn"
            " FROM raw.client_profile_changes"
            " WHERE commit_ts >= %s AND commit_ts < %s"
            " GROUP BY client_id"
            " ORDER BY client_id",
            (from_date, to_date),
        )
        affected = cur.fetchall()

    fk_columns = fk_columns_into(conn, "warehouse", "dim_client_risk_snapshot")
    event_date_cols = fact_event_date_columns()

    events_replayed = 0
    for client_id, reset_from_lsn in affected:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"cdc_apply:{client_id}",))

            to_repoint, created_temp_key = _repoint_facts_for_reset(
                conn, client_id, reset_from_lsn, fk_columns, event_date_cols
            )

            cur.execute(
                "SELECT warehouse.reset_client_for_reload(%s, %s)", (client_id, reset_from_lsn)
            )
            cur.execute(
                "SELECT lsn, commit_ts, op, after FROM raw.client_profile_changes"
                " WHERE client_id = %s AND lsn >= %s ORDER BY lsn",
                (client_id, reset_from_lsn),
            )
            events = cur.fetchall()
            for lsn, commit_ts, op, after in events:
                if op == "delete":
                    # Mirrors scd2_apply's own guard (layer3_warehouse.py, Step
                    # 6 dual review finding): reset_client_for_reload only
                    # reactivates a prior row "immediately before p_from_lsn"
                    # if one exists (sql/04) — for a client whose entire
                    # lifetime, including its delete, falls inside the reload
                    # window, nothing gets reactivated, so apply_cdc_event's
                    # tombstone step would find no source row to copy from and
                    # silently insert nothing while still advancing the
                    # watermark (Sonnet dual-review finding, Step 7).
                    cur.execute(
                        "SELECT 1 FROM warehouse.dim_client_risk_snapshot"
                        " WHERE client_id = %s LIMIT 1",
                        (client_id,),
                    )
                    if cur.fetchone() is None:
                        cur.execute(
                            "INSERT INTO quarantine.rejected_rows"
                            " (table_name, reason_code, severity, raw_payload)"
                            " VALUES (%s, %s, %s, %s)",
                            (
                                "dim_client_risk_snapshot",
                                "delete_with_no_baseline",
                                "WARNING",
                                Jsonb({"client_id": client_id, "lsn": lsn, "op": op}),
                            ),
                        )
                        events_replayed += 1
                        continue
                cur.execute(
                    "SELECT warehouse.apply_cdc_event(%s, %s, %s, %s, %s)",
                    (client_id, lsn, commit_ts, op, Jsonb(after) if after is not None else None),
                )
                events_replayed += 1

            if to_repoint:
                _reresolve_facts_after_reload(conn, client_id, to_repoint, created_temp_key)

    return {
        "clients_reset": [client_id for client_id, _ in affected],
        "events_replayed": events_replayed,
    }


def dump_state(conn) -> str:
    """A deterministic, human-diffable text snapshot of everything a reload
    can touch: every `dim_client_risk_snapshot` row (all versions, not just
    current, since a repaired-but-superseded version like CL001's lsn 1004 is
    exactly what a reload is supposed to add) plus every fact row's resolved
    FK. Used by `scripts/verify.sh` to assert idempotency at the SQL level
    (Opus dual-review finding F4, Step 7) instead of trusting a bare `airflow
    dags test` exit code, which says nothing about whether the two runs
    actually landed on the same warehouse state."""
    lines = []
    with conn.cursor() as cur:
        cur.execute(
            "SELECT client_id, source_lsn, is_current, is_deleted,"
            " risk_category, account_balance_usd, account_status, valid_from, valid_to"
            " FROM warehouse.dim_client_risk_snapshot"
            " WHERE source_lsn >= 0"
            " ORDER BY client_id, source_lsn"
        )
        for row in cur.fetchall():
            lines.append("snapshot|" + "|".join(str(v) for v in row))

        for fact_table in sorted(fact_event_date_columns()):
            schema, table = split_target(fact_table)
            pk_col = primary_key_columns(conn, schema, table)[0]
            cur.execute(
                f"SELECT {pk_col}, risk_snapshot_key FROM {fact_table} ORDER BY {pk_col}"
            )
            for pk_value, fk_value in cur.fetchall():
                lines.append(f"fact|{fact_table}|{pk_value}|{fk_value}")
    return "\n".join(lines)


if __name__ == "__main__":
    if "--dump-state" not in sys.argv:
        print("usage: python -m deriv_pipeline.reload --dump-state", file=sys.stderr)
        sys.exit(2)
    from deriv_pipeline.db import get_connection

    _conn = get_connection()
    try:
        print(dump_state(_conn))
    finally:
        _conn.close()
