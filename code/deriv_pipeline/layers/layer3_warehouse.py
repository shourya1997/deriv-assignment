"""layer3: dispatches on each layer3[i].strategy. Only `fact_upsert` exists
for the Step 3 walking skeleton; `dimension_upsert`/`scd2_apply`/
`scd2_baseline_seed` land in later steps (see TASK.md)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from deriv_pipeline.config import TableConfig
from deriv_pipeline.dims.dim_date import ensure_date
from deriv_pipeline.layers.common import primary_key_columns, split_target


def _resolve_client_key(conn, client_id: str, on_miss: str | None) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT client_key FROM warehouse.dim_client WHERE client_id = %s", (client_id,))
        row = cur.fetchone()
        if row is not None:
            return row[0]
        if on_miss != "inferred_member_on_miss":
            raise ValueError(f"no dim_client row for client_id={client_id!r} and no fk_resolution rule")
        # Inferred-member pattern (part2_data_model.md "Late-arriving dimension
        # members"): stub in with only client_id known, is_inferred=true; a
        # later real client_signup/client_profile load backfills in place.
        cur.execute(
            """
            INSERT INTO warehouse.dim_client (client_id, is_inferred)
            VALUES (%s, true)
            ON CONFLICT (client_id) DO NOTHING
            """,
            (client_id,),
        )
        cur.execute("SELECT client_key FROM warehouse.dim_client WHERE client_id = %s", (client_id,))
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"dim_client row for client_id={client_id!r} still missing after insert")
        return row[0]


def _resolve_risk_snapshot_key(conn, client_id: str, event_ts: datetime, strategy: str | None) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT warehouse.resolve_risk_snapshot_key(%s, %s)", (client_id, event_ts)
        )
        key = cur.fetchone()[0]
        if key is not None:
            return key
        if strategy != "snapshotted_fk":
            raise ValueError(f"no risk snapshot for client_id={client_id!r} and no fk_resolution rule")
        # Walking-skeleton stand-in for the real, ordered G2 baseline seed
        # (ADR-2, implemented properly as a bootstrap_warehouse step in
        # Step 4). Only seed when this client has ZERO existing REAL
        # (source_lsn >= 0) snapshot rows — a miss for a client who already
        # has real (CDC-derived) history means event_ts predates their
        # earliest real valid_from, not "no baseline yet", and blindly
        # seeding a wide-open row would overlap that real history and make
        # resolve_risk_snapshot_key's un-ordered LIMIT 1 nondeterministic
        # (the failure mode ADR-2 names as why the real seed must be ordered
        # before any CDC apply — Step 3 dual review finding, independently
        # confirmed by both reviewers).
        #
        # A client can have multiple deposits at distinct event_ts within a
        # single Step 3 run (no real baseline exists yet for ANY date), so
        # this stopgap must support seeding more than one window per client
        # without them ever overlapping each other:
        #   - each window is the narrowest possible, [event_ts, event_ts +
        #     1us) — since event_ts is always a deposit_date at midnight,
        #     distinct dates never overlap, and the same date reuses the
        #     same window (idempotent, no ON CONFLICT race).
        #   - source_lsn is always negative here (never a real CDC lsn, and
        #     distinct per window via MIN(source_lsn) - 1), which is also
        #     how "real history exists" is distinguished above.
        # Step 4's real bootstrap can identify and replace every source_lsn
        # < 0 row for a client once its properly-ordered baseline lands.
        cur.execute(
            "SELECT 1 FROM warehouse.dim_client_risk_snapshot WHERE client_id = %s AND source_lsn >= 0 LIMIT 1",
            (client_id,),
        )
        if cur.fetchone() is not None:
            raise ValueError(
                f"client_id={client_id!r} already has real risk snapshot history, but none covers "
                f"event_ts={event_ts!r} — needs a real backfill/reconciliation decision, not a "
                f"blind sentinel seed"
            )
        cur.execute(
            "SELECT COALESCE(MIN(source_lsn), 0) - 1 FROM warehouse.dim_client_risk_snapshot WHERE client_id = %s",
            (client_id,),
        )
        stopgap_lsn = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO warehouse.dim_client_risk_snapshot
                (client_id, risk_category, account_balance_usd, account_status,
                 valid_from, valid_to, is_current, source_lsn)
            VALUES (%s, 'unknown', 0.00, 'unknown', %s, %s, false, %s)
            ON CONFLICT ON CONSTRAINT uq_client_lsn DO NOTHING
            """,
            (client_id, event_ts, event_ts + timedelta(microseconds=1), stopgap_lsn),
        )
        cur.execute(
            "SELECT warehouse.resolve_risk_snapshot_key(%s, %s)", (client_id, event_ts)
        )
        row = cur.fetchone()
        if row is None or row[0] is None:
            raise ValueError(f"risk snapshot still unresolved for client_id={client_id!r} at {event_ts!r}")
        return row[0]


def fact_upsert(cfg: TableConfig, layer3_target, conn) -> int:
    """NOTE: the SELECT/INSERT column lists below are hardcoded to
    vendor_deposits' staging shape / fact_deposits' fact shape —
    `layer3_target.columns` is defined in config.py but deliberately unused
    here. Before a second fact_upsert-strategy table (e.g. client_trades.yml,
    Step 5+) can reuse this function, it needs to read its column list from
    `layer3_target.columns` instead of the literals below (Step 3 dual
    review finding; acceptable as scoped for the Step 3 walking skeleton
    since only vendor_deposits exists)."""
    schema, table = split_target(cfg.layer2.target)
    fact_schema, fact_table = split_target(layer3_target.target)
    pk_cols = primary_key_columns(conn, fact_schema, fact_table)
    fk = layer3_target.fk_resolution

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT deposit_id, client_id, deposit_date, amount_usd, exchange_rate, "
            f"fee_usd, processing_days, payment_method, currency_original, status, "
            f"is_late_arrival FROM {schema}.{table}"
        )
        rows = cur.fetchall()
        columns = [d.name for d in cur.description]

    loaded = 0
    for row in rows:
        r = dict(zip(columns, row))
        event_ts = datetime.combine(r["deposit_date"], datetime.min.time(), tzinfo=timezone.utc)
        client_key = _resolve_client_key(conn, r["client_id"], fk.get("dim_client"))
        risk_snapshot_key = _resolve_risk_snapshot_key(
            conn, r["client_id"], event_ts, fk.get("risk_snapshot")
        )
        date_key = ensure_date(conn, r["deposit_date"])

        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {fact_schema}.{fact_table}
                    (deposit_id, client_key, risk_snapshot_key, date_key, deposit_date,
                     amount_usd, exchange_rate, fee_usd, processing_days, payment_method,
                     currency_original, status, source_system, is_late_arrival)
                VALUES (%(deposit_id)s, %(client_key)s, %(risk_snapshot_key)s, %(date_key)s,
                        %(deposit_date)s, %(amount_usd)s, %(exchange_rate)s, %(fee_usd)s,
                        %(processing_days)s, %(payment_method)s, %(currency_original)s,
                        %(status)s, 'vendor', %(is_late_arrival)s)
                ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE SET
                    client_key = EXCLUDED.client_key,
                    risk_snapshot_key = EXCLUDED.risk_snapshot_key,
                    date_key = EXCLUDED.date_key,
                    amount_usd = EXCLUDED.amount_usd,
                    exchange_rate = EXCLUDED.exchange_rate,
                    fee_usd = EXCLUDED.fee_usd,
                    processing_days = EXCLUDED.processing_days,
                    payment_method = EXCLUDED.payment_method,
                    currency_original = EXCLUDED.currency_original,
                    status = EXCLUDED.status,
                    is_late_arrival = EXCLUDED.is_late_arrival
                """,
                {
                    "deposit_id": r["deposit_id"],
                    "client_key": client_key,
                    "risk_snapshot_key": risk_snapshot_key,
                    "date_key": date_key,
                    "deposit_date": r["deposit_date"],
                    "amount_usd": r["amount_usd"],
                    "exchange_rate": r["exchange_rate"],
                    "fee_usd": r["fee_usd"],
                    "processing_days": r["processing_days"],
                    "payment_method": r["payment_method"],
                    "currency_original": r["currency_original"],
                    "status": r["status"],
                    "is_late_arrival": r["is_late_arrival"],
                },
            )
        loaded += 1
    return loaded


_STRATEGY_DISPATCH = {"fact_upsert": fact_upsert}


def load(cfg: TableConfig, conn) -> int:
    total = 0
    for layer3_target in cfg.layer3:
        handler = _STRATEGY_DISPATCH.get(layer3_target.strategy)
        if handler is None:
            raise NotImplementedError(f"layer3 strategy {layer3_target.strategy!r} not yet implemented")
        total += handler(cfg, layer3_target, conn)
    return total
