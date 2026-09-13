"""layer3: dispatches on each layer3[i].strategy. Only `fact_upsert` exists
for the Step 3 walking skeleton; `dimension_upsert`/`scd2_apply`/
`scd2_baseline_seed` land in later steps (see TASK.md)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from deriv_pipeline.config import TableConfig
from deriv_pipeline.dims.dim_date import ensure_date
from deriv_pipeline.layers.common import _data_dir, primary_key_columns, split_target
from deriv_pipeline.transforms import earliest_op_per_client


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


def _resolve_dimension_fk(conn, dim_target: str, dim_natural_key: str, dim_surrogate_key: str, value):
    if value is None:
        # A NULL source value (e.g. no assigned_manager) means "no FK", not
        # "lookup failed" — dim_client's manager_key is a nullable FK
        # precisely for this case (Step 4 dual review finding).
        return None
    dim_schema, dim_table = split_target(dim_target)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {dim_surrogate_key} FROM {dim_schema}.{dim_table} WHERE {dim_natural_key} = %s",
            (value,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(
            f"dimension_upsert: no {dim_target} row with {dim_natural_key}={value!r} — "
            f"check bootstrap_warehouse task ordering (the referenced dimension must load first)"
        )
    return row[0]


def dimension_upsert(cfg: TableConfig, layer3_target, conn) -> int:
    """Generic owned-column upsert: `layer3_target.columns` names the target
    columns THIS source owns and never touches columns another source owns
    (e.g. client_signup and client_profile each own a disjoint subset of
    dim_client's columns). `fk_resolution[col]`, if present, resolves that
    column from another already-loaded dimension by natural key instead of
    copying the raw staged value straight across."""
    schema, table = split_target(cfg.layer2.target)
    target_schema, target_table = split_target(layer3_target.target)
    natural_key_col = cfg.source.natural_key[0]
    owned_cols = layer3_target.columns or []
    fk = layer3_target.fk_resolution

    source_cols = [natural_key_col] + [fk[c]["from_column"] if c in fk else c for c in owned_cols]
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(source_cols)} FROM {schema}.{table}")
        rows = cur.fetchall()
        col_names = [d.name for d in cur.description]

    insert_cols = [natural_key_col, *owned_cols]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in owned_cols)
    set_clause += ", is_inferred = false, updated_at = now()" if set_clause else "is_inferred = false, updated_at = now()"

    loaded = 0
    for raw_row in rows:
        r = dict(zip(col_names, raw_row))
        values = {natural_key_col: r[natural_key_col]}
        for col in owned_cols:
            if col in fk:
                rule = fk[col]
                values[col] = _resolve_dimension_fk(
                    conn, rule["dim_target"], rule["dim_natural_key"],
                    rule["dim_surrogate_key"], r[rule["from_column"]],
                )
            else:
                values[col] = r[col]
        placeholders = ", ".join(f"%({c})s" for c in insert_cols)
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {target_schema}.{target_table} ({', '.join(insert_cols)}, is_inferred) "
                f"VALUES ({placeholders}, false) "
                f"ON CONFLICT ({natural_key_col}) DO UPDATE SET {set_clause}",
                values,
            )
        loaded += 1
    return loaded


def _repoint_and_clear_stopgap_snapshots(conn, client_id: str, real_key: int) -> None:
    """Part of applying ADR-2's real baseline seed: Step 3's on-demand
    stopgap (ADR-7) may have already inserted negative-source_lsn rows for
    this client into a persistent, shared dev/test DB before this bootstrap
    ever ran. Any fact row that FK'd into one of those stopgap rows is
    repointed at the real baseline before the stopgap rows are deleted, so
    this never violates the FK constraint or leaves a dangling reference."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT risk_snapshot_key FROM warehouse.dim_client_risk_snapshot "
            "WHERE client_id = %s AND source_lsn < 0",
            (client_id,),
        )
        stopgap_keys = [r[0] for r in cur.fetchall()]
        if not stopgap_keys:
            return
        # Walk pg_constraint for real FKs into dim_client_risk_snapshot,
        # rather than information_schema.columns matched by column name
        # alone — the latter also matches views (which can't be UPDATEd)
        # and would miss a same-named column that isn't actually an FK.
        cur.execute(
            """
            SELECT conrelid::regclass::text, a.attname
            FROM pg_constraint c
            JOIN unnest(c.conkey) WITH ORDINALITY AS ck(attnum, ord) ON true
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ck.attnum
            WHERE c.contype = 'f'
              AND c.confrelid = 'warehouse.dim_client_risk_snapshot'::regclass
            """
        )
        fk_columns = cur.fetchall()
        for fact_table, fk_column in fk_columns:
            cur.execute(
                f"UPDATE {fact_table} SET {fk_column} = %s WHERE {fk_column} = ANY(%s)",
                (real_key, stopgap_keys),
            )
        # Delete exactly the rows just repointed (not "whatever matches the
        # predicate now") — a DELETE keyed off a fresh predicate re-query
        # could remove a row inserted by another writer after the SELECT
        # above without ever repointing it (Step 4 dual review finding).
        cur.execute(
            "DELETE FROM warehouse.dim_client_risk_snapshot WHERE risk_snapshot_key = ANY(%s)",
            (stopgap_keys,),
        )


def _seed_baseline_and_repoint(
    conn, target_schema: str, target_table: str, client_id: str,
    risk_category: str, account_balance_usd, account_status: str,
) -> bool:
    """Inserts one source_lsn=0 baseline row for client_id (no-op if one
    already exists), then repoints/clears any Step 3 stopgap rows for that
    client onto it. Returns whether a new baseline row was actually inserted
    (vs. already existing from a prior run) so the caller's count reflects
    real work done, not candidates considered."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT 1 FROM {target_schema}.{target_table} "
            f"WHERE client_id = %s AND source_lsn > 0 LIMIT 1",
            (client_id,),
        )
        if cur.fetchone() is not None:
            # Real CDC history already applied for this client (Step 6+) — a
            # wide-open 1970-9999 baseline would overlap it, reintroducing
            # the exact nondeterministic-resolve_risk_snapshot_key failure
            # mode ADR-2/ADR-7 exist to prevent. Skip rather than seed blindly.
            return False
        cur.execute(
            f"""
            INSERT INTO {target_schema}.{target_table}
                (client_id, risk_category, account_balance_usd, account_status,
                 valid_from, valid_to, is_current, source_lsn)
            VALUES (%(client_id)s, %(risk_category)s, %(account_balance_usd)s,
                    %(account_status)s, '1970-01-01', '9999-12-31', true, 0)
            ON CONFLICT ON CONSTRAINT uq_client_lsn DO NOTHING
            """,
            {
                "client_id": client_id, "risk_category": risk_category,
                "account_balance_usd": account_balance_usd, "account_status": account_status,
            },
        )
        newly_inserted = cur.rowcount > 0
        cur.execute(
            f"SELECT risk_snapshot_key FROM {target_schema}.{target_table} "
            f"WHERE client_id = %s AND source_lsn = 0",
            (client_id,),
        )
        real_key = cur.fetchone()[0]
    _repoint_and_clear_stopgap_snapshots(conn, client_id, real_key)
    return newly_inserted


def scd2_baseline_seed(cfg: TableConfig, layer3_target, conn) -> int:
    """ADR-2's real G2 fix: one baseline warehouse.dim_client_risk_snapshot
    row per client_profile-derived client, at source_lsn=0,
    valid_from='1970-01-01', is_current=true — via `ON CONFLICT ON
    CONSTRAINT uq_client_lsn DO NOTHING` so re-running is a no-op. Excludes
    any client whose earliest CDC event (read directly from
    `layer3_target.cdc_source_glob` — client_profile_changes doesn't have
    its own table config until Step 6) is an 'insert': that client had no
    pre-existing state for a baseline row to represent.

    Also seeds orphan clients (ADR-2: CL099/CL031-shaped clients referenced
    only by fact tables, absent from client_signup/client_profile entirely)
    with sentinel values, found generically as "still has a Step 3 stopgap
    row and never appeared in this run's client_profile rows" rather than by
    hardcoded client_id — they would otherwise be stuck on a permanently
    non-current, 1-microsecond-wide stopgap forever."""
    schema, table = split_target(cfg.layer2.target)
    target_schema, target_table = split_target(layer3_target.target)

    excluded_client_ids: set[str] = set()
    if layer3_target.cdc_source_glob:
        records = []
        for path in sorted(_data_dir().glob(layer3_target.cdc_source_glob)):
            for line in path.read_text().splitlines():
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        earliest = earliest_op_per_client(records)
        excluded_client_ids = {cid for cid, op in earliest.items() if op == "insert"}

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT client_id, risk_category, account_balance_usd, account_status "
            f"FROM {schema}.{table}"
        )
        rows = cur.fetchall()
        col_names = [d.name for d in cur.description]

    seeded = 0
    profile_client_ids: set[str] = set()
    for raw_row in rows:
        r = dict(zip(col_names, raw_row))
        client_id = r["client_id"]
        profile_client_ids.add(client_id)
        if client_id in excluded_client_ids:
            continue
        if _seed_baseline_and_repoint(
            conn, target_schema, target_table, client_id,
            r["risk_category"], r["account_balance_usd"], r["account_status"],
        ):
            seeded += 1

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT DISTINCT client_id FROM {target_schema}.{target_table} WHERE source_lsn < 0"
        )
        stopgap_client_ids = {r[0] for r in cur.fetchall()}
    orphan_client_ids = stopgap_client_ids - profile_client_ids - excluded_client_ids
    for client_id in orphan_client_ids:
        if _seed_baseline_and_repoint(
            conn, target_schema, target_table, client_id, "unknown", 0.00, "unknown",
        ):
            seeded += 1

    return seeded


_STRATEGY_DISPATCH = {
    "fact_upsert": fact_upsert,
    "dimension_upsert": dimension_upsert,
    "scd2_baseline_seed": scd2_baseline_seed,
}


def load(cfg: TableConfig, conn) -> int:
    total = 0
    for layer3_target in cfg.layer3:
        handler = _STRATEGY_DISPATCH.get(layer3_target.strategy)
        if handler is None:
            raise NotImplementedError(f"layer3 strategy {layer3_target.strategy!r} not yet implemented")
        total += handler(cfg, layer3_target, conn)
    return total
