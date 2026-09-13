"""layer3: dispatches on each layer3[i].strategy. Only `fact_upsert` exists
for the Step 3 walking skeleton; `dimension_upsert`/`scd2_apply`/
`scd2_baseline_seed` land in later steps (see TASK.md)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from psycopg.types.json import Jsonb

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
        # Serialize the read-then-insert stopgap allocation below per
        # client_id across connections/transactions: once client_deposit and
        # client_trades (Step 5) both call this concurrently for the same
        # client at different event dates, two transactions could otherwise
        # both read the same MIN(source_lsn) via MVCC, compute the same
        # stopgap_lsn, and have the loser's window silently swallowed by the
        # ON CONFLICT DO NOTHING below while its own event_ts never gets a
        # window — surfacing as a spurious "still unresolved" raise with no
        # real data problem (Step 5 dual review finding, confirmed
        # independently by both reviewers). Transaction-scoped: released
        # automatically at commit/rollback, no explicit unlock needed.
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"risk_snapshot_stopgap:{client_id}",))
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
    """Generic fact load: `client_key`/`risk_snapshot_key`/`date_key` are
    always resolved (via `layer3_target.event_date_column`, the staging
    column holding this fact's event date); `layer3_target.columns` are
    copied straight across from staging by identical name; any entry in
    `fk_resolution` whose value is a dict (not one of the two special
    strings "inferred_member_on_miss"/"snapshotted_fk") resolves that target
    column via another dimension's natural key, same shape as
    `dimension_upsert`'s `fk_resolution` (e.g. fact_trades' `instrument_key`);
    `layer3_target.literals` writes a fixed value per row (e.g.
    `source_system`) instead of copying one from staging. Generalized off
    vendor_deposits' original hardcoded shape once client_deposit/
    client_trades needed to reuse it (Step 5; the hardcoding itself was a
    flagged, deliberately-deferred finding from Step 3's dual review)."""
    schema, table = split_target(cfg.layer2.target)
    fact_schema, fact_table = split_target(layer3_target.target)
    pk_cols = primary_key_columns(conn, fact_schema, fact_table)
    fk = layer3_target.fk_resolution
    natural_key_col = cfg.source.natural_key[0]
    event_date_col = layer3_target.event_date_column
    plain_cols = layer3_target.columns or []
    dim_fk_cols = {col: rule for col, rule in fk.items() if isinstance(rule, dict)}
    literals = layer3_target.literals

    source_cols = sorted(
        {natural_key_col, "client_id", event_date_col, *plain_cols,
         *(rule["from_column"] for rule in dim_fk_cols.values())}
    )
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(source_cols)} FROM {schema}.{table}")
        rows = cur.fetchall()
        columns = [d.name for d in cur.description]

    # dict.fromkeys dedupes while preserving order: source_cols is a deduped
    # set, but this list is a flat concatenation, so a config where a
    # `columns`/`literals`/dim_fk_cols entry collides with another (or with
    # one of the four always-present generated keys) would otherwise emit
    # the same column twice in the INSERT and get rejected by postgres
    # (Step 5 dual review finding).
    insert_cols = list(dict.fromkeys([
        natural_key_col, "client_key", "risk_snapshot_key", "date_key", event_date_col,
        *plain_cols, *dim_fk_cols.keys(), *literals.keys(),
    ]))
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in insert_cols if c not in pk_cols)
    placeholders = ", ".join(f"%({c})s" for c in insert_cols)

    loaded = 0
    for row in rows:
        r = dict(zip(columns, row))
        event_date = r[event_date_col]
        event_ts = datetime.combine(event_date, datetime.min.time(), tzinfo=timezone.utc)
        client_key = _resolve_client_key(conn, r["client_id"], fk.get("dim_client"))
        risk_snapshot_key = _resolve_risk_snapshot_key(
            conn, r["client_id"], event_ts, fk.get("risk_snapshot")
        )
        date_key = ensure_date(conn, event_date)

        values = {
            natural_key_col: r[natural_key_col],
            "client_key": client_key,
            "risk_snapshot_key": risk_snapshot_key,
            "date_key": date_key,
            event_date_col: event_date,
            **{c: r[c] for c in plain_cols},
            **{
                col: _resolve_dimension_fk(
                    conn, rule["dim_target"], rule["dim_natural_key"],
                    rule["dim_surrogate_key"], r[rule["from_column"]],
                    nullable=rule.get("nullable", True),
                )
                for col, rule in dim_fk_cols.items()
            },
            **literals,
        }
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {fact_schema}.{fact_table} ({', '.join(insert_cols)}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE SET {set_clause}",
                values,
            )
        loaded += 1
    return loaded


def _resolve_dimension_fk(
    conn, dim_target: str, dim_natural_key: str, dim_surrogate_key: str, value, *, nullable: bool = True,
):
    if value is None:
        # A NULL source value (e.g. no assigned_manager) means "no FK", not
        # "lookup failed" — dim_client's manager_key is a nullable FK
        # precisely for this case (Step 4 dual review finding). But this
        # helper is now also reused (Step 5) for NOT NULL fact FKs (e.g.
        # fact_trades.instrument_key) where a NULL source value must raise a
        # clear, diagnostic error here rather than bypass it and surface as
        # a bare NotNullViolation from the INSERT (Step 5 dual review
        # finding) — callers opt into that via `nullable=False`.
        if not nullable:
            raise ValueError(
                f"no source value to resolve {dim_target}'s FK (nullable=False for this rule) —"
                f" check the staging row's source column for a missing/unmapped value"
            )
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
            f"no {dim_target} row with {dim_natural_key}={value!r} — "
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


def scd2_apply(cfg: TableConfig, layer3_target, conn) -> int:
    """Calls `warehouse.apply_cdc_event()` by name — the one config-declared
    exception to a generic upsert (ADR-1) — once per staging row, in true
    file-arrival order (`staging_seq`, ADR-10), never resorted: the watermark
    inside `apply_cdc_event` is the sole staleness guard by design, and
    CL001's real file-order events (lsn 1005, then 1004 — stale, quarantined
    by the function itself — then 1006) are the proof this isn't a
    constructed example. A quarantined row still counts as "processed" here
    (it satisfies the generic layer3 assertion: a layer3 row *or* a
    quarantine row per layer2 row) since apply_cdc_event, not this loop,
    decides accept vs. quarantine.

    Three fixes from Step 6 dual review (both Opus and Sonnet independently
    confirmed the first):

    1. The replay SELECT is filtered by a semi-join against
       `warehouse.cdc_watermark`, excluding any row this client's watermark
       has already passed. `apply_cdc_event`'s own stale-lsn branch is not
       idempotent — it writes a fresh `quarantine.rejected_rows` row every
       time it's called with an already-applied lsn — so an unfiltered
       replay on an `@daily` schedule re-quarantines the client's entire
       history on every run (empirically: run1 quarantine=2 rows, run2=14).
       This filter makes an already-applied row a true no-op at the Python
       level, not just "rejected again silently."
    2. `apply_cdc_event`'s `SELECT ... FOR UPDATE` on `cdc_watermark` takes no
       lock at all when no row yet exists for that client — it just returns
       no rows — so two concurrent first-applies for the same client could
       both see NULL and collide on the watermark INSERT. A per-client
       `pg_advisory_xact_lock` (same pattern already used in
       `_resolve_risk_snapshot_key` above) serializes this without touching
       the locked `sql/03` function.
    3. A `delete` for a client with zero existing `dim_client_risk_snapshot`
       rows is a silent triple no-op in `apply_cdc_event`: its tombstone
       SELECT finds nothing to copy, so no dimension row is written, no
       quarantine row is written either, yet the watermark still advances —
       contradicting this function's own "a layer3 row *or* a quarantine
       row" invariant and making the gap unrecoverable by a plain re-run.
       Checked explicitly here and quarantined by name instead.
    """
    schema, table = split_target(cfg.layer2.target)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT s.client_id, s.lsn, s.commit_ts, s.op, s.after"
            f" FROM {schema}.{table} AS s"
            f" WHERE NOT EXISTS ("
            f"   SELECT 1 FROM warehouse.cdc_watermark AS w"
            f"   WHERE w.client_id = s.client_id AND s.lsn <= w.last_applied_lsn"
            f" )"
            f" ORDER BY s.staging_seq"
        )
        rows = cur.fetchall()

    processed = 0
    for client_id, lsn, commit_ts, op, after in rows:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"cdc_apply:{client_id}",))
            if op == "delete":
                cur.execute(
                    "SELECT 1 FROM warehouse.dim_client_risk_snapshot WHERE client_id = %s LIMIT 1",
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
                    processed += 1
                    continue
            cur.execute(
                "SELECT warehouse.apply_cdc_event(%s, %s, %s, %s, %s)",
                (client_id, lsn, commit_ts, op, Jsonb(after) if after is not None else None),
            )
        processed += 1
    return processed


_STRATEGY_DISPATCH = {
    "fact_upsert": fact_upsert,
    "dimension_upsert": dimension_upsert,
    "scd2_baseline_seed": scd2_baseline_seed,
    "scd2_apply": scd2_apply,
}


def load(cfg: TableConfig, conn) -> int:
    total = 0
    for layer3_target in cfg.layer3:
        handler = _STRATEGY_DISPATCH.get(layer3_target.strategy)
        if handler is None:
            raise NotImplementedError(f"layer3 strategy {layer3_target.strategy!r} not yet implemented")
        total += handler(cfg, layer3_target, conn)
    return total
