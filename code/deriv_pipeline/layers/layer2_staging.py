"""layer2: read raw.*'s as-landed payloads, apply expected_columns/aliases
(schema-drift detection, per source_file since drift is a per-file-header
fact), cast to the staging table's real column types (introspected via
information_schema — config declares no type map of its own), apply the
optional late_arrival rule, and upsert per the declared conflict_strategy."""
from __future__ import annotations

from datetime import date as _date

from psycopg.types.json import Jsonb

from deriv_pipeline.config import TableConfig
from deriv_pipeline.layers.common import column_types, primary_key_columns, split_target
from deriv_pipeline.transforms import compute_late_arrival, resolve_header


def _stage_typed_passthrough(cfg: TableConfig, conn, raw_schema, raw_table, schema, table) -> int:
    """A raw table with no `payload` column (e.g. client_profile_changes, a
    typed CDC log — see layer1_raw.load's docstring) already carries real
    column names straight from the source parser: no header/alias/drift
    resolution applies, just copy `natural_key ∪ expected_columns` across
    unchanged. Ordered by `raw_seq` (not a plain scan) and written to
    staging one row at a time so staging's own `staging_seq` bigserial lands
    in the exact same relative order scd2_apply must later replay in (ADR-10
    — apply_cdc_event's watermark is the sole staleness guard, no batch
    sort)."""
    pk_cols = primary_key_columns(conn, schema, table)
    raw_types = column_types(conn, raw_schema, raw_table)
    value_cols = list(dict.fromkeys([*cfg.source.natural_key, *cfg.source.expected_columns]))

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(value_cols)} FROM {raw_schema}.{raw_table} ORDER BY raw_seq"
        )
        rows = cur.fetchall()
        col_names = [d.name for d in cur.description]

    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in value_cols if c not in pk_cols)
    placeholders = ", ".join(f"%({c})s" for c in value_cols)
    sql = (
        f"INSERT INTO {schema}.{table} ({', '.join(value_cols)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE SET {set_clause}"
    )
    for raw_row in rows:
        r = dict(zip(col_names, raw_row))
        params = {
            c: (Jsonb(r[c]) if raw_types.get(c) == "jsonb" and r[c] is not None else r[c])
            for c in value_cols
        }
        with conn.cursor() as cur:
            cur.execute(sql, params)

    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {schema}.{table}")
        return cur.fetchone()[0]


def stage(cfg: TableConfig, conn) -> int:
    """Returns the number of distinct rows now present in the staging table
    (natural-key deduped) — not the number of raw rows processed, which may
    be higher when a natural key is re-delivered across multiple files."""
    raw_schema, raw_table = split_target(cfg.layer1.target)
    schema, table = split_target(cfg.layer2.target)

    raw_types = column_types(conn, raw_schema, raw_table)
    if "payload" not in raw_types:
        return _stage_typed_passthrough(cfg, conn, raw_schema, raw_table, schema, table)

    with conn.cursor() as cur:
        cur.execute(f"SELECT source_file, payload FROM {raw_schema}.{raw_table} ORDER BY source_file")
        raw_rows = cur.fetchall()

    late_cfg = cfg.source.late_arrival
    types = column_types(conn, schema, table)
    pk_cols = primary_key_columns(conn, schema, table)
    update_columns = cfg.layer2.update_columns or []

    # Cached by the row's own key set, not by source_file: a CSV file has one
    # physical header shared by every row, so this coincides with "per file"
    # for CSV sources — but a JSON array has no shared header line, and rows
    # within the same file can carry different key sets (e.g. client_deposit
    # .json's DEP012 uses `credit_card` where every other row uses
    # `payment_method`). Keying the cache on source_file alone would resolve
    # every row in that file using whichever row happened to be seen first,
    # silently misclassifying the rest (Step 5 finding, caught by
    # test_client_deposit_loads_into_shared_fact_deposits_as_internal).
    header_cache: dict[frozenset[str], tuple[dict[str, str], bool]] = {}

    for source_file, payload in raw_rows:
        cache_key = frozenset(payload.keys())
        if cache_key not in header_cache:
            resolved = resolve_header(list(payload.keys()), cfg.source.expected_columns, cfg.source.aliases)
            header_cache[cache_key] = (resolved.colmap, resolved.schema_drift_detected)
        colmap, schema_drift_detected = header_cache[cache_key]

        row_out: dict[str, str] = {}
        for observed, canonical in colmap.items():
            if observed in payload:
                row_out[canonical] = payload[observed]

        is_late_arrival = False
        if late_cfg is not None:
            event_date_col = late_cfg["event_date_col"]
            event_date = _date.fromisoformat(row_out[event_date_col])
            is_late_arrival = compute_late_arrival(
                source_file, event_date, late_cfg["delivery_date"]["pattern"],
                late_cfg["threshold_days"],
            )

        row_out["schema_drift_detected"] = schema_drift_detected
        row_out["is_late_arrival"] = is_late_arrival

        # Not every staging table tracks drift/late-arrival (e.g.
        # client_signup/client_profile have neither column) — only persist
        # the flags the target table actually has room for.
        cols = [c for c in row_out.keys() if c in types]
        # schema_drift_detected/is_late_arrival are sticky: once true for a
        # natural key (any file/redelivery), they must stay true even if a
        # later redelivery looks "clean" on its own — OR-combine against the
        # existing row instead of overwriting.
        sticky_flag_cols = {"schema_drift_detected", "is_late_arrival"}
        # dedupe while preserving order: update_columns may already list a
        # flag column, and appending the flags unconditionally would then
        # emit "col = ... , col = ..." twice for the same column, which
        # postgres rejects as "multiple assignments to same column".
        set_cols = list(dict.fromkeys([*update_columns, *sticky_flag_cols]))
        set_clauses = []
        for c in set_cols:
            if c not in cols:
                continue
            if c in sticky_flag_cols:
                set_clauses.append(f"{c} = {table}.{c} OR EXCLUDED.{c}")
            else:
                set_clauses.append(f"{c} = EXCLUDED.{c}")
        # True no-op on exact redelivery (part1_pipeline.md): only bump
        # staged_at when some tracked column's value actually changed.
        distinct_checks = " OR ".join(f"{table}.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in set_cols if c in cols)
        staged_at_clause = (
            f"staged_at = CASE WHEN {distinct_checks} THEN now() ELSE {table}.staged_at END"
            if distinct_checks else f"staged_at = {table}.staged_at"
        )
        set_clauses.append(staged_at_clause)
        assignments = ", ".join(set_clauses)
        placeholders = ", ".join(
            f"%({c})s::{types[c]}" if c in types else f"%({c})s" for c in cols
        )
        sql = (
            f"INSERT INTO {schema}.{table} ({', '.join(cols)}, staged_at) "
            f"VALUES ({placeholders}, now()) "
            f"ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE SET {assignments}"
        )
        params = {c: row_out[c] for c in cols}
        with conn.cursor() as cur:
            cur.execute(sql, params)

    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {schema}.{table}")
        return cur.fetchone()[0]
