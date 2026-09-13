"""layer2: read raw.*'s as-landed payloads, apply expected_columns/aliases
(schema-drift detection, per source_file since drift is a per-file-header
fact), cast to the staging table's real column types (introspected via
information_schema — config declares no type map of its own), apply the
optional late_arrival rule, and upsert per the declared conflict_strategy."""
from __future__ import annotations

from datetime import date as _date

from deriv_pipeline.config import TableConfig
from deriv_pipeline.layers.common import column_types, primary_key_columns, split_target
from deriv_pipeline.transforms import compute_late_arrival, resolve_header


def stage(cfg: TableConfig, conn) -> int:
    """Returns the number of distinct rows now present in the staging table
    (natural-key deduped) — not the number of raw rows processed, which may
    be higher when a natural key is re-delivered across multiple files."""
    raw_schema, raw_table = split_target(cfg.layer1.target)
    schema, table = split_target(cfg.layer2.target)

    with conn.cursor() as cur:
        cur.execute(f"SELECT source_file, payload FROM {raw_schema}.{raw_table} ORDER BY source_file")
        raw_rows = cur.fetchall()

    late_cfg = cfg.source.late_arrival
    types = column_types(conn, schema, table)
    pk_cols = primary_key_columns(conn, schema, table)
    update_columns = cfg.layer2.update_columns or []

    # header resolution is per source_file (drift is a property of that
    # file's header, not of any individual row).
    header_cache: dict[str, tuple[dict[str, str], bool]] = {}

    for source_file, payload in raw_rows:
        if source_file not in header_cache:
            resolved = resolve_header(list(payload.keys()), cfg.source.expected_columns, cfg.source.aliases)
            header_cache[source_file] = (resolved.colmap, resolved.schema_drift_detected)
        colmap, schema_drift_detected = header_cache[source_file]

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

        cols = list(row_out.keys())
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
