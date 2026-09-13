"""layer1: land source files as-is into `raw.*` — no aliasing, no typing, no
drift detection (that's layer2's job, see layer2_staging.py's module
docstring). Tag+rerun-safe: re-running against the same files is a no-op
content-wise (ON CONFLICT DO UPDATE on the same payload)."""
from __future__ import annotations

import csv
import json

from psycopg.types.json import Jsonb

from deriv_pipeline.config import TableConfig
from deriv_pipeline.layers.common import _data_dir, column_types, primary_key_columns, split_target


def _read_csv_rows(path):
    with path.open(newline="") as f:
        yield from csv.DictReader(f)


def _read_json_rows(path):
    records = json.loads(path.read_text())
    if not isinstance(records, list):
        raise ValueError(f"layer1_raw: {path} must contain a JSON array of objects")
    yield from records


def _read_jsonl_rows(path):
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            yield json.loads(line)


_FORMAT_READERS = {"csv": _read_csv_rows, "json": _read_json_rows, "jsonl": _read_jsonl_rows}


def load(cfg: TableConfig, conn) -> int:
    """Lands every file matching cfg.source.glob into cfg.layer1.target.
    Returns the number of rows landed (across all matched files).

    Two raw-table shapes, detected generically from the target's own columns
    (no per-table special-casing): most sources land as natural_key +
    source_file + a single jsonb `payload` (typing/drift resolution is
    layer2's job); `client_profile_changes` (a CDC log, Step 6) is the one
    exception — sql/04's reload driver reads it by real column name, so its
    raw table is typed columns directly, no `payload` column at all. A raw
    table with no `payload` column lands `natural_key ∪ expected_columns`
    straight across by name instead, jsonb-wrapping whichever of those the
    target types as jsonb (e.g. `before`/`after`)."""
    read_rows = _FORMAT_READERS.get(cfg.source.format)
    if read_rows is None:
        raise NotImplementedError(f"layer1_raw: unsupported source format {cfg.source.format!r}")

    schema, table = split_target(cfg.layer1.target)
    pk_cols = primary_key_columns(conn, schema, table)
    natural_key_cols = list(cfg.source.natural_key)
    types = column_types(conn, schema, table)

    files = sorted(_data_dir().glob(cfg.source.glob))
    if not files:
        raise ValueError(
            f"layer1_raw: no files matched glob {cfg.source.glob!r} in {_data_dir()}"
        )

    conflict_clause = ", ".join(pk_cols)

    if "payload" in types:
        insert_cols = natural_key_cols + ["source_file", "payload"]
        placeholders = (
            ", ".join(f"%({c})s" for c in natural_key_cols) + ", %(source_file)s, %(payload)s"
        )
        sql = (
            f"INSERT INTO {schema}.{table} ({', '.join(insert_cols)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_clause}) DO UPDATE SET "
            f"payload = EXCLUDED.payload, source_file = EXCLUDED.source_file"
        )

        landed = 0
        with conn.cursor() as cur:
            for path in files:
                for row in read_rows(path):
                    params = {col: row[col] for col in natural_key_cols}
                    params["source_file"] = path.name
                    params["payload"] = Jsonb(row)
                    cur.execute(sql, params)
                    landed += 1
        return landed

    value_cols = list(dict.fromkeys([*natural_key_cols, *cfg.source.expected_columns]))
    insert_cols = value_cols + ["source_file"]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in insert_cols if c not in pk_cols)
    placeholders = ", ".join(f"%({c})s" for c in insert_cols)
    sql = (
        f"INSERT INTO {schema}.{table} ({', '.join(insert_cols)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_clause}) DO UPDATE SET {set_clause}"
    )

    landed = 0
    with conn.cursor() as cur:
        for path in files:
            for row in read_rows(path):
                # .get(c), not row[c]: a source that omits a key entirely
                # (rather than emitting it as explicit null) must not raise
                # a bare KeyError and drop the whole batch — this typed path
                # has no drift/quarantine handling of its own (unlike the
                # jsonb-payload path above, which defers to layer2's
                # resolve_header), so a missing key becomes NULL here and
                # is enforced by the column's own NOT NULL constraint if
                # that's wrong (Step 6 dual review finding).
                params = {
                    c: (Jsonb(row.get(c)) if types.get(c) == "jsonb" and row.get(c) is not None else row.get(c))
                    for c in value_cols
                }
                params["source_file"] = path.name
                cur.execute(sql, params)
                landed += 1
    return landed
