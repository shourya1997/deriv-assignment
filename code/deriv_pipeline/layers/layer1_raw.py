"""layer1: land source files as-is into `raw.*` — no aliasing, no typing, no
drift detection (that's layer2's job, see layer2_staging.py's module
docstring). Tag+rerun-safe: re-running against the same files is a no-op
content-wise (ON CONFLICT DO UPDATE on the same payload)."""
from __future__ import annotations

import csv
import json

from psycopg.types.json import Jsonb

from deriv_pipeline.config import TableConfig
from deriv_pipeline.layers.common import _data_dir, primary_key_columns, split_target


def _read_csv_rows(path):
    with path.open(newline="") as f:
        yield from csv.DictReader(f)


def _read_json_rows(path):
    records = json.loads(path.read_text())
    if not isinstance(records, list):
        raise ValueError(f"layer1_raw: {path} must contain a JSON array of objects")
    yield from records


_FORMAT_READERS = {"csv": _read_csv_rows, "json": _read_json_rows}


def load(cfg: TableConfig, conn) -> int:
    """Lands every file matching cfg.source.glob into cfg.layer1.target.
    Returns the number of rows landed (across all matched files)."""
    read_rows = _FORMAT_READERS.get(cfg.source.format)
    if read_rows is None:
        raise NotImplementedError(f"layer1_raw: unsupported source format {cfg.source.format!r}")

    schema, table = split_target(cfg.layer1.target)
    pk_cols = primary_key_columns(conn, schema, table)
    natural_key_cols = list(cfg.source.natural_key)

    files = sorted(_data_dir().glob(cfg.source.glob))
    if not files:
        raise ValueError(
            f"layer1_raw: no files matched glob {cfg.source.glob!r} in {_data_dir()}"
        )

    conflict_clause = ", ".join(pk_cols)
    insert_cols = natural_key_cols + ["source_file", "payload"]
    placeholders = ", ".join(f"%({c})s" for c in natural_key_cols) + ", %(source_file)s, %(payload)s"
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
