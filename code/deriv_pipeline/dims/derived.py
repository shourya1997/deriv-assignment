"""derived_dimension engine: one row per distinct value of source_column,
either read from an already-staged table (source_table) or, when the
upstream table isn't onboarded as a `kind: table` config yet, directly from
its raw data/ file (raw_source_glob) — see config.py's DerivedDimensionConfig
docstring-equivalent comment. `derived_columns` optionally maps a value-keyed
lookup onto extra target columns (e.g. dim_instrument's asset_class)."""
from __future__ import annotations

import json

from deriv_pipeline.config import DerivedDimensionConfig
from deriv_pipeline.layers.common import _data_dir, split_target


def _distinct_values_from_staging(cfg: DerivedDimensionConfig, conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT DISTINCT {cfg.source_column} FROM staging.{cfg.source_table} "
            f"WHERE {cfg.source_column} IS NOT NULL ORDER BY {cfg.source_column}"
        )
        return [r[0] for r in cur.fetchall()]


def _distinct_values_from_raw(cfg: DerivedDimensionConfig) -> list[str]:
    files = sorted(_data_dir().glob(cfg.raw_source_glob))
    if not files:
        raise ValueError(
            f"derived_dimension {cfg.name!r}: no files matched glob "
            f"{cfg.raw_source_glob!r} in {_data_dir()}"
        )
    values = set()
    for path in files:
        records = json.loads(path.read_text())
        for record in records:
            value = record.get(cfg.source_column)
            if value is not None:
                values.add(value)
    return sorted(values)


def load(cfg: DerivedDimensionConfig, conn) -> int:
    values = (
        _distinct_values_from_raw(cfg)
        if cfg.raw_source_glob
        else _distinct_values_from_staging(cfg, conn)
    )
    schema, table = split_target(cfg.target)

    extra_cols = list(cfg.derived_columns.keys())
    insert_cols = [cfg.target_key_column, *extra_cols]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in extra_cols) or None

    loaded = 0
    with conn.cursor() as cur:
        for value in values:
            row = {cfg.target_key_column: value}
            for col in extra_cols:
                value_map = cfg.derived_columns[col]
                if value not in value_map:
                    raise ValueError(
                        f"derived_dimension {cfg.name!r}: no {col!r} mapping for "
                        f"{cfg.source_column}={value!r}"
                    )
                row[col] = value_map[value]

            placeholders = ", ".join(f"%({c})s" for c in insert_cols)
            conflict_action = (
                f"DO UPDATE SET {set_clause}" if set_clause else "DO NOTHING"
            )
            cur.execute(
                f"INSERT INTO {schema}.{table} ({', '.join(insert_cols)}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT ({cfg.target_key_column}) {conflict_action}",
                row,
            )
            # ON CONFLICT DO NOTHING no-ops (row.rowcount == 0) on a rerun
            # against an already-seeded value — only count actual writes.
            loaded += cur.rowcount
    return loaded
