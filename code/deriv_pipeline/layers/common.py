"""Small DB-introspection helpers shared by layer1/layer2/layer3, so each
layer stays generic (dispatches on config, never hand-cases a table name)."""
from __future__ import annotations

DATA_DIR = None  # set lazily below to avoid a hard import-time dependency on db.py


def _data_dir():
    global DATA_DIR
    if DATA_DIR is None:
        from deriv_pipeline.db import REPO_ROOT

        DATA_DIR = REPO_ROOT / "data"
    return DATA_DIR


def split_target(target: str) -> tuple[str, str]:
    schema, _, table = target.partition(".")
    if not table:
        raise ValueError(f"target {target!r} must be 'schema.table'")
    return schema, table


def primary_key_columns(conn, schema: str, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.constraint_schema = kcu.constraint_schema
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
            """,
            (schema, table),
        )
        cols = [r[0] for r in cur.fetchall()]
    if not cols:
        raise ValueError(f"{schema}.{table} has no primary key — cannot upsert")
    return cols


def column_types(conn, schema: str, table: str) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, udt_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return {r[0]: r[1] for r in cur.fetchall()}
