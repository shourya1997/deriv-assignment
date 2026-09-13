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


def fk_columns_into(conn, ref_schema: str, ref_table: str) -> list[tuple[str, str]]:
    """Every (referencing_table, fk_column) pair with a real FK into
    ref_schema.ref_table, found by walking pg_constraint rather than matching
    information_schema.columns by column name alone — the latter also
    matches views (which can't be UPDATEd) and would miss a same-named
    column that isn't actually an FK (Step 4 dual review finding, first used
    by layer3_warehouse's stopgap-snapshot repoint, Step 7's historical
    reload repoint uses the same walk)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT conrelid::regclass::text, a.attname
            FROM pg_constraint c
            JOIN unnest(c.conkey) WITH ORDINALITY AS ck(attnum, ord) ON true
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ck.attnum
            WHERE c.contype = 'f'
              AND c.confrelid = %s::regclass
            """,
            (f"{ref_schema}.{ref_table}",),
        )
        return cur.fetchall()


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
