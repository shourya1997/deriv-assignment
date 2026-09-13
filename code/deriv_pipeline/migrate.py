"""Explicit ordered migration manifest — never docker-entrypoint-initdb.d (not
re-runnable, can't interleave gap-fill DDL with the locked sql/01-04 files,
unusable from pytest). Tracked in schema_migrations so re-running is a no-op —
proved by test_harness_db.py::test_rerunning_migrations_is_a_noop, which calls
run_migrations() a second time after the session fixture already applied it.

Pinned order (dependency-driven, see ARCHITECTURE_DECISIONS.md ADR-1/G1):
  000_schemas -> sql/01 -> sql/02 -> 005_raw -> 006_staging -> 007_quarantine ->
  sql/03 -> sql/04 -> 008_data_quality -> 009_reconciliation
quarantine must exist before sql/03 runs (it writes quarantine.rejected_rows);
raw must exist before sql/04's SELECT reads raw.client_profile_changes.
"""
import hashlib
from pathlib import Path

from deriv_pipeline.db import REPO_ROOT, get_connection

MANIFEST = [
    REPO_ROOT / "code" / "migrations" / "000_schemas.sql",
    REPO_ROOT / "sql" / "01_dimensions.sql",
    REPO_ROOT / "sql" / "02_facts.sql",
    REPO_ROOT / "code" / "migrations" / "005_raw.sql",
    REPO_ROOT / "code" / "migrations" / "006_staging.sql",
    REPO_ROOT / "code" / "migrations" / "007_quarantine.sql",
    REPO_ROOT / "sql" / "03_cdc_apply.sql",
    REPO_ROOT / "sql" / "04_backfill_reset.sql",
    REPO_ROOT / "code" / "migrations" / "008_data_quality.sql",
    REPO_ROOT / "code" / "migrations" / "009_reconciliation.sql",
    REPO_ROOT / "code" / "migrations" / "010_cdc_ingestion_order.sql",
]

_BOOTSTRAP = """
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    filename    text PRIMARY KEY,
    checksum    text NOT NULL,
    applied_at  timestamptz NOT NULL DEFAULT now()
);
"""


def _checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_migrations(conn=None) -> list[str]:
    """Applies every manifest file not yet recorded, in order. Returns the
    filenames actually applied this call (empty on a clean no-op re-run)."""
    own_conn = conn is None
    conn = conn or get_connection()
    applied: list[str] = []
    try:
        with conn.cursor() as cur:
            cur.execute(_BOOTSTRAP)
            # Advisory lock scoped to this one transaction: two concurrent
            # run_migrations() calls would otherwise both pass the "not yet
            # applied" check and race on the same CREATE TABLE/FUNCTION DDL,
            # failing with a confusing duplicate-object error instead of one
            # waiting cleanly for the other (Step 1 dual review finding).
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (8675309,))
            for path in MANIFEST:
                name = str(path.relative_to(REPO_ROOT))
                checksum = _checksum(path)
                cur.execute(
                    "SELECT checksum FROM public.schema_migrations WHERE filename = %s",
                    (name,),
                )
                row = cur.fetchone()
                if row is not None:
                    if row[0] != checksum:
                        raise RuntimeError(
                            f"migration drift: {name} checksum changed since it was applied"
                        )
                    continue
                cur.execute(path.read_text())
                cur.execute(
                    "INSERT INTO public.schema_migrations (filename, checksum) VALUES (%s, %s)",
                    (name, checksum),
                )
                applied.append(name)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()
    return applied


if __name__ == "__main__":
    applied = run_migrations()
    if applied:
        print(f"applied {len(applied)} migration(s): {', '.join(applied)}")
    else:
        print("no pending migrations")
