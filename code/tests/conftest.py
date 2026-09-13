import os

import pytest

os.environ["DERIV_TEST"] = "1"

from deriv_pipeline.db import get_connection
from deriv_pipeline.migrate import run_migrations


@pytest.fixture(scope="session", autouse=True)
def _migrated_db():
    """Runs the full migration manifest against deriv_test once per test session.
    Idempotent by design (schema_migrations), so re-running the suite is safe."""
    run_migrations()


@pytest.fixture()
def db_conn():
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()
