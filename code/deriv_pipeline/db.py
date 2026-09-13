"""Connection helper. One env var drives which database a process talks to —
`DERIV_TEST=1` (set by the pytest fixtures) swaps in the test DSN so unit/
integration tests never touch the live `deriv` database."""
import os

import psycopg

REPO_ROOT = __import__("pathlib").Path(__file__).resolve().parents[2]


def dsn() -> str:
    if os.environ.get("DERIV_TEST"):
        return os.environ.get(
            "DERIV_TEST_DSN", "postgresql://deriv:deriv@localhost:5432/deriv_test"
        )
    return os.environ.get("DERIV_DSN", "postgresql://deriv:deriv@localhost:5432/deriv")


def get_connection() -> psycopg.Connection:
    return psycopg.connect(dsn(), autocommit=False)
