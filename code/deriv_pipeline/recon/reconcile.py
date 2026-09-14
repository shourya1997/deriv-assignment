"""Step 9: generic reconciliation diff, driven by config/reconciliations/*.yml
(ReconciliationConfig.left/right are each a full SQL SELECT returning the
`key` columns for one source's rows — see config.py's docstring). A key
tuple present on one side and absent on the other is a discrepancy, written
to data_quality.reconciliation_discrepancies. The run's discrepancy count is
also logged as a dq_check_results row (reusing dq.log_check) so
dq_table_health's regression tracking covers reconciliation too, per
part1_pipeline.md section 3's "surfaced in the dq_table_health view" claim.
"""
from __future__ import annotations

from psycopg.types.json import Jsonb

from deriv_pipeline.config import ReconciliationConfig
from deriv_pipeline.dq import log_check


def _key_tuples(conn, sql: str) -> set[tuple]:
    # ponytail: a set collapses duplicate key tuples on either side, so a
    # double-posted vendor deposit (same client/date/amount, different
    # deposit_id) reconciles clean instead of flagging a multiplicity
    # mismatch. Upgrade to collections.Counter (multiset diff) if a source
    # ever ships true duplicate rows — the shipped data has none today.
    with conn.cursor() as cur:
        cur.execute(sql)
        return {tuple(row) for row in cur.fetchall()}


def run_reconciliation(cfg: ReconciliationConfig, conn, run_id: str) -> int:
    """Returns the number of discrepancies found this run."""
    left_keys = _key_tuples(conn, cfg.left)
    right_keys = _key_tuples(conn, cfg.right)

    discrepancies = [(key, "missing_right") for key in left_keys - right_keys]
    discrepancies += [(key, "missing_left") for key in right_keys - left_keys]

    with conn.cursor() as cur:
        # A re-run under the same run_id (e.g. `airflow dags test` twice for
        # the same execution date, as verify.sh does for idempotency) must
        # replace this run's rows, not add a second copy on top of them.
        cur.execute(
            "DELETE FROM data_quality.reconciliation_discrepancies"
            " WHERE reconciliation_name = %s AND run_id = %s",
            (cfg.name, run_id),
        )
        for key_tuple, discrepancy_type in discrepancies:
            # str() every value (Decimal/date aren't JSON-serializable as-is,
            # and the discrepancy row only needs to be human-readable, not
            # round-tripped back into a typed value).
            natural_key = dict(zip(cfg.key, (str(v) for v in key_tuple), strict=True))
            cur.execute(
                "INSERT INTO data_quality.reconciliation_discrepancies"
                " (reconciliation_name, run_id, natural_key, discrepancy_type)"
                " VALUES (%s, %s, %s, %s)",
                (cfg.name, run_id, Jsonb(natural_key), discrepancy_type),
            )

    log_check(
        conn, run_id, cfg.name, f"reconciliation:{cfg.name}", "WARNING",
        passed=not discrepancies, unexpected_count=len(discrepancies),
    )
    return len(discrepancies)
