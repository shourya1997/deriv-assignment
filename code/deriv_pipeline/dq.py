"""Step 8 — data quality dispatch, run from `run_dq_checks` in every generated
DAG (dags/dag_factory.py). Exactly one table (vendor_deposits) gets a real
Great Expectations suite, per ADR-4/ADR-5's GE-version-coupling risk; every
other table's checks are config-declared SQL assertions (`layer2.dq_checks`
in config.py), routed through the identical severity -> quarantine path a GE
suite would use. Both paths log one `data_quality.dq_check_results` row per
check, sharing one `run_id` per DAG run (Airflow's own `context["run_id"]`,
no separate XCom plumbing needed) so `dq_table_health`'s "vs. immediately
preceding run" comparison in migrations/008_data_quality.sql is well-defined.

A failed CRITICAL check is logged and quarantined (a summary row — see
`_record_result`'s docstring below for why not a per-row reject) but does
*not* fail the task or block `load_layer3`. part1_pipeline.md's own example
("a negative-amount deposit... not loaded into fact_deposits") describes
per-row exclusion, which needs the offending natural key(s) threaded from
GE's result (`unexpected_index_list`) into layer3's fact_upsert filter — real
work, out of Step 8's scope, and blocking the *whole table's* load on one bad
row would also fail `airflow dags test table__vendor_deposits` forever, since
that row is a permanent fixture of the shipped data (breaking `make verify`).
# ponytail: table-level block/skip only, not per-row exclusion from
# fact_upsert; upgrade path is teaching run_dq_checks to return failing
# natural keys and layer3_warehouse to filter them out of its load query."""
from __future__ import annotations

from psycopg.types.json import Jsonb

from deriv_pipeline.config import TableConfig
from deriv_pipeline.db import dsn
from deriv_pipeline.layers.common import split_target

# The one real GE suite (part1_pipeline.md's own example): expectation_type ->
# (kwargs, severity). Hardcoded rather than loaded from a suite YAML file —
# there is exactly one, and a generic YAML->expectation loader for a single
# fixed suite is speculative machinery this prototype doesn't need. Keyed by
# expectation_type (not a positional list) so labeling a GE result doesn't
# depend on `result.results` coming back in suite order.
_GE_SUITES: dict[str, dict[str, tuple[dict, str]]] = {
    "staging_vendor_deposits": {
        "expect_column_values_to_be_between": ({"column": "amount_usd", "min_value": 0.01}, "CRITICAL"),
        "expect_column_values_to_not_be_null": ({"column": "deposit_id"}, "CRITICAL"),
        "expect_column_values_to_be_in_set": (
            {"column": "payment_method", "value_set": ["bank_transfer", "credit_card", "e_wallet"]},
            "WARNING",
        ),
    },
}


def log_check(conn, run_id: str, table_name: str, check_name: str, severity: str, passed: bool, unexpected_count: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO data_quality.dq_check_results"
            " (run_id, table_name, check_name, severity, passed, unexpected_count)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
            (run_id, table_name, check_name, severity, passed, unexpected_count),
        )


def _record_result(conn, table_name: str, run_id: str, check_name: str, severity: str, passed: bool, unexpected_count: int) -> bool:
    log_check(conn, run_id, table_name, check_name, severity, passed, unexpected_count)
    if not passed and severity == "CRITICAL":
        # A table-level assertion has no single offending natural key to
        # quarantine (unlike apply_cdc_event's per-row rejects), so this logs
        # one summary row per failed CRITICAL check rather than one per bad
        # row — the failing row count is on the row itself for follow-up.
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO quarantine.rejected_rows"
                " (table_name, reason_code, severity, raw_payload)"
                " VALUES (%s, %s, 'CRITICAL', %s)",
                (table_name, check_name, Jsonb({"run_id": run_id, "unexpected_count": unexpected_count})),
            )
    return passed


def run_sql_assertion(conn, table_name: str, run_id: str, check) -> bool:
    """`check.sql` must return a single row/column: the count of rows failing
    the assertion (0 = pass)."""
    with conn.cursor() as cur:
        cur.execute(check.sql)
        row = cur.fetchone()
    unexpected_count = row[0] if row else 0
    passed = unexpected_count == 0
    return _record_result(conn, table_name, run_id, check.name, check.severity, passed, unexpected_count)


def run_sql_assertions(cfg: TableConfig, conn, run_id: str) -> list[str]:
    """Returns the names of any CRITICAL checks that failed."""
    failed_critical = []
    for check in cfg.layer2.dq_checks:
        if not run_sql_assertion(conn, cfg.layer2.target, run_id, check) and check.severity == "CRITICAL":
            failed_critical.append(check.name)
    return failed_critical


def run_ge_suite(cfg: TableConfig, conn, run_id: str) -> list[str]:
    """Returns the names of any CRITICAL expectations that failed."""
    import great_expectations as gx  # heavy import, only paid when a GE suite actually runs

    suite_name = cfg.layer2.ge_suite
    if suite_name not in _GE_SUITES:
        raise ValueError(f"dq.run_ge_suite: unknown GE suite {suite_name!r} (layer2.ge_suite on {cfg.name})")
    expectations = _GE_SUITES[suite_name]
    schema, table = split_target(cfg.layer2.target)

    context = gx.get_context(mode="ephemeral")
    datasource = context.sources.add_or_update_sql(name="dq", connection_string=dsn())
    asset = datasource.add_table_asset(name=table, table_name=table, schema_name=schema)
    suite = context.add_or_update_expectation_suite(suite_name)
    validator = context.get_validator(batch_request=asset.build_batch_request(), expectation_suite=suite)
    for expectation_type, (kwargs, _severity) in expectations.items():
        getattr(validator, expectation_type)(**kwargs)
    result = validator.validate()

    failed_critical = []
    for ge_result in result.results:
        expectation_type = ge_result.expectation_config.expectation_type
        _, severity = expectations[expectation_type]
        column = ge_result.expectation_config.kwargs.get("column")
        check_name = f"{expectation_type}:{column}"
        # Trust GE's own verdict rather than re-deriving pass/fail from
        # unexpected_count — an expectation using `mostly=` can legitimately
        # succeed with unexpected_count > 0 (none of the 3 above do, but a
        # future one might).
        count = ge_result.result.get("unexpected_count")
        unexpected_count = count if count is not None else (0 if ge_result.success else 1)
        passed = _record_result(conn, cfg.layer2.target, run_id, check_name, severity, ge_result.success, unexpected_count)
        if not passed and severity == "CRITICAL":
            failed_critical.append(check_name)
    return failed_critical


def run_dq_checks(cfg: TableConfig, conn, run_id: str) -> list[str]:
    """Returns the names of any CRITICAL checks that failed (logged and
    quarantined either way — see module docstring for why this doesn't raise
    and block load_layer3)."""
    failed_critical = []
    if cfg.layer2.ge_suite:
        failed_critical += run_ge_suite(cfg, conn, run_id)
    failed_critical += run_sql_assertions(cfg, conn, run_id)
    return failed_critical
