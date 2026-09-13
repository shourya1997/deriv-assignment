-- G1 fix — dq_check_results log + dq_table_health regression view (the user's own
-- addition on top of the AI-suggested edge cases, per PROMPTS.md Part 1). Fed by
-- both the one real GE suite and the config-declared SQL assertions, through the
-- same `run_id` per DAG run (via XCom) so "vs. immediately preceding run" is
-- well-defined per table.
CREATE TABLE data_quality.dq_check_results (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id           text NOT NULL,
    table_name       text NOT NULL,
    check_name       text NOT NULL,
    severity         text NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    passed           boolean NOT NULL,
    unexpected_count int NOT NULL DEFAULT 0,
    executed_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_dq_check_results_table_time ON data_quality.dq_check_results (table_name, executed_at);

-- Regression definition, per PROMPTS.md: compare each run's failure count against
-- the immediately preceding run for that table, not a fixed threshold.
CREATE VIEW data_quality.dq_table_health AS
WITH run_summary AS (
    SELECT
        table_name,
        run_id,
        MIN(executed_at) AS run_at,
        COUNT(*) FILTER (WHERE NOT passed) AS failed_checks,
        COUNT(*) AS total_checks
    FROM data_quality.dq_check_results
    GROUP BY table_name, run_id
),
ranked AS (
    SELECT
        table_name, run_id, run_at, failed_checks, total_checks,
        LAG(failed_checks) OVER (PARTITION BY table_name ORDER BY run_at) AS prev_failed_checks
    FROM run_summary
)
SELECT
    table_name, run_id, run_at, failed_checks, total_checks, prev_failed_checks,
    (prev_failed_checks IS NOT NULL AND failed_checks > prev_failed_checks) AS is_regression
FROM ranked;
