# Deriv Data Engineering Assignment

Design and documentation for a production-grade data engineering solution for a financial
trading platform: ingesting a vendor deposit feed and a CDC change-log into a Postgres
warehouse, modeling it for point-in-time analytics, and extending the design to a unified
real-time + batch architecture.

The full assignment brief is in [ASSIGNMENT.md](ASSIGNMENT.md).

## How to navigate this repo

| File | Covers |
|---|---|
| [part1_pipeline.md](part1_pipeline.md) | Pipeline design: layer architecture (Postgres schemas `raw`→`staging`→`warehouse`/`quarantine`), idempotency, late/missing data reconciliation, CDC delete handling, and a Great-Expectations-based data quality framework with per-table health/regression tracking. |
| [part2_data_model.md](part2_data_model.md) | Kimball star schema (facts/dimensions, grain, ERD), the `dim_client` / `dim_client_risk_snapshot` SCD Type 4 mini-dimension split, the snapshotted-FK join strategy, late-arriving dimension handling (inferred members), and SCD2 historization including the historical-reload mechanism. |
| [part3_architecture.md](part3_architecture.md) | Lambda-style real-time (Debezium→Kafka→Flink) + batch architecture for fraud detection, the latency/consistency trade-offs, secure partner API access, and a build-vs-buy analysis (Meltano + Great Expectations vs. custom build) for onboarding new payment processors. |
| [sql/](sql/) | DDL and functions referenced from Part 2: dimension/fact tables, the CDC apply function (SCD2 + tombstone logic), and the historical-reload/watermark-reset mechanism. |
| [PROMPTS.md](PROMPTS.md) | Every AI-assisted design decision in this repo, grouped by part — the actual questions asked, what was recommended, what was decided, and where the AI's recommendation was corrected or overridden. |
| [data/](data/) | The assignment's input files (four warehouse JSON tables, three vendor CSV extracts, one CDC JSONL stream) that every design decision in this repo is grounded in. |

## Platform choices

- **PostgreSQL** as the target warehouse.
- **Airflow** for orchestration (dbt deferred — plain SQL for now, layerable later without a
  schema change).
- **Kimball star schema** for the dimensional model.
- **SCD Type 2** (via a Type 4 mini-dimension split) for the CDC-tracked client attributes.

See [PROMPTS.md](PROMPTS.md) for the full reasoning behind each of these and every
downstream decision.

## Status

- `README.md`, `PROMPTS.md`, `part1_pipeline.md`, `part2_data_model.md`,
  `part3_architecture.md`, and `sql/` — **done**, satisfy every MUST COMPLETE and VALIDATION
  requirement in [ASSIGNMENT.md](ASSIGNMENT.md).
- [code/](code/) (optional runnable prototype) — **done**. A config-driven, dockerized,
  TDD-built implementation: Postgres + Airflow, one generated DAG per table config, real
  data loaded from [data/](data/) exercising the idempotency, watermark, and SCD2 logic from
  `sql/` end-to-end. `cd code && docker compose up -d --build && make verify`. See
  [code/README.md](code/README.md) for architecture and documented trade-offs, and
  [code/PROGRESS.md](code/PROGRESS.md) / [code/ARCHITECTURE_DECISIONS.md](code/ARCHITECTURE_DECISIONS.md)
  for the full build log.
