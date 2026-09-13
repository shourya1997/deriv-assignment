# deriv-assignment — project conventions

Deriv data engineering take-home: design docs + SQL for a trading-platform pipeline. See
[README.md](README.md) for the deliverable map and [ASSIGNMENT.md](ASSIGNMENT.md) for the
original brief.

## Platform decisions (do not silently deviate from these)

- **PostgreSQL** as the target warehouse — not BigQuery/Snowflake/Databricks.
- **Airflow only for orchestration** — dbt is explicitly deferred; don't introduce dbt models
  unless the user asks for that phase to start.
- **Kimball star schema**, not Data Vault.
- **SCD Type 2** for CDC-tracked client attributes, implemented as an **SCD Type 4
  mini-dimension split**: `dim_client` (SCD1, static attrs) + `dim_client_risk_snapshot`
  (SCD2, the 3 volatile attrs). Fact tables use a **snapshotted FK at load time**, not a
  dynamic `BETWEEN` join.
- **Great Expectations** is the data-quality check-definition/execution engine (Part 1 and
  any future source); results still land in the Postgres `data_quality.dq_check_results`
  table for cross-run regression tracking, since GE doesn't natively do that.
- **Meltano** (not Fivetran) is the default recommendation for new-source extraction,
  conditional on zero-engineering fit for that source's auth/protocol.

## Working conventions

- **Every design decision in this repo came from an interactive grilling session, not a
  single prompt.** [PROMPTS.md](PROMPTS.md) is the log of that process — when you make a new
  AI-assisted design decision (adding a source, changing a mechanism, extending Part 3),
  append to `PROMPTS.md` in the same format: the question/prompt, what was recommended,
  what was decided, and explicitly note it if the user corrected or overrode a recommendation.
  A one-line "used AI for X" entry does not satisfy the assignment's validation requirement —
  don't write one.
- SQL referenced from `part2_data_model.md` lives in `sql/`, numbered by load order
  (`01_dimensions.sql` before `02_facts.sql`, since facts FK into dimensions).
- Keep `part1_pipeline.md` / `part2_data_model.md` / `part3_architecture.md` self-contained:
  inline the SQL and explanation together. Code without accompanying explanation doesn't
  score per the assignment's own constraints.
- `code/` prototype is optional and currently **not started** — see README.md Status. If
  building it, it should exercise the actual `sql/` schema against `data/`, not reimplement
  the logic separately.
