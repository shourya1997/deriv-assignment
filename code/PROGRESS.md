# code/ build — progress log

Append-only. One entry per phase, never rewritten retroactively. Answers "what state is the
repo actually in" independent of git history.

## Step 0 — bootstrap

- Confirmed git root: `git rev-parse --show-toplevel` → `/Users/shouryasengupta/Vibe/projects/deriv-assignment`.
  cwd for this session started at parent `Vibe/`, now corrected — all commits target this repo.
- Logged the architecture pivot (config-driven/auto-generated override) and the G1/G2 gaps in
  root `PROMPTS.md`, per `CLAUDE.md`'s required AI-prompt-log format.
- Created this file, `TASK.md`, `ARCHITECTURE_DECISIONS.md`.
- No code written yet. `code/` dir exists but is otherwise empty.
- Open issues carried into Step 1: none yet — nothing has run.

## Step 1 — harness

- Built: `docker-compose.yml` (postgres/airflow-init/airflow-scheduler/airflow-webserver/tests),
  `docker/airflow/Dockerfile`+`requirements.txt`, `docker/postgres/initdb/00_create_databases.sh`,
  `deriv_pipeline/db.py`+`migrate.py`, gap-fill migrations `000_schemas` → `005_raw` →
  `006_staging` → `007_quarantine` → `008_data_quality` → `009_reconciliation` (G1 fix),
  `pyproject.toml`, `scripts/verify.sh` (partial — steps 3-5 land with later phases), `Makefile`.
- Tests green: 30/30 (`tests/unit/test_harness.py`, `tests/integration/test_harness_db.py`) —
  run both via a local venv against a bare `docker compose up -d postgres`, AND via the
  documented `docker compose --profile test run --rm tests` path after fixing the dual-review
  findings below. `docker compose up -d --build` also verified end-to-end: `airflow-init`
  applies all 10 manifest migrations against the real `deriv` DB, `airflow-scheduler`/
  `airflow-webserver` come up healthy.
- Dual review (Opus + Sonnet) caught 1 shared blocker and several real gaps, all fixed:
  - **Blocker (both reviewers):** `airflow-init` called `python -m deriv_pipeline.config
    --validate-all`, a module that doesn't exist until Step 2 — `docker compose up -d` could
    never succeed. Removed the line from `airflow-init`'s command; will be re-added in Step 2.
  - **Blocker (Opus):** `airflow-init`'s multi-line `bash -c` had no `set -e`, so a failed
    `airflow db migrate` would be masked by the next command's exit code. Added
    `set -euo pipefail`.
  - **Major (Opus):** Postgres healthcheck (`pg_isready`, no host) went green during
    `initdb.d` bootstrap, before `00_create_databases.sh` had actually created the databases —
    a cold-start race. Changed to `psql -d airflow -c 'select 1' -h 127.0.0.1`, and re-added
    the `postgres: service_healthy` condition to the 3 services whose `depends_on` override had
    silently dropped it (YAML merge keys don't deep-merge).
  - **Major (both):** no `.dockerignore` — the 564MB local `.venv`, `.pytest_cache`, `.env`
    (with the Postgres password) were all going into the build context/image. Added
    `code/.dockerignore`.
  - **Major (Opus):** `AIRFLOW_UID=50000` hardcoded default only works on macOS/Windows
    (Docker Desktop remaps ownership) — breaks bind-mount writes on Linux. Documented
    `AIRFLOW_UID=$(id -u)` in `.env.example`.
  - **Major (Sonnet):** Airflow 2.10.5 pins `SQLAlchemy<2.0`, which has no native psycopg3
    dialect — GE's planned `SqlAlchemyExecutionEngine` (ADR-4, Step 8) would have had no
    working Postgres driver. Switched to `great_expectations[postgresql]` (pulls
    `psycopg2-binary`).
  - **Major (Opus, verified by actually building the image):** GE install was unconstrained
    against Airflow's own pins. Added `--constraint
    .../constraints-2.10.5/constraints-3.11.txt` to the Dockerfile's pip install — image now
    builds clean with no downgrade warnings.
  - **Minor (Opus):** `migrate.py` had no advisory lock — two concurrent `run_migrations()`
    calls would race on the same DDL. Added `pg_advisory_xact_lock`.
  - **Minor (Opus):** `staging.client_deposit` was missing `schema_drift_detected`/
    `is_late_arrival` even though `client_deposit.json` has the same `payment_method`/`method`
    drift as the vendor feed (e.g. `DEP012`). Added both columns.
  - **Minor (Opus):** added `test_apply_cdc_event_stale_lsn_quarantines` so the manifest's
    `007_quarantine`-before-`sql/03` ordering is load-bearing, not just asserted in a comment.
  - **Minor (Sonnet):** `tests` service redundantly `pip install`ed the package over the
    network at container start even though the image already has it baked in — dropped.
  - Stale docstring/comment wording fixed in `migrate.py` and `verify.sh`.
- Open issues carried into Step 2: `airflow-init` needs `python -m deriv_pipeline.config
  --validate-all` added back once `deriv_pipeline/config.py` exists.

## Step 2 — config schema

- Built `deriv_pipeline/config.py`: `TableConfig`/`DerivedDimensionConfig`/
  `GeneratedDimensionConfig`/`ReconciliationConfig` dataclasses + `.load()`/`.load_all()`,
  a single enumerate-and-dispatch pass over `config/tables/*.yml` (`_load_all_table_dir`),
  `validate_all()`, and a `python -m deriv_pipeline.config --validate-all` CLI re-added to
  `airflow-init`'s command in `docker-compose.yml` (removed in Step 1 since the module didn't
  exist yet). First real config shipped: `config/tables/vendor_deposits.yml`.
- Tests green: 42/42, locally (venv against a bare `docker compose up -d postgres`) and via
  the containerized `docker compose --profile test run --rm tests` path. `docker compose up
  -d --build` end-to-end: `airflow-init` prints `config validation OK: 1 config(s) —
  vendor_deposits`, scheduler/webserver come up healthy.
- Dual review (Opus + Sonnet) converged independently on the same 3 real bugs, plus Opus
  found a 4th; all fixed:
  - **Major (both reviewers, independently):** `source.natural_key` (and
    `expected_columns`/`update_columns`/`layer3[].columns`) only checked truthiness, not
    type — `natural_key: deposit_id` (missing YAML brackets) is a truthy non-empty string,
    silently accepted, and would later be iterated character-by-character instead of as one
    column name. Added `_require_str_list()`: rejects anything that isn't a YAML list of
    strings, with the file path in the error.
  - **Major (both reviewers, independently):** `LayerTarget(**raw["layer1"])` /
    `Layer3Target(**entry)` unpacked raw YAML dicts straight into dataclass constructors —
    a typo'd key (`conflict_stratgey`) or missing required key raised a bare `TypeError`/
    `KeyError` with no file path, and the CLI's `except Exception: print(str(exc))` discarded
    the traceback entirely, so `airflow-init`'s one-shot log gave no way to find the broken
    file. Wrapped every dataclass construction to re-raise `ValueError(f"{path}: ...")`, and
    the CLI now also prints the full traceback.
  - **Major (Opus):** an unrecognized/typo'd `kind` (`kind: tabel`, or a `.yaml` extension)
    was silently skipped by every one of the three per-kind `load_all()` filters — a broken
    config file produced a green `--validate-all` and a silently-missing DAG, the worst
    failure mode for a config-driven design. Replaced the three independent filter-and-load
    passes with one `_load_all_table_dir()` that reads each file's `kind` once and raises on
    anything outside `{table, derived_dimension, generated_dimension}` — this also fixed
    Sonnet's separately-flagged double-YAML-parse inefficiency as a side effect.
  - **Major (Opus):** `validate_all()` treated zero configs found (missing/misconfigured
    `CONFIG_DIR`, wrong `DERIV_CONFIG_DIR`, empty directory) as success — `airflow-init` would
    print `config validation OK: 0 config(s)` and exit 0. Now raises `ValueError` if
    `config/tables/*.yml` is empty. Also fixed: a missing/misspelled `--validate-all` flag
    used to fall through and exit 0 silently — now exits 2 with a usage message.
  - **Minor (Opus):** `config.py` imported `REPO_ROOT` from `db.py`, which does `import
    psycopg` at module scope — a pure-YAML validation pass needlessly depended on the DB
    driver. `config.py` now computes `REPO_ROOT` itself.
  - **Minor (Opus):** empty/comment-only YAML files raised an opaque `AttributeError`
    (`None.get(...)`). `_load_yaml()` now raises a clear `ValueError` naming the path.
  - **Minor (Opus):** `GeneratedDimensionConfig` didn't check `from_date <= to_date` — an
    inverted range would silently produce an empty `dim_date`. Now raises.
  - New tests added for all of the above:
    `test_rejects_natural_key_as_scalar`, `test_layer_target_typo_raises_value_error_with_path`,
    `test_rejects_unknown_kind_in_table_dir`, `test_validate_all_raises_on_empty_config_dir`,
    `test_rejects_generated_dimension_from_after_to`.
- Open issue noted, not yet fixed (Sonnet, minor, low priority): `config/reconciliations/`
  is currently empty and untracked by git (git doesn't track empty dirs) — a fresh clone will
  simply lack the directory until `vendor_feed.yml` lands in Step 9.
  `ReconciliationConfig.load_all()` already treats a missing directory as "zero results", not
  an error, so this is a documented gap rather than a bug — revisit if Step 9 needs an earlier
  placeholder.

## Step 3 — walking skeleton: vendor_deposits end-to-end

- Built the generic three-layer engine (`deriv_pipeline/layers/{common,layer1_raw,
  layer2_staging,layer3_warehouse}.py`), pure transforms (`deriv_pipeline/transforms.py`:
  `resolve_header` for schema-drift detection, `compute_late_arrival` for the
  filename-derived delivery-date rule), the on-demand `dims/dim_date.py` (reused as-is by
  Step 4's `generated_dimension` engine), the Airflow dynamic-DAG factory
  (`dags/dag_factory.py`, one DAG shape: `land_layer1 >> stage_layer2 >> run_dq_checks >>
  load_layer3`, `run_dq_checks` a no-op placeholder until Step 8), and `scripts/verify.sh`'s
  remaining steps (config validate → pytest → `airflow dags list` → `airflow dags test` x2
  per DAG for idempotency).
- Tests green: 60/60 (added: 6 `test_transforms.py`, 2 `test_dim_date.py`, 7
  `test_vendor_deposits_pipeline.py` integration tests against the real
  `config/tables/vendor_deposits.yml` + real `data/deposits_vendor_*.csv` files, 3
  `test_dag_factory.py` airflow-only tests) — run both locally (venv) and via
  `docker compose --profile test run --rm tests`. Full `scripts/verify.sh` run end-to-end:
  config validation OK, 60/60 pytest, `airflow dags list` shows `table__vendor_deposits`,
  `airflow dags test` run twice both reached `state=success`.
- Dual review (Opus + Sonnet) found 10 + 2 issues (1 overlapping), all fixed:
  - **Most severe (both reviewers, independently):** the on-demand sentinel seed in
    `layer3_warehouse.py::_resolve_risk_snapshot_key` (Step 3's deliberate stand-in for
    ADR-2's real, properly-ordered G2 baseline seed, which lands in Step 4) originally
    blindly inserted a wide-open `1970-9999, is_current=true` row on ANY resolution miss.
    This could (a) collide with/be silently shadowed by Step 4's real ordered baseline seed,
    and (b) even self-violate the "at most one matching row per instant" invariant
    `resolve_risk_snapshot_key`'s un-ordered `LIMIT 1` depends on, since a single Step 3 run
    can see the same client at multiple distinct `deposit_date`s with no real history for
    any of them yet. Fixed: only seed when the client has zero *real* (`source_lsn >= 0`)
    snapshot rows — a miss against real history now raises, signaling "needs a real
    backfill decision" instead of guessing; each stopgap window is the narrowest possible
    (`[event_ts, event_ts + 1us)`, since `event_ts` is always a deposit_date at midnight,
    so distinct dates never overlap and the same date is idempotently reused); each gets its
    own strictly-negative `source_lsn` (`MIN(source_lsn) - 1` per client) so multiple windows
    per client don't collide on `uq_client_lsn`, and Step 4's real bootstrap can find/replace
    every `source_lsn < 0` row per client once it lands. Documented in-line since this is a
    load-bearing design decision, not just a bug fix — see ADR-7.
  - **Opus:** unchecked `None` after the retry `fetchone()` in both `_resolve_client_key` and
    `_resolve_risk_snapshot_key` — added explicit raises instead of a bare `NoneType` crash.
  - **Opus:** `layer2_staging.py`'s upsert overwrote `schema_drift_detected`/`is_late_arrival`
    on conflict instead of OR-combining — a clean redelivery could un-flag a natural key that
    a dirtier earlier delivery had correctly flagged. Now `{table}.{col} OR EXCLUDED.{col}`.
  - **Opus:** `layer2_staging.py`'s `update_columns + [flag columns]` wasn't deduplicated —
    a config listing a flag column in `update_columns` would emit "multiple assignments to
    same column" and fail at the SQL level. Deduplicated via `dict.fromkeys`.
  - **Opus:** `layer1_raw.py`'s `ON CONFLICT DO UPDATE SET payload = EXCLUDED.payload` never
    refreshed `source_file` — harmless today (source_file is part of vendor_deposits' PK) but
    a latent bug for any future single-column-PK raw table. Added
    `source_file = EXCLUDED.source_file`.
  - **Opus:** `staged_at = now()` applied unconditionally, contradicting `part1_pipeline.md`'s
    documented true-no-op-on-redelivery design. Now gated behind an `IS DISTINCT FROM` check
    across every tracked column; a no-op redelivery leaves `staged_at` untouched.
  - **Opus:** `layers/common.py::primary_key_columns()`'s join from
    `information_schema.table_constraints` to `key_column_usage` was missing the
    `table_schema`/`table_name` condition (joined on `constraint_name`/`constraint_schema`
    only) — a real, if currently dormant, cross-table bug. Added the missing join condition.
  - **Opus:** `scripts/verify.sh`'s DAG-test loop could pass vacuously if `airflow dags list`
    errored or returned nothing (`2>/dev/null` plus an empty `for`-loop doesn't trip
    `set -e`). Now captures the dag-id list into a variable and explicitly fails with a clear
    message if it's empty; dropped the `2>/dev/null` swallowing.
  - **Opus:** `test_layer3_inferred_member_pattern_for_orphan_client` only ever exercised the
    "miss → stub" branch of `_resolve_client_key`, never proving the "already exists" branch
    is distinguishable. Now also seeds a real (`is_inferred=false`) `dim_client` row for CL001
    before running layer3 and asserts it's reused as-is (not re-stubbed, not duplicated).
  - **Opus:** `test_full_pipeline_is_idempotent_end_to_end` only compared `count(*)` across
    two runs, which can't catch a second run silently reshuffling FK keys or re-seeding
    sentinels while leaving the row count flat. Now compares a full-content snapshot (every
    fact column, plus `dim_client`/`dim_client_risk_snapshot` counts) across both runs.
  - **Minor:** the hardcoded `date_key == 20240301` literal assertion is now derived from the
    fetched row's own `deposit_date` at test time.
  - **Sonnet (acceptable as scoped per both reviewers, documented not generalized):**
    `fact_upsert()`'s column lists are hardcoded to vendor_deposits'/fact_deposits' shape;
    `Layer3Target.columns` is defined but unused. Added an explicit in-code note on
    `fact_upsert()` itself (not just the module docstring) that this needs to read from
    `layer3_target.columns` before a second `fact_upsert`-strategy table can reuse it.
- Re-ran the full suite (60/60) and `scripts/verify.sh` end-to-end after applying every fix
  above — all green, both `airflow dags test` runs still `state=success`.
- Open issues carried into Step 4: the Step 3 sentinel-seed stopgap leaves `source_lsn < 0`
  rows in `warehouse.dim_client_risk_snapshot` for any client whose deposits had no real
  baseline yet — Step 4's real, ordered ADR-2 `bootstrap_warehouse` baseline seed must find
  and supersede/replace these (by `source_lsn < 0`) rather than assume a clean table, since
  this is a persistent live DB shared across steps.

## Step 4 — dimensions properly + G2 baseline seed

- Added `dims/generated.py` (`dim_date`, reuses Step 3's `dim_date.ensure_date()` over a
  configured date range) and `dims/derived.py` (`dim_manager` from staged `client_signup`,
  `dim_instrument` directly from raw `data/client_trades.json` since that table has no
  `kind: table` config until Step 5). Added `dim_manager.yml`/`dim_instrument.yml`/`dim_date.yml`.
- Extended `config.py`: `Layer3Target.cdc_source_glob`; `DerivedDimensionConfig` gained
  `target_key_column` (required), `raw_source_glob` (alternative to `source_table`, exactly one
  required), `derived_columns` (value-mapping for extra target columns).
- Onboarded `client_signup.yml`/`client_profile.yml` as real `kind: table` configs, both using a
  new **owned-column `dimension_upsert`** strategy into the shared `warehouse.dim_client` —
  each source's `layer3[].columns` lists only the columns it owns; `client_signup` resolves
  `assigned_manager` into `dim_manager`'s surrogate key via a new `fk_resolution` block.
- Added `layers/layer3_warehouse.py::scd2_baseline_seed` — the real ADR-2 G2 baseline: one
  `source_lsn=0`, `valid_from='1970-01-01'`, `is_current=true` row per client_profile-derived
  client, excluding any client whose earliest CDC event (read directly from
  `client_profile_changes.jsonl` via the new pure `transforms.py::earliest_op_per_client()`
  helper) is an `insert` (currently CL030). Also repoints/clears any Step 3 stopgap rows
  (ADR-7) onto the real baseline once it lands (`_repoint_and_clear_stopgap_snapshots`).
- Hand-authored `dags/bootstrap_warehouse.py` (ADR-1): the one DAG expressing cross-config
  topological order `dag_factory.py`'s per-table loop can't — stage client_signup → build
  dim_manager (+ dim_instrument/dim_date, independent) → upsert client_signup's owned columns
  → stage + upsert client_profile's owned columns and seed the real baseline → `bootstrap_complete`
  sentinel for Step 6+'s CDC DAG to depend on.
- Added JSON-array format support to `layer1_raw.py` (previously CSV-only, via a
  `_FORMAT_READERS` dispatch) and generalized `layer2_staging.py::stage()` to work for staging
  tables lacking `schema_drift_detected`/`is_late_arrival` columns (client_signup, client_profile).
- Added `tests/integration/test_dimensions_pipeline.py` (11 tests) plus config/transform unit
  tests. Fixed `scripts/verify.sh` to run `airflow dags test bootstrap_warehouse` (twice) before
  the loop over all `table__*` DAGs, since the standalone `table__client_signup` DAG test needs
  `dim_manager` already populated.
- Full suite: 75/75 passing. `scripts/verify.sh`: PASS end-to-end.

**Dual review (Opus + Sonnet), fixes applied** — see ADR-8 for full detail:
  - NULL FK value crash in `_resolve_dimension_fk` (both reviewers, independently) — fixed to
    return `None` immediately on a `None` input instead of crashing on an always-missing lookup.
  - Missing `cdc_source_glob` validation for `scd2_baseline_seed` at config-load time (Sonnet) —
    fixed: `TableConfig.load()` now raises `ValueError` if absent.
  - TOCTOU gap in `_repoint_and_clear_stopgap_snapshots` (Opus) — fixed: the final DELETE now
    uses the exact key list captured by the initial SELECT, not a re-evaluated predicate.
  - `information_schema.columns`-based fact-table FK discovery could match views or the
    dimension's own PK column (Opus) — fixed: replaced with a `pg_constraint`/`pg_attribute`
    walk for true FKs.
  - **Orphan-client baseline gap (Sonnet #3 / Opus #4, independently convergent, highest
    confidence)** — the first implementation only seeded baselines for clients present in
    `client_profile`, leaving CL099/CL031-shaped orphans permanently stuck on a Step 3 stopgap.
    Fixed generically: `scd2_baseline_seed` now also seeds a sentinel baseline
    (`risk_category='unknown'`, `account_balance_usd=0.00`, `account_status='unknown'`) for any
    client found via "has a stopgap row AND never appeared in this run's client_profile rows,"
    not hardcoded client IDs. Rewrote
    `test_scd2_baseline_seed_gives_orphan_client_sentinel_baseline_and_clears_stopgaps` (was
    `..._leaves_orphan_client_stopgaps_untouched`, which locked in the old, wrong behavior).
  - Missing DAG edge between the client_signup and client_profile branches (Opus) — fixed:
    `bootstrap_warehouse.py` now sequences `upsert_client_signup_dimension >> stage_client_profile`
    instead of running them as independent branches, since both write into the same
    `warehouse.dim_client` row.
  - `@daily` schedule on both new table configs vs. static one-time snapshot files, racing
    against `bootstrap_warehouse`'s hand-ordered sequencing (Opus) — fixed: both configs'
    `orchestration.schedule` changed to `null`.
  - No guard against overlapping real CDC history (Opus High #5, new) — fixed:
    `scd2_baseline_seed` now skips any client with an existing `source_lsn > 0` row, so it
    becomes a safe no-op once Step 6's CDC apply exists rather than creating a second
    overlapping `is_current=true` row.
  - Unconditional `+= 1` counters overstating work done on a no-op rerun (Opus, partial) —
    fixed in both `derived.py::load()` and `scd2_baseline_seed` via `cur.rowcount`/a dedicated
    newly-inserted check.
- **Deferred as accepted limitations** (documented in ADR-8, not fixed this phase): repeated-
  `columns:`-entry dedup guard (Sonnet, no shipped config exercises it); JSON float vs. Decimal
  precision (Sonnet, acceptable at prototype scale); multi-column natural keys / column-name
  collisions across dimensions (Opus, no shipped config needs it); `derived.py` hardcoding the
  `staging.` schema prefix instead of resolving the referenced config's real `layer2.target`
  (Opus, latent forward-compat gap); hard-fail on an unmapped `derived_columns` value (Opus,
  deliberate fail-loud behavior per project convention, not a bug).

## Step 5 — client_deposit.yml, client_trades.yml

- Generalized `layers/layer3_warehouse.py::fact_upsert` off vendor_deposits' Step 3 hardcoded
  column shape (a documented, deliberately-deferred finding) into a fully config-driven
  strategy, via two new `Layer3Target` fields — `event_date_column` (the staging column used
  both for `dim_date` resolution and as the risk-snapshot lookup timestamp) and `literals`
  (target_column → fixed value per row, e.g. `source_system`) — plus extending `fk_resolution`
  entries to accept a dict shape (`from_column`/`dim_target`/`dim_natural_key`/
  `dim_surrogate_key`, reusing `dimension_upsert`'s `_resolve_dimension_fk`) alongside the two
  existing special strings.
- Added `client_deposit.yml` (JSON source, shares `warehouse.fact_deposits` with
  `vendor_deposits` via a `source_system` literal and disjoint `deposit_id` natural keys) and
  `client_trades.yml` (JSON source, new `warehouse.fact_trades` target, resolves
  `instrument_key` via the new dict-shaped `fk_resolution` against `dim_instrument`).
- **Found and fixed via TDD** (not a deferred/documented finding — caught by a failing test in
  this phase's own red-green cycle): `layers/layer2_staging.py`'s header-resolution cache was
  keyed by `source_file`, correct for CSV (one physical header per file) but wrong for JSON,
  which has no shared header line — `client_deposit.json`'s `DEP012` row uses `credit_card`
  where every other row in the same file uses `payment_method`, and the per-file cache silently
  misclassified every row using whichever header was cached first. Fixed by rekeying on
  `frozenset(payload.keys())` (the row's own key set) instead — a strict generalization,
  identical behavior for CSV, correct for JSON's per-row heterogeneity.
- Added `tests/integration/test_client_deposit_and_trades_pipeline.py` (5 tests, incl. the
  vendor/internal `fact_deposits` coexistence check and a NULL-`instrument` negative test added
  post-review) and extended `test_config.py`'s shipped-config name list.
- Full suite: 86/86 passing. `scripts/verify.sh`: PASS end-to-end.

**Dual review (Opus + Sonnet)** — first attempt: both agents hit a session-wide rate limit
(HTTP 429) before producing findings; retried after reset. Second attempt used `isolation:
worktree` for both agents, which silently gave them a checkout with no uncommitted diff (and,
for the Sonnet agent, no `code/` tree at all) — both would have rubber-stamped nothing found
had the Sonnet agent not refused to fabricate a review instead. Retried a third time without
worktree isolation; both agents then reviewed the real diff and converged independently on the
same central finding. **Fixes applied**, see ADR-9 for full detail:
  - **Cross-DAG stopgap-lsn race (both reviewers, independently, highest confidence)** —
    `_resolve_risk_snapshot_key`'s stopgap allocation is a read-then-insert with no lock;
    `client_deposit.yml`/`client_trades.yml` are independent `@daily` DAGs with no edge between
    them, and 17 of ~20 shared `client_id`s have different event dates between the two feeds —
    concurrent runs could both compute the same `stopgap_lsn` and have the loser's window
    silently dropped by `ON CONFLICT DO NOTHING`, then raise a spurious "still unresolved"
    error. Fixed with `pg_advisory_xact_lock(hashtext(...))` serializing the allocation per
    `client_id`, transaction-scoped (no explicit unlock needed).
  - **`bootstrap_warehouse` sequencing reasoning was factually wrong (Opus)** — the premise
    "these tables only read shared dims" is false (`fact_upsert` does insert into `dim_client`
    and `dim_client_risk_snapshot`); it happens to be safe there only because both are
    idempotent `ON CONFLICT DO NOTHING`s. The real gap is `client_trades`' hard, non-creatable
    prerequisite on `warehouse.dim_instrument` (only `bootstrap_warehouse` populates it, which
    is `schedule=None`/manual, with no edge to the `@daily` generated DAGs) — on a fresh deploy
    the first run would raise. Fixed: new `orchestration.requires_dim_instrument: true` flag on
    `client_trades.yml`, honored by `dag_factory.py` via a `PythonSensor` (`mode="reschedule"`)
    polling `dim_instrument`'s existence before `land_layer1` — an `ExternalTaskSensor` wasn't
    viable since `bootstrap_warehouse` has no comparable `execution_date` to match against.
  - **NULL semantics reused from a nullable FK for a NOT NULL one (both reviewers)** —
    `_resolve_dimension_fk`'s `None → None` short-circuit was written for `dim_client
    .manager_key` (nullable); reusing it unguarded for `fact_trades.instrument_key` (NOT NULL)
    meant a trade with a missing instrument silently staged as NULL and crashed later on a bare
    `NotNullViolation`. Fixed: the helper gained a `nullable: bool = True` keyword (default
    preserves existing `dimension_upsert` behavior), and `client_trades.yml`'s `instrument_key`
    rule sets `nullable: false` to raise a clear, diagnostic error instead.
  - **Missing config-load-time validation for `fact_upsert`'s three new shapes (both
    reviewers)** — a missing `event_date_column` crashed with a bare `TypeError` deep in
    `fact_upsert` at DAG runtime; a scalar `literals` crashed with `AttributeError`; a dict
    `fk_resolution` rule missing a key crashed with `KeyError`; a typo'd sentinel string (e.g.
    `inferred_member` instead of `inferred_member_on_miss`) silently flipped behavior with no
    error at all. Fixed: `config.py`'s `TableConfig.load()` now validates all four cases for
    `strategy: fact_upsert` entries at config-load time, matching the existing
    `scd2_baseline_seed`/`cdc_source_glob` pattern. Added 4 negative unit tests.
  - **`insert_cols` built by flat concatenation, not deduped like `source_cols` (Opus)** — any
    config overlap (e.g. a `columns` entry colliding with a `literals` key or a generated key
    name) would emit the same column twice in the INSERT and be rejected by postgres. Fixed:
    `list(dict.fromkeys(...))`, the same idiom already used in `layer2_staging.py` for the same
    reason. Not currently triggered by any shipped config, but the function is now fully generic.
  - Misleading error message: `_resolve_dimension_fk`'s "no such row" raise said
    `"dimension_upsert: ..."` unconditionally, which is wrong when the raise comes from
    `fact_upsert` (Opus) — fixed to a caller-agnostic message.
- **Deferred as accepted limitations** (documented in ADR-9, not fixed this phase): NULL literal
  defeating a fact column's `DEFAULT` (Opus, no shipped config's data exercises it — all-columns
  INSERT behavior may be revisited if a future config needs it); `event_date_column`'s dual use
  for both `dim_date` and the risk-snapshot timestamp silently truncating time-of-day if ever
  pointed at a `timestamptz` column, and the CDC-era one-day-staleness risk that creates once
  Step 6 lands real CDC (Opus, architecturally latent, not reachable by any current config);
  unenforced disjoint-`deposit_id` assumption between `vendor_deposits`/`client_deposit` sharing
  `fact_deposits` (Opus, no shipped data collides, `ReconciliationConfig` exists for this exact
  pair but isn't wired to enforce it until Step 9).

## Step 6 — client_profile_changes.yml (scd2_apply / CDC)

- New `client_profile_changes.yml`, the second and last config-declared exception to a generic
  layer3 upsert (ADR-1/ADR-3): `strategy: scd2_apply` calls the locked `warehouse.apply_cdc_event()`
  (`sql/03`) once per staged row, in true file-arrival order.
- Generalized `layer1_raw.py`/`layer2_staging.py` to handle a second raw-table shape, detected
  generically via `information_schema` column introspection (`"payload" in column_types(...)`),
  never by hardcoded table name: `client_profile_changes` lands as typed columns directly (both
  raw and staging), since `sql/04`'s reload driver (Step 7) needs to read it by real column name,
  unlike every other source's `natural_key + jsonb payload` shape.
- New `raw_seq`/`staging_seq` bigserial ordering columns (migration 010): a plain `SELECT` with no
  `ORDER BY` gives no ordering guarantee, and `apply_cdc_event`'s watermark check is the *sole*
  staleness guard by design (no batch sort) — so the replaying SELECT must preserve true
  file-arrival order or a real out-of-order event could apply in the wrong order. Verified against
  real data: CL001's file order is lsn 1005, then 1004 (stale), then 1006 (applied last, wins).
- New `orchestration.requires_scd2_baseline` flag + `wait_for_scd2_baseline` `PythonSensor`, same
  cross-DAG-race-avoidance shape as Step 5's `requires_dim_instrument`: gates the `@daily`
  `client_profile_changes` DAG behind `bootstrap_warehouse`'s (`schedule=None`) baseline-seed step
  having run first (ADR-2's ordering invariant, re-derived for Step 6 — see ARCHITECTURE_DECISIONS.md
  ADR-10 for why it still matters even though `apply_cdc_event` itself tolerates either order).
- 4 new integration tests against the real shipped `client_profile_changes.jsonl` (out-of-order
  lsn quarantine + file-order apply, tombstone delete, insert-with-no-baseline, replay
  idempotency) + 1 new DAG-shape test. Full suite: 92/92 passing (91 + 1 added post-review).
  `scripts/verify.sh`: PASS end-to-end (config validation, full DAG list, every generated DAG
  including `table__client_profile_changes` run twice via `airflow dags test`).

**Dual review (Opus + Sonnet)**, both directly against the repo (no `isolation: worktree`, per
ADR-9's lesson). Both independently confirmed the same most-severe finding; Opus's pass went
further and caught two additional real bugs Sonnet didn't surface. **All 6 confirmed findings
fixed**, see ARCHITECTURE_DECISIONS.md ADR-10 for full detail:
  - **Unbounded quarantine growth on every rerun (both reviewers, highest confidence, empirically
    reproduced: run1 quarantine=2 rows, run2=14)** — `scd2_apply`'s replay `SELECT` was
    unconditional, re-scanning the *entire* staging table on every call; `apply_cdc_event`'s
    stale-lsn branch is not idempotent (it writes a fresh `quarantine.rejected_rows` row every
    time it's called with an already-applied lsn), so an `@daily` schedule would re-quarantine a
    client's whole history every single day, burying the one real signal (a genuine out-of-order
    event) in permanently-growing replay noise. Fixed: the replay `SELECT` is now filtered by a
    semi-join against `warehouse.cdc_watermark` (`WHERE NOT EXISTS (... s.lsn <= w.last_applied_lsn)`),
    making an already-applied row a true no-op at the Python level. The idempotency test was
    extended to assert on `quarantine.rejected_rows` count too, not just the snapshot table (the
    original version of this test would have passed while this bug happened).
  - **`wait_for_scd2_baseline`'s check was a one-time global existence test, but the invariant is
    per-client and ongoing (Opus)** — `EXISTS(...WHERE source_lsn = 0)` is satisfied forever after
    bootstrap_warehouse's *first* run, even for a client onboarded later with no baseline of their
    own; combined with `scd2_baseline_seed`'s "skip if already has real history" guard, the miss
    would be permanent and unrecoverable through normal operation. Fixed: the sensor now also
    checks, set-based, that every client with a staged CDC event (other than one whose *only*
    event is an `insert`) already has a `source_lsn = 0` baseline row — kept alongside the
    original global check as a floor, since on a fresh deploy `staging.client_profile_changes` is
    still empty when this sensor first runs (this DAG's own `land_layer1`/`stage_layer2` haven't
    executed yet), so the per-client check alone would pass vacuously.
  - **Concurrent DAG runs could crash on the `cdc_watermark` insert (Opus)** — `sql/03`'s
    `SELECT ... FOR UPDATE` takes no lock at all when no watermark row exists yet for a client; two
    concurrent first-applies for the same client could both see `NULL` and collide on the
    watermark `INSERT`'s PK, rolling back the whole run with a raw `UniqueViolation`. Since
    `sql/03` is locked, fixed in `scd2_apply` instead: a per-client
    `pg_advisory_xact_lock(hashtext(...))`, the same pattern already used in
    `_resolve_risk_snapshot_key` for an analogous stopgap race (ADR-9).
  - **A `delete` for a client with zero existing snapshot rows was a silent triple no-op (Opus)** —
    `apply_cdc_event`'s tombstone `SELECT` finds nothing to copy for such a client, so no dimension
    row is written; no quarantine row is written either; yet the watermark still advances,
    contradicting `scd2_apply`'s own "a layer3 row *or* a quarantine row per layer2 row" invariant
    and making the gap unrecoverable by a plain rerun. Fixed: `scd2_apply` now checks explicitly
    for an existing snapshot row before calling `apply_cdc_event` on a `delete`, and quarantines by
    name (`delete_with_no_baseline`) instead of proceeding. New regression test added.
  - **The typed-passthrough branch used `row[c]`, not `.get(c)` (both reviewers)** — a CDC source
    that omits a key entirely (rather than emitting explicit `null`, e.g. many WAL decoders on
    insert) would raise an uncaught `KeyError` and drop the whole batch with nothing landing in
    quarantine; this path has no drift/quarantine handling of its own (unlike the jsonb-payload
    path, which defers to layer2's `resolve_header`). Fixed: `.get(c)`, so a missing key becomes
    `NULL` and is enforced by the column's own constraints if that's wrong, rather than crashing.
  - **No config-load-time validation tied `scd2_apply`'s hardcoded column list, its target, or its
    orchestration flag to the actual config (Opus)** — `scd2_apply` hardcodes
    `client_id, lsn, commit_ts, op, after` and always writes to
    `warehouse.dim_client_risk_snapshot`, but nothing checked `expected_columns` actually included
    those names, that `target` was is that table, or that `requires_scd2_baseline` was set — a typo
    in any of the three would silently NULL the dimension, silently ignore a misconfigured target,
    or silently reintroduce the ADR-2 ordering race. Fixed: `config.py` now validates all three for
    `strategy: scd2_apply` entries at config-load time, matching the existing
    `fact_upsert`/`scd2_baseline_seed` pattern.
- **No findings deferred this phase** — all 6 confirmed findings from both reviewers were fixed
  before considering Step 6 done.
