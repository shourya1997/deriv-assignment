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

## Step 7 — historical reload (cdc_historical_reload DAG)

- New `deriv_pipeline/reload.py`: `historical_reload(conn, from_date, to_date)` re-issues `sql/04`'s
  own driver query (`SELECT client_id, MIN(lsn) ... GROUP BY client_id`, bound to runtime params,
  never hardcoded to November) and, per affected client, calls the locked
  `warehouse.reset_client_for_reload()` then replays `raw.client_profile_changes` from the reset
  point through `apply_cdc_event()` **in lsn order** — deliberately different from `scd2_apply`'s
  streaming file-arrival-order replay (ADR-11), and exactly what lets a reload permanently repair a
  version the streaming apply's watermark-only, no-sort design drops forever (real case: CL001's
  file order is lsn 1005 then 1004 — quarantined as stale by `scd2_apply` — then 1006; a reload
  over CL001's window inserts 1004 as a real non-current row without changing the final current
  state).
- Same per-client `pg_advisory_xact_lock(hashtext('cdc_apply:' || client_id))` `scd2_apply` already
  takes, so a reload can never interleave with a concurrent streaming apply for the same client.
- New `dags/cdc_historical_reload.py`: hand-authored (no `kind: table` config a reload could be
  generated from — it iterates clients, not one table), `schedule=None` (operator-triggered repair,
  not recurring ingestion), `from_date`/`to_date` as Airflow `Param`s, `max_active_runs=1`.
- 6 new integration tests against real shipped data (repairs the dropped stale lsn version, keeps
  an already-deleted client deleted — strengthened to assert the key itself changed, not just the
  final flags — window with no overlap touches nothing — strengthened to assert full warehouse
  state via a new `dump_state()` helper, not just the return value — idempotent rerun, and two new
  FK-repoint tests added post-review, below) + 1 new DAG-shape test. Full suite: 99/99 passing
  (97 + 2 added post-review). `scripts/verify.sh`: PASS end-to-end against the live `deriv` DB,
  both `cdc_historical_reload` runs returning identical
  `{'clients_reset': ['CL001', 'CL002', 'CL009', 'CL012', 'CL014', 'CL019', 'CL022', 'CL025'],
  'events_replayed': 11}` and now asserted equal via a new `dump_state()`-based SQL-level diff, not
  just a bare exit code (see F4 below).

**Dual review (Opus + Sonnet)**, both directly against the repo (no `isolation: worktree`, per
ADR-9's lesson). Sonnet found 1 bug also independently found by Opus; Opus found 3 more beyond
that. **All 4 confirmed findings fixed**, see ARCHITECTURE_DECISIONS.md ADR-11 for full detail:
  - **F2 — a `delete` for a client with zero existing snapshot rows was a silent no-op that still
    advanced the watermark (both reviewers)** — the exact `delete_with_no_baseline` bug `scd2_apply`
    already guards against (Step 6), re-drifted into `reload.py`'s first draft because the replay
    loop called `apply_cdc_event` unconditionally. Fixed with the identical guard: check for an
    existing `dim_client_risk_snapshot` row before a `delete` replay; on a miss, quarantine
    (`delete_with_no_baseline`, `severity='WARNING'`) and skip instead of calling `apply_cdc_event`.
  - **F1 — `reset_client_for_reload`'s unconditional DELETE can raise `ForeignKeyViolation` against
    `fact_deposits`/`fact_trades` (Opus, highest severity)** — both fact tables FK into
    `dim_client_risk_snapshot` with no `ON DELETE` clause (`sql/02_facts.sql`), and the delete
    removes every row with `source_lsn >= reset_from_lsn` — including a client's *current* row,
    which any of their fact rows dated after the CDC window would already be FK'd into. Latent in
    the shipped demo data purely because every CDC-affected client's fact rows predate all CDC
    activity, so they always resolve to the baseline row. Fixed: new `_repoint_facts_for_reset()` /
    `_reresolve_facts_after_reload()` in `reload.py`, built on a new shared
    `layers/common.fk_columns_into()` helper (extracted from `layer3_warehouse.py`'s pre-existing
    stopgap-repoint code, which now calls the same helper instead of duplicating the `pg_constraint`
    walk) and a new config-driven `config.fact_event_date_columns()` map. Before the reset, any fact
    row FK'd into a doomed key is repointed onto the row that will be reactivated below
    `reset_from_lsn`, or — when the client's *entire* history falls inside the window and nothing
    survives to reactivate — a temporary sentinel row using the same negative-`source_lsn`/
    `is_current=false` stopgap convention `_resolve_risk_snapshot_key` already established (ADR-7).
    After replay, each repointed fact row is re-resolved via `warehouse.resolve_risk_snapshot_key()`
    against its own event-date column, and the sentinel row (if created) is deleted once nothing
    references it. Two new tests added: a fact row FK'd into CL001's current version across a
    reload, and the deep combined case (a synthetic CDC-only client, no baseline at all, whose sole
    version is both doomed and fact-referenced) forcing the sentinel-row path specifically.
  - **F3 — deadlock risk between a reload run and a concurrent streaming `scd2_apply` run (Opus)** —
    `historical_reload` held every affected client's advisory lock until one final whole-transaction
    commit, and its driver query's `GROUP BY` gave no ordering guarantee, so two runs could acquire
    the same set of per-client locks in different orders. Fixed with `ORDER BY client_id` on the
    driver query (deterministic acquisition order) plus `max_active_runs=1` on the DAG (removes the
    cross-run case entirely rather than requiring an operator runbook caveat).
  - **F4 — `verify.sh`'s two `cdc_historical_reload` idempotency runs asserted nothing beyond exit
    code 0 (Opus)** — a broken run that still exited 0 (e.g. one that silently touched the wrong
    client) would have passed. Fixed: new `reload.dump_state()` (a deterministic snapshot of every
    `dim_client_risk_snapshot` row and every fact row's resolved FK) invoked via a new
    `python -m deriv_pipeline.reload --dump-state` CLI entrypoint, diffed between the two runs.
- **No findings deferred this phase** — all 4 confirmed findings from both reviewers were fixed
  before considering Step 7 done.

## Step 8 — Great Expectations (one real suite) + SQL-assertion DQ for the rest

- Scope narrower than the plan's prose suggested: `data_quality.dq_check_results` (the log table)
  and `data_quality.dq_table_health` (the regression VIEW, `LAG(failed_checks) OVER (PARTITION BY
  table_name ORDER BY run_at)`) already existed from an earlier gap-fill migration
  (`008_data_quality.sql`) — Step 8's job was to populate the log with real check executions, not
  build the regression view.
- New `deriv_pipeline/dq.py`: `run_dq_checks(cfg, conn, run_id)` dispatches to `run_ge_suite`
  (vendor_deposits only, via GE 0.18.22's ephemeral `SqlAlchemyExecutionEngine` DataContext —
  verified against the real API with a live smoke test inside the `airflow-scheduler` container,
  per ADR-4/ADR-5's GE-version-coupling risk) or `run_sql_assertions` (config-declared SQL, every
  other table). Both paths log one `data_quality.dq_check_results` row per check and, on a failed
  CRITICAL check, insert one summary row into `quarantine.rejected_rows` (no per-row natural key to
  target for a table-level assertion, unlike CDC's per-row rejects).
- Wired into `dags/dag_factory.py`: replaced the `_noop_dq_checks` placeholder (already wired into
  every generated DAG's `run_dq_checks` task since an earlier phase) with `_run_dq_checks(cfg,
  **context)`, reusing Airflow's own injected `context["run_id"]` as the shared run id — no new
  XCom plumbing needed.
- Config schema (`config.py`): new `DqCheck` dataclass (`name`/`sql`/`severity`), `LayerTarget.
  dq_checks: list[DqCheck]`. Strict validation: must live under `layer2` (the only place `dq.py`
  reads), must be a list of dicts, `name`/`sql` must be strings, `severity` must be one of
  `INFO`/`WARNING`/`CRITICAL`, no duplicate names — every failure raises `ValueError` naming the
  config file's path. Added `dq_checks:` blocks to `client_deposit.yml`, `client_trades.yml`,
  `client_profile.yml`, `client_signup.yml`, `client_profile_changes.yml` (`vendor_deposits.yml`
  already had `ge_suite: staging_vendor_deposits` from an earlier phase). Every check's SQL is
  NULL-safe (`WHERE x IS NULL OR x <cmp> ...`) since the columns involved are all nullable.
- **Decision: a failed CRITICAL check is logged + quarantined but does not raise or block
  `load_layer3`.** `part1_pipeline.md`'s own example implies per-row exclusion from
  `fact_deposits`, which needs GE's `unexpected_index_list`/natural-key info threaded into
  `layer3_warehouse.py`'s load query — real work outside Step 8's scope. More importantly, the real
  shipped `vendor_deposits` data has a *permanent* negative-amount row (`VDEP001`/`CL003`/
  `-250.00`); raising on any CRITICAL failure would make `run_dq_checks` fail forever for that
  table, and `scripts/verify.sh`'s `set -euo pipefail` + `airflow dags test table__vendor_deposits`
  step would abort every single run — permanently breaking the "make verify green" gate this
  entire project's per-phase ritual depends on. Documented as an explicit `# ponytail:` corner-cut
  in `dq.py`'s module docstring (table-level block/skip only, not per-row exclusion; upgrade path:
  teach `run_dq_checks` to return failing natural keys, teach `layer3_warehouse` to filter them out
  of its load query). See ARCHITECTURE_DECISIONS.md ADR-12.
- 15 new tests (`tests/unit/test_config.py`: 5 for `dq_checks` schema validation;
  `tests/integration/test_dq_checks.py`: 4, incl. the one real GE suite against real
  vendor_deposits data, confirming the known negative-amount row trips the CRITICAL expectation and
  gets quarantined without raising; `tests/integration/test_dq_table_health.py`: 3, confirming the
  pre-existing regression view's `LAG`-based semantics with synthetic run pairs, since `dq.py`
  itself never reads this view). Full suite: 111/111 passing. `scripts/verify.sh`: PASS end-to-end
  against the live `deriv` DB, `table__vendor_deposits`'s `run_dq_checks` task succeeding (not
  raising) despite the real CRITICAL failure, `load_layer3` still running after it.

**Dual review (Opus + Sonnet)**, both directly against the repo (no `isolation: worktree`, per
ADR-9's lesson). Confirmed findings fixed:
  - **NULL-blindness in SQL assertions (Opus, must-fix)** — a bare `WHERE amount_usd <= 0`
    evaluates to `NULL` (not counted as failing) for a `NULL` amount_usd, and the staging columns
    involved are all nullable. Fixed: rewrote every check to `WHERE x IS NULL OR x <cmp> ...`.
  - **`dq_checks` under the wrong label silently produces zero DQ coverage (Opus, must-fix)** —
    `dq.py` only ever reads `cfg.layer2.dq_checks`; placing the block under `layer1`/`layer3` in a
    config would pass validation and just never run. Fixed: `_build_dq_checks` now rejects
    `dq_checks` declared anywhere but `layer2`.
  - **Config validation gaps (both reviewers, must-fix)** — non-list `dq_checks`, non-dict entries,
    non-string `name`/`sql`, unknown `severity` values, and duplicate `name`s within one table's
    list all silently passed through. Fixed: each now raises `ValueError` naming the config path.
  - **Test-DB pollution (Sonnet, must-fix)** — `conftest.py`'s `db_conn` fixture only rolls back on
    teardown; GE needs a separate connection to see staged rows, and `dq_table_health`'s tests need
    each synthetic run committed separately (Postgres `now()` is fixed for a transaction's life),
    so both new integration test files write real commits the fixture can't clean up. Fixed with
    explicit `_purge_run()`/`_purge()` helpers (`DELETE ... WHERE run_id/table_name = ...` +
    commit) in `try/finally` blocks.
  - **GE suite keyed by position, not by expectation type (both reviewers, nice-to-have)** — the
    original `_GE_SUITES` design zipped `validator.validate().results` against a positional list,
    fragile if GE ever reorders results. Fixed: keyed by `expectation_type` instead.
  - **"Raise on CRITICAL failure" (Opus, must-fix as originally stated) — applied, then reverted.**
    I applied it first (matching `part1_pipeline.md`'s literal wording and Opus's finding), updated
    the corresponding test to `pytest.raises(...)`, and confirmed the full suite still passed. Only
    then, working through `scripts/verify.sh`'s `set -e` semantics against the real permanent
    vendor_deposits edge case (above), did I catch that this would break `verify.sh` forever —
    reverted to the non-raising, log+quarantine-only design with the `# ponytail:`-documented
    corner-cut instead. Not a reviewer-caught bug; a design conflict I found myself while verifying
    the reviewers' own suggested fix against the project's own hard gate.

## Step 9 — reconciliation (vendor_feed)

- Redesigned the Step 2 `ReconciliationConfig` stub (`name`/`left`/`right`/`key`/
  `compare_columns`, `left`/`right` bare table names, `key` a single string) — that schema never
  matched real semantics, since vendor (`VDEP###`) and internal (`DEP###`) deposit IDs are
  disjoint namespaces. New shape: `name: str`, `key: list[str]`, `left: str`, `right: str`, where
  `left`/`right` are each a complete SQL SELECT returning the `key` columns, in order, for one
  source — reuses Step 8's `dq_checks` convention of config-declared SQL rather than teaching the
  engine generic join construction for a design with exactly one real instance.
- New `deriv_pipeline/recon/reconcile.py`: `run_reconciliation(cfg, conn, run_id)` reads both
  sides via `cfg.left`/`cfg.right`, diffs key sets in both directions (`missing_right`/
  `missing_left`), writes each discrepancy to the pre-existing (gap-fill migration 009)
  `data_quality.reconciliation_discrepancies`, and reuses `dq.log_check` (Step 8) with
  `table_name=cfg.name` to get `dq_table_health` regression coverage "for free" — zero changes to
  Step 8's already-committed code or the view, per `part1_pipeline.md`'s claim that reconciliation
  is "surfaced in the dq_table_health view."
- New real config `config/reconciliations/vendor_feed.yml`: both sides join `fact_deposits` to
  `dim_client` (for `client_id` — `fact_deposits` only has the surrogate `client_key`), filtered
  by `source_system = 'vendor'`/`'internal'`.
- Wired into `dags/dag_factory.py` via the same generation pattern as the table-DAG loop:
  `ReconciliationConfig.load_all()` → one `reconcile_{name}` DAG per config, single task calling
  `reconcile.run_reconciliation`, reusing Airflow's own `context["run_id"]` exactly like
  `_run_dq_checks`. **Deliberately no cross-DAG sensor** gating this on the table DAGs having run
  first (unlike `requires_dim_instrument`/`requires_scd2_baseline`) — `scripts/verify.sh`'s own
  DAG-test ordering already runs `bootstrap_warehouse → table__* → reconcile_vendor_feed →
  cdc_historical_reload`, matching the plan's stated verification order; judged unrequested
  complexity for a non-blocking reporting job in this prototype (both reviewers treated this as a
  defensible judgment call, not a bug).
- `tests/integration/test_reconciliation.py`: the real risk here (stated explicitly in the plan)
  is that vendor and internal deposits share **zero** natural-key matches in the shipped data, so
  "discrepancies is non-empty" alone would pass even for a broken engine. The real test instead
  asserts the exact symmetric difference recomputed independently from the DB (not from `cfg.left`/
  `cfg.right` themselves, to avoid a tautological test), plus a positive control: clone one real
  internal row into a matching vendor row and assert it's correctly excluded — which also shifts
  the expected discrepancy count by exactly one (my first draft's `expected = |vendor|+|internal|`
  computed *before* the clone was off by one; fixed by recomputing `vendor_keys ^ internal_keys`
  *after* the clone, and by asserting each `by_type` count against the correct set difference, not
  the full side's count).
- Full suite: 113/113 passing. `scripts/verify.sh`: PASS end-to-end, including a new
  `reconcile_vendor_feed` run-twice block between the `table__*` loop and `cdc_historical_reload`.

**Dual review (Opus + Sonnet)**, both directly against the repo (no `isolation: worktree`, per
ADR-9's lesson). Both independently converged on the same idempotency bug; each also found
issues the other didn't. **All applied fixes below**, see ARCHITECTURE_DECISIONS.md ADR-13 for
full detail:
  - **Non-idempotent re-run under the same run_id (both reviewers, highest confidence — and
    demonstrated live by `verify.sh`'s own new idempotency block)** — `run_reconciliation` only
    ever INSERTed; `airflow dags test <dag> 2024-03-01` derives a deterministic run_id from the
    execution date, so the "run 2, idempotency" block in `verify.sh` silently doubled every
    discrepancy row (and the `dq_check_results` row) under the same run_id, with nothing
    asserting row counts to catch it. Fixed: `run_reconciliation` now `DELETE`s any existing
    `(reconciliation_name, run_id)` rows before inserting this run's discrepancies.
  - **`left`/`right` accepted any YAML value, including `None` (both reviewers)** — only presence
    was checked, not type; a malformed/empty `left:` value passed `--validate-all` cleanly and
    only surfaced as an opaque `psycopg`/`TypeError` inside the Airflow task. Fixed:
    `ReconciliationConfig.load()` now raises `ValueError` naming the path if `left`/`right` aren't
    strings, matching every other risky field in `config.py`.
  - **`ReconciliationConfig.load_all()` silently returned `[]` for a missing directory (Opus)** —
    reintroduced the exact "fake 0-configs success" bug `_load_all_table_dir`/`validate_all`'s own
    docstrings cite as a prior fix (Step 2); a vanished `config/reconciliations` bind mount would
    give a green `--validate-all` and silently stop reconciliation forever. Fixed: now raises,
    matching the table-dir loader's fail-fast behavior.
  - **No cross-directory name-collision guard between table configs and reconciliation configs
    (Sonnet)** — `log_check` writes into `dq_check_results.table_name` using `cfg.name`, and that
    column is shared with real tables; nothing stopped a future reconciliation config from sharing
    a name with a table config, silently merging their `dq_table_health` regression buckets.
    Fixed: `validate_all()` now raises on any duplicate name across both.
  - **Set-based key comparison collapses duplicate key tuples (Opus)** — a double-posted vendor
    deposit (same client/date/amount, different `deposit_id`) reconciles clean instead of flagging
    a multiplicity mismatch, since `_key_tuples` returns a `set`. No shipped data exercises this
    (verified: 22 vendor / 20 internal rows, no duplicate key tuples either side). Documented as a
    deliberate `# ponytail:` corner-cut in `reconcile.py` rather than rewritten to a
    `collections.Counter`-based multiset diff — upgrade path named in the comment.
- **Not fixed, judged acceptable**: `key`-list-vs-SELECT-column-order mismatch is unvalidated
  beyond `zip(..., strict=True)`'s count check (Opus) — an order mismatch (not a count mismatch)
  would silently mislabel `natural_key` jsonb with no runtime signal; the column-order contract is
  already documented in `ReconciliationConfig`'s docstring, and building generic validation for it
  would mean parsing/introspecting the config's own arbitrary SQL — judged out of scope for a
  one-instance config. `dq_check_results` duplicating on a same-run_id re-run for *table* DQ
  checks (Sonnet, noted as pre-existing) — `dq.log_check` itself is unchanged, insert-only Step 8
  code already exercised by every `table__*` DAG's existing run-twice block in `verify.sh`; not a
  regression introduced by Step 9, out of this phase's scope.

## Step 10 — DAG factory completeness + auto-generation guardrail tests

- Scope per the plan: the four named checks. Two already existed from earlier phases
  (`test_no_dag_import_errors`, `test_dags_dir_contains_only_factory_and_named_hand_authored_files`)
  — this phase's real work was the two that didn't:
  `test_dag_factory_generates_one_dag_per_table_config` (+ a reconciliation-config counterpart, not
  named in the plan but the same class of gap after Step 9 added a second `load_all()`-driven DAG
  loop) and `test_generated_task_dependencies_are_layer1_2_3_order`, generic across every shipped
  `kind: table` config rather than the handful spot-checked by name in pre-existing tests.
- This was meant to be a thin verification pass over already-correct code (built incrementally
  across Steps 3-9), not a rewrite — and that held: no `dag_factory.py` bug was found, only test
  gaps and one real config-validation gap (below).
- Full suite: 116/116 passing. `scripts/verify.sh`: PASS end-to-end.

**Dual review (Opus + Sonnet)**, both directly against the repo (no `isolation: worktree`, per
ADR-9's lesson). **All confirmed findings fixed:**
  - **Vacuous-pass risk in the new dag-count tests (Opus)** — `generated == expected` where both
    sides derive from the same `load_all()` call proves "the factory looped over whatever
    `load_all` returned," not "the expected tables exist"; a broken/empty config dir would make
    both sides `set()` and pass. Fixed: added `assert len(cfgs) == len(expected) > 0` pinning both
    a non-empty count and (see next finding) no name collision.
  - **Set comparison can't catch a duplicate config `name:` (Sonnet)** — two `kind: table` YAML
    files sharing a `name:` would have the second silently overwrite the first in
    `dag_factory.py`'s `globals()[f"table__{cfg.name}"] = ...` (one DAG lost, no error), but the
    expected-set comprehension collapses the duplicate into one string too, so `generated ==
    expected` would still hold. Fixed by the same `len(cfgs) == len(expected)` check above — a
    duplicate name makes the two lengths diverge.
  - **Vacuous-pass risk in the new ordering test (Opus)** — a zero-config `TableConfig.load_all()`
    would make the `for` loop body never execute, "passing" a broken factory. Fixed: added
    `assert table_cfgs` before the loop.
  - **Sensor wiring only checked downstream of `land_layer1`, not that a leading sensor is
    actually connected to it (Opus)** — a config whose sensor is constructed but whose `sensor >>
    land_layer1` edge got dropped in a refactor would leave a dangling root task and still pass;
    only the two named per-table tests pin this, not the generic loop. Fixed: the ordering test
    now also asserts `land_layer1`'s upstream task set equals exactly the DAG's non-core tasks
    (i.e. any sensor present, and nothing unexpected).
  - **`requires_dim_instrument` has no config-load-time enforcement, unlike its `scd2_apply`/
    `requires_scd2_baseline` sibling (Opus)** — `config.py` already makes
    `requires_scd2_baseline: true` mandatory whenever `layer3.strategy: scd2_apply` is declared
    (Step 6), precisely because a missing/typo'd flag silently drops the sensor and reintroduces
    the Step 5 cross-DAG race — but the identical hazard for `fk_resolution` rules targeting
    `warehouse.dim_instrument` was unguarded. Fixed: `TableConfig.load()` now raises if any
    `fact_upsert` `fk_resolution` dict rule has `dim_target: warehouse.dim_instrument` without
    `orchestration.requires_dim_instrument: true` — the exact symmetric check.
- **Minor, noted not changed**: pre-existing tests' `list(x.downstream_task_ids) == [...]` pattern
  relies on `downstream_task_ids` (a set) happening to iterate as a single-element list — harmless
  while there's exactly one downstream task, not worth churning the already-passing tests that use
  it (Opus, minor). New tests use `set(...) == {...}` instead.
