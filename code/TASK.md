# code/ build — phase checklist

Source of truth for "what phase am I on." Read this first in a fresh session, not scrollback.
Full spec: `/Users/shouryasengupta/.claude/plans/compiled-wobbling-quilt.md`.

- [x] Step 0 — pivot logged in PROMPTS.md, git root confirmed, TASK/PROGRESS/ARCHITECTURE_DECISIONS.md created
- [x] Step 1 — harness: compose file, Dockerfile, db.py, migrate.py, gap-fill migrations
- [x] Step 2 — config schema (fixture-driven tests) + one real config (vendor_deposits.yml)
- [x] Step 3 — walking skeleton: vendor_deposits end-to-end (layer1→2→3, one generated DAG, `make verify` green)
- [x] Step 4 — dimensions properly (dim_manager/dim_instrument/dim_date, client_signup/client_profile) + G2 baseline seed
- [x] Step 5 — add client_deposit.yml, client_trades.yml
- [x] Step 6 — add client_profile_changes.yml (scd2_apply / CDC)
- [x] Step 7 — historical reload (cdc_historical_reload DAG)
- [x] Step 8 — Great Expectations (one real suite) + SQL-assertion DQ for the rest
- [ ] Step 9 — reconciliation (vendor_feed)
- [ ] Step 10 — DAG factory completeness + auto-generation guardrail tests
- [ ] Step 11 — docs (code/README.md, root README.md status, CLAUDE.md note)

Per-phase ritual (every step above): update tracker artifact "in progress" → implement TDD →
dual review (Opus + Sonnet) → apply confirmed findings → update PROGRESS.md/TASK.md/
ARCHITECTURE_DECISIONS.md → git commit (in `deriv-assignment`, never parent `Vibe/`) → update
tracker artifact "done" with commit hash → compact context.
