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
