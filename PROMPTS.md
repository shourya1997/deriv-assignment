# AI Prompts Log

This assignment was built through an interactive, question-by-question design interview with
Claude (Claude Code), rather than a single "write the whole thing" prompt. Each section below
records the actual decisions surfaced during that interview: the question posed, the options
considered, the choice made, and — where relevant — the reasoning that overrode the AI's own
recommendation.

---

## Root architecture decisions (apply across all parts)

**Prompt (paraphrased):** "Interview me on the design tree for this assignment before writing
anything — target warehouse platform, orchestration tool, dimensional modeling approach, and
SCD type for the CDC-tracked client attributes."

**AI recommended:** Databricks/Delta Lake + dbt/Airflow + Kimball star + SCD Type 2.

**Decided:**
- **PostgreSQL** as the target warehouse (not Databricks — simpler, matches the
  assignment's DuckDB/Postgres/SQLite tool list, no need for a lakehouse at this data
  volume).
- **Airflow only for now**, dbt deferred to a later phase — plain SQL executed by Airflow
  operators; dbt can be layered on top of the same staging→warehouse SQL without a schema
  change later.
- **Kimball star schema** — accepted the AI's recommendation as-is.
- **SCD Type 2** for `risk_category`/`account_balance_usd`/`account_status` — accepted the
  AI's recommendation as-is, on the reasoning that the CDC data shows multiple same-day
  changes per client (e.g. `CL001` changes three times in one afternoon), which an SCD
  Type 3 "previous value" column can't represent correctly.

---

## Part 1 — Pipeline Design & Reconciliation

**Prompt (paraphrased), idempotency:** "How does the pipeline ensure re-running it doesn't
create duplicates — file manifest, hash, watermark, or merge key?"

**AI recommended:** a file-manifest table *and* natural-key `ON CONFLICT`, layered.

**Decided:** natural key only (`deposit_id` for deposits; `(client_id, source_lsn)` unique
constraint for CDC). Rejected the manifest layer as unnecessary bookkeeping — the natural key
is already sufficient and cheaper to maintain.

---

**Prompt (paraphrased), CDC ordering:** "The CDC log arrives out of LSN order — how do you
guarantee correct apply order?"

**AI recommended:** sort the batch by `lsn` *and* keep a per-entity watermark table, as a
belt-and-suspenders approach.

**Decided:** **watermark only**, no batch-level `ORDER BY`. Explicit reasoning given: sorting
is an expensive operation (full scan + sort every run), while a per-`client_id` watermark
check is an indexed point lookup per row and is correct regardless of arrival order. This is
a deliberate rejection of the AI's "do both" recommendation on cost grounds.

*(Follow-up: asked the AI to explain what an LSN actually is before finalizing this — see
inline explanation in the conversation. Confirmed understanding before locking the
watermark-only decision.)*

---

**Prompt (paraphrased), source-delete handling:** "How do you represent a CDC delete without
destroying history, given the assignment's hard constraint against hard deletes?"

**AI recommended:** end-date the current SCD2 row + insert a terminal tombstone row.

**Decided:** accepted as recommended.

---

**Prompt (paraphrased), architecture layers:** "What do the landing/staging/target layers
look like concretely on Postgres?"

**AI recommended:** a four-schema layout (`raw`, `staging`, `warehouse`, `quarantine`), over
two simpler two- and three-schema alternatives.

**Decided:** accepted as recommended — the dedicated `quarantine` schema directly supports
the assignment's data-quality bonus criterion (differentiated severity + specific
on-failure action, not "log and continue").

---

**Prompt (paraphrased), late/missing data:** "The vendor CSVs arrive late and contain
back-dated records — how does the pipeline self-reconcile without manual intervention?"

**AI recommended:** both a late-arrival flag (per-row) and a scheduled reconciliation job
(catches rows that never arrive at all).

**Decided:** accepted both, since they were shown to catch genuinely different failure
modes — a flag can only fire on data that has arrived, so a job that runs independently of
any given file's arrival is needed to catch data that never shows up.

---

**Prompt (paraphrased), edge cases:** Given the negative deposit in `VDEP001`, the duplicate
`deposit_id`s across vendor files, the `payment_method`→`method` schema drift, and orphan
`client_id`s (`CL099`, `CL031`) with no matching `client_signup` row — which of these should
be formally documented with severity + handling strategy?

**Decided:** all four, plus an additional requirement not originally scoped by the AI: **a
per-table data-quality health-reporting framework** (a `dq_check_results` log table +
`dq_table_health` view), so that data-quality issues are tracked across runs, not just
reported once. This was the user's own addition, layered on top of the four AI-suggested
edge cases.

**Follow-up decision, regression definition:** the AI proposed comparing each run's failure
count against the *immediately preceding* run (not a fixed threshold or a rolling average).
Confirmed as-is — "prior run is fine."

---

## Part 2 — Data Model & Historization

**Prompt (paraphrased), mini-dimension split:** "Should the volatile CDC-tracked attributes
live in the same `dim_client` table as static attributes, or a separate mini-dimension?"

**AI recommended:** split into a separate `dim_client_risk_snapshot` mini-dimension.

**Decided:** accepted as recommended.

---

**Prompt (paraphrased):** "Do a web search of how other companies handle fact-to-SCD2
dimension joins (snapshotted FK vs dynamic range join) and tell me the key takeaways."

**AI searched and reported:** Kimball's own documentation confirms the snapshotted-FK
pattern as standard practice (temporal surrogate keys, "no BETWEEN clause needed"); the
mini-dimension split matches the named pattern **SCD Type 4**; production systems sometimes
go further with **SCD Type 7** (dual surrogate keys: a point-in-time key + a durable
always-current key) for retroactive-analysis use cases.

**Decided:** single snapshotted FK at load time (the simpler Type 4 pattern), not the Type 7
dual-key variant — judged as more complexity than this assignment's scope needs, though
named in the doc as the upgrade path if retroactive fraud re-screening ever needs it.

---

**Prompt (paraphrased), `dim_instrument` attributes:** "Should `dim_instrument` carry an
`asset_class` attribute, given the traded instruments span FX/Commodity/Crypto/Index?"

**AI recommended:** yes, add `asset_class`.

**Decided:** accepted as recommended.

---

**Prompt (paraphrased), late-arriving dimensions:** Re-confirmed the inferred-member pattern
proposed earlier (for `CL099`/`CL031`, deposits with no matching `client_signup` row).

**Decided:** accepted as recommended — inferred-member pattern.

---

**Prompt (paraphrased), degenerate dimensions:** "Should low-cardinality attributes
(`payment_method`, `direction`, `trade_status`, `currency_original`, `assigned_manager`) be
degenerate dimensions or separate dimension tables?"

**AI recommended:** degenerate for all of them except `assigned_manager`, which gets its own
thin `dim_manager` table (a real business entity vs. a fixed-value tag).

**Decided:** accepted as recommended.

---

**Prompt (paraphrased), historical reload mechanism — user pushback:** The AI initially
recommended "Airflow backfill + explicit delete-and-reinsert." The user pushed back: *"Airflow
backfill via logical_date, because ON CONFLICT it will update, think about this"* — arguing
the existing idempotency mechanism should make backfill self-sufficient without extra logic.

**AI re-examined and disagreed, with specifics:** traced through the actual SQL and found the
CDC apply path uses `ON CONFLICT ... DO NOTHING` (not `DO UPDATE`, unlike the vendor-deposit
path), and is additionally gated by the watermark check running *before* the conflict check —
meaning a plain backfill of already-applied `lsn` values is a guaranteed no-op (rejected as
stale), not a self-correcting update. This was flagged back to the user rather than silently
complying with the original ask, since the user's proposed mechanism would not actually
achieve what a "historical reload" is for (fixing wrong data).

**Decided (by the user, after the pushback):** Option B — Airflow backfill **plus** an
explicit per-client watermark/row reset step
([`reset_client_for_reload`](sql/04_backfill_reset.sql)), scoped to the specific client and
`lsn` range rather than a blanket reset, so a correction to one client's history never
disturbs another client's already-correct data.

---

## Part 3 — TL Extension

**Prompt (paraphrased), streaming tool choice:** Re-asked the Round-2 question that had gone
unanswered earlier — Debezium/Kafka/Flink vs. LISTEN/NOTIFY vs. dual-write vs. managed
alternative, for the real-time fraud signal.

**AI recommended:** Debezium → Kafka → Flink/ksqlDB.

**Decided:** accepted as recommended.

---

**Prompt (paraphrased), real-time/batch coexistence:** "Does adding the real-time path
replace/restructure the existing Part 1/2 batch pipeline, or run independently?"

**AI recommended:** Lambda-style — two independent paths reading the same Postgres source
(WAL-tailing for real-time, replica reads for batch), leaving Parts 1-2 unchanged.

**Decided:** accepted as recommended.

---

**Prompt (paraphrased):** "Do a survey of how this is achieved in industry" (re: the
latency/consistency trade-off and synchronous-vs-asynchronous fraud scoring).

**AI searched and reported:** production fraud pipelines on Kafka+Flink report end-to-end
latency consistently under 250-300ms; Lambda architecture is validated as a real
fraud-detection pattern with a "dual-check" property (speed layer scores fast, batch layer
re-audits with richer historical context); research flags a real exploitable gap between
synchronous rule-checks and asynchronous ML scoring in production systems (~280ms window
cited in one paper).

**Decided:** added a **synchronous pre-check layer** (simple threshold rules in the
application write path) alongside the async Flink ML-scoring path — a design element not in
the AI's original proposal, added specifically because the web search surfaced a concrete,
named production risk (the sync/async gap exploit) that the original design didn't address.
Also adopted the "batch layer as fraud re-auditor" framing from the research as an explicit
part of the architecture, not just a coexistence detail.

---

**Prompt (paraphrased), external API consumer:** "What does the partner see, and how is
access controlled, given the Lambda split?"

**AI recommended:** partner reads from the batch warehouse only (never the real-time path or
OLTP primary), with row-level security scoped to the partner's own clients.

**Decided:** accepted as recommended, confirmed scope (partner sees only their own referred
clients, not all clients).

---

**Prompt (paraphrased), build vs buy:** Initial AI recommendation was "buy the extraction
(Fivetran-style), build the rules."

**User corrected the AI's tooling assumption:** "we can use something like Meltano, which is
[an] open source ingestion framework. Fivetran is very expensive... [use it] if all of the
input criteria to ingest data... are supported out-of-the-box... For data quality... I'll use
a tool called Great Expectations... this will help us standardize the type of tests... buying
or building entirely depends on what features are we getting out of the box."

**AI incorporated the correction and asked one clarifying question:** whether Great
Expectations should also replace Part 1's already-built custom `dq_check_results` framework,
or apply only to the new processor being onboarded in Part 3b.

**User decided:** replace Part 1's framework too, for consistency across the whole pipeline —
this required retrofitting `part1_pipeline.md` §5 (GE Expectation Suites now define the
checks; the Postgres `dq_check_results` table is kept, but repurposed as the regression/health
trend log fed by GE's validation results, rather than the primary check-definition mechanism).

**Final recommendation, as revised by the user's correction:** Meltano for extraction
(conditional on zero-engineering fit for this specific processor's auth/protocol) + Great
Expectations for validation (same framework as Part 1, not a new one per source) + fully
custom build for the staging/reconciliation logic regardless of extraction tooling.
