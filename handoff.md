# Handoff: Self-Healing Pipeline Skills + Fake-Data Test Pipelines

## Goal

Build a set of generic, composable skills under `.agents/skills/` that Bruin Cloud scheduled agents can use to perform routine data-engineering work end-to-end: triaging failures, diagnosing root cause, running backfills, investigating schema drift and data quality, explaining anomalies, opening maintenance PRs, and reporting to Slack. Then create fake-data pipelines that inject realistic issues so the skills can be exercised locally without touching production.

The skills target Bruin CLI + Bruin MCP + Slack + GitHub. They must be industry-agnostic and never assume a specific schema or domain.

## Current State

All 9 skills are written. README is in place. 3 fake-data pipelines are written and validate clean with `bruin validate`. Nothing has been run yet — no `bruin run`, no skill invocation against the fake data, no Bruin Cloud integration test.

The skills are paper-correct (consistent shape, decision trees, guardrails) but unverified against real Bruin output. The Bruin CLI commands referenced (`bruin internal status`, `bruin lineage`, exact flags) are based on my best understanding and will need tightening once a real run surfaces what's actually available.

## Files in Flight

Nothing currently being edited. Last write was `handoff.md` itself.

## Changed

### New: `.agents/skills/` — 9 skill files + README

```
.agents/skills/
├── README.md                              overview, skill map, run shapes, per-skill reference
├── pipeline-triage/SKILL.md               entry-point dispatcher; 9 issue classes; never modifies state
├── pipeline-diagnose/SKILL.md             single-asset forensics; error-pattern library
├── pipeline-backfill/SKILL.md             partition-by-partition reruns; 8 pre-flight checks
├── schema-drift-check/SKILL.md            8 drift types mapped to default responses
├── data-quality-investigate/SKILL.md      9 failure modes; walks failing rows to root cause
├── freshness-sla-check/SKILL.md           7 stale classes incl. weekend/holiday awareness
├── anomaly-investigate/SKILL.md           dimension attribution; never auto-fixes
├── maintenance-pr/SKILL.md                only repo-writing skill; allow-listed change types
└── pipeline-report/SKILL.md               Slack output; severity → format; dedup against last hour
```

Pre-existing: `.agents/skills/create-dashboard/SKILL.md` (untouched).

Every SKILL.md follows the same 7-section contract: When to use → Inputs → Context to gather → Decision tree → Actions & guardrails → Verification → Reporting.

### New: 3 fake-data Bruin pipelines

```
fake-shop/
├── pipeline.yml                                    duckdb-default, daily, start 2026-01-01
├── assets/raw/orders.py                            quality-fail + anomaly + freshness injections
├── assets/raw/products.py                          schema-drift injection (column rename 2026-04-01)
├── assets/raw/requirements.txt                     pandas==2.2.2
├── assets/staging/daily_revenue.sql                breaks at schema drift; has revenue-spike check
└── assets/staging/daily_orders.sql                 anomaly-detection metric surface

fake-iot/
├── pipeline.yml                                    duckdb-default, hourly
├── assets/raw/sensor_readings.py                   type-narrow + impossible-value + late-arriving
├── assets/raw/requirements.txt
└── assets/staging/hourly_sensor_stats.sql          ingest-lag check

fake-webevents/
├── pipeline.yml                                    duckdb-default, daily
├── assets/raw/pageviews.py                         anomaly + new-segment + freshness injections
├── assets/raw/requirements.txt
└── assets/staging/daily_pageviews.sql              enum-only check
```

All injections are deterministic — Python generators seed `random.Random` from `sha256(date)`. Issues are documented at the top of each generator file (date + class + which skill should detect it).

Issue → skill mapping:

| Date | Pipeline | Issue | Detects via |
|---|---|---|---|
| 2026-03-01 | fake-iot | float→int temperature (type-narrowed) | schema-drift-check (escalates) |
| 2026-04-01 | fake-shop | products column rename | schema-drift-check → maintenance-pr |
| 2026-04-10 | fake-iot | temp = 999 (sensor malfunction) | data-quality-investigate (source-bug) |
| 2026-05-10 | fake-iot | readings 1 day late | data-quality-investigate (late-arriving) |
| 2026-05-15 | fake-shop | duplicate order_ids | data-quality-investigate → backfill |
| 2026-05-15 | fake-webevents | Arc browser appears | schema-drift-check (enum-value-added) |
| 2026-05-18 | fake-webevents | ID country pageview spike | anomaly-investigate (single-dim-driver) |
| 2026-05-20 | fake-shop | TR country revenue spike (~4x) | anomaly-investigate + revenue check |
| today, today-1 | fake-shop | empty days | freshness-sla-check |
| today, today-1, today-2 | fake-webevents | empty days | freshness-sla-check |

### Git status

Three new top-level dirs (`.agents/`, `fake-iot/`, `fake-shop/`, `fake-webevents/`) — all currently untracked. Nothing committed in this session. Pre-existing branch: `arsalann/self-healing-pipeline-skills`.

## Failed Attempts

- **Initial validate call** — `bruin validate fake-shop fake-iot fake-webevents` only validated the first pipeline. Had to run each separately. Worth checking whether the CLI supports a multi-pipeline arg or whether the agent needs to loop.
- **`.agents/skills/create-dashboard`** — appeared in the directory listing when I created `.agents/`. Turned out it pre-existed (it was untracked in git status at session start, not created by this session). Did not touch it.

No dead ends in the design itself — the 9-skill atomic shape and `.context/` handoff pattern held up across all skills without needing to refactor any earlier ones. The fake-data pipelines all validate clean on the first write.

## Open Questions / Known Gaps

1. **Exact Bruin CLI commands** — used `bruin internal status` and `bruin lineage` where I have confidence, and stayed vague ("Bruin MCP or CLI equivalent") where the exact command varies. A first real run will tell us which need pinning.
2. **Bruin MCP surface** — skills assume read access to run state, run history, asset definitions, and dependency graph. Haven't confirmed exact MCP tool names.
3. **Slack channel allow-list** — `pipeline-report` references "channels not on the allow-list" but the list itself is not configured anywhere. Needs a config file or env var when wired up.
4. **`.context/` cleanup policy** — findings accumulate. No expiry / rotation defined. Probably fine for testing; will need a sweep policy in production.
5. **DuckDB connection path** — `duckdb-default` points at `duckdb.db` in repo root. If multiple fake pipelines run concurrently they share the same DB file; that's fine for testing but worth flagging.

## Next Steps (recommended order)

1. **Run one pipeline locally** — `bruin run fake-shop --start-date 2026-01-01 --end-date 2026-03-31` (pre-drift window) to confirm the happy path works and produces clean data in DuckDB.
2. **Run through the drift point** — extend to `--end-date 2026-04-15` to force the products column-rename failure on `staging.daily_revenue`. This is the most representative failure mode and a good first target for `pipeline-triage`.
3. **Dry-run `pipeline-triage`** — invoke it against fake-shop's broken state. Watch which Bruin CLI / MCP calls it actually needs vs. what's documented in the skill. Tighten the SKILL.md against reality.
4. **Wire up Bruin MCP** — confirm which MCP tools exist for run state, lineage, and triggers. Update skill `Context to gather` sections with exact tool names.
5. **Test the full chain on one issue** — pick the schema-drift case (most actionable). Run triage → schema-drift-check → maintenance-pr. Verify a real PR opens against the right branch with the right files modified.
6. **Add Slack configuration** — channel allow-list, default channel per severity, on-call rotation source. Without this, `pipeline-report` can't actually post.
7. **Commit and PR** — once at least one end-to-end chain works, commit the skills + fake pipelines on the existing branch (`arsalann/self-healing-pipeline-skills`) and open a PR.

Do not skip step 3 — the skills are written from first principles and will have gaps that only a real run will surface. Tightening them after one real invocation is much cheaper than rewriting after deployment.

## Skill Review Checklist

Purpose: walk through every created self-healing skill one skill/function at a time. If feedback is given, record it under the current skill/function without addressing it immediately. Advance only when the user says: `approved, next.`

Review status values:
- `current` — under review now
- `pending` — not reviewed yet
- `approved` — user approved this item
- `feedback noted` — user gave feedback that has been recorded

### 1. `pipeline-triage`

Skill purpose: entry-point dispatcher that reads current Bruin pipeline state, classifies issues, and routes each issue class to the correct specialist skill. It must not repair anything directly.

- Status: `current`

Functions / features to review:

1. **Use boundary**
   - Status: `current`
   - What it covers: when to use triage, when not to use it, and the rule that every self-healing run starts here.
   - Feedback:

2. **Inputs**
   - Status: `pending`
   - What it covers: `pipeline`, `alert_id`, `since`, and `severity_floor`.
   - Feedback:

3. **Context gathering**
   - Status: `pending`
   - What it covers: Bruin status, recent failures, quality failures, freshness, schema warnings, recent commits, and snapshot caching in `.context/`.
   - Feedback:

4. **Issue classification**
   - Status: `pending`
   - What it covers: the 9 issue classes: `transient`, `source-down`, `schema-drift`, `quality-fail`, `stale`, `anomaly`, `code-regression`, `capacity`, and `unknown`.
   - Feedback:

5. **Routing decision tree**
   - Status: `pending`
   - What it covers: grouping issues by class and invoking specialist skills once per class.
   - Feedback:

6. **Guardrails**
   - Status: `pending`
   - What it covers: read-only behavior, `.context/` snapshot writes, no direct repair, no skipped classification.
   - Feedback:

7. **Verification and reporting**
   - Status: `pending`
   - What it covers: post-specialist re-read, unresolved-route detection, and report payload shape for `pipeline-report`.
   - Feedback:

### 2. `pipeline-diagnose`

Skill purpose: read-only single-asset forensics that gathers logs, dependency state, error patterns, and commits to produce a root-cause hypothesis.

- Status: `pending`

Functions / features to review:

1. **Use boundary**
   - Status: `pending`
   - What it covers: when to diagnose a single asset and when to use triage or another specialist instead.
   - Feedback:

2. **Inputs**
   - Status: `pending`
   - What it covers: `asset`, optional `run_id`, and optional `pipeline`.
   - Feedback:

3. **Context gathering**
   - Status: `pending`
   - What it covers: asset definition, successful/failed run history, error fingerprint, upstream/downstream state, git history, connection health, and resource signals.
   - Feedback:

4. **Error pattern library**
   - Status: `pending`
   - What it covers: common failure fingerprints and their suggested next skills.
   - Feedback:

5. **Decision tree**
   - Status: `pending`
   - What it covers: upstream-first elimination, pattern matching, code-regression check, capacity check, and unknown escalation.
   - Feedback:

6. **Guardrails**
   - Status: `pending`
   - What it covers: read-only behavior, no speculation without evidence, and `.context/diag-*` output.
   - Feedback:

7. **Verification and reporting**
   - Status: `pending`
   - What it covers: diagnosis report shape, recommended next skill, and feedback loop for pattern-library improvement.
   - Feedback:

### 3. `pipeline-backfill`

Skill purpose: safely rerun Bruin assets or pipelines for a scoped time range after a fix, transient failure, or historical-data correction.

- Status: `pending`

Functions / features to review:

1. **Use boundary**
   - Status: `pending`
   - What it covers: valid backfill scenarios and cases that must not be treated as backfills.
   - Feedback:

2. **Inputs**
   - Status: `pending`
   - What it covers: `asset`, `start`, `end`, `reason`, `mode`, and `dry_run`.
   - Feedback:

3. **Pre-flight checks**
   - Status: `pending`
   - What it covers: asset health, range limits, past-only range, upstream coverage, downstream awareness, idempotency, row-count sanity, and connector quotas.
   - Feedback:

4. **Execution plan**
   - Status: `pending`
   - What it covers: dry-run approval flow and one-partition-at-a-time execution.
   - Feedback:

5. **Bruin CLI commands**
   - Status: `pending`
   - What it covers: validation, single-partition runs, downstream propagation, and full refresh commands.
   - Feedback:

6. **Guardrails**
   - Status: `pending`
   - What it covers: auto-allowed scope, approval-required scope, and never-allowed operations.
   - Feedback:

7. **Verification and reporting**
   - Status: `pending`
   - What it covers: partition-level checks, full-range checks, and `.context/backfill-*` report shape.
   - Feedback:

### 4. `schema-drift-check`

Skill purpose: compare declared asset schema to live source schema, classify drift, and either propose maintenance or escalate risky changes.

- Status: `pending`

Functions / features to review:

1. **Use boundary**
   - Status: `pending`
   - What it covers: schema-drift triggers and exclusions for value-quality or missing-row issues.
   - Feedback:

2. **Inputs**
   - Status: `pending`
   - What it covers: `asset`, optional `suspected_column`, and optional `source_connection`.
   - Feedback:

3. **Context gathering**
   - Status: `pending`
   - What it covers: declared schema, live source schema, 100-row sample, source changelog, downstream consumers, and last successful schema.
   - Feedback:

4. **Drift classification**
   - Status: `pending`
   - What it covers: `column-added`, `column-removed`, `column-renamed`, `type-narrowed`, `type-widened`, `nullability-change`, `enum-value-added`, and `cardinality-shift`.
   - Feedback:

5. **Decision tree**
   - Status: `pending`
   - What it covers: schema diffing, risk ordering, PR proposal, escalation, and anomaly rerouting.
   - Feedback:

6. **Guardrails**
   - Status: `pending`
   - What it covers: read-only source behavior, approval for risky changes, and required downstream listing.
   - Feedback:

7. **Verification and reporting**
   - Status: `pending`
   - What it covers: PR verification expectations and `.context/drift-*` report shape.
   - Feedback:

### 5. `data-quality-investigate`

Skill purpose: investigate failed Bruin quality checks by finding failing rows, tracing them to source data, classifying the failure mode, and recommending the next action.

- Status: `pending`

Functions / features to review:

1. **Use boundary**
   - Status: `pending`
   - What it covers: quality-check failures and exclusions for schema, freshness, or new-check design.
   - Feedback:

2. **Inputs**
   - Status: `pending`
   - What it covers: `asset`, `check_name`, and optional `run_id`.
   - Feedback:

3. **Context gathering**
   - Status: `pending`
   - What it covers: check definition, failing rows, row profile, first-failure point, source comparison, recent transform changes, and failure volume.
   - Feedback:

4. **Failure mode library**
   - Status: `pending`
   - What it covers: `source-bug`, `transform-bug`, `late-arriving-data`, `check-too-strict`, `seasonality-miss`, `boundary-condition`, `dedup-window-too-short`, `cardinality-explosion`, and `genuine-regression`.
   - Feedback:

5. **Decision tree**
   - Status: `pending`
   - What it covers: re-running check SQL, profiling, bisecting history, source comparison, and mode classification.
   - Feedback:

6. **Guardrails**
   - Status: `pending`
   - What it covers: read-only investigation, approval for check changes or fix PRs, no row deletion or check silencing.
   - Feedback:

7. **Verification and reporting**
   - Status: `pending`
   - What it covers: fix verification and `.context/dq-*` report shape.
   - Feedback:

### 6. `freshness-sla-check`

Skill purpose: identify stale Bruin assets, infer or read SLAs, classify why data is late, and route or retrigger narrowly when safe.

- Status: `pending`

Functions / features to review:

1. **Use boundary**
   - Status: `pending`
   - What it covers: scheduled freshness checks, stale triage, and exclusions for failed or wrong-valued data.
   - Feedback:

2. **Inputs**
   - Status: `pending`
   - What it covers: optional `pipeline` and `grace_minutes`.
   - Feedback:

3. **SLA source resolution**
   - Status: `pending`
   - What it covers: explicit `meta.freshness`, pipeline schedule, partition cadence, and historical median interval.
   - Feedback:

4. **Context gathering**
   - Status: `pending`
   - What it covers: asset inventory, last success, last attempt, UTC now, upstream state, scheduler state, and source-system status.
   - Feedback:

5. **Stale classification**
   - Status: `pending`
   - What it covers: `upstream-stale`, `source-down`, `scheduler-missed`, `run-stuck`, `attempted-failed`, `genuine-stale`, and `weekend-or-holiday`.
   - Feedback:

6. **Decision tree and deduplication**
   - Status: `pending`
   - What it covers: freshness age calculation, stale filtering, root-cause collapse, and full findings.
   - Feedback:

7. **Guardrails**
   - Status: `pending`
   - What it covers: safe retriggering, approval-required run kills or bulk retriggers, no fabricated SLAs.
   - Feedback:

8. **Verification and reporting**
   - Status: `pending`
   - What it covers: post-retrigger checks and `.context/freshness-*` report shape, including `unmonitored`.
   - Feedback:

### 7. `anomaly-investigate`

Skill purpose: explain a metric spike or dip using baseline comparison, dimension attribution, upstream checks, and code-change review without modifying data.

- Status: `pending`

Functions / features to review:

1. **Use boundary**
   - Status: `pending`
   - What it covers: metric anomalies and exclusions for failed runs, schema issues, missing data, or unverified real-world causality.
   - Feedback:

2. **Inputs**
   - Status: `pending`
   - What it covers: `metric`, `window`, optional `dimensions`, and optional `baseline`.
   - Feedback:

3. **Context gathering**
   - Status: `pending`
   - What it covers: metric history, baseline, magnitude, dimension breakdowns, upstream row counts/distributions, commits, and external calendar.
   - Feedback:

4. **Attribution pattern library**
   - Status: `pending`
   - What it covers: `single-dimension-driver`, `pipeline-double-count`, `pipeline-undercount`, `new-segment`, `lost-segment`, `seasonality-not-modeled`, `definitional-change`, `upstream-distribution-shift`, and `unexplained`.
   - Feedback:

5. **Decision tree**
   - Status: `pending`
   - What it covers: baseline computation, noise threshold, slicing, upstream signals, commit review, attribution coverage, and routing.
   - Feedback:

6. **Guardrails**
   - Status: `pending`
   - What it covers: read-only analysis, no data correction, no unevidenced causality, and no over-attribution.
   - Feedback:

7. **Verification and reporting**
   - Status: `pending`
   - What it covers: downstream verification expectations and `.context/anomaly-*` report shape.
   - Feedback:

### 8. `maintenance-pr`

Skill purpose: open narrowly scoped maintenance pull requests based on finding files from other skills. It is the only repo-writing skill.

- Status: `pending`

Functions / features to review:

1. **Use boundary**
   - Status: `pending`
   - What it covers: finding-gated PR creation and exclusions for feature work, refactors, or behavior changes.
   - Feedback:

2. **Inputs**
   - Status: `pending`
   - What it covers: `finding_file`, optional `branch_name`, and optional `draft`.
   - Feedback:

3. **Allowed change types**
   - Status: `pending`
   - What it covers: column add/rename/remove, type widen/narrow, check threshold adjustment, dedup-window adjustment, dependency bumps, and dead-code removal.
   - Feedback:

4. **Pre-flight checks**
   - Status: `pending`
   - What it covers: finding validation, clean working tree, duplicate PR search, allow-list check, local validation, and secret scanning.
   - Feedback:

5. **PR construction**
   - Status: `pending`
   - What it covers: branch naming, commit message, PR title/body, verification checklist, rollback notes, and wording discipline.
   - Feedback:

6. **Decision tree**
   - Status: `pending`
   - What it covers: finding parse, approval gate, scratch branch edits, validation failure abort, commit, push, and draft PR creation.
   - Feedback:

7. **Guardrails**
   - Status: `pending`
   - What it covers: scoped edits, no direct base-branch pushes, no merge, no non-draft PR without approval.
   - Feedback:

8. **Verification and reporting**
   - Status: `pending`
   - What it covers: PR URL return, CI start note, finding-file annotation, and `pipeline-report` handoff.
   - Feedback:

### 9. `pipeline-report`

Skill purpose: post structured Slack status, incident, or digest messages so humans can understand what the self-healing run did and what still needs attention.

- Status: `pending`

Functions / features to review:

1. **Use boundary**
   - Status: `pending`
   - What it covers: final reporting for every self-healing run, specialist findings, scheduled digests, and escalations.
   - Feedback:

2. **Inputs**
   - Status: `pending`
   - What it covers: `channel`, `severity`, `subject`, optional `source_files`, optional `thread_ts`, and optional `mentions`.
   - Feedback:

3. **Severity format**
   - Status: `pending`
   - What it covers: `info`, `warn`, `error`, and `critical` formatting, mentions, destinations, and no silent severity changes.
   - Feedback:

4. **Incident message structure**
   - Status: `pending`
   - What it covers: subject, pipeline/time/run metadata, what happened, what was done, what needs attention, evidence, and suggested follow-up.
   - Feedback:

5. **Digest message structure**
   - Status: `pending`
   - What it covers: daily/weekly digest format, resolved/open sections, recurring patterns, and severity limits.
   - Feedback:

6. **Decision tree**
   - Status: `pending`
   - What it covers: input validation, source-file summarization, Slack deduplication, thread replies, posting, and finding-file annotation.
   - Feedback:

7. **Guardrails**
   - Status: `pending`
   - What it covers: channel allow-list, on-call mention rules, no secrets/full row dumps, and no dropped duplicate reports.
   - Feedback:

8. **Verification and reporting**
   - Status: `pending`
   - What it covers: Slack permalink, finding-file annotation, acknowledgment tracking, and `.context/reports-*.jsonl` record.
   - Feedback:
