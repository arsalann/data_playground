# Handoff: Self-Healing Pipeline Skills + Fake-Data Test Pipelines

## Goal

Build a set of generic, composable skills under `.agents/skills/` that Bruin Cloud scheduled agents can use to perform routine data-engineering work end-to-end: triaging failures, diagnosing root cause, running backfills, investigating schema drift and data quality, explaining anomalies, opening maintenance PRs, and reporting to Slack. Then create fake-data pipelines that inject realistic issues so the skills can be exercised locally without touching production.

The skills target Bruin CLI + Bruin MCP + Slack + GitHub. They must be industry-agnostic and never assume a specific schema or domain.

## Current State

All 9 skills are written. README is in place. 3 fake-data pipelines are written and validate clean with `bruin validate`. Nothing has been run yet — no `bruin run`, no skill invocation against the fake data, no Bruin Cloud integration test.

The skills are paper-correct (consistent shape, decision trees, guardrails) but unverified against real Bruin Cloud output. Bruin Cloud command references have been tightened against docs/source where possible, but a real Cloud MCP/CLI run is still needed to validate response shapes, URLs, and any MCP-only actions.

Update 2026-05-26: review is paused at `data-quality-investigate` → `Verification and reporting`. Feedback from reviewed skills has been applied to the skill files through this point, plus global Cloud/MCP rules were propagated across all 9 skills. Operational runs are now documented as Bruin Cloud-only; local `bruin run` is explicitly forbidden for these skills except fake-data local testing.

Update 2026-05-26 later: full skill review is complete. Remaining feedback from `freshness-sla-check` and `maintenance-pr` has been applied to skill files and the skills README.

## Files in Flight

Review paused after `data-quality-investigate` → `Verification and reporting`. Skill files and `handoff.md` have been updated to address feedback; no tests or Cloud skill invocations have been run after those edits.

## Changed

### New: `.agents/skills/` — 9 skill files + README

```
.agents/skills/
├── README.md                              overview, skill map, run shapes, per-skill reference
├── pipeline-triage/SKILL.md               entry-point dispatcher; 9 issue classes; never modifies state
├── pipeline-diagnose/SKILL.md             single-asset forensics; error-pattern library
├── pipeline-backfill/SKILL.md             Cloud interval reruns/triggers; materialization and reversibility pre-flight checks
├── schema-drift-check/SKILL.md            8 drift types mapped to default responses
├── data-quality-investigate/SKILL.md      9 failure modes; walks failing rows to root cause
├── freshness-sla-check/SKILL.md           7 stale classes incl. weekend/holiday awareness
├── anomaly-investigate/SKILL.md           dimension attribution; never auto-fixes
├── maintenance-pr/SKILL.md                only repo-writing skill; allow-listed change types
└── pipeline-report/SKILL.md               Slack output; severity → format; dedup against last hour
```

Pre-existing: `.agents/skills/create-dashboard/SKILL.md` (untouched).

Every SKILL.md follows the same 7-section contract: When to use → Inputs → Context to gather → Decision tree → Actions & guardrails → Verification → Reporting.

### Updated from review feedback on 2026-05-26

Files updated:

```
.agents/skills/README.md
.agents/skills/pipeline-triage/SKILL.md
.agents/skills/pipeline-diagnose/SKILL.md
.agents/skills/pipeline-backfill/SKILL.md
.agents/skills/schema-drift-check/SKILL.md
.agents/skills/data-quality-investigate/SKILL.md
.agents/skills/freshness-sla-check/SKILL.md
.agents/skills/anomaly-investigate/SKILL.md
.agents/skills/maintenance-pr/SKILL.md
.agents/skills/pipeline-report/SKILL.md
handoff.md
```

Changes applied:

- Added global Bruin Cloud-only rule: use Bruin Cloud MCP first, then `bruin cloud ... --output json`; do not use local `bruin run` for operational execution.
- Added requirement to use the `.bruin.yml` `bruin` connection named `bruin-cloud` for the Bruin Cloud API token, or `BRUIN_CLOUD_API_KEY` populated from that connection when CLI connection-name selection is ambiguous.
- Cross-referenced Bruin Cloud command usage against official docs and local Bruin source. The user-provided `lisbon-v1` path was not present; local verification used `/Users/bear/conductor/workspaces/bruin/brasilia/` plus the official docs URL.
- Added Cloud command references for projects, pipelines, validation errors, runs, run diagnosis, assets, instances, logs, triggers, and reruns.
- Clarified that `bruin cloud runs trigger` is pipeline-level and interval-scoped; asset-level execution must be confirmed through Bruin Cloud MCP or escalated to humans.
- Added repo evidence expectations: inspect asset files, `bruin lineage`, git history, and recent PRs/code changes before assigning cause.
- Reworked `pipeline-backfill` around Bruin Cloud trigger/rerun flows, interval terminology, materialization strategy risk, source/raw table protection, reversibility, size/duration checks, and a 90% confidence threshold.
- Prohibited source/raw table full refreshes and delete-style operations in backfill guardrails.
- Added `schema-drift-check` support for observed type drift, including columns declared as one type but historically null until incompatible values arrive.
- Added asset-level lineage via `bruin lineage <asset-file-path>` and inferred column-level lineage requirements for schema drift source and downstream impact.
- Updated data quality investigation to use Cloud check/run context, first-failure intervals, recent PR/code review, lineage, and Cloud-only backfill handoff.
- Propagated Cloud/MCP/repo-evidence language to freshness, anomaly, maintenance PR, and report skills for consistency.

Additional changes applied after the full review:

- Removed the nonexistent `meta.freshness` assumption from `freshness-sla-check`.
- Reworked freshness expectation inference around asset descriptions, pipeline schedule, warehouse table metadata, max freshness columns such as `MAX(extracted_at)`, `MAX(inserted_at)`, `MAX(dt)`, and table growth history.
- Added stale classifications for `table-frozen` and `growth-regression`.
- Added approval-required path from `freshness-sla-check` to `maintenance-pr` for documenting expected freshness/growth behavior in asset descriptions and adding custom checks.
- Added `maintenance-pr` change types for `asset-description-update`, `column-description-update`, `custom-check-create`, and `custom-check-update`.
- Changed maintenance PR branch naming to start with `self-healing/`; PR title format now uses `[self-healing]`.
- Added end-to-end test requirement for maintenance PRs only when a safe dev/shadow/sandbox environment exists. If not, PR/report must clearly state: `NOT TESTED END TO END — MUST BE TESTED BEFORE DEPLOYMENT`.

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
- **Bruin source path mismatch** — user-provided `/Users/bear/conductor/workspaces/bruin/lisbon-v1/...` files were not present locally. Equivalent Bruin docs/source files were found under `/Users/bear/conductor/workspaces/bruin/brasilia/...` and used for command verification alongside the official docs URL.

No dead ends in the design itself — the 9-skill atomic shape and `.context/` handoff pattern held up across all skills without needing to refactor any earlier ones. The fake-data pipelines all validate clean on the first write.

## Open Questions / Known Gaps

1. **Exact Bruin Cloud MCP response shapes** — CLI commands have been cross-checked against docs/source, but MCP tool names and response payloads still need confirmation in a real connected session.
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

- Status: `approved`

Functions / features to review:

1. **Use boundary**
   - Status: `approved`
   - What it covers: when to use triage, when not to use it, and the rule that every self-healing run starts here.
   - Feedback:
     - Specify that the Bruin Cloud API key should come from the connection named `bruin-cloud`.
     - Specify that all skills should utilize Bruin MCP and Bruin docs.
     - For Bruin Cloud commands, cross-reference Bruin docs and source code to determine the actual supported commands and usage.
     - References to check:
       - https://getbruin.com/docs/bruin/commands/cloud.html#cloud-command
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/docs/cloud/mcp-setup.md`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/main.go`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/cmd/cloud.go`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/cmd/cloud_test.go`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/pkg/bruincloud/api.go`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/pkg/bruincloud/api_test.go`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/pkg/bruincloud/config.go`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/pkg/bruincloud/db.go`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/pkg/bruincloud/types.go`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/docs/commands/cloud.md`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/pkg/config/connections.go`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/pkg/config/manager.go`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/pkg/config/manager_test.go`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/pkg/config/testdata/simple.yml`
       - `/Users/bear/conductor/workspaces/bruin/lisbon-v1/pkg/config/testdata/simple_win.yml`

2. **Inputs**
   - Status: `approved`
   - What it covers: `pipeline`, `alert_id`, `since`, and `severity_floor`.
   - Feedback:

3. **Context gathering**
   - Status: `approved`
   - What it covers: Bruin status, recent failures, quality failures, freshness, schema warnings, recent commits, and snapshot caching in `.context/`.
   - Feedback:

4. **Issue classification**
   - Status: `approved`
   - What it covers: the 9 issue classes: `transient`, `source-down`, `schema-drift`, `quality-fail`, `stale`, `anomaly`, `code-regression`, `capacity`, and `unknown`.
   - Feedback:

5. **Routing decision tree**
   - Status: `approved`
   - What it covers: grouping issues by class and invoking specialist skills once per class.
   - Feedback:

6. **Guardrails**
   - Status: `approved`
   - What it covers: read-only behavior, `.context/` snapshot writes, no direct repair, no skipped classification.
   - Feedback:

7. **Verification and reporting**
   - Status: `approved`
   - What it covers: post-specialist re-read, unresolved-route detection, and report payload shape for `pipeline-report`.
   - Feedback:

### 2. `pipeline-diagnose`

Skill purpose: read-only single-asset forensics that gathers logs, dependency state, error patterns, and commits to produce a root-cause hypothesis.

- Status: `approved`

Functions / features to review:

1. **Use boundary**
   - Status: `approved`
   - What it covers: when to diagnose a single asset and when to use triage or another specialist instead.
   - Feedback:

2. **Inputs**
   - Status: `approved`
   - What it covers: `asset`, optional `run_id`, and optional `pipeline`.
   - Feedback:

3. **Context gathering**
   - Status: `approved`
   - What it covers: asset definition, successful/failed run history, error fingerprint, upstream/downstream state, git history, connection health, and resource signals.
   - Feedback:

4. **Error pattern library**
   - Status: `approved`
   - What it covers: common failure fingerprints and their suggested next skills.
   - Feedback:

5. **Decision tree**
   - Status: `approved`
   - What it covers: upstream-first elimination, pattern matching, code-regression check, capacity check, and unknown escalation.
   - Feedback:

6. **Guardrails**
   - Status: `approved`
   - What it covers: read-only behavior, no speculation without evidence, and `.context/diag-*` output.
   - Feedback:

7. **Verification and reporting**
   - Status: `approved`
   - What it covers: diagnosis report shape, recommended next skill, and feedback loop for pattern-library improvement.
   - Feedback:

### 3. `pipeline-backfill`

Skill purpose: safely rerun Bruin assets or pipelines for a scoped time range after a fix, transient failure, or historical-data correction.

- Status: `approved`

Functions / features to review:

1. **Use boundary**
   - Status: `approved`
   - What it covers: valid backfill scenarios and cases that must not be treated as backfills.
   - Feedback:
     - Evaluate the asset's incremental/materialization strategy before any rerun. `merge`, `time_interval`, `delete+insert`, `append`, `create+replace`, and full-refresh behavior differ, so reruns must be crafted around the exact strategy.
     - Explicitly scope and document these considerations before acting:
       - What is the incremental strategy?
       - Where does the data come from? If data is accidentally deleted, can it be restored?
       - Is the table a source/raw table, mid-level table, or final report table?
       - If it is a source/raw table, take extra precautions. Full refresh or any operation that deletes source data must be strictly prohibited.
       - What is the table size?
       - How long do runs usually take and how much data do they process?
       - If the data is large, require explicit human approval.
       - Consider breaking large reruns into smaller time intervals.
       - Is a full refresh actually needed?
     - If the agent is less than 90% sure about the correct action or the possible negative consequences, alert a human to intervene.
     - Always optimize for reducing negative impact and consequences.
     - Actions should be reversible when possible. Avoid irreversible operations, such as accidentally full-refreshing a production source table and deleting raw data that cannot be restored or would be costly to re-ingest.
     - Based on these considerations, classify actions into: strictly prohibited, human approval required, or hand off entirely to humans.

2. **Inputs**
   - Status: `approved`
   - What it covers: `asset`, `start`, `end`, `reason`, `mode`, and `dry_run`.
   - Feedback:

3. **Pre-flight checks**
   - Status: `approved`
   - What it covers: asset health, range limits, past-only range, upstream coverage, downstream awareness, idempotency, row-count sanity, and connector quotas.
   - Feedback:

4. **Execution plan**
   - Status: `approved`
   - What it covers: dry-run approval flow and one-partition-at-a-time execution.
   - Feedback:
     - Clarify what "partition" means in this skill. It should mean a logical rerun slice, usually one schedule interval or one date/time window passed through `--start-date` / `--end-date`, not necessarily a physical database partition.
     - If the asset has a real warehouse partition column, the logical rerun slice should align with it where possible, but the skill must not assume every asset is physically partitioned.
     - Prefer `interval` terminology over `partition` terminology because Bruin runs are scoped by time intervals.
     - Check whether intervals are necessary and meaningful for the asset before using interval-based reruns. Some assets may not use intervals at all, or may use them incorrectly; the backfill plan must account for that instead of assuming interval slicing is valid.

5. **Bruin CLI commands**
   - Status: `approved`
   - What it covers: validation, single-partition runs, downstream propagation, and full refresh commands.
   - Feedback:
     - Current skill text incorrectly suggests creating/running local Bruin runs. These skills are intended strictly for Bruin Cloud operations only.
     - Replace local execution assumptions with Bruin Cloud command usage and Bruin MCP usage.
     - Use Bruin Cloud API credentials from the `bruin-cloud` connection.
     - Commands to cross-reference and incorporate:
       - `bruin cloud --help`
       - `bruin cloud projects --help`
       - `bruin cloud projects list`
       - `bruin cloud pipelines --help`
       - `bruin cloud pipelines list --help`
       - `bruin cloud pipelines list --project-id 01krk817ys2j45frftg1q4xfgv`
       - `bruin cloud pipelines get --help`
       - `bruin cloud pipelines get --project-id 01krk817ys2j45frftg1q4xfgv --name contoso-v2`
       - `bruin cloud pipelines enable --help`
       - `bruin cloud pipelines enable --project-id 01krk817ys2j45frftg1q4xfgv --pipeline contoso-v2`
       - `bruin cloud runs --help`
       - `bruin cloud runs list --help`
       - `bruin cloud runs list --project-id 01krk817ys2j45frftg1q4xfgv --pipeline contoso-v2 --limit 20`
       - `bruin cloud runs trigger --help`
       - `bruin cloud runs trigger --project-id 01krk817ys2j45frftg1q4xfgv --pipeline contoso-v2 --start-date 2015-01-01T00:00:00Z --end-date 2026-05-25T00:00:00Z`
       - `bruin cloud runs get --help`
       - `bruin cloud runs get --project-id 01krk817ys2j45frftg1q4xfgv --pipeline contoso-v2 --latest`
     - Before finalizing command guidance, verify exact supported flags and behavior against Bruin docs and source code listed in the earlier `pipeline-triage` feedback.

6. **Guardrails**
   - Status: `approved`
   - What it covers: auto-allowed scope, approval-required scope, and never-allowed operations.
   - Feedback:

7. **Verification and reporting**
   - Status: `approved`
   - What it covers: partition-level checks, full-range checks, and `.context/backfill-*` report shape.
   - Feedback:

### 4. `schema-drift-check`

Skill purpose: compare declared asset schema to live source schema, classify drift, and either propose maintenance or escalate risky changes.

- Status: `approved`

Functions / features to review:

1. **Use boundary**
   - Status: `approved`
   - What it covers: schema-drift triggers and exclusions for value-quality or missing-row issues.
   - Feedback:
     - Use this skill when the observed data type changes, for example a column declared as `INTEGER` had always been null and then starts receiving string values.

2. **Inputs**
   - Status: `approved`
   - What it covers: `asset`, optional `suspected_column`, and optional `source_connection`.
   - Feedback:
     - Inputs/context should include full asset-level and column-level lineage.
     - Asset-level lineage can come from `bruin lineage <asset-file-path>`.
     - After asset-level lineage is gathered, the agent should inspect each asset's query/source and infer column-level dependencies.
     - Column-level lineage is needed to understand both the source of schema drift and downstream impact.
     - Example Bruin lineage command shape:
       - `bruin lineage contoso-v2/assets/contoso_v2_reports/engineering_report.sql`
       - `bruin lineage contoso-v2/assets/contoso_v2_staging/engineering_velocity.sql`
     - Example lineage output includes upstream dependencies with asset paths and downstream dependencies with asset paths.

3. **Context gathering**
   - Status: `approved`
   - What it covers: declared schema, live source schema, 100-row sample, source changelog, downstream consumers, and last successful schema.
   - Feedback:

4. **Drift classification**
   - Status: `approved`
   - What it covers: `column-added`, `column-removed`, `column-renamed`, `type-narrowed`, `type-widened`, `nullability-change`, `enum-value-added`, and `cardinality-shift`.
   - Feedback:

5. **Decision tree**
   - Status: `approved`
   - What it covers: schema diffing, risk ordering, PR proposal, escalation, and anomaly rerouting.
   - Feedback:
     - As part of schema drift decisions, check recent PRs and code changes to determine whether the drift or failure aligns with a repo change.
     - More generally, these skills should assume the agent has repo access and should inspect previous changes, PRs, and relevant git history when reasoning about pipeline behavior.

6. **Guardrails**
   - Status: `approved`
   - What it covers: read-only source behavior, approval for risky changes, and required downstream listing.
   - Feedback:

7. **Verification and reporting**
   - Status: `approved`
   - What it covers: PR verification expectations and `.context/drift-*` report shape.
   - Feedback:

### 5. `data-quality-investigate`

Skill purpose: investigate failed Bruin quality checks by finding failing rows, tracing them to source data, classifying the failure mode, and recommending the next action.

- Status: `approved`

Functions / features to review:

1. **Use boundary**
   - Status: `approved`
   - What it covers: quality-check failures and exclusions for schema, freshness, or new-check design.
   - Feedback:

2. **Inputs**
   - Status: `approved`
   - What it covers: `asset`, `check_name`, and optional `run_id`.
   - Feedback:

3. **Context gathering**
   - Status: `approved`
   - What it covers: check definition, failing rows, row profile, first-failure point, source comparison, recent transform changes, and failure volume.
   - Feedback:

4. **Failure mode library**
   - Status: `approved`
   - What it covers: `source-bug`, `transform-bug`, `late-arriving-data`, `check-too-strict`, `seasonality-miss`, `boundary-condition`, `dedup-window-too-short`, `cardinality-explosion`, and `genuine-regression`.
   - Feedback:

5. **Decision tree**
   - Status: `approved`
   - What it covers: re-running check SQL, profiling, bisecting history, source comparison, and mode classification.
   - Feedback:

6. **Guardrails**
   - Status: `approved`
   - What it covers: read-only investigation, approval for check changes or fix PRs, no row deletion or check silencing.
   - Feedback:

7. **Verification and reporting**
   - Status: `approved`
   - What it covers: fix verification and `.context/dq-*` report shape.
   - Feedback:

### 6. `freshness-sla-check`

Skill purpose: identify stale Bruin assets, infer freshness expectations, classify why data is late, and route or retrigger narrowly when safe.

- Status: `approved`

Functions / features to review:

1. **Use boundary**
   - Status: `approved`
   - What it covers: scheduled freshness checks, stale triage, and exclusions for failed or wrong-valued data.
   - Feedback:

2. **Inputs**
   - Status: `approved`
   - What it covers: optional `pipeline` and `grace_minutes`.
   - Feedback:

3. **Freshness expectation source resolution**
   - Status: `approved`
   - What it covers: asset description, pipeline schedule, warehouse table metadata, max freshness columns, table growth history, and historical Cloud run cadence.
   - Feedback:
     - There is no `meta.freshness` tag in Bruin assets. Remove this assumption from the skill.
     - Do not invent Bruin asset metadata that does not exist.
     - Freshness should be inferred from the asset description when it states the intended cadence or processing window, for example a monthly asset that processes the previous month's financial performance.
     - Freshness should be inferred by checking table growth over time, for example `SELECT extracted_at, COUNT(*) FROM table_xyz GROUP BY 1 ORDER BY 1 DESC`; slower-than-usual growth or a frozen table indicates stale data.
     - Always check warehouse table metadata, especially last-updated time.
     - Compare table metadata with pipeline schedule. If a table was last updated 3 days ago but the pipeline schedule is hourly, that mismatch is a freshness signal.

4. **Context gathering**
   - Status: `approved`
   - What it covers: asset inventory, last success, last attempt, UTC now, upstream state, scheduler state, source-system status, table metadata, max freshness columns, and growth history.
   - Feedback:
     - Add table metadata such as warehouse table `last_updated_at`.
     - Query max freshness columns where available, such as `MAX(inserted_at)`, `MAX(extracted_at)`, `MAX(dt)`, partition max date, or similar timestamp/date columns.
     - Use these table-level signals to infer freshness context alongside Bruin Cloud run state.

5. **Stale classification**
   - Status: `approved`
   - What it covers: `upstream-stale`, `source-down`, `scheduler-missed`, `run-stuck`, `attempted-failed`, `genuine-stale`, and `weekend-or-holiday`.
   - Feedback:

6. **Decision tree and deduplication**
   - Status: `approved`
   - What it covers: freshness age calculation, stale filtering, root-cause collapse, and full findings.
   - Feedback:

7. **Guardrails**
   - Status: `approved`
   - What it covers: safe retriggering, approval-required run kills or bulk retriggers, no fabricated freshness expectations.
   - Feedback:
     - Add an approval-required action to create a maintenance PR that updates asset metadata/description with the inferred freshness expectation or expected growth pattern.
     - Example: if the table historically grows 5m-10m rows per day but has grown only 1m rows per day for the past 2 days, and the issue was a join filtering data, the agent should propose documenting the expected rows-per-day range in the asset description.
     - The same PR should add a custom quality check when appropriate, for example: `SELECT COUNT(*) > 5000000 FROM table_xyz WHERE extracted_at = (SELECT MAX(extracted_at) FROM table_xyz)`.
     - This should require approval because it changes asset metadata and quality checks.

8. **Verification and reporting**
   - Status: `approved`
   - What it covers: post-retrigger checks and `.context/freshness-*` report shape, including `unmonitored`.
   - Feedback:

### 7. `anomaly-investigate`

Skill purpose: explain a metric spike or dip using baseline comparison, dimension attribution, upstream checks, and code-change review without modifying data.

- Status: `approved`

Functions / features to review:

1. **Use boundary**
   - Status: `approved`
   - What it covers: metric anomalies and exclusions for failed runs, schema issues, missing data, or unverified real-world causality.
   - Feedback:

2. **Inputs**
   - Status: `approved`
   - What it covers: `metric`, `window`, optional `dimensions`, and optional `baseline`.
   - Feedback:

3. **Context gathering**
   - Status: `approved`
   - What it covers: metric history, baseline, magnitude, dimension breakdowns, upstream row counts/distributions, commits, and external calendar.
   - Feedback:

4. **Attribution pattern library**
   - Status: `approved`
   - What it covers: `single-dimension-driver`, `pipeline-double-count`, `pipeline-undercount`, `new-segment`, `lost-segment`, `seasonality-not-modeled`, `definitional-change`, `upstream-distribution-shift`, and `unexplained`.
   - Feedback:

5. **Decision tree**
   - Status: `approved`
   - What it covers: baseline computation, noise threshold, slicing, upstream signals, commit review, attribution coverage, and routing.
   - Feedback:

6. **Guardrails**
   - Status: `approved`
   - What it covers: read-only analysis, no data correction, no unevidenced causality, and no over-attribution.
   - Feedback:

7. **Verification and reporting**
   - Status: `approved`
   - What it covers: downstream verification expectations and `.context/anomaly-*` report shape.
   - Feedback:

### 8. `maintenance-pr`

Skill purpose: open narrowly scoped maintenance pull requests based on finding files from other skills. It is the only repo-writing skill.

- Status: `approved`

Functions / features to review:

1. **Use boundary**
   - Status: `approved`
   - What it covers: finding-gated PR creation and exclusions for feature work, refactors, or behavior changes.
   - Feedback:

2. **Inputs**
   - Status: `approved`
   - What it covers: `finding_file`, optional `branch_name`, and optional `draft`.
   - Feedback:

3. **Allowed change types**
   - Status: `approved`
   - What it covers: column add/rename/remove, type widen/narrow, asset/column description updates, custom check create/update, check threshold adjustment, dedup-window adjustment, dependency bumps, and dead-code removal.
   - Feedback:
     - Add updating asset descriptions as an allowed maintenance PR change type.
     - Add updating column-level descriptions as an allowed maintenance PR change type.
     - Add creating custom checks as an allowed maintenance PR change type.
     - Add updating existing custom checks as an allowed maintenance PR change type.

4. **Pre-flight checks**
   - Status: `approved`
   - What it covers: finding validation, clean working tree, duplicate PR search, allow-list check, local validation, and secret scanning.
   - Feedback:

5. **PR construction**
   - Status: `approved`
   - What it covers: branch naming, commit message, PR title/body, verification checklist, rollback notes, and wording discipline.
   - Feedback:
     - Branch names for maintenance PRs should start with `self-healing/` instead of `auto/`.

6. **Decision tree**
   - Status: `approved`
   - What it covers: finding parse, approval gate, scratch branch edits, validation failure abort, commit, push, and draft PR creation.
   - Feedback:

7. **Guardrails**
   - Status: `approved`
   - What it covers: scoped edits, no direct base-branch pushes, no merge, no non-draft PR without approval.
   - Feedback:
     - As part of PRs, the agent should try to test changes end to end only if a development, shadow, sandbox, or otherwise safe non-production environment exists.
     - If no safe non-production environment exists, the PR/report must state very clearly that the changes have not been tested end to end and must be tested before deployment.
     - Do not run end-to-end tests against production unless explicitly approved by a human and the test is safe.

8. **Verification and reporting**
   - Status: `approved`
   - What it covers: PR URL return, CI start note, finding-file annotation, and `pipeline-report` handoff.
   - Feedback:

### 9. `pipeline-report`

Skill purpose: post structured Slack status, incident, or digest messages so humans can understand what the self-healing run did and what still needs attention.

- Status: `approved`

Functions / features to review:

1. **Use boundary**
   - Status: `approved`
   - What it covers: final reporting for every self-healing run, specialist findings, scheduled digests, and escalations.
   - Feedback:

2. **Inputs**
   - Status: `approved`
   - What it covers: `channel`, `severity`, `subject`, optional `source_files`, optional `thread_ts`, and optional `mentions`.
   - Feedback:

3. **Severity format**
   - Status: `approved`
   - What it covers: `info`, `warn`, `error`, and `critical` formatting, mentions, destinations, and no silent severity changes.
   - Feedback:

4. **Incident message structure**
   - Status: `approved`
   - What it covers: subject, pipeline/time/run metadata, what happened, what was done, what needs attention, evidence, and suggested follow-up.
   - Feedback:

5. **Digest message structure**
   - Status: `approved`
   - What it covers: daily/weekly digest format, resolved/open sections, recurring patterns, and severity limits.
   - Feedback:

6. **Decision tree**
   - Status: `approved`
   - What it covers: input validation, source-file summarization, Slack deduplication, thread replies, posting, and finding-file annotation.
   - Feedback:

7. **Guardrails**
   - Status: `approved`
   - What it covers: channel allow-list, on-call mention rules, no secrets/full row dumps, and no dropped duplicate reports.
   - Feedback:

8. **Verification and reporting**
   - Status: `approved`
   - What it covers: Slack permalink, finding-file annotation, acknowledgment tracking, and `.context/reports-*.jsonl` record.
   - Feedback:
