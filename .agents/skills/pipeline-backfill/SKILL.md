---
name: pipeline-backfill
description: Safely trigger or rerun Bruin Cloud pipeline runs for a specific time range. Validates scope, checks dependencies and materialization risk, and executes only through Bruin Cloud MCP or Bruin Cloud CLI. Use when a fix has been merged, a transient failure needs a retry, or historical data was wrong and needs regeneration.
argument-hint: "<pipeline> <start> <end>"
---

# Pipeline Backfill

The most dangerous skill in the set — backfills can overwrite good data, double-count rows, or saturate connectors. The guardrails matter more than the speed.

This is a Bruin Cloud skill. Use Bruin Cloud MCP first; use `bruin cloud ... --output json` as the CLI fallback. `bruin-cloud` is the repo convention for the `.bruin.yml` `bruin` connection, but the CLI cannot select that connection by name; export `BRUIN_CLOUD_API_KEY` when multiple `bruin` connections exist. Do not use local `bruin run` for operational execution.

## When to Use

- A code or schema fix has merged and historical intervals need to be regenerated.
- A transient failure was diagnosed; the affected runs need to be retried.
- An upstream source republished data for a past window.
- A human asks "rerun X for date range Y".

Do not use for: routine first-time runs (those happen on schedule), running an asset or pipeline that has never succeeded (there is no "back" to fill), source/raw table full refreshes, destructive reruns whose consequences are not reversible, or "just rerun everything" requests without a scoped range.

## Inputs

| Input | Required | Example | Notes |
|---|---|---|---|
| `project_id` | no | `01krk817ys2j45frftg1q4xfgv` | Bruin Cloud project ID. Required if the account can see multiple projects and it cannot be inferred. |
| `pipeline` | yes | `wikipedia-ai-trends` | Bruin Cloud pipeline name. Cloud CLI run triggers are pipeline-level. |
| `asset` | no | `marts.daily_top_articles` | Asset that motivated the rerun. Used for risk analysis, lineage, and verification. Do not assume Cloud CLI can trigger a single asset. |
| `start` | yes | `2026-05-01T00:00:00Z` | Bruin interval start date or timestamp. Prefer the pipeline's Bruin interval format; do not assume date-bound inclusivity beyond Cloud's run-window semantics. |
| `end` | yes | `2026-05-21T00:00:00Z` | Bruin interval end date or timestamp. Prefer the pipeline's Bruin interval format; do not assume date-bound inclusivity beyond Cloud's run-window semantics. |
| `reason` | yes | `schema fix for view_count column` | Free text. Logged with the run for audit. |
| `mode` | no | `trigger` \| `rerun` | Default is `trigger` for a new interval run. Use `rerun` only for an existing Cloud run ID. |
| `run_id` | no | `run_01HXYZ...` | Required for `mode: rerun`; optional for investigating existing failed runs. |
| `dry_run` | no | `true` \| `false` | Default `true` on first invocation, `false` only after preview is approved. |

## Pre-flight Checks

Run all of these before any Bruin Cloud run is triggered or rerun. A single failure aborts the backfill plan.

1. **Cloud project/pipeline exists** - resolve `project_id`, confirm the pipeline with `bruin cloud pipelines get --project-id <project-id> --name <pipeline> --output json`, and check Cloud validation errors.
2. **Run history exists** - use `bruin cloud runs list --project-id <project-id> --pipeline <pipeline> --limit 20 --output json`; abort if the pipeline has never succeeded.
3. **Range is reasonable** - `end - start <= 90 days` without explicit override. Larger ranges need human approval and usually smaller intervals.
4. **Range is in the past** - `end < now`. Backfilling future intervals is always a mistake.
5. **Interval semantics** - decide whether Bruin intervals are necessary and meaningful for this pipeline/asset. Some assets may not use intervals, or may use them incorrectly; if interval slicing is not meaningful, escalate instead of pretending it is safe.
6. **Materialization strategy** - inspect the asset and affected downstream assets. Explicitly classify supported strategies including `append`, `merge`, `delete+insert`, `time_interval`, `create+replace`, `truncate+insert`, `ddl`, `scd2_by_time`, `scd2_by_column`, `datavault_hub`, `datavault_link`, `datavault_satellite`, and any full-refresh behavior. If the strategy is not understood well enough to estimate consequences, escalate instead of running.
7. **Data source and reversibility** - identify where the data comes from and whether deleted data can be restored. If restoration is impossible, uncertain, or very expensive, require human intervention.
8. **Layer risk** - classify the table as source/raw, mid-level, or final/report. Source/raw tables require extra precautions; full refresh or any delete-style operation on source/raw tables is strictly prohibited.
9. **Upstream coverage** - every dependency must have successful Cloud runs covering the same interval. If any upstream interval is missing, abort with a list of missing intervals.
10. **Downstream awareness** - list downstream assets. If any are currently running, wait or abort.
11. **Size and duration** - estimate table size, data processed, and normal run duration from Cloud history/warehouse metadata. Large or expensive reruns require explicit human approval and smaller intervals.
12. **Row count sanity** - estimate output volume from a sample interval. If estimated total > 10x historical, abort and require approval.
13. **Connector quotas** - if the asset writes to a quota-limited destination or reads a rate-limited source, check current usage. Abort if the backfill would exhaust the day's budget.
14. **Confidence threshold** - if the agent is less than 90% confident in the action or the negative consequences, stop and hand off to a human.

## Decision Tree

```
checks = run_preflight(input)
if checks.failed:
    return abort(checks.failures)

if input.dry_run is None or True:
    plan = build_backfill_plan(input)
    emit_plan_for_approval(plan)
    return  # do not execute on dry run

# Execute one Bruin interval at a time when intervals are meaningful.
# An interval is a Bruin run time window, not necessarily a physical database partition.
for interval in intervals_in_range(input.start, input.end):
    result = bruin_cloud_trigger_or_rerun(
        project_id=input.project_id,
        pipeline=input.pipeline,
        start=interval.start,
        end=interval.end,
        run_id=input.run_id if input.mode == 'rerun' else None,
    )
    if result.failed:
        return abort_partial(
            completed=intervals_done,
            failed=interval,
            error=result.error,
        )
    run = poll_latest_run(
        project_id=input.project_id,
        pipeline=input.pipeline,
        interval=interval,
    )
    verify_interval(input.pipeline, input.asset, interval, run.run_id)

return success(intervals=intervals_done)
```

## Bruin Cloud Commands

```shell
# Resolve projects and pipeline state.
bruin cloud projects list --output json
bruin cloud pipelines list --project-id <project-id> --output json
bruin cloud pipelines get --project-id <project-id> --name <pipeline> --output json
bruin cloud pipelines errors --output json

# Inspect recent runs and the latest state.
bruin cloud runs list --project-id <project-id> --pipeline <pipeline> --limit 20 --output json
bruin cloud runs get --project-id <project-id> --pipeline <pipeline> --latest --output json
bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline> --latest --output json

# Trigger a new interval run.
bruin cloud runs trigger --project-id <project-id> --pipeline <pipeline> --start-date <start> --end-date <end> --output json

# Rerun an existing Cloud run, optionally only failed assets.
bruin cloud runs rerun --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --only-failed --output json
bruin cloud runs rerun --project-id <project-id> --pipeline <pipeline> --latest --only-failed --output json

# Trigger/rerun returns a success envelope, not a run ID. Poll and verify.
bruin cloud runs list --project-id <project-id> --pipeline <pipeline> --limit 1 --output json
bruin cloud runs get --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --output json

# Inspect asset instances and logs for verification.
bruin cloud instances list --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --output json
bruin cloud instances failed-logs --project-id <project-id> --pipeline <pipeline> --run-id <run-id> --output json
```

Always set `--start-date` and `--end-date` explicitly for Cloud triggers. Do not use local `bruin run`, local `--full-refresh`, or any local asset-scoped execution as a substitute for Bruin Cloud behavior.

## Actions & Guardrails

- **Auto-allowed**: pre-flight checks, dry-run plan generation, and at most a single small Cloud rerun/trigger where the affected pipeline previously succeeded for the same interval shape, the run is non-prod or explicitly auto-approved, and the action is reversible.
- **Requires approval**: ranges > 7 days, production pipelines, large/expensive tables, `append` reruns where data already exists, `merge` or `delete+insert` reruns with unclear keys/windows, any source connector with quota risk, and any Cloud rerun touching more than one interval.
- **Human-only**: ambiguous or irreversible changes, confidence < 90%, unclear materialization semantics, uncertain restore path, full refresh needs, source/raw table delete risk, and any operation where a wrong action could destroy or make raw data costly to recover.
- **Never allowed**: backfilling future dates, backfilling without a `reason`, triggering while the pipeline/run is still in progress, skipping pre-flight checks, local operational runs, source/raw table full refresh, delete-style operations on source/raw tables, or running the whole range in one Cloud trigger when smaller meaningful intervals exist.

If approval is required, emit the plan and stop. Do not poll for approval — the caller resumes the skill with `dry_run: false` once approved.

## Verification

After each interval:

1. Confirm the Bruin Cloud run status is `success`.
2. Confirm row count is within 50%-200% of the historical mean for that day-of-week / day-of-month, whichever is the asset's natural seasonality.
3. Confirm quality checks and asset instances are successful in Bruin Cloud.
4. Capture the Cloud run ID and URL from `runs list`/`runs get` after trigger or rerun polling.
5. If verification fails, stop the backfill (do not proceed to next interval) and report.

After the full range:

1. Re-check the original failing condition to confirm the fix held.
2. List downstream assets and Cloud runs that may still need reruns, but do not auto-cascade unless the approved Cloud plan explicitly included them.

## Output

Write a backfill record to `.context/backfill-<asset>-<timestamp>.yml` and return that path:

```yaml
asset: marts.daily_top_articles
pipeline: wikipedia-ai-trends
project_id: 01krk817ys2j45frftg1q4xfgv
range: 2026-05-01 to 2026-05-21
reason: schema fix for view_count column
mode: trigger
materialization_strategy: merge
layer: mid-level
intervals_attempted: 21
intervals_succeeded: 21
intervals_failed: 0
cloud_runs:
  - run_id: manual__2026-05-22T14:30:00Z
    url: https://cloud.getbruin.com/...
row_counts:
  before: 4_218_000
  after: 4_222_400
  delta_pct: +0.1
duration_total: 14m32s
verified: true
downstream_blocked_until_rerun:
  - marts.weekly_trends
  - dashboards.top_articles
```

A backfill is not complete until the record is written. Partial backfills must explicitly say so in the report.
