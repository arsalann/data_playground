---
name: pipeline-backfill
description: Safely rerun a Bruin asset or pipeline for a specific time range. Validates scope, checks dependencies, and executes the backfill via Bruin CLI with the right flags. Use when a fix has been merged, a transient failure needs a retry, or historical data was wrong and needs regeneration.
argument-hint: "<asset> <start> <end>"
---

# Pipeline Backfill

The most dangerous skill in the set — backfills can overwrite good data, double-count rows, or saturate connectors. The guardrails matter more than the speed.

## When to Use

- A code or schema fix has merged and historical partitions need to be regenerated.
- A transient failure was diagnosed; the affected runs need to be retried.
- An upstream source republished data for a past window.
- A human asks "rerun X for date range Y".

Do not use for: routine first-time runs (those happen on schedule), running an asset that has never succeeded (use a manual `bruin run` instead — there is no "back" to fill), or "just rerun everything" requests without a scoped range.

## Inputs

| Input | Required | Example | Notes |
|---|---|---|---|
| `asset` | yes | `marts.daily_top_articles` | Single asset or comma-separated list. Use `--downstream` flag to include downstream. |
| `start` | yes | `2026-05-01` | Inclusive start date or timestamp. |
| `end` | yes | `2026-05-21` | Inclusive end date or timestamp. |
| `reason` | yes | `schema fix for view_count column` | Free text. Logged with the run for audit. |
| `mode` | no | `replace` \| `append` \| `merge` | Default `replace`. Append only valid for append-only tables. |
| `dry_run` | no | `true` \| `false` | Default `true` on first invocation, `false` only after preview is approved. |

## Pre-flight Checks

Run all of these before any `bruin run` is issued. A single failure aborts the backfill.

1. **Asset exists and is healthy** - last run succeeded, definition validates with `bruin validate`.
2. **Range is reasonable** - `end - start <= 90 days` without explicit override. Larger ranges need human approval.
3. **Range is in the past** - `end < now`. Backfilling future partitions is always a mistake.
4. **Upstream coverage** - every dependency must have successful runs covering the same range. If any upstream partition is missing, abort with a list of missing partitions.
5. **Downstream awareness** - list all downstream assets. If any are currently running, wait or abort.
6. **Idempotency** - the asset must declare `materialization.strategy` as one of `replace`, `merge`, `append`. If it is `time_interval` or unset, abort and ask for explicit `mode`.
7. **Row count sanity** - estimate output volume from a sample partition. If estimated total > 10x historical, abort and require approval.
8. **Connector quotas** - if the asset writes to a quota-limited destination (e.g. BigQuery slot pool, an API with daily limits), check current usage. Abort if the backfill would exhaust the day's budget.

## Decision Tree

```
checks = run_preflight(input)
if checks.failed:
    return abort(checks.failures)

if input.dry_run is None or True:
    plan = build_backfill_plan(input)
    emit_plan_for_approval(plan)
    return  # do not execute on dry run

# Execute one partition at a time, not the whole range in one call.
# This bounds the blast radius if something goes wrong mid-backfill.
for partition in partitions_in_range(input.start, input.end):
    result = bruin_run(
        asset=input.asset,
        start=partition.start,
        end=partition.end,
        full_refresh=(input.mode == 'replace'),
    )
    if result.failed:
        return abort_partial(
            completed=partitions_done,
            failed=partition,
            error=result.error,
        )
    verify_partition(input.asset, partition)

return success(partitions=partitions_done)
```

## Bruin CLI Commands

```shell
# Validate before running.
bruin validate --pipeline <pipeline>

# Single-partition backfill (preferred — one partition per call).
bruin run --asset <asset> --start-date <YYYY-MM-DD> --end-date <YYYY-MM-DD>

# With downstream propagation (use sparingly; review the plan first).
bruin run --asset <asset> --downstream --start-date ... --end-date ...

# Full refresh for non-partitioned assets.
bruin run --asset <asset> --full-refresh
```

Always set `--start-date` and `--end-date` explicitly even when they look redundant — implicit defaults vary by materialization type and have surprised humans before.

## Actions & Guardrails

- **Auto-allowed**: pre-flight checks, dry-run plan generation, backfills where `end - start <= 7 days` AND the asset has succeeded on the same range before.
- **Requires approval**: ranges > 7 days, any backfill touching `prod` connections, `--downstream` flag, `mode: append` on a table that already has data in the range.
- **Never allowed**: backfilling future dates, backfilling without a `reason`, backfilling an asset whose last run is still in progress, skipping pre-flight checks, running the entire range as one `bruin run` call (always partition).

If approval is required, emit the plan and stop. Do not poll for approval — the caller resumes the skill with `dry_run: false` once approved.

## Verification

After each partition:

1. Confirm the run exit code is 0.
2. Confirm row count is within 50%-200% of the historical mean for that day-of-week / day-of-month, whichever is the asset's natural seasonality.
3. Run any custom checks declared on the asset.
4. If verification fails, stop the backfill (do not proceed to next partition) and report.

After the full range:

1. Re-query the asset with the original failing condition to confirm the fix held.
2. List downstream assets that should now be re-run, but do not auto-cascade unless `--downstream` was passed.

## Reporting

Write a backfill record to `.context/backfill-<asset>-<timestamp>.yml` and post a Slack message via `pipeline-report`:

```yaml
asset: marts.daily_top_articles
range: 2026-05-01 to 2026-05-21
reason: schema fix for view_count column
mode: replace
partitions_attempted: 21
partitions_succeeded: 21
partitions_failed: 0
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
