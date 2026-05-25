---
name: data-quality-investigate
description: Investigate a failed Bruin quality check. Walk the failing rows back to their source, classify the failure mode, and either propose a fix or hand off. Use when a custom_check or column check fails on an otherwise successful run.
argument-hint: "<asset> <check_name>"
---

# Data Quality Investigate

A failed quality check is a question, not an answer. The check tells us _what_ is wrong (e.g. "duplicate primary keys exist") but rarely _why_. This skill answers the why.

## When to Use

- A Bruin column check or `custom_checks` block failed on a successful asset run.
- A downstream consumer reported wrong numbers and we suspect a check should have caught it.
- `pipeline-triage` classified an issue as `quality-fail`.

Do not use for: schema problems (use `schema-drift-check`), missing/late data (use `freshness-sla-check`), or designing new quality checks (that is a human task).

## Inputs

| Input | Required | Example | Notes |
|---|---|---|---|
| `asset` | yes | `marts.daily_top_articles` | The asset whose check failed. |
| `check_name` | yes | `unique_article_id_per_day` | The specific check that failed. |
| `run_id` | no | `run_01HXYZ...` | Defaults to most recent failed check. |

## Context to Gather

1. **Check definition** - read the exact SQL or column-check rule. Note the threshold (e.g. `count(*) = 0`).
2. **Failing rows** - rerun the check's SELECT (not the `count(*) = 0` wrapper) to get the actual offending rows. Cap at 1000.
3. **Failing-row profile** - distribution of the offending rows across natural dimensions: time, source, category, anything in the asset's partition key.
4. **First-failure point** - run the check against historical partitions until you find the first partition where it would have failed. This is the regression date.
5. **Source comparison** - for a sample of failing rows, pull the corresponding source records. Is the bug in the source, in the transform, or in the check itself?
6. **Recent transform changes** - `git log` on the asset's SQL file since the first-failure date.
7. **Volume** - what fraction of total rows are failing? 0.01% is different from 30%.

## Failure Mode Library

| Mode | Signal | Default response |
|---|---|---|
| `source-bug` | Failing rows look wrong in the source too | Report to source owner, do not fix in transform |
| `transform-bug` | Source is correct, transform produces wrong output, lines up with a recent commit | Open fix PR, then backfill |
| `late-arriving-data` | Failing rows correspond to natural-key collisions where a "new" record arrived after the "old" one with the same key | Adjust dedup logic; this is a design issue, not a one-time bug |
| `check-too-strict` | Threshold is unrealistic — e.g. expecting zero nulls in a column that has always had a small null rate | Loosen the check (with approval); do not silently adjust threshold |
| `seasonality-miss` | Failing rows correspond to a recurring real-world event (holiday, market close, vendor maintenance window) | Add seasonality exclusion to the check, not the data |
| `boundary-condition` | All failing rows are at partition boundaries (midnight UTC, month-end) | Timezone or window-edge bug; fix in transform |
| `dedup-window-too-short` | Failing rows are duplicates separated by more than the current dedup window | Widen the window or change dedup key |
| `cardinality-explosion` | Failing rows are a one-time burst from a single source identifier | Likely upstream issue; rate-limit or filter |
| `genuine-regression` | New failure pattern with no clear source change, no transform change, no seasonality | Escalate — these are the dangerous ones |

## Decision Tree

```
check = read_check_definition(asset, check_name)
failing = run_check_select(check, limit=1000)
if failing.empty:
    return result(status='resolved-since-alert')

profile = profile_failing_rows(failing, asset.partition_keys + asset.natural_keys)
first_failure = bisect_history(check, asset)
volume = count_failing_total / count_total_in_partition

# Compare to source for a sample.
source_sample = pull_source_records(failing.sample(20))
source_has_same_issue = compare_for_mismatch(source_sample, failing.sample(20))

mode = classify_mode(
    profile=profile,
    first_failure=first_failure,
    source_has_same_issue=source_has_same_issue,
    recent_commits=git_log(asset.file, since=first_failure),
    volume=volume,
)

return result(
    mode=mode,
    failing_rows_sample=failing.head(50),
    profile=profile,
    first_failure_date=first_failure,
    volume_pct=volume * 100,
    next_action=recommend_action(mode),
)
```

## Actions & Guardrails

- **Auto-allowed**: run read-only queries (the check SELECT, source comparisons, history scans), write investigation report to `.context/`.
- **Requires approval**: changing the check definition (e.g. loosening a threshold), opening a PR with a transform fix, triggering a backfill after a fix.
- **Never allowed**: deleting failing rows to make the check pass, modifying historical data outside a sanctioned backfill, silencing the check.

## Verification

A fix is verified when:

1. The check passes on the partition where it originally failed.
2. The check still passes on a sample of historical partitions (no regression).
3. The volume of failing rows in any newly-rerun partition is < the pre-fix volume.

If a check was loosened rather than a bug fixed, the report must include explicit acknowledgment of the new error budget.

## Reporting

Write to `.context/dq-<asset>-<check>-<timestamp>.yml`:

```yaml
asset: marts.daily_top_articles
check: unique_article_id_per_day
investigated_at: 2026-05-22T14:30:00Z
mode: late-arriving-data
volume:
  failing_rows: 142
  total_rows: 1_204_300
  pct: 0.012
first_failure_partition: 2026-05-15
recent_commits:
  - sha: abc123
    message: 'speed up dedup using row_number'
    date: 2026-05-14
    relevance: high — changed dedup logic the day before first failure
sample_failing_rows: .context/dq-sample-rows.csv
recommendation:
  action: open fix PR widening dedup window from 1h to 24h, then backfill from 2026-05-15
  pr_skill: maintenance-pr
  backfill_skill: pipeline-backfill
```

The report must always include the first-failure date — without it, downstream backfill scope is guesswork.
