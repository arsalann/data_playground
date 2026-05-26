---
name: data-quality-investigate
description: Investigate a failed Bruin quality check. Walk the failing rows back to their source, classify the failure mode, and either propose a fix or hand off. Use when a custom check or column check fails on an otherwise successful run.
argument-hint: "<asset> <check_name>"
---

# Data Quality Investigate

A failed quality check is a question, not an answer. The check tells us _what_ is wrong (e.g. "duplicate primary keys exist") but rarely _why_. This skill answers the why.

Use Bruin Cloud MCP for Cloud run/check state and Bruin docs/source-verified `bruin cloud ... --output json` commands as fallback. `bruin-cloud` is the repo convention for the `.bruin.yml` `bruin` connection, but the CLI cannot select that connection by name; export `BRUIN_CLOUD_API_KEY` when multiple `bruin` connections exist. Local repo inspection is allowed for check definitions, lineage, and git/PR history; local `bruin run` is not an operational fallback.

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
| `project_id` | no | `01krk817ys2j45frftg1q4xfgv` | Bruin Cloud project ID for run/check context. |
| `pipeline` | no | `wikipedia-ai-trends` | Bruin Cloud pipeline name, inferred from the asset or triage snapshot when possible. |

## Context to Gather

1. **Cloud check/run state** - use Bruin Cloud MCP or `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json` to identify failed checks and affected assets.
2. **Cloud instance details/logs** - use exact CLI forms for the failed check/asset:
   - `bruin cloud instances get --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --asset <asset> --output json`
   - `bruin cloud instances logs --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) (--asset <asset> [--step-name <step>] | --step-id <step-id>) --output json`
   - `bruin cloud instances failed-logs --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`
3. **Check definition** - read the exact SQL or column-check rule from the repo. Custom checks live in top-level `custom_checks` entries with required `name` and `query`, optional `description`, optional `value`, optional `count`, and optional `blocking`; omitted `blocking` behaves as `true` at runtime. If `value` is omitted, Bruin expects the query output to match integer zero. If `count` is set, Bruin wraps the query with `SELECT count(*) FROM (<query>)` and compares that result. Column checks live under `columns[].checks[]` with `name`, optional `value`, and optional `blocking`; omitted `blocking` is true.
4. **Failing rows** - rerun only read-only diagnostic SELECTs against the warehouse when credentials and cost controls allow it; otherwise use Cloud check output/logs. Use `bruin query --connection <connection> --query <sql> --description "failing-row sample for <asset> <check>" --limit 1000 --output json` or `bruin query --asset <asset-file> --query <sql> --description "failing-row sample for <asset> <check>" --limit 1000 --output json`; dry-run expensive scans first.
5. **Failing-row profile** - distribution of the offending rows across natural dimensions: time, source, category, anything in the asset's Bruin interval or natural key.
6. **First-failure point** - check historical Cloud runs/intervals until you find the first interval where it would have failed. This is the regression interval.
7. **Source comparison** - for a sample of failing rows, pull the corresponding source records. Is the bug in the source, in the transform, or in the check itself?
8. **Recent transform changes** - inspect `git log` and recent PRs/branches touching the asset and upstreams since the first-failure interval.
9. **Lineage** - use `bruin lineage <asset-file-path> --output json --full` and repo inspection to understand upstream source tables and downstream impact. Include `--variant <variant>` for variant-backed pipelines.
10. **Volume** - what fraction of total rows are failing? 0.01% is different from 30%.

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

profile = profile_failing_rows(failing, asset.interval_keys + asset.natural_keys)
first_failure = bisect_history(check, asset)
volume = count_failing_total / count_total_in_interval

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

- **Auto-allowed**: inspect Bruin Cloud run/check state, run capped read-only diagnostic queries (the check SELECT, source comparisons, history scans) with `--description`, `--limit`, `--output json`, and optional `--dry-run`, inspect git/PR history, write investigation report to `.context/`.
- **Requires approval**: changing the check definition (e.g. loosening a threshold), opening a PR with a transform fix, triggering a Bruin Cloud backfill/rerun after a fix.
- **Never allowed**: deleting failing rows to make the check pass, modifying historical data outside a sanctioned Bruin Cloud backfill, silencing the check, or using local operational runs as a shortcut.

## Verification

A fix is verified operationally from Cloud state. Local `bruin run --only checks <asset-file>` is appropriate only for static validation or fake-data test pipelines. If targeted local check execution is appropriate, use `bruin run --only checks --single-check <check-id> <asset-file>`; `<check-id>` is the Bruin-generated check ID, not necessarily the display `check_name`.

A fix is verified when:

1. The check passes on the interval where it originally failed.
2. The check still passes on a sample of historical intervals (no regression).
3. The volume of failing rows in any newly-rerun interval is < the pre-fix volume.

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
first_failure_interval: 2026-05-15T00:00:00Z/2026-05-16T00:00:00Z
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

The report must always include the first-failure interval/date — without it, downstream Bruin Cloud backfill scope is guesswork.
