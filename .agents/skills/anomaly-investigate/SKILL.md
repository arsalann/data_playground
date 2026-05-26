---
name: anomaly-investigate
description: Explain a metric spike or dip by slicing dimensions and checking upstream changes. Produces a hypothesis with evidence; never auto-fixes the data. Use when a tracked metric is out of expected range but no pipeline error or quality check fired.
argument-hint: "<metric or asset> <date or window>"
---

# Anomaly Investigate

The asset ran. The checks passed. The number still looks wrong. This skill is the one that finds out why — by attribution, not by guesswork.

Use Bruin Cloud MCP for run/asset context and Bruin docs/source-verified `bruin cloud ... --output json` commands as fallback. `bruin-cloud` is the repo convention for the `.bruin.yml` `bruin` connection, but the CLI cannot select that connection by name; export `BRUIN_CLOUD_API_KEY` when multiple `bruin` connections exist. Local repo inspection is allowed for metric definitions, lineage, and git/PR history; local operational runs are not.

## When to Use

- A monitored metric (revenue, sign-ups, pageviews, latency) is N standard deviations outside its expected range.
- A human asks "why did X spike/drop on date Y".
- `pipeline-triage` classified an issue as `anomaly`.
- A dashboard consumer reported "this number looks off".

Do not use for: failed runs (use `pipeline-diagnose`), schema problems (use `schema-drift-check`), data missing entirely (use `freshness-sla-check`), or proving a real-world business event happened (we can surface signals, not interpret causation).

## Inputs

| Input | Required | Example | Notes |
|---|---|---|---|
| `metric` | yes | `daily_pageviews` or `marts.daily_top_articles.total_views` | Metric or asset+column. |
| `window` | yes | `2026-05-21` or `2026-05-15..2026-05-21` | Single date or range to investigate. |
| `project_id` | no | `01krk817ys2j45frftg1q4xfgv` | Bruin Cloud project ID for Cloud run/asset context. |
| `pipeline` | no | `wikipedia-ai-trends` | Bruin Cloud pipeline name, inferred from the metric asset when possible. |
| `dimensions` | no | `[country, source, category]` | If known, the dimensions worth slicing. Inferred from the asset's columns otherwise. |
| `baseline` | no | `last_28d_same_dow` | How "expected" is defined. Default: same day-of-week over last 4 weeks, median. |

## Context to Gather

1. **Metric history** - at least 28 days of daily values, plus the anomalous window. Use `bruin query --asset <asset-file> --query <sql> --description "anomaly history for <metric>" --output json` or `bruin query --connection <connection> --query <sql> --description "anomaly history for <metric>" --output json`; dry-run expensive scans first.
2. **Baseline** - median + IQR over the baseline window, same-day-of-week when seasonality matters.
3. **Anomaly magnitude** - how many IQRs or sigmas off, and absolute delta (both matter — small percentage on a tiny base is noise).
4. **Dimension breakdowns** - the metric sliced by each candidate dimension, comparing anomalous window to baseline. Find dimensions where the anomaly concentrates. Use `bruin query --connection <connection> --query <sql> --description "anomaly slice for <metric> by <dimension>" --limit 1000 --timeout 120 --output json`; dry-run wide scans before execution.
5. **Bruin Cloud context** - use Cloud run/asset/instance history to confirm runs and checks passed for the anomalous window:
   - `bruin cloud runs list --project-id <project-id> --pipeline <pipeline> --limit 20 --output json`
   - `bruin cloud runs get --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`
   - `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`
   - `bruin cloud instances list --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`
   "Checks passed" means Cloud run status is `success` or the inspected asset instance is not `failed` / `checks_failed`, depending on the API response being inspected.
6. **Upstream row counts** - did the source row count change meaningfully on the anomalous date? Use read-only `bruin query` with `--description`, `--limit` for samples, `--timeout`, and optional `--dry-run`.
7. **Upstream value distributions** - did a column's distribution shift, even if row count was normal? Use read-only `bruin query` with audit/cost controls.
8. **Code changes** - inspect `git log` and recent PRs/branches on the metric's defining asset and upstreams over the last 30 days.
9. **Lineage** - use `bruin lineage <asset-file-path> --output json --full` and repo inspection to identify upstream metric sources and downstream consumers. Include `--variant <variant>` for variant-backed pipelines.
10. **External calendar** - holidays, product launches, known maintenance windows, paid acquisition pulses. If a calendar isn't accessible, list this as "not checked".

## Anomaly Attribution Patterns

| Pattern | Signal | Default explanation |
|---|---|---|
| `single-dimension-driver` | One dimension value accounts for >50% of the delta vs. baseline | The named segment moved; report it |
| `pipeline-double-count` | Row count in upstream is normal, but metric is 2x or 3x; interval key missing from dedup | Data quality issue — route to `data-quality-investigate` |
| `pipeline-undercount` | Upstream row count dropped but no source failure logged; interval incomplete | Often `freshness-sla-check` will catch this; route there |
| `new-segment` | Anomaly is entirely from a dimension value that did not exist in baseline | New product/region/source launched — confirm with calendar |
| `lost-segment` | A previously-present dimension value disappeared from the data | Source removed it (schema drift?) or business stopped collecting — escalate |
| `seasonality-not-modeled` | Anomaly aligns with a recurring date (month-end, payday, holiday) and prior occurrences show same shape | Baseline is wrong, not the data — recommend updating baseline calc |
| `definitional-change` | A recent commit changed how the metric is computed | The number is "right" by the new definition; surface the commit |
| `upstream-distribution-shift` | Row count steady but a categorical or numeric distribution shifted | Often a real business change; report without claiming causation |
| `unexplained` | None of the above attribute > 50% of the delta | Report as unexplained with full breakdown |

## Decision Tree

```
history = pull_metric_history(input.metric, days=28+window_len)
baseline = compute_baseline(history, method=input.baseline)
observed = pull_metric(input.metric, window=input.window)
magnitude = compute_magnitude(observed, baseline)

if magnitude.abs_z_score < 2 and magnitude.delta_abs < material_threshold(input.metric):
    return result(status='within-noise', baseline=baseline, observed=observed)

dimensions = input.dimensions or infer_dimensions(input.metric)
breakdowns = {dim: slice_metric(input.metric, dim, input.window, baseline) for dim in dimensions}

upstream_signals = {
    'row_counts': pull_upstream_row_counts(input.metric.upstream, input.window),
    'distributions': pull_upstream_distributions(input.metric.upstream, input.window),
}

recent_commits = git_log(input.metric.upstream_files, since='30d')

attributed = attribute(magnitude, breakdowns, upstream_signals, recent_commits, external_calendar())

return result(
    status='anomaly-explained' if attributed.coverage > 0.5 else 'anomaly-unexplained',
    magnitude=magnitude,
    primary_drivers=attributed.drivers,
    coverage_pct=attributed.coverage,
    recommended_next=route_next(attributed),
)
```

## Actions & Guardrails

- **Auto-allowed**: read metric/asset data, inspect Bruin Cloud run/asset state, run slicing queries, read source row counts, inspect git/PR history, write report to `.context/`.
- **Requires approval**: nothing — this skill is read-only.
- **Never allowed**: local operational runs, claiming a cause without an evidence link, "correcting" anomalous data, suppressing the anomaly from a dashboard or alert, attributing > 100% of the delta (a common failure when overlapping dimensions are double-counted).

## Verification

This skill's output is verified by what happens after:

- If `pipeline-double-count` or similar pipeline cause was attributed, the follow-up fix in `data-quality-investigate` + `pipeline-backfill` should restore the metric to baseline. If it doesn't, the attribution was wrong.
- If a real-world cause was attributed (new segment, seasonality, external event), no fix is expected — the metric stays at its new level, and the baseline should be updated for future runs.

Record the verification outcome on the report file so attribution accuracy can be tracked.

## Reporting

Write to `.context/anomaly-<metric>-<window>-<timestamp>.yml`:

```yaml
metric: marts.daily_top_articles.total_views
window: 2026-05-21
investigated_at: 2026-05-22T14:30:00Z
magnitude:
  observed: 4_812_000
  baseline_median: 2_140_000
  abs_z_score: 4.8
  delta_pct: +124.9
primary_drivers:
  - dimension: country
    value: TR
    contribution_pct: 78
    note: TR went from 4% of baseline pageviews to 51% of observed
  - dimension: article_category
    value: news_politics
    contribution_pct: 14
coverage_pct: 92
unexplained_pct: 8
upstream_signals:
  row_count_delta_pct: +120
  distribution_shifts: [country, article_category]
recent_commits_examined: 6
recent_commits_relevant: 0
attribution: single-dimension-driver + upstream-distribution-shift
recommended_next:
  action: human-review
  note: |
    Surface as a real-world traffic event concentrated in Turkey on news_politics.
    No pipeline action recommended. Update baseline if event is sustained > 7 days.
```

A report with `coverage_pct < 50` is `anomaly-unexplained` — say so, do not stretch attribution to make the report look complete.
