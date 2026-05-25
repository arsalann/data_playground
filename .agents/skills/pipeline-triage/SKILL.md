---
name: pipeline-triage
description: Entry point for self-healing pipeline work. Pull current pipeline state from Bruin Cloud, classify what is wrong, and route to the right follow-up skill. Use this when an alert fires, a scheduled tick runs, or a human asks "what is broken right now".
argument-hint: "[pipeline name | alert id | 'all']"
---

# Pipeline Triage

The dispatcher skill. Every self-healing run starts here. The job is to look at pipeline state, decide what (if anything) is wrong, and hand off to a specialist skill. This skill must not perform repairs itself.

## When to Use

- A Bruin Cloud alert webhook fired and an agent was woken up.
- A scheduled tick runs (hourly, every 15m, etc.) and there is no specific alert.
- A human asks "what is broken", "what needs attention", or "is the pipeline healthy".
- Another skill needs a fresh state snapshot before acting.

Do not use for: targeted single-asset work (use `pipeline-diagnose`), running fixes (use the specialist skills), or producing the human-facing summary (use `pipeline-report`).

## Inputs

| Input | Required | Example | Notes |
|---|---|---|---|
| `pipeline` | yes | `wikipedia-ai-trends` | Bruin pipeline name. `all` = scan every pipeline the agent has access to. |
| `alert_id` | no | `alert_01HXYZ...` | If invoked from an alert, the alert ID for context. |
| `since` | no | `6h`, `24h`, `1d` | Lookback window for failed runs. Default `24h`. |
| `severity_floor` | no | `warn`, `error`, `critical` | Ignore anything below this. Default `warn`. |

## Context to Gather

Run these in order. Do not skip steps — silence on one signal is itself a signal.

1. **Pipeline state** - `bruin internal status --pipeline <name>` (or Bruin MCP equivalent) for the asset graph and last-run state of every asset.
2. **Recent failures** - list assets where the last run failed, was skipped due to upstream failure, or is still running past its expected duration.
3. **Quality check failures** - any `custom_checks` or column checks that failed in the last `since` window.
4. **Freshness** - any asset whose last successful run is older than its declared SLA / `materialization.partition_by` cadence.
5. **Schema warnings** - any `bruin validate` warnings about columns drifting from source.
6. **Recent commits** - `git log --since="<since>" -- <pipeline_path>` so we know whether a human change is likely the cause.

Cache the raw output in `.context/triage-<timestamp>.json` so downstream skills can read it without re-querying.

## Classification

Every issue gets exactly one primary class. Pick the first that matches:

| Class | Signal | Route to |
|---|---|---|
| `transient` | Single failed run, error matches retry pattern (timeout, 5xx, deadlock, connection reset), no recent code change | retry once via `pipeline-backfill` with `--scope=last-run` |
| `source-down` | Multiple assets that share an upstream source connector all failing with auth/connectivity errors | `pipeline-report` only — do not retry until source is verified |
| `schema-drift` | Error mentions unknown column, type mismatch, missing field, or `bruin validate` flags drift | `schema-drift-check` |
| `quality-fail` | Run succeeded but custom checks or column checks failed | `data-quality-investigate` |
| `stale` | Run did not fail but data is past its freshness SLA | `freshness-sla-check` |
| `anomaly` | Run succeeded, checks passed, but a tracked metric is out of expected range | `anomaly-investigate` |
| `code-regression` | Failure started immediately after a commit touching the failing asset | `pipeline-report` with link to commit; do not auto-revert |
| `capacity` | OOM, quota exceeded, slot exhaustion, query timeout on a query that historically ran fine | `pipeline-report` — capacity changes need human approval |
| `unknown` | None of the above match | `pipeline-diagnose` to gather more context |

## Decision Tree

```
state = gather_context()
issues = []
for asset in state.failing_assets:
    issues.append(classify(asset))
for asset in state.stale_assets:
    issues.append({class: 'stale', asset})
for check in state.failed_checks:
    issues.append({class: 'quality-fail', check})

if not issues:
    emit("pipeline healthy", level=info)
    return

# Group by class so we hand off once per class, not once per asset.
batches = group_by_class(issues)
for class, items in batches:
    route(class, items)

# Always finish with a report, even if every issue was handed off.
invoke pipeline-report with batches
```

## Actions & Guardrails

This skill is read-only with one exception:

- **Auto-allowed**: read pipeline state, write triage snapshot to `.context/`, invoke other skills.
- **Requires approval**: nothing — this skill never modifies pipelines or repo state directly.
- **Never allowed**: skipping the classification step (every issue gets a class, even if the class is `unknown`), batching multiple unrelated issues into one specialist invocation.

If the agent cannot reach Bruin Cloud, it must emit a `source-down` style report and stop. Do not assume "no failures" from missing data.

## Verification

After routing, re-read the triage snapshot 5 minutes after the last specialist skill finishes. Any class that did not move to `resolved` or `escalated` is a failure of the triage routing — log it and re-classify.

## Reporting

Hand the batched issue list to `pipeline-report` with this shape:

```yaml
pipeline: wikipedia-ai-trends
window: 24h
snapshot: .context/triage-20260522T1430Z.json
issues:
  - class: schema-drift
    severity: error
    assets: [raw.wikipedia_pageviews]
    routed_to: schema-drift-check
    handle: skill-run-abc123
  - class: stale
    severity: warn
    assets: [marts.daily_top_articles]
    routed_to: freshness-sla-check
    handle: skill-run-def456
healthy_assets: 42
```

Never emit a "pipeline healthy" report unless every asset checked returned a result — partial state is not health.
