---
name: pipeline-triage
description: Entry point for self-healing pipeline work. Pull current pipeline state from Bruin Cloud, classify what is wrong, and route to the right follow-up skill. Use this when an alert fires, a scheduled tick runs, or a human asks "what is broken right now".
argument-hint: "[pipeline name | alert id | 'all']"
---

# Pipeline Triage

The dispatcher skill. Every self-healing run starts here. The job is to look at pipeline state, decide what (if anything) is wrong, and hand off to a specialist skill. This skill must not perform repairs itself.

This is a Bruin Cloud skill. Use Bruin Cloud MCP first; use `bruin cloud ... --output json` as the CLI fallback. The Bruin Cloud API token must come from the `.bruin.yml` `bruin` connection named `bruin-cloud` or from `BRUIN_CLOUD_API_KEY` populated from that connection. Do not use local `bruin run` for operational execution.

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
| `project_id` | no | `01krk817ys2j45frftg1q4xfgv` | Bruin Cloud project ID. If omitted, resolve through Bruin Cloud MCP or `bruin cloud projects list --output json`; require it when multiple projects are visible. |
| `alert_id` | no | `alert_01HXYZ...` | If invoked from an alert, the alert ID for context. |
| `since` | no | `6h`, `24h`, `1d` | Lookback window for failed runs. Default `24h`. |
| `severity_floor` | no | `warn`, `error`, `critical` | Ignore anything below this. Default `warn`. |

## Context to Gather

Run these in order. Do not skip steps — silence on one signal is itself a signal.

1. **Project resolution** - use Bruin Cloud MCP or `bruin cloud projects list --output json`; if more than one project is visible and no `project_id` was supplied, stop and ask the caller for a project.
2. **Pipeline inventory** - use Bruin Cloud MCP or `bruin cloud pipelines list --project-id <project-id> --output json`; for one pipeline, also run `bruin cloud pipelines get --project-id <project-id> --name <pipeline> --output json`.
3. **Pipeline validation errors** - use Bruin Cloud MCP or `bruin cloud pipelines errors --output json` and filter to the pipeline/project where the response allows it.
4. **Recent runs** - use Bruin Cloud MCP or `bruin cloud runs list --project-id <project-id> --pipeline <pipeline> --limit 20 --output json`.
5. **Latest run details** - use Bruin Cloud MCP or `bruin cloud runs get --project-id <project-id> --pipeline <pipeline> --latest --output json`.
6. **Latest run diagnosis** - use Bruin Cloud MCP or `bruin cloud runs diagnose --project-id <project-id> --pipeline <pipeline> --latest --output json` to capture failed assets, failed checks, and log fingerprints.
7. **Asset and instance state** - use Bruin Cloud MCP or `bruin cloud assets list`, `bruin cloud instances list`, and `bruin cloud instances failed-logs` for the relevant run.
8. **Freshness** - compare last successful Cloud run/asset instance timestamps to declared SLA, schedule, or historical cadence.
9. **Schema warnings** - use Cloud validation errors first. If repo files are available, inspect asset definitions and static `bruin lineage` output as supporting evidence only.
10. **Recent repo changes** - inspect `git log`, recent branches/PRs when available, and touched asset/upstream files so code changes are considered before assigning cause.

Cache the raw output in `.context/triage-<timestamp>.json` so downstream skills can read it without re-querying.

## Classification

Every issue gets exactly one primary class. Pick the first that matches:

| Class | Signal | Route to |
|---|---|---|
| `transient` | Single failed run, error matches retry pattern (timeout, 5xx, deadlock, connection reset), no recent code change | retry once via `pipeline-backfill` using Bruin Cloud rerun/trigger only |
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

- **Auto-allowed**: read Bruin Cloud pipeline/run/asset state through MCP or `bruin cloud`, read repo/git/PR context, write triage snapshot to `.context/`, invoke other skills.
- **Requires approval**: nothing — this skill never modifies pipelines or repo state directly.
- **Never allowed**: local operational runs, skipping the classification step (every issue gets a class, even if the class is `unknown`), batching multiple unrelated issues into one specialist invocation.

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
