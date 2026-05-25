---
name: freshness-sla-check
description: Find Bruin assets past their freshness SLA and classify why. Distinguishes between "upstream is late", "source is down", "scheduler missed a tick", and "this is a real outage". Use on a recurring schedule or when triage flags 'stale'.
argument-hint: "[pipeline | 'all']"
---

# Freshness SLA Check

Late data is the most common pipeline failure mode and the easiest to miss because nothing actually _errors_ — runs just stop arriving. This skill makes the silence audible.

## When to Use

- Scheduled tick (every 15 min or hourly) to catch missed runs.
- `pipeline-triage` flagged one or more assets as `stale`.
- A human asks "is the data fresh".
- After a maintenance window, to confirm catch-up has happened.

Do not use for: failed runs that produced errors (use `pipeline-diagnose`), data that arrived but has wrong values (use `data-quality-investigate`), or assets that simply have no SLA declared.

## Inputs

| Input | Required | Example | Notes |
|---|---|---|---|
| `pipeline` | no | `wikipedia-ai-trends` \| `all` | Defaults to `all`. |
| `grace_minutes` | no | `15` | Tolerance beyond declared SLA before flagging. Default 10% of cadence, min 5 minutes. |

## SLA Sources

A freshness SLA can come from any of these. Use the most specific available:

1. **Explicit `meta.freshness`** on the asset YAML (e.g. `freshness: 1h`).
2. **Schedule cadence** from the pipeline (`schedule: daily` → SLA of 24h + grace).
3. **Materialization partition** (`partition_by: ds` → expected one new partition per day).
4. **Historical median interval** between successful runs over the last 30 runs. Use this only if nothing more specific is declared.

If none of these are available, the asset has no SLA — list it as `unmonitored` and skip.

## Context to Gather

1. **Asset inventory** - every asset in scope, with its declared or inferred SLA.
2. **Last successful run** - timestamp per asset.
3. **Last attempted run** - so we can distinguish "no attempt" from "attempt failed".
4. **Now** - current UTC time. Always use UTC; mixing timezones here causes false alarms.
5. **Upstream state** - for any stale asset, is the upstream also stale?
6. **Scheduler state** - is the Bruin Cloud scheduler healthy? A scheduler outage will look like 100 simultaneous SLA misses.
7. **Source-system status** - if a source publishes a status page or has a known maintenance window, factor it in.

## Stale Asset Classification

| Class | Definition | Default response |
|---|---|---|
| `upstream-stale` | Asset is stale, AND a declared upstream is also stale by an equal or greater amount | Re-run check on the upstream, not this asset |
| `source-down` | Asset has no upstreams, source connector is failing health checks | `pipeline-report` — wait, do not retry |
| `scheduler-missed` | Last attempt timestamp is older than expected; scheduler appears to have skipped | Retrigger the run via Bruin CLI |
| `run-stuck` | Last attempt is recent and still in progress past 3x median duration | Investigate or kill (requires approval); do not start another run |
| `attempted-failed` | Last attempt failed — should have been caught by triage, but list it here too | Route to `pipeline-diagnose` |
| `genuine-stale` | None of the above; data simply has not arrived | Report and escalate |
| `weekend-or-holiday` | Source naturally pauses (markets, business systems); current pause matches historical pattern | Mark as expected, do not alert |

## Decision Tree

```
assets = list_assets_with_slas(input.pipeline)
stale = []
for asset in assets:
    sla = resolve_sla(asset)
    last_ok = last_successful_run(asset)
    age = now - last_ok
    if age > sla + grace(input.grace_minutes, sla):
        stale.append(asset)

if not stale:
    return result(status='all-fresh', checked=len(assets))

# Classify each stale asset.
findings = []
for asset in stale:
    upstreams = upstream_states(asset)
    classification = classify_stale(asset, upstreams, scheduler_state(), source_health(asset.source))
    findings.append({asset, classification, age, sla})

# Deduplicate: if 10 assets are upstream-stale because of one root cause, surface the root once.
roots = collapse_to_roots(findings)
return result(status='stale-detected', roots=roots, full=findings)
```

## Actions & Guardrails

- **Auto-allowed**: read run history, read source-system health endpoints, write findings to `.context/`, retrigger a single missed run when class is `scheduler-missed` and the run has never succeeded for that partition.
- **Requires approval**: killing a stuck run, retriggering more than 3 missed runs in one invocation, declaring a pause "expected" without historical evidence.
- **Never allowed**: marking an asset fresh that has no recent successful run, fabricating an SLA when none is declared, alerting on `weekend-or-holiday` cases that match historical pause patterns.

## Verification

After retriggering a `scheduler-missed` run:

1. Confirm the new run completed within the SLA + grace.
2. Confirm row counts are within historical range for that partition.
3. Re-check the asset's freshness — it should now be `fresh`. If not, re-classify.

## Reporting

Write to `.context/freshness-<timestamp>.yml`:

```yaml
checked_at: 2026-05-22T14:30:00Z
pipeline: all
assets_total: 87
assets_fresh: 81
assets_stale: 4
assets_unmonitored: 2
roots:
  - root_cause: source-down
    source: wikipedia_api
    affected_assets:
      - raw.wikipedia_pageviews
      - raw.wikipedia_articles
    stale_for: 3h17m
    next_action: pipeline-report; do not retry until source health recovers
  - root_cause: scheduler-missed
    affected_assets:
      - marts.daily_top_articles
    stale_for: 1h42m
    next_action: retriggered via bruin run; will re-verify in 15 min
unmonitored:
  - raw.experimental_feed
  - raw.legacy_archive
```

A freshness report must always include `unmonitored` — silent assets are a known risk surface, not an oversight.
