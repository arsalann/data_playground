---
name: freshness-sla-check
description: Find Bruin assets past their inferred freshness expectations and classify why. Distinguishes between "upstream is late", "source is down", "scheduler missed a tick", "table stopped advancing", and "this is a real outage". Use on a recurring schedule or when triage flags 'stale'.
argument-hint: "[pipeline | 'all']"
---

# Freshness SLA Check

Late data is the most common pipeline failure mode and the easiest to miss because nothing actually _errors_ — runs just stop arriving. This skill makes the silence audible.

Use Bruin Cloud MCP for freshness/run state and Bruin docs/source-verified `bruin cloud ... --output json` commands as fallback. `bruin-cloud` is the repo convention for the `.bruin.yml` `bruin` connection, but the CLI cannot select that connection by name; export `BRUIN_CLOUD_API_KEY` when multiple `bruin` connections exist. Do not use local `bruin run` for operational retriggers.

## When to Use

- Scheduled tick (every 15 min or hourly) to catch missed runs.
- `pipeline-triage` flagged one or more assets as `stale`.
- A human asks "is the data fresh".
- After a maintenance window, to confirm catch-up has happened.

Do not use for: failed runs that produced errors (use `pipeline-diagnose`) or data that arrived but has wrong values (use `data-quality-investigate`). If no freshness expectation can be inferred from description, schedule, table metadata, or history, list the asset as `unmonitored`.

## Inputs

| Input | Required | Example | Notes |
|---|---|---|---|
| `pipeline` | no | `wikipedia-ai-trends` \| `all` | Defaults to `all`. |
| `project_id` | no | `01krk817ys2j45frftg1q4xfgv` | Bruin Cloud project ID. Required if multiple projects are visible and it cannot be inferred. |
| `grace_minutes` | no | `15` | Tolerance beyond inferred cadence before flagging. Default 10% of cadence, min 5 minutes. |

## Freshness Expectation Sources

A freshness expectation can come from any of these. Use the strongest available evidence. `meta.freshness`, if present, is a project convention and not a documented Bruin freshness SLA field; document the expected local format before relying on it. Do not invent Bruin metadata fields.

1. **Asset description** - infer cadence or processing window from documented intent, e.g. "runs monthly and processes the previous month's financial performance".
2. **Schedule cadence** - compare the pipeline schedule to table movement, e.g. an hourly pipeline whose table was last updated 3 days ago is suspicious. Official docs prefer `@daily`, `@hourly`, or cron; source validation also accepts `daily`, `hourly`, `weekly`, and `monthly`.
3. **Warehouse table metadata** - table `last_updated_at`, partition metadata, row count, size, and other warehouse-native freshness indicators.
4. **Freshness columns** - max values of timestamp/date columns such as `MAX(extracted_at)`, `MAX(inserted_at)`, `MAX(dt)`, partition max date, or equivalent columns present in the table.
5. **Table growth pattern** - compare recent growth to historical growth, e.g. `SELECT extracted_at, COUNT(*) FROM table_xyz GROUP BY 1 ORDER BY 1 DESC`; slower-than-usual growth or a frozen table is a freshness signal.
6. **Historical Cloud run cadence** - median interval between successful runs over the last 30 runs. Use this only as supporting evidence, because a successful run does not prove the table advanced.

If none of these are available, list the asset as `unmonitored` and skip freshness alerting. The report must say which evidence was missing.

## Context to Gather

1. **Asset inventory** - every asset in scope, with its inferred freshness expectation. Use Bruin Cloud MCP or `bruin cloud assets list --project-id <project-id> --pipeline <pipeline> --output json`. For `pipeline: all`, enumerate `bruin cloud projects list --output json`, then `bruin cloud pipelines list --project-id <project-id> --output json`, then run the per-pipeline commands; there is no single Cloud CLI `--pipeline all` flag.
2. **Last successful run** - timestamp per asset/instance from Bruin Cloud.
3. **Last attempted run** - from `bruin cloud runs list --project-id <project-id> --pipeline <pipeline> --limit 20 --output json` and `bruin cloud instances list --project-id <project-id> --pipeline <pipeline> (--run-id <run-id> | --latest) --output json`, so we can distinguish "no attempt" from "attempt failed".
4. **Now** - current UTC time. Always use UTC; mixing timezones here causes false alarms.
5. **Upstream state** - for any stale asset, is the upstream also stale?
6. **Scheduler state** - is the Bruin Cloud scheduler healthy? A scheduler outage will look like many simultaneous freshness misses.
7. **Source-system status** - if a source publishes a status page or has a known maintenance window, factor it in.
8. **Warehouse table metadata** - last updated timestamp, partition metadata, row count, table size, and any other warehouse-native freshness fields available. Use `bruin query --connection <connection> --query <sql> --description "freshness metadata probe for <asset>" --output json`; dry-run expensive scans first.
9. **Max freshness columns** - query `MAX(extracted_at)`, `MAX(inserted_at)`, `MAX(dt)`, partition max date, or similar date/timestamp columns when present with `bruin query --asset <asset-file> --query <sql> --description "freshness max-column probe for <asset>" --output json`.
10. **Growth history** - query recent row growth by freshness column/partition and compare it to historical ranges with `bruin query --connection <connection> --query <sql> --description "freshness growth-history probe for <asset>" --limit 1000 --output json`. If recent growth is materially below historical growth, classify that as a table-state freshness signal even if Cloud runs succeeded.
11. **Repo context** - inspect asset descriptions and recent PRs/commits when freshness expectations or recent logic changes are relevant.

## Stale Asset Classification

| Class | Definition | Default response |
|---|---|---|
| `upstream-stale` | Asset is stale, AND a declared upstream is also stale by an equal or greater amount | Re-run check on the upstream, not this asset |
| `source-down` | Asset has no upstreams, source connector is failing health checks | human escalation — wait, do not retry |
| `scheduler-missed` | Last attempt timestamp is older than expected; scheduler appears to have skipped | Retrigger through Bruin Cloud MCP or `bruin cloud runs trigger --project-id <project-id> --pipeline <pipeline> --start-date <start> --end-date <end> --output json` only |
| `run-stuck` | Last attempt is recent and still in progress past 3x median duration | Investigate or kill (requires approval); do not start another run |
| `attempted-failed` | Last attempt failed — should have been caught by triage, but list it here too | Route to `pipeline-diagnose` |
| `table-frozen` | Bruin Cloud runs may be succeeding, but warehouse table metadata or max freshness columns have not advanced | Investigate source/transform; propose metadata/check PR if the expected growth pattern can be documented |
| `growth-regression` | Table is still growing, but recent row growth is materially below historical range | Route to `data-quality-investigate` or propose a custom check if root cause is understood |
| `genuine-stale` | None of the above; data simply has not arrived | Report and escalate |
| `weekend-or-holiday` | Source naturally pauses (markets, business systems); current pause matches historical pattern | Mark as expected, do not alert |

## Decision Tree

```
assets = list_assets_with_freshness_expectations(input.pipeline)
stale = []
for asset in assets:
    expectation = infer_freshness_expectation(
        asset.description,
        asset.pipeline_schedule,
        warehouse_table_metadata(asset),
        max_freshness_columns(asset),
        table_growth_history(asset),
        cloud_run_history(asset),
    )
    if expectation.unmonitored:
        continue
    last_ok = last_successful_run(asset)
    table_state = current_table_freshness(asset)
    age = max(now - last_ok, now - table_state.last_advanced_at)
    if violates(expectation, last_ok, table_state, grace=input.grace_minutes):
        stale.append(asset)

if not stale:
    return result(status='all-fresh', checked=len(assets))

# Classify each stale asset.
findings = []
for asset in stale:
    upstreams = upstream_states(asset)
    classification = classify_stale(asset, upstreams, scheduler_state(), source_health(asset.source), table_state)
    findings.append({asset, classification, age, expectation, table_state})

# Deduplicate: if 10 assets are upstream-stale because of one root cause, surface the root once.
roots = collapse_to_roots(findings)
return result(status='stale-detected', roots=roots, full=findings)
```

## Actions & Guardrails

- **Auto-allowed**: read Bruin Cloud run history, read source-system health endpoints, read warehouse table metadata/freshness columns/growth history, write findings to `.context/`, retrigger a single missed Cloud interval when class is `scheduler-missed`, the run is not source/raw destructive, and the interval has never succeeded.
- **Requires approval**: killing a stuck run, retriggering more than 3 missed runs in one invocation, declaring a pause "expected" without historical evidence, or opening a maintenance PR to document inferred freshness/growth expectations and add/update custom checks.
- **Never allowed**: local operational runs, marking an asset fresh without recent Cloud and table-level evidence, fabricating a Bruin freshness metadata field, treating project-convention `meta.freshness` as official Bruin behavior, treating "no metadata found" as fresh, or alerting on `weekend-or-holiday` cases that match historical pause patterns.

When a stable freshness or growth expectation is discovered, propose a `maintenance-pr` only with approval. The PR may update the asset description and add a custom check. Example:

```sql
SELECT COUNT(*) > 5000000
FROM table_xyz
WHERE extracted_at = (SELECT MAX(extracted_at) FROM table_xyz)
```

Use observed historical ranges in the description/check, e.g. "expected 5m-10m rows per day", and include the evidence behind that range in the finding.

## Verification

After retriggering a `scheduler-missed` run:

1. Confirm the new run completed within the inferred cadence + grace.
2. Confirm table metadata advanced.
3. Confirm max freshness columns advanced, such as `MAX(extracted_at)`, `MAX(inserted_at)`, or `MAX(dt)`.
4. Confirm row counts/growth are within historical range for that interval.
5. Re-check the asset's freshness — it should now be `fresh`. If not, re-classify.

## Reporting

Write to `.context/freshness-<timestamp>.yml`:

```yaml
checked_at: 2026-05-22T14:30:00Z
pipeline: all
assets_total: 87
assets_fresh: 81
assets_stale: 4
assets_unmonitored: 2
table_signals:
  - asset: marts.daily_top_articles
    table_last_updated_at: 2026-05-22T13:55:00Z
    max_extracted_at: 2026-05-22T00:00:00Z
    recent_growth_rows: 1_024_000
    historical_growth_rows_range: 5_000_000-10_000_000
roots:
  - root_cause: source-down
    source: wikipedia_api
    affected_assets:
      - raw.wikipedia_pageviews
      - raw.wikipedia_articles
    stale_for: 3h17m
    next_action: human escalation; do not retry until source health recovers
  - root_cause: scheduler-missed
    affected_assets:
      - marts.daily_top_articles
    stale_for: 1h42m
    next_action: retriggered via Bruin Cloud; will re-verify in 15 min
  - root_cause: growth-regression
    affected_assets:
      - marts.daily_revenue
    evidence: recent growth 1.0m rows/day vs historical 5m-10m rows/day
    next_action: propose maintenance-pr to document expected growth and add custom check; approval required
unmonitored:
  - raw.experimental_feed
  - raw.legacy_archive
```

A freshness report must always include `unmonitored` and must distinguish "Cloud run is fresh but table is stale" from "Cloud run is stale". Silent assets are a known risk surface, not an oversight.
