# fake-webevents

Local DuckDB fixture pipeline for testing self-healing pipeline skills against web analytics data. It generates deterministic pageview events across countries, browsers, devices, and page paths, then injects a country-driven traffic spike, a new browser segment, and a recent multi-day source gap.

This pipeline is safe for local `bruin run` testing. It is not a production-pattern pipeline and does not exercise Bruin Cloud actions directly.

## Assets

- `raw.pageviews` (`assets/raw/pageviews.py`) generates pageview events with session, user, country, browser, device, path, event time, and event date.
- `staging.daily_pageviews` (`assets/staging/daily_pageviews.sql`) aggregates daily pageviews by country and browser.

## Skill Scenarios

| Scenario | Date/window | Trigger asset/check | Expected skill path | Expected classification |
|---|---:|---|---|---|
| Indonesia traffic spike | `2026-05-18` | Metric `staging.daily_pageviews.pageviews` | `pipeline-triage` -> `anomaly-investigate` -> `pipeline-report` | `anomaly`, `single-dimension-driver` with `country=ID` |
| New browser segment | Starts `2026-05-15` | `staging.daily_pageviews` check `known_browsers_only` | `pipeline-triage` -> `data-quality-investigate` or `schema-drift-check` -> `pipeline-report` | `quality-fail` with enum/new-segment context; schema drift class `enum-value-added` when treated as source contract drift |
| Multi-day recent gap | Today, yesterday, and two days ago | `raw.pageviews` returns 0 rows for the latest three dates | `pipeline-triage` -> `freshness-sla-check` -> `pipeline-report` | `stale`, likely `source-down`, `table-frozen`, or `genuine-stale` depending Cloud/table evidence |
| Backfill/rerun risk | Any scoped rerun over already-loaded events | `raw.pageviews` uses `append` materialization | `pipeline-backfill` dry run -> `pipeline-report` | Approval required for append reruns where data already exists |

## What This Pipeline Covers

- `pipeline-triage`: anomaly, quality-fail, and stale routing.
- `anomaly-investigate`: single-dimension driver attribution and new-segment analysis.
- `data-quality-investigate`: failed categorical-contract check investigation.
- `schema-drift-check`: `enum-value-added` classification for a source contract change.
- `freshness-sla-check`: recent source gap classification.
- `pipeline-backfill`: dry-run risk assessment for append materialization.
- `pipeline-report`: final incident, warning, or digest summary.

It does not directly test Slack posting, GitHub PR creation, Bruin Cloud rerun execution, capacity failures, code-regression attribution, or transient Cloud failures. Those require Cloud/Slack/GitHub context or synthetic Cloud run metadata outside this data fixture.

## Useful Commands

```bash
bruin validate fake-webevents --output json

# Traffic-spike anomaly fixture.
bruin run fake-webevents/assets/raw/pageviews.py --start-date 2026-04-20 --end-date 2026-05-18
bruin run fake-webevents/assets/staging/daily_pageviews.sql

# New browser segment / enum-value-added fixture.
bruin run fake-webevents/assets/raw/pageviews.py --start-date 2026-05-15 --end-date 2026-05-15
bruin run fake-webevents/assets/staging/daily_pageviews.sql
bruin run --only checks fake-webevents/assets/staging/daily_pageviews.sql

# Freshness fixture: use current dates to hit the intentional empty-source window.
bruin run fake-webevents/assets/raw/pageviews.py --start-date $(date -u -v-2d +%F) --end-date $(date -u +%F)

# Lineage and static context.
bruin lineage fake-webevents/assets/staging/daily_pageviews.sql --output json --full
```

## Expected Notes for Agents

- The Arc browser is a deliberate contract failure, not a bad row to delete. The right response is to classify it, document downstream impact, and route to report or a gated maintenance PR if a contract update is approved.
- The recent gap is relative to the machine date, so it remains useful for freshness checks over time.
- Treat local `bruin run` as allowed only because this is a fake-data test pipeline.
