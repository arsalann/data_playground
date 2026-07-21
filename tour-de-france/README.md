# Tour de France 2026: Race for Yellow

Daily Bruin pipeline and DAC dashboard for the 2026 men's Tour de France. It
preserves source snapshots so that late corrections can be audited while all
reports use the latest published result for each stage.

## Source

- [ProCyclingStats: Tour de France 2026](https://www.procyclingstats.com/race/tour-de-france/2026)
  — stage-result and per-stage general-classification (GC) pages. Data remains
  subject to ProCyclingStats terms of use and may be corrected after publication.

## Assets

- `raw.stage_snapshots` — append-only, browser-compatible snapshots of published
  stages 1–21, including complete stage-result and top-30 GC JSON payloads.
- `staging.stage_latest` — latest complete source snapshot for each stage.
- `staging.stage_results` — normalized stage placings, winner margin, and a
  transparent result-shape classification.
- `staging.gc_standings` — normalized top-30 GC standings, gaps to yellow, and
  reported rank movement after each completed stage.
- `staging.gc_top30_team_presence` — team rider counts in the GC top 30.
- `report.race_kpis` — one-row race summary for the dashboard KPI cards.
- `report.gc_gaps_by_stage` — gaps to positions 2, 3, and 5 after each stage.
- `report.yellow_margin_change` — stage-to-stage movement in the leader's margin.
- `report.current_gc_top10` — current top-10 GC table.
- `report.stage_outcomes` — completed-stage winner and result-shape table.

## Schedule and refresh

The pipeline cron (`30 19 4-26 7 *`) runs at 19:30 UTC, which is 21:30 in
Europe/Paris during the 2026 Tour. It is intentionally limited to July 4–26;
the raw asset also rejects dates outside that race window and rest days.

Start the dashboard locally with:

```bash
dac serve --dir tour-de-france/dashboard-dac --port 8321
```

Then open <http://localhost:8321>.

DAC v0.6.0 serves current query results on page load and live-reloads definition
changes. It no longer supports the earlier dashboard-level timed-refresh or
theme fields; reload the page to query the most recently materialized pipeline
outputs between scheduled pipeline runs.

## Development commands

```bash
# Validate pipeline definitions
bruin validate tour-de-france/

# Smoke-test one completed stage (end date is exclusive; adjust while live)
bruin run --start-date 2026-07-12 --end-date 2026-07-13 \
  tour-de-france/assets/raw/stage_snapshots.py

# Rebuild downstream transformations from the raw snapshot
bruin run --downstream tour-de-france/assets/raw/stage_snapshots.py

# Test transformation logic with fixtures
bruin unit-test tour-de-france/

# Validate and execute dashboard queries
dac validate --dir tour-de-france/dashboard-dac
dac check --dir tour-de-france/dashboard-dac
```

## Limitations

- PCS is the sole source; no official timing-feed reconciliation is attempted.
- Published results and classifications can be revised. The dashboard reports
  only the newest saved snapshot, while raw history remains available for audit.
- “Candidate breakaway” is a reproducible result-shape heuristic based on a
  winner margin above 10 seconds; it is not an official tactical classification.
- The dashboard concerns the 2026 men's Tour only. It is not a historical
  backfill or a Tour de France Femmes tracker.
