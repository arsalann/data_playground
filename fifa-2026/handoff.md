# Handoff

## Goal

Build a FIFA 2026 live tracker inside the existing `fifa-2026` Bruin pipeline and render it as a Bruin DAC dashboard. The latest user request was to reshape the live tracker into two tabs:

- **Current Live Matches**: list live matches first and show match detail plus minute-by-minute/player/team analysis where the data source supports it.
- **Overall Tournament Summary**: summarize the tournament by stage and group.

## Current State

The pipeline now has a working no-key live tracker based on **worldcup26.ir** endpoints. It ingests current fixtures, scores/status, teams, group standings, and stadiums into BigQuery.

The DAC dashboard is `fifa-2026/dashboard-dac/dashboards/fifa-2026-live.yml` with two tabs:

- `Current Live Matches`
  - KPI strip for live/scheduled/completed/total goals.
  - Live/upcoming scoreboard sorted live first, then scheduled, then completed.
  - Selected match detail table. Because DAC does not support row-click drilldowns in the installed build, this panel follows the first live match, or the next scheduled match before kickoff.
  - Source-limited event feed. It shows scorer strings when present; otherwise it shows an explicit source limitation.
- `Overall Tournament Summary`
  - KPI strip for total fixtures, next kickoff, goals per completed match, and last source update.
  - Stacked stage progress chart.
  - Stacked group progress chart.
  - Latest group standings table.

The free source currently reports the tournament before kickoff: `104` scheduled matches, `0` live, `0` completed.

## Files In Flight

- `fifa-2026/assets/fifa_raw/live_games.py`
- `fifa-2026/assets/fifa_raw/live_group_standings.py`
- `fifa-2026/assets/fifa_raw/live_stadiums.py`
- `fifa-2026/assets/fifa_raw/live_teams.py`
- `fifa-2026/assets/fifa_staging/live_matches.sql`
- `fifa-2026/assets/fifa_staging/live_group_standings.sql`
- `fifa-2026/assets/fifa_staging/live_stadiums.sql`
- `fifa-2026/assets/fifa_staging/live_teams.sql`
- `fifa-2026/assets/fifa_reports/live_overview.sql`
- `fifa-2026/assets/fifa_reports/live_scoreboard.sql`
- `fifa-2026/assets/fifa_reports/live_current_match_detail.sql`
- `fifa-2026/assets/fifa_reports/live_event_feed.sql`
- `fifa-2026/assets/fifa_reports/live_match_status_by_day.sql`
- `fifa-2026/assets/fifa_reports/live_stage_summary.sql`
- `fifa-2026/assets/fifa_reports/live_group_summary.sql`
- `fifa-2026/assets/fifa_reports/live_venue_load.sql`
- `fifa-2026/assets/fifa_reports/live_group_table.sql`
- `fifa-2026/dashboard-dac/dashboards/fifa-2026-live.yml`
- `fifa-2026/README.md`
- `fifa-2026/AGENTS.md`
- `fifa-2026/handoff.md`

## Changed

- Added four Bruin Python raw assets for live tracker endpoints.
- Added staging assets that deduplicate append snapshots and normalize match/team/stadium/standing fields.
- Added report assets for the scoreboard, selected match detail, source-limited event feed, stage summary, group summary, venue load, group table, and overview KPIs.
- Added a two-tab DAC live tracker dashboard.
- Updated pipeline docs with live-tracker run commands and source limitations.
- Pulled latest `origin/main` before finalizing; the branch fast-forwarded to `b34479f` and the live-tracker work reapplied without conflicts.
- Removed unsupported `hideName` fields from the existing `fifa-2026.yml` dashboard because the installed `dac 0.2.2` schema rejects them.

## Failed Attempts

- `worldcup26.ir/get/game?id=1` returned `Route not found`; `/get/game/1` returned an API error. `/get/game/{source_object_id}` works but does not expose richer fields than `/get/games`.
- The no-key source does not provide possession, shots, cards, substitutions, lineups, xG, or true minute-by-minute player/team events.
- The installed DAC build does not support row-click drilldowns from a table into a detail panel.
- First raw append run had BigQuery schema inference problems when scorer columns were all null and when a composite primary key was used. Fixed by adding synthetic snapshot IDs and forcing empty scorer strings instead of all-null columns.
- Parallel Bruin raw runs hit a temporary mmap file collision; sequential runs succeeded.
- The in-app browser backend was not available, so visual verification used local Playwright instead.

## Next Steps

- Add a licensed or API-keyed live-stat provider, likely API-Football, for minute-by-minute events, player/team stats, cards, substitutions, lineups, possession, shots, and xG.
- Replace the source-limited `live_event_feed` placeholder with provider-backed event rows.
- If DAC adds row-click support, wire scoreboard row selection into selected-match detail; until then the detail panel follows first live match or next scheduled match.
- Schedule live raw assets at a higher cadence during match windows and a lower cadence outside match windows.
- Re-run:
  - `bruin validate fifa-2026/`
  - `dac validate --dir fifa-2026/dashboard-dac`
  - `dac check --dir fifa-2026/dashboard-dac`
  - `dac serve --dir fifa-2026/dashboard-dac --port 8321`
