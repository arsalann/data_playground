# fifa-2026

Multi-hypothesis broad-survey pipeline for the **2026 FIFA World Cup** (June 11 – July 19, 2026, hosted by USA / Canada / Mexico across 16 cities). Built with Bruin against BigQuery, with a multi-tab Bruin DAC dashboard.

This pipeline now also includes a **FIFA 2026 Live Match Tracker** DAC dashboard backed by free current-state endpoints from `worldcup26.ir`.

The pipeline ingests data covering five investigative angles, leaving the choice of final narrative to be made after looking at what the data shows:

| # | Hypothesis | Data primitives |
|---|---|---|
| H1 | Heat-risk concentration per match | ERA5 climatology + Meteostat METAR at venue grid |
| H2 | Travel burden across 48 group-stage teams | Schedule × venue coords × team home coords |
| H3 | Polymarket implied odds vs. FIFA April-2026 ranking | Polymarket Gamma + CLOB; FIFA ranking snapshot |
| H4 | Altitude effect on scoring at Mexico City + Guadalajara | Historical WC + Liga MX match panel |
| H5 | Stadium capacity vs. expected demand | Capacity × metro population × Google Trends team interest |

## Pipeline shape

```
fifa-2026/
  pipeline.yml
  AGENTS.md                                # methodology + sources + caveats
  assets/
    fifa_raw/
      tournament_manifest.yml              # source-of-truth seed: 16 venues, 48 teams, 12 groups, 104 matches
      tournament_data.py                   # writes schedule, venues, teams, groups from the manifest
      fifa_world_ranking.py
      polymarket_fifa_markets.py
      polymarket_fifa_clob.py
      openmeteo_climatology.py
      meteostat_hourly.py
      historical_wc_matches.py             # 2010–2022 WC matches with venue elevation
      city_demographics.py                 # metro pop seed
    fifa_staging/                          # join + clean to analytical primitives
    fifa_reports/                          # one report table per hypothesis (h1..h5)
  dashboard-dac/
    dashboards/
      fifa-2026.yml                        # 5-tab DAC dashboard
      themes/ibm-cb-dark.yml
      queries/                             # any per-widget drill-in SQL
```

## Why a manifest, not Wikipedia scraping

The 104-match schedule, 16 venues, and 48-team group draw are stable public data finalised on 2025-12-05. Scraping is fragile and would put the pipeline at the mercy of Wikipedia layout changes. The manifest is the single source of truth for these primitives; a manifest-backed Python asset simply unloads them into BigQuery raw tables. Time-varying signals (Polymarket prices, FIFA ranking, weather) come from live APIs.

## Run

```bash
# Validate
bruin validate fifa-2026/

# Raw layer (one-shot)
bruin run --tag fifa_raw fifa-2026/

# Staging + reports
bruin run --tag fifa_staging fifa-2026/
bruin run --tag fifa_reports fifa-2026/

# Dashboard
cd fifa-2026 && bruin dac serve --dir . --port 8321
```

## Live tracker

The live tracker ingests free no-key endpoints from **[worldcup26.ir](https://worldcup26.ir/api-docs)** and renders two DAC tabs:

- **Current Live Matches** - live/upcoming scoreboard, selected match detail, and source-limited event feed.
- **Overall Tournament Summary** - tournament progress by stage, group progress, and latest group standings.

- `/get/games` -> `fifa_raw.live_games`: scoreline, status, kickoff, teams, stadium, and scorer strings.
- `/get/teams` -> `fifa_raw.live_teams`: team names, FIFA codes, flags, and group assignments.
- `/get/groups` -> `fifa_raw.live_group_standings`: group table points, record, goals for/against, and goal difference.
- `/get/stadiums` -> `fifa_raw.live_stadiums`: stadium, host city/country, region, and capacity.

Run the live tracker layer:

```bash
bruin run fifa-2026/assets/fifa_raw/live_teams.py
bruin run fifa-2026/assets/fifa_raw/live_stadiums.py
bruin run fifa-2026/assets/fifa_raw/live_games.py
bruin run fifa-2026/assets/fifa_raw/live_group_standings.py
bruin run fifa-2026/assets/fifa_staging/live_teams.sql
bruin run fifa-2026/assets/fifa_staging/live_stadiums.sql
bruin run fifa-2026/assets/fifa_staging/live_matches.sql
bruin run fifa-2026/assets/fifa_staging/live_group_standings.sql
bruin run fifa-2026/assets/fifa_reports/live_overview.sql
bruin run fifa-2026/assets/fifa_reports/live_current_match_detail.sql
bruin run fifa-2026/assets/fifa_reports/live_event_feed.sql
bruin run fifa-2026/assets/fifa_reports/live_match_status_by_day.sql
bruin run fifa-2026/assets/fifa_reports/live_stage_summary.sql
bruin run fifa-2026/assets/fifa_reports/live_group_summary.sql
bruin run fifa-2026/assets/fifa_reports/live_venue_load.sql
bruin run fifa-2026/assets/fifa_reports/live_group_table.sql
bruin run fifa-2026/assets/fifa_reports/live_scoreboard.sql
```

Validate and serve the DAC dashboard:

```bash
dac validate --dir fifa-2026/dashboard-dac
dac check --dir fifa-2026/dashboard-dac
dac serve --dir fifa-2026/dashboard-dac --port 8321
# open http://localhost:8321
```

The no-key source provides scores, fixtures, standings, teams, stadiums, and scorer strings. It does **not** expose true row-click drilldowns, detailed in-match statistics, minute-by-minute player actions, possession, shots, cards, lineups, substitutions, or xG. For those fields, add a quota-aware API-Football ingestion asset after configuring a free API key in Bruin secrets.

## Verified data sources

- **Open-Meteo Historical Weather API** — ERA5 reanalysis 2010–2024, June–July, 16 grid points.
- **Meteostat hourly** — METAR observations at the primary ICAO of each host venue.
- **Polymarket Gamma + CLOB** — `tag_slug=fifa` events + per-token price history.
- **inside.fifa.com** — April 2026 Men's Ranking (best-effort; falls back to manifest seed if blocked).
- **Wikipedia per-tournament results** — historical World Cup matches 2010 / 2014 / 2018 / 2022 with venue elevations.

## Limitations (load-bearing — show in dashboard footnotes)

- METAR observations are sampled at the top of every UTC hour, so sub-hour temperature spikes are invisible. Same caveat as `polymarket-weather` — never cross-aggregate Meteostat with Open-Meteo.
- WBGT in `match_climatology` is a Stull (2011) approximation; ERA5 has no globe-temperature observation.
- Polymarket FIFA-2026 winner liquidity is concentrated in the top ~10 teams; tail-team prices may be stale (we carry `last_trade_at` and `volume_24h`).
- Tournament manifest team home-city coordinates are capital-city proxies — flag in H2 footnote.
- Demand score in H5 is a heuristic composite, not a market signal.
