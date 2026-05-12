# fifa-2026

Multi-hypothesis broad-survey pipeline for the **2026 FIFA World Cup** (June 11 – July 19, 2026, hosted by USA / Canada / Mexico across 16 cities). Built with Bruin against BigQuery, with a multi-tab Bruin DAC dashboard.

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
