# AGENTS.md — `fifa-2026`

Pipeline-specific build log, conventions, and methodology. Read before modifying any asset here. Defers to root `/AGENTS.md` for repo-wide conventions and only adds pipeline-specific overrides.

## Origin

Built as a **broad survey** of the 2026 FIFA World Cup (USA / Canada / Mexico, June 11 – July 19, 2026) covering five hypotheses simultaneously, so the final narrative can be picked from what the data shows. The user explicitly chose breadth-of-clean-data over depth-on-any-one-hypothesis at scoping.

## Hypotheses

| # | Hypothesis |
|---|---|
| H1 | Heat-risk concentration: per-match expected WBGT / heat-band at venue+kickoff hour from ERA5 climatology (2010–2024) with Meteostat METAR cross-check. |
| H2 | Travel burden across 48 group-stage teams: km, altitude delta, tz shift across each team's 3 group fixtures. |
| H3 | Polymarket implied odds vs. FIFA April-2026 ranking: per-team gap between market-implied and ranking-implied probabilities; flag systematic mispricing with stale-price guards. |
| H4 | Altitude effect on scoring at Mexico City + Guadalajara: historical goals/match by altitude band from prior World Cups (2010–2022). |
| H5 | Stadium capacity vs. expected demand: capacity vs. heuristic demand score (metro pop + Google-Trends interest by participating teams). |

## Strict rules adopted

Inherited from root `/AGENTS.md` and from saved memory feedback:

1. **Manifest as source of truth.** The 104-match schedule, 16 venues, 48 teams, and 12 groups are encoded in `assets/fifa_raw/tournament_manifest.yml`. A single Python asset (`tournament_data.py`) unloads it into four raw tables. No Wikipedia scraping for the tournament structure — too fragile.
2. **Methodology parity with `polymarket-weather`.**
    - Open-Meteo grid (`source='openmeteo_grid'`) and Meteostat stations (`source='meteostat'`) are kept distinct in staging — never cross-aggregated.
    - Meteostat is sampled at top-of-hour, which can hide sub-hour anomalies; this caveat is carried as `methodology_note` in `match_climatology` and surfaced in H1 footnotes.
    - Polymarket ingestion uses the same `fetch_with_retry` + paginated event pattern as `polymarket-weather/assets/polymarket_weather_raw/polymarket_markets.py`.
3. **Verify spatial scope.** Each station's reported lat/lon must be within 0.05° of the configured value at ingestion; logged as a sanity check.
4. **Verify data claims.** Every number cited in `README.md`, asset descriptions, and the dashboard must be derived from a warehouse query — not hand-written. Use approximate language ("likely top-3 hottest matches") until a query confirms.
5. **No sensational framing.** "Likely under-subscribed", "expected heat band", "implied gap". Never "scandal", "rigged", "shock".
6. **Chart anatomy.** Title = what the chart is, caption = the insight, footnote = sources + tools + limits. Per-tab footnotes use the verbatim text-widget pattern from `polymarket-weather.yml`.
7. **`bruin ai enhance` pitfalls.** Always run `bruin validate` immediately after enhance, diff the result, and never apply bulk regex edits to YAML — rewrite a corrupted asset by hand.
8. **All Bruin-managed Python assets**: `image: python:3.11`, structured logging, `extracted_at = datetime.now(timezone.utc)`, `BRUIN_START_DATE` / `BRUIN_END_DATE` env vars with defaults, retry + exponential backoff with 5 retries, `time.sleep(0.5)` between requests, return partial data on persistent rate limit.

## Data lineage

```
tournament_manifest.yml
  └─ tournament_data.py        ─►  fifa_schedule
                               ─►  host_venues
                               ─►  qualified_teams
                               ─►  groups

openmeteo_climatology.py       ─┐
meteostat_hourly.py            ─┴─►  match_climatology   ─►  h1_match_heat_risk
fifa_schedule + host_venues  ──┘

fifa_schedule + host_venues + qualified_teams ──►  matches_enriched
matches_enriched              ─►  team_travel_segments  ─►  h2_team_travel_burden

polymarket_fifa_markets.py    ─┐
polymarket_fifa_clob.py       ─┴─►  team_market_implied_prob ─┐
fifa_world_ranking.py         ─────►  team_ranking_april2026 ─┴─►  h3_market_vs_ranking

historical_wc_matches.py      ─────►  altitude_match_panel    ───►  h4_altitude_scoring

host_venues + city_demographics + (google trends best-effort) ─►  stadium_demand_inputs ─►  h5_capacity_vs_demand
```

## Things future agents should not do

- Do not reintroduce Wikipedia scraping for tournament structure — manifest is source of truth.
- Do not mix `source='openmeteo_grid'` and `source='meteostat'` rows without explicit filtering in staging.
- Do not drop the `methodology_note` column from `match_climatology` — it's load-bearing for the dashboard footnote.
- Do not commit `.bruin.yml`, `credentials/`, or `.env*` files.
- Do not re-run a `create+replace` raw asset without explicit `BRUIN_START_DATE` / `BRUIN_END_DATE` env vars — Bruin's default schedule interval is one day, which will overwrite the full historical window with a single day of data.
