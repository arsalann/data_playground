# AGENTS.md — `polymarket-weather`

Pipeline-specific build log, rules, and conventions. Read this before modifying any asset in this directory. Defers to the root `/AGENTS.md` for repo-wide conventions and only adds pipeline-specific overrides.

## Origin

This pipeline was built in response to The Guardian's 2026-04-23 reporting on alleged tampering of the Paris–Charles de Gaulle temperature sensor in conjunction with Polymarket "Highest temperature in Paris" daily markets that resolved on 2026-04-06 (winning bucket 21 °C) and 2026-04-15 (winning bucket 22 °C). Combined winnings ~$34 k. Météo-France filed a complaint for "tampering of an automated data processing system". Polymarket subsequently switched the resolution source for daily Paris markets from CDG (Wunderground LFPG) to Le Bourget / Bonneuil-en-France (Wunderground LFPB) starting on the 2026-04-19 event.

The user (a senior data engineer at a leading journalist team, journalist-grade output expected) requested a thorough, multi-station, sub-hour investigation plus a survey of the broader 2026 weather-betting universe.

## User instructions (verbatim, original session)

> follow the same format as other pipelines and create a pipeline that further analyzes this hairdryer incident (e.g. comparing the temperatures recorded by paris airport vs other weather stations in paris) and also let's analyze other polymarket bets around temperature and weather and analyze weather data against it
>
> be very thorough and act like a senior advanced data analyst and scientist working at a leading journalist team, do not stop until you have gotten to the bottom of this and have fully analyzed the paris incident and investigated other weather related betting events that have happened in 2026.
>
> create the entire project inside a new pipeline folder named "polymarket-weather" and let's recreate all the assets (you can reference the other polymarket pipeline but do not use the existing tables or assets) - use "polymarket_weather" with the suffices _raw _staging _report for the bigquery dataset name
>
> at each phase, run the bruin ai enhance to properly generate metadata and context (i.e. after ingestion assets are created and ran, after staging assets are created and ran, after report assets are created and ran)
>
> after the initial analysis and investigation is completed, update the already existing metadata and descriptions and documentation according to the findings - update it at the very end again when the PR is created
>
> document the entire chat, instructions, rules, and steps inside the pipeline folders' own AGENTS.md file
>
> create a context layer explaining the entire pipeline inside a pipeline-level README.md file
>
> read the existing AGENTS.md and follow it strictly

## Strict rules adopted

Inherited from root `/AGENTS.md` and from saved memory feedback:

1. **No reuse of existing polymarket tables.** This pipeline recreates every polymarket asset in `polymarket_weather_raw`. No `depends:` on `raw.polymarket_markets` or `raw.polymarket_price_history`.
2. **Identical spatial methodology.** All six Paris stations queried via the same Meteostat call, same chunking, same retry. The Open-Meteo gridded reanalysis is labelled `source='openmeteo_grid'` and never mixed into station aggregates.
3. **Verify spatial scope.** At ingestion time each station's reported lat/lon is asserted within 0.05° of the configured value and logged. The seventh "candidate" Villacoublay station I had originally planned to identify as `07145` was corrected to `07147` after this verification — `07145` is Trappes.
4. **Verify data claims.** Every number cited in `README.md`, asset descriptions, and the dashboard must be derived from a warehouse query — not hand-written. Approximate language ("≥3°C residual") is preferred when a precise number isn't yet computed. The README's Findings section was filled in only after running queries against the populated warehouse.
5. **No sensational framing.** "Alleged tampering", "anomaly", "disagreement". Never "scandal", "trick", "scam", "rigged", "hack".
6. **Verify rendered output.** Drove the running Streamlit app with Playwright (headless Chromium, `device_scale_factor=2`), screenshot every `[data-testid="stVegaLiteChart"]`, and read each screenshot. Caught and fixed two issues this way: (a) initial dashboard threw `StreamlitSecretNotFoundError` because my fallback to ADC was guarded by `"key" in st.secrets` which itself raises when no toml is present, fixed by wrapping in try/except; (b) `paris_rank` metric showed `#39 of 54` because `idxmax()` returns the original DataFrame index, not the post-sort rank — fixed with `reset_index(drop=True)`. (c) The Apr 6 / Apr 15 price filter joined ticks to events on `DATE(ts_local_paris) = event_local_date`, but bucket markets trade for days before resolution, so the Yes-price chart showed multi-day noise; fixed by joining on `market_id` and filtering ticks to the 24-hour window ending at event resolution.
7. **`bruin ai enhance` pitfalls.** Always run `bruin validate` immediately after enhance, diff the result, and never apply bulk regex edits to YAML — rewrite a corrupted asset by hand instead. Specific corrections required after each phase:
    - Phase 1 raw: removed AI-introduced `unique` checks on `market_id`, `slug`, `clob_token_ids`, `condition_id` (the raw table is `append`-strategy, so duplicates across snapshots are expected). Removed `positive` checks on `precipitation_mm`, `wind_speed_kmh`, `sunshine_minutes` in `station_daily` (zero is valid). Removed `unique` on `station_id` (composite PK).
    - Phase 2 staging: removed `not_null` on `paris_daily_april_2026` (boolean is null when end_date is missing) and on `temp_max_grid`, `bucket_grid` in `market_resolutions` (grid data lags by 1-2 days).
    - Phase 3 report: no corrections required.
8. **All Bruin-managed Python assets**: `image: python:3.11`, structured logging (`logging.basicConfig` + module logger), `extracted_at = datetime.now(timezone.utc)`, `BRUIN_START_DATE` / `BRUIN_END_DATE` env vars with sensible defaults, retry + exponential backoff with 5 retries, `time.sleep(0.5)` between requests, return partial data on persistent rate limit, append + dedup-in-staging where appropriate.

## Build phases (executed)

| Phase | Outcome |
|---|---|
| 0 | Scaffold: directory tree, `pipeline.yml`, README + AGENTS skeletons. `bruin validate` clean. |
| 1 | Raw layer: 5 Python assets in `polymarket_weather_raw`. AI-enhanced metadata, problematic checks stripped, all 5 assets pass quality checks on full window data. |
| 2 | Staging layer: 6 SQL assets in `polymarket_weather_staging`. AI-enhanced metadata, problematic checks stripped. Symmetric anomaly threshold (`ABS(peer_residual) ≥ 3 AND ABS(peer_z) ≥ 2`) replaces an earlier internal-z-based threshold that was too tight on warm-up days. |
| 3 | Report pre-aggregations: 4 SQL assets. AI-enhanced. |
| 4 | Streamlit dashboard with 5 sections + Playwright verification. Reference screenshots saved in `docs/screenshots/`. |
| 5 | Findings extracted from the warehouse and folded back into asset descriptions and the pipeline-level README. |
| 6 | PR opened; this AGENTS.md and the README updated again with final numbers. |

## Data lineage

```
station_hourly        ─┐
                       ├─►  temperature_hourly  ─┬─►  anomaly_residuals       ─►  april_residuals
openmeteo_grid        ─┘                         │                            │
                                                 │                            └─►  spike_evidence
station_daily         ──►  temperature_daily ────┘
station_hourly        ─────────────────────────────►  market_resolutions      ─►  counterfactual_summary
polymarket_markets    ─►  markets_enriched       ─┬─►  market_resolutions
polymarket_prices     ─►  prices_enriched        ─┘                           ─►  weather_markets_overview
```

## File-name / asset-name discipline

Per root `AGENTS.md`: `<parent_folder>.<file_stem>` is the asset name and the BigQuery destination. So `assets/polymarket_weather_raw/station_hourly.py` → `polymarket_weather_raw.station_hourly`. Never decouple file name from asset name.

## Things future agents should not do

- Do not reintroduce any reference to the legacy `raw.polymarket_markets` / `raw.polymarket_price_history` tables.
- Do not mix Open-Meteo grid data into station aggregates without an explicit `source` filter.
- Do not hand-edit `accepted_values` checks added by `bruin ai enhance` without first verifying against the data — they may be too restrictive for new edge cases (e.g. `outcome_label` would need to expand if a multi-outcome market is captured).
- Do not commit `.bruin.yml`, `credentials/`, `.streamlit/secrets.toml`, or any `.env*` file.
- Do not re-run a `create+replace` raw asset without explicit `BRUIN_START_DATE` / `BRUIN_END_DATE` env vars — Bruin's default schedule interval is one day, which will overwrite the full historical window with a single day of data. (This bit me once during Phase 2; staging assets failed because `temperature_daily` had no April rows after the post-AI-enhance re-run pulled today only.)

## Verified findings (computed from the warehouse during Phase 5)

| Metric | Value |
|---|---|
| CDG hourly Apr 6 17:00 UTC | 21.0 °C |
| Peer-station hourly Apr 6 17:00 UTC | 15.4–17.0 °C |
| CDG peer-residual / peer-z Apr 6 17:00 UTC | +4.0 °C / 3.0 |
| Apr 6 21°C-bucket Yes-price 12:00–16:00 UTC range | 0.10 % – 0.70 % |
| Apr 6 21°C-bucket first crossing 50 % | 2026-04-06 17:20:48 UTC |
| CDG hourly Apr 15 18:00 UTC | 12.7 °C |
| Peer-station hourly Apr 15 18:00 UTC | 15.2–17.2 °C |
| CDG peer-residual / peer-z Apr 15 18:00 UTC | −3.2 °C / −3.08 |
| Apr 15 22°C-bucket Yes-price 12:00–16:00 UTC range | 0.25 % – 0.55 % |
| Apr 15 22°C-bucket first crossing 50 % | 2026-04-15 19:50:35 UTC |
| Apr 6 event volume / Apr 15 event volume | $778,403 / $590,527 |
| Apr 6 daily max — CDG / Orly / Le Bourget / Montsouris / grid | 21.0 / 18.0 / 17.0 / 16.9 / 16.6 °C |
| Apr 15 daily max — CDG / Orly / Le Bourget / Montsouris / grid | 18.0 / 17.7 / 18.0 / 17.5 / 18.1 °C |
| Apr 15 Polymarket-resolved bucket | 22 °C |
| April 2026 Paris daily events with all sources agreeing | 0 of 29 |
| April 2026 days with majority disagreement (≥5 of 7) | 13 of 29 |
| Resolution source through 2026-04-18 | Wunderground LFPG (Charles de Gaulle) |
| Resolution source from 2026-04-19 onwards | Wunderground LFPB (Bonneuil / Le Bourget area) |
| Strongest April CDG anomaly (most negative residual) | 2026-04-27 18:00 UTC, −5.7 °C |
| 2026 Paris weather-betting volume | $9,923,899 |
| Paris rank by 2026 weather-betting volume | #11 of 54 cities |
| 2026 total weather-market volume | $318 M across 2,918 events |

## Verification commands

```bash
# Pipeline-level
bruin validate --fast polymarket-weather/

# End-to-end run on a small slice
BRUIN_START_DATE=2026-04-01 BRUIN_END_DATE=2026-04-30 bruin run polymarket-weather/

# Spike check
bruin query --connection bruin-playground-arsalan --query "
  SELECT * FROM bruin-playground-arsalan.polymarket_weather_staging.anomaly_residuals
  WHERE source_id='07157' AND ts_utc='2026-04-06 17:00 UTC'
"

# Counterfactual disagreement check
bruin query --connection bruin-playground-arsalan --query "
  SELECT event_local_date, winning_bucket_observed, bucket_cdg, bucket_orly, bucket_le_bourget,
         bucket_montsouris, bucket_grid, disagreement_count_vs_observed
  FROM bruin-playground-arsalan.polymarket_weather_staging.market_resolutions
  WHERE event_local_date IN ('2026-04-06','2026-04-15')
"

# Dashboard
python3 -m streamlit run polymarket-weather/assets/polymarket_weather_report/streamlit_app.py
```
