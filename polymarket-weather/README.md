# Polymarket Weather

Forensic data pipeline investigating the April 2026 Paris-Charles de Gaulle temperature-sensor allegations and the broader 2026 Polymarket weather-market universe. Built with Bruin + BigQuery + Streamlit + Altair.

## Background

On 2026-04-06 and 2026-04-15 the temperature sensor at Paris–Charles de Gaulle airport (ICAO LFPG, Meteostat WMO 07157) recorded sharp short-lived deviations around 18:30 local time. The same sensor was Polymarket's resolution source for that day's "Highest temperature in Paris" market. Combined winnings on the two days were reported at ~$34 k. France's national meteorological agency Météo-France filed a complaint citing tampering with an automated data-processing system. Polymarket subsequently switched its Paris resolution source.

This pipeline reproduces and extends that investigation against five neighbouring weather stations and an independent gridded reanalysis.

## Headline findings (verified from the warehouse)

Numbers below are queried from the BigQuery staging and report tables — not hand-written. Re-run the queries in `polymarket-weather/docs/findings.sql` to refresh.

- **Apr 6, 17:00 UTC (19:00 Paris local)**: CDG hourly = **21.0 °C**; the five Paris peer stations sat at **15.4–17.0 °C**; CDG peer-median residual = **+4.0 °C**, peer-z = **3.0**. CDG was the only station registering a reading near 21 °C.
- **Apr 6 21 °C-bucket Yes price**: traded between **0.10 % and 0.70 %** all day from 12:00 UTC up to 16:00 UTC; first crossed 50 % at **17:20:48 UTC** (≈ 19 minutes after the elevated CDG hourly reading), then stayed above 99 %.
- **Apr 15, 18:00 UTC (20:00 Paris local)**: CDG hourly = **12.7 °C**; the five Paris peer stations sat at **15.2–17.2 °C**; CDG peer-median residual = **−3.2 °C**, peer-z = **−3.08**. The hourly Meteostat archive shows the *recovery* from a 12-minute spike rather than the spike itself; daily-max from CDG via Meteostat was 18 °C, while Polymarket resolved on a Wunderground daily-max of 22 °C.
- **Apr 6 event volume**: **$778,403**; Apr 15 event volume: **$590,527** — the two highest-volume Paris daily-weather events of April 2026.
- **Counterfactual resolutions** for April 2026 (29 events):
  - **0** of 29 days had every alternative source agreeing with Polymarket's observed bucket
  - **13** of 29 days had a majority (≥5 of 7) of alternative sources disagreeing with Polymarket
  - **19** of 29 days had CDG itself disagreeing with ≥4 of 6 alternative sources (a sign that CDG had a different daily max than its neighbours)
- **Resolution-source switch**: events resolving 2026-04-01 through 2026-04-18 cited `wunderground.com/history/daily/fr/paris/LFPG`; events resolving 2026-04-19 onwards cited `wunderground.com/history/daily/fr/bonneuil-en-france/LFPB` (the Le Bourget area). The switch happened **four days before** The Guardian's report.
- **Anomaly scan**: across all 6 stations × 30 days × 24 hours = 3,768 hourly observations in April 2026, 24 hours flagged as `is_anomaly` (|peer-residual| ≥ 3 °C and |peer-z| ≥ 2). 8 of those were CDG, across 7 distinct days. The strongest single-station deviation in April was a **−5.7 °C** drop at CDG on 2026-04-27 18:00 UTC — beyond the two days reported in the press.
- **Wider context**: Paris weather betting in 2026 totalled **$9.92 M** across 74 events, ranking **#11 of 54** weather-market cities; the 2026 weather-market universe totalled **$318 M** across 2,918 events.

## Stations

Identical methodology for all six (Meteostat hourly, NOAA-ISD-derived; same chunking, same retry, same source flag). Open-Meteo gridded reanalysis at Paris centre is added as an independent baseline tagged `source='openmeteo_grid'` and never aggregated alongside stations.

| Name | ICAO | Meteostat ID | Lat | Lon | Elev | Role |
|---|---|---|---|---|---|---|
| Paris–Charles de Gaulle | LFPG | 07157 | 49.017 | 2.533 | 118 m | suspect sensor |
| Paris–Le Bourget | LFPB | 07150 | 48.967 | 2.450 | 66 m | post-incident replacement |
| Paris–Montsouris | — | 07156 | 48.817 | 2.333 | 75 m | urban historical reference |
| Paris–Orly | LFPO | 07149 | 48.733 | 2.400 | 89 m | second airport |
| Villacoublay | LFPV | 07147 | 48.767 | 2.200 | 177 m | military |
| Trappes | LFPT | 07145 | 48.767 | 2.017 | 168 m | semi-rural radiosonde |

## Data sources

- **Meteostat** (`meteostat` Python package; data from NOAA ISD + DWD) — hourly + daily station observations.
- **Open-Meteo Historical Weather API** (`https://archive-api.open-meteo.com/v1/archive`) — hourly ERA5-based reanalysis for Paris centre. CC BY 4.0, no auth.
- **Polymarket Gamma API** (`https://gamma-api.polymarket.com/events`) — market metadata via the `tag_slug=weather` filter and per-city `series_slug` (paris-daily-weather, etc). No auth.
- **Polymarket CLOB API** (`https://clob.polymarket.com/prices-history`) — sub-hour price history per outcome at `fidelity=1`. No auth.

## Asset map

### Raw — `polymarket_weather_raw`

| Asset | Type | Strategy | Description |
|---|---|---|---|
| `station_hourly` | python | create+replace | Hourly temperature, dew point, humidity, pressure, wind, precip for the six Paris stations from Meteostat. Verifies returned lat/lon within 0.05° of configured value. |
| `station_daily` | python | create+replace | Daily aggregates for the same six stations, 2010+, for climatology baselines. |
| `openmeteo_grid` | python | create+replace | Open-Meteo hourly reanalysis at Paris centre (48.857, 2.353); independent of any single station. |
| `polymarket_markets` | python | append | Gamma API; weather-tagged events plus 29 known city `*-daily-weather` series, exploded to one row per inner-market temperature bucket. |
| `polymarket_prices` | python | append | CLOB price history (`fidelity=1`) for every clob token id in the latest snapshot of `polymarket_markets`. Always keeps every paris-daily-weather market; caps the rest by volume via `POLYMARKET_PRICES_LIMIT`. |

### Staging — `polymarket_weather_staging`

| Asset | Depends on | Description |
|---|---|---|
| `temperature_hourly` | station_hourly, openmeteo_grid | Long panel: source, source_id, ts_utc, ts_local_paris, temp_c. PK (source, source_id, ts_utc). |
| `temperature_daily` | station_daily, station_hourly, openmeteo_grid | Long daily panel; prefers hourly-derived daily max so the result matches what Polymarket would have observed from real-time feeds. |
| `anomaly_residuals` | temperature_hourly | Per (station, ts_utc): peer-median residual + peer-based robust z-score; `is_anomaly = ABS(peer_residual) ≥ 3 AND ABS(peer_z) ≥ 2`. |
| `markets_enriched` | polymarket_markets | Dedup on market_id; classify by city / period / metric; parse bucket_value from slug; flag `paris_daily_april_2026`. |
| `prices_enriched` | polymarket_prices, markets_enriched | Dedup on (token_id, ts_utc); attach city / period / metric / bucket_value / bucket_kind. |
| `market_resolutions` | markets_enriched, prices_enriched, temperature_daily | For each Paris daily April 2026 event: observed bucket vs counterfactual bucket per station and grid. |

### Report — `polymarket_weather_report`

Pre-aggregations consumed by the dashboard:

| Asset | Description |
|---|---|
| `spike_evidence` | Apr 5–7 and Apr 14–16 — every station × hour, joined with anomaly flags. |
| `april_residuals` | April 2026 hour×day grid of CDG peer-median residuals for the calendar heatmap. |
| `counterfactual_summary` | Long format of `market_resolutions`: one row per (event_local_date, alt_source). |
| `weather_markets_overview` | City × period × month aggregate for the broader 2026 weather-betting overview. |

The Streamlit dashboard (`streamlit_app.py`) tells five stories: the two suspect days, cross-station anomaly heatmap, counterfactual resolutions, trader behaviour during the spikes, and the wider 2026 weather-betting landscape. All charts use the Wong-2011 colorblind palette; colour is always paired with stroke-dash, shape, or position. Reference screenshots are stored in `docs/screenshots/`.

## Run

```bash
# Validate
bruin validate polymarket-weather/

# First-pass smoke test (recommended)
BRUIN_START_DATE=2026-04-01 BRUIN_END_DATE=2026-04-30 \
  bruin run polymarket-weather/assets/polymarket_weather_raw/station_hourly.py
POLYMARKET_MAX_EVENTS=2000 \
  bruin run polymarket-weather/assets/polymarket_weather_raw/polymarket_markets.py

# Full window
BRUIN_START_DATE=2024-01-01 BRUIN_END_DATE=$(date -v-1d +%Y-%m-%d) \
  bruin run polymarket-weather/assets/polymarket_weather_raw/station_hourly.py
BRUIN_START_DATE=2024-01-01 BRUIN_END_DATE=$(date -v-1d +%Y-%m-%d) \
  bruin run polymarket-weather/assets/polymarket_weather_raw/openmeteo_grid.py
BRUIN_START_DATE=2010-01-01 BRUIN_END_DATE=$(date -v-1d +%Y-%m-%d) \
  bruin run polymarket-weather/assets/polymarket_weather_raw/station_daily.py
bruin run polymarket-weather/assets/polymarket_weather_raw/polymarket_markets.py
POLYMARKET_PRICES_LIMIT=400 \
  bruin run polymarket-weather/assets/polymarket_weather_raw/polymarket_prices.py

# Staging + reports
bruin run polymarket-weather/assets/polymarket_weather_staging/
bruin run polymarket-weather/assets/polymarket_weather_report/

# Dashboard
streamlit run polymarket-weather/assets/polymarket_weather_report/streamlit_app.py
```

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `BRUIN_START_DATE` | varies per asset | Start of station/openmeteo window |
| `BRUIN_END_DATE` | yesterday | End of station/openmeteo window |
| `POLYMARKET_MAX_EVENTS` | 8000 | Cap on Gamma pagination across all city series |
| `POLYMARKET_PRICES_LIMIT` | 0 (no cap) | Cap on the *non-Paris* CLOB price fetches; all paris-daily-weather markets are always fetched |

## Methodology and limitations

- **Spatial methodology**: identical Meteostat hourly call for every station, identical 30-day chunking, identical retry and tolerance check. The Open-Meteo gridded reanalysis is labelled separately and never aggregated with stations.
- **Anomaly definition**: a station hour is flagged when its peer-median residual is ≥ 3 °C in magnitude AND its robust peer-z is ≥ 2 in magnitude. The threshold is symmetric so both upward spikes and downward drops are surfaced. Station-internal rolling baselines are kept in the table but are not part of the flag (the diurnal cycle inflates the rolling std on warm-up days, hiding genuine spikes).
- **Counterfactual buckets**: each station's daily max is rounded to whole degrees Celsius and clamped to [14, 24] to match the Polymarket bucket scheme (≤14, 15..23, ≥24). The Polymarket-observed bucket comes from the `Yes`-side outcome whose final tick price was the highest in the event.
- **Sub-hour vs hourly resolution**: Meteostat hourly is the densest free METAR-derived archive; the Polymarket-cited Wunderground feed reports more frequently, and the alleged 12-minute spikes can fall between Meteostat top-of-hour samples (this is what we observe on April 15 — the spike itself is missing from the hourly archive even though the recovery into the next hour is visible).
- **Out of scope**: identifying or naming a suspect; analysing the viral hairdryer footage; wallet-level on-chain trade attribution.
- **No sensational framing**: the dashboard, asset descriptions and this README use "alleged", "anomaly", "disagreement". The pipeline does not characterise the incident as conclusive tampering — the data shows divergence between CDG and every other regional sensor on two specific days, plus a Polymarket-observed daily max that no other source supports.
