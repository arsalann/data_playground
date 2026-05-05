/* @bruin

name: fifa_staging.match_climatology
type: bq.sql
description: |
  Per-match climatological weather summary at the venue + kickoff hour (±2h),
  computed across the 14-year June-July ERA5 climatology window from
  `fifa_raw.openmeteo_climatology`. Drives H1 (heat-risk concentration).

  For each match we summarise temp, humidity, dew point, and wind across all
  hourly observations falling in the same calendar day-of-year + hour bin
  (±2h) across the climatology years. We then compute apparent temperature
  (BoM Steadman formula) and assign a heat band per US NWS heat-index
  thresholds.

  A `methodology_note` column carries the load-bearing caveat that ERA5 is on a
  ~9 km grid, no globe-temperature observation is available, and the apparent
  temperature is a Steadman approximation — not true WBGT. The same caveat is
  surfaced in the H1 dashboard footnote.

  We deliberately use Open-Meteo only here (`source='openmeteo_grid'`); the
  Meteostat METAR cross-check is computed separately so we don't conflate
  reanalysis with point observations. METAR top-of-hour sampling may miss
  sub-hour spikes — same caveat as the polymarket-weather Paris analysis.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - staging
  - weather

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_staging.matches_enriched
  - fifa_raw.openmeteo_climatology
  - fifa_raw.meteostat_hourly

@bruin */

WITH match_hours AS (
  SELECT
    match_id,
    venue_id,
    kickoff_local,
    EXTRACT(MONTH FROM kickoff_local)     AS kickoff_month,
    EXTRACT(DAY   FROM kickoff_local)     AS kickoff_day,
    EXTRACT(HOUR  FROM kickoff_utc)       AS kickoff_hour_utc,
    venue_city,
    stadium,
    venue_elevation_m,
    roof_type
  FROM `bruin-playground-arsalan.fifa_staging.matches_enriched`
),
era5 AS (
  SELECT
    venue_id,
    EXTRACT(MONTH FROM ts_utc) AS month,
    EXTRACT(DAY   FROM ts_utc) AS day,
    EXTRACT(HOUR  FROM ts_utc) AS hour,
    temp_c,
    humidity_pct,
    dew_point_c,
    wind_speed_kmh
  FROM `bruin-playground-arsalan.fifa_raw.openmeteo_climatology`
  WHERE temp_c IS NOT NULL
),
era5_window AS (
  SELECT
    m.match_id,
    m.venue_id,
    AVG(e.temp_c)            AS mean_temp_c,
    APPROX_QUANTILES(e.temp_c, 100)[OFFSET(95)]  AS p95_temp_c,
    APPROX_QUANTILES(e.temp_c, 100)[OFFSET(5)]   AS p05_temp_c,
    AVG(e.humidity_pct)      AS mean_humidity_pct,
    AVG(e.dew_point_c)       AS mean_dew_point_c,
    AVG(e.wind_speed_kmh)    AS mean_wind_speed_kmh,
    COUNT(*)                 AS n_hours
  FROM match_hours m
  JOIN era5 e
    ON e.venue_id = m.venue_id
   AND e.month   = m.kickoff_month
   AND e.day     = m.kickoff_day
   AND ABS(e.hour - m.kickoff_hour_utc) <= 2
  GROUP BY m.match_id, m.venue_id
),
metar_check AS (
  SELECT
    venue_id,
    EXTRACT(MONTH FROM ts_utc) AS month,
    EXTRACT(DAY   FROM ts_utc) AS day,
    EXTRACT(HOUR  FROM ts_utc) AS hour,
    AVG(temp_c)                AS mean_metar_temp_c
  FROM `bruin-playground-arsalan.fifa_raw.meteostat_hourly`
  WHERE temp_c IS NOT NULL
  GROUP BY 1, 2, 3, 4
),
metar_window AS (
  SELECT
    m.match_id,
    AVG(c.mean_metar_temp_c) AS metar_mean_temp_c,
    COUNT(*)                 AS n_metar_hours
  FROM match_hours m
  JOIN metar_check c
    ON c.venue_id = m.venue_id
   AND c.month   = m.kickoff_month
   AND c.day     = m.kickoff_day
   AND ABS(c.hour - m.kickoff_hour_utc) <= 2
  GROUP BY m.match_id
)
SELECT
  m.match_id,
  m.venue_id,
  m.venue_city,
  m.stadium,
  m.kickoff_local,
  m.kickoff_hour_utc,
  m.venue_elevation_m,
  m.roof_type,
  /* ERA5-derived metrics */
  ROUND(e.mean_temp_c,         2) AS mean_temp_c,
  ROUND(e.p95_temp_c,          2) AS p95_temp_c,
  ROUND(e.p05_temp_c,          2) AS p05_temp_c,
  ROUND(e.mean_humidity_pct,   2) AS mean_humidity_pct,
  ROUND(e.mean_dew_point_c,    2) AS mean_dew_point_c,
  ROUND(e.mean_wind_speed_kmh, 2) AS mean_wind_speed_kmh,
  e.n_hours                       AS n_era5_hours,
  /* Apparent temp (BoM Steadman): T + 0.33e - 0.70*WS_ms - 4.0 */
  ROUND(
    e.mean_temp_c
    + 0.33 * (
        (e.mean_humidity_pct / 100.0)
        * 6.105
        * EXP(17.27 * e.mean_temp_c / (237.7 + e.mean_temp_c))
      )
    - 0.70 * (e.mean_wind_speed_kmh / 3.6)
    - 4.0,
    2
  ) AS apparent_temp_c,
  /* Heat band (US NWS heat-index thresholds applied to apparent temp). */
  CASE
    WHEN e.mean_temp_c IS NULL THEN NULL
    WHEN (
      e.mean_temp_c
      + 0.33 * ((e.mean_humidity_pct / 100.0) * 6.105 * EXP(17.27 * e.mean_temp_c / (237.7 + e.mean_temp_c)))
      - 0.70 * (e.mean_wind_speed_kmh / 3.6) - 4.0
    ) < 27 THEN 'Low'
    WHEN (
      e.mean_temp_c
      + 0.33 * ((e.mean_humidity_pct / 100.0) * 6.105 * EXP(17.27 * e.mean_temp_c / (237.7 + e.mean_temp_c)))
      - 0.70 * (e.mean_wind_speed_kmh / 3.6) - 4.0
    ) < 32 THEN 'Moderate'
    WHEN (
      e.mean_temp_c
      + 0.33 * ((e.mean_humidity_pct / 100.0) * 6.105 * EXP(17.27 * e.mean_temp_c / (237.7 + e.mean_temp_c)))
      - 0.70 * (e.mean_wind_speed_kmh / 3.6) - 4.0
    ) < 37 THEN 'High'
    ELSE 'Extreme'
  END AS heat_band,
  /* Meteostat cross-check */
  ROUND(mc.metar_mean_temp_c, 2) AS metar_mean_temp_c,
  mc.n_metar_hours,
  CAST(
    'Apparent temp computed via BoM Steadman formula (T + 0.33e - 0.70 WS - 4.0). '
    'Bands use US NWS heat-index thresholds applied to apparent temp. '
    'ERA5 reanalysis on a ~9km grid; no globe-temperature available so this is not true WBGT. '
    'Meteostat METAR is sampled at the top of every UTC hour, so sub-hour spikes are invisible.'
    AS STRING
  ) AS methodology_note,
  CURRENT_TIMESTAMP() AS staged_at
FROM match_hours m
LEFT JOIN era5_window  e ON e.match_id = m.match_id
LEFT JOIN metar_window mc ON mc.match_id = m.match_id
