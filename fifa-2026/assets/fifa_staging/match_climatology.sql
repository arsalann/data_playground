/* @bruin

name: fifa_staging.match_climatology
type: bq.sql
description: |
  Per-match climatological weather summary at the venue + kickoff hour (±2h),
  computed across the most-recent decade of June-July ERA5 reanalysis
  (2015-2024) from `fifa_raw.openmeteo_climatology`. Used for the 2026
  expected apparent-temperature surface; the 1980s baseline rows in the same
  raw table are NOT included here (they go to `venue_decadal_warming`).

  We summarise temp, humidity, dew point, and wind across all hourly
  observations falling in the same calendar day-of-year + hour bin (±2h).
  Then compute apparent temperature (BoM Steadman) and assign a US-NWS
  heat-index band.

  We also derive a `roof_status_assumed` for each match. FIFA has not
  published a roof-state-by-match policy, so we assume retractable / fixed
  roofs are CLOSED whenever the open-air apparent temp would exceed 30 °C
  or precipitation looks likely. Open-roof venues are always 'open'. The
  `open_air_effective` boolean is true only when a player on the pitch
  actually experiences the climatological apparent temperature — i.e.
  open-roof, OR retractable that we expect to be open.

  Methodology caveats:
   - ERA5 is on a ~9 km grid; single-stadium microclimate is smoothed.
   - Apparent temp is BoM Steadman, not true WBGT.
   - The roof-state heuristic is editorial — the dashboard footnote calls
     this out as an analyst assumption.
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

@bruin */

WITH match_hours AS (
  SELECT
    match_id,
    venue_id,
    kickoff_local,
    kickoff_utc,
    EXTRACT(MONTH FROM kickoff_local)     AS kickoff_month,
    EXTRACT(DAY   FROM kickoff_local)     AS kickoff_day,
    EXTRACT(HOUR  FROM kickoff_utc)       AS kickoff_hour_utc,
    venue_city,
    venue_country,
    stadium,
    venue_elevation_m,
    roof_type,
    stage,
    group_id
  FROM `bruin-playground-arsalan.fifa_staging.matches_enriched`
),
era5_recent AS (
  SELECT
    venue_id,
    EXTRACT(MONTH FROM ts_utc) AS month,
    EXTRACT(DAY   FROM ts_utc) AS day,
    EXTRACT(HOUR  FROM ts_utc) AS hour,
    temp_c,
    humidity_pct,
    dew_point_c,
    wind_speed_kmh,
    precipitation_mm
  FROM `bruin-playground-arsalan.fifa_raw.openmeteo_climatology`
  WHERE temp_c IS NOT NULL
    AND EXTRACT(YEAR FROM ts_utc) BETWEEN 2015 AND 2024
),
era5_window AS (
  SELECT
    m.match_id,
    m.venue_id,
    AVG(e.temp_c)                               AS mean_temp_c,
    APPROX_QUANTILES(e.temp_c, 100)[OFFSET(95)] AS p95_temp_c,
    APPROX_QUANTILES(e.temp_c, 100)[OFFSET(5)]  AS p05_temp_c,
    AVG(e.humidity_pct)                         AS mean_humidity_pct,
    AVG(e.dew_point_c)                          AS mean_dew_point_c,
    AVG(e.wind_speed_kmh)                       AS mean_wind_speed_kmh,
    AVG(e.precipitation_mm)                     AS mean_precip_mm,
    /* Probability of >=0.1mm precipitation in any of the matched hours */
    SAFE_DIVIDE(
      COUNTIF(e.precipitation_mm >= 0.1),
      COUNT(*)
    )                                           AS prob_precip,
    /* Probability that hourly temp >= 30C in window */
    SAFE_DIVIDE(
      COUNTIF(e.temp_c >= 30),
      COUNT(*)
    )                                           AS prob_temp_ge30,
    COUNT(*)                                    AS n_hours
  FROM match_hours m
  JOIN era5_recent e
    ON e.venue_id = m.venue_id
   AND e.month   = m.kickoff_month
   AND e.day     = m.kickoff_day
   AND ABS(e.hour - m.kickoff_hour_utc) <= 2
  GROUP BY m.match_id, m.venue_id
),
with_apparent AS (
  SELECT
    m.*,
    e.mean_temp_c,
    e.p95_temp_c,
    e.p05_temp_c,
    e.mean_humidity_pct,
    e.mean_dew_point_c,
    e.mean_wind_speed_kmh,
    e.mean_precip_mm,
    e.prob_precip,
    e.prob_temp_ge30,
    e.n_hours,
    (
      e.mean_temp_c
      + 0.33 * ((e.mean_humidity_pct / 100.0) * 6.105 *
                EXP(17.27 * e.mean_temp_c / (237.7 + e.mean_temp_c)))
      - 0.70 * (e.mean_wind_speed_kmh / 3.6)
      - 4.0
    ) AS apparent_temp_c_raw
  FROM match_hours m
  LEFT JOIN era5_window e ON e.match_id = m.match_id
)
SELECT
  match_id,
  stage,
  group_id,
  venue_id,
  venue_city,
  venue_country,
  stadium,
  kickoff_local,
  kickoff_utc,
  kickoff_hour_utc,
  venue_elevation_m,
  roof_type,
  ROUND(mean_temp_c,         2) AS mean_temp_c,
  ROUND(p95_temp_c,          2) AS p95_temp_c,
  ROUND(p05_temp_c,          2) AS p05_temp_c,
  ROUND(mean_humidity_pct,   2) AS mean_humidity_pct,
  ROUND(mean_dew_point_c,    2) AS mean_dew_point_c,
  ROUND(mean_wind_speed_kmh, 2) AS mean_wind_speed_kmh,
  ROUND(mean_precip_mm,      2) AS mean_precip_mm,
  ROUND(prob_precip,         3) AS prob_precip,
  ROUND(prob_temp_ge30,      3) AS prob_temp_ge30,
  n_hours                       AS n_era5_hours,
  ROUND(apparent_temp_c_raw, 2) AS apparent_temp_c,
  CASE
    WHEN apparent_temp_c_raw IS NULL    THEN NULL
    WHEN apparent_temp_c_raw < 27       THEN 'Low'
    WHEN apparent_temp_c_raw < 32       THEN 'Caution'
    WHEN apparent_temp_c_raw < 39       THEN 'Extreme Caution'
    WHEN apparent_temp_c_raw < 51       THEN 'Danger'
    ELSE 'Extreme Danger'
  END AS heat_band,
  /* Roof-state heuristic. FIFA has not published a per-match policy. */
  CASE
    WHEN roof_type = 'open'        THEN 'open'
    WHEN roof_type = 'fixed_canopy' THEN 'open'  -- canopy is partial cover, treated as open for heat exposure
    WHEN roof_type = 'retractable' AND apparent_temp_c_raw >= 30 THEN 'closed_likely'
    WHEN roof_type = 'retractable' AND prob_precip >= 0.20       THEN 'closed_likely'
    WHEN roof_type = 'retractable'                               THEN 'open_likely'
    ELSE 'unknown'
  END AS roof_status_assumed,
  /* True if a pitch player meaningfully experiences the climatological conditions. */
  (roof_type = 'open'
    OR roof_type = 'fixed_canopy'
    OR (roof_type = 'retractable' AND apparent_temp_c_raw < 30 AND prob_precip < 0.20)
  ) AS open_air_effective,
  CAST(
    'Apparent temp = BoM Steadman (T + 0.33e - 0.70 WS - 4.0). '
    'Bands = US NWS heat-index thresholds applied to apparent temp. '
    'ERA5 reanalysis on a ~9km grid, no globe-temperature available so this is not true WBGT. '
    'Climatology window: 2015-2024 June-July. '
    'Roof-state assumption: open-roof always open; retractable assumed CLOSED when '
    'apparent temp >= 30C or precip prob >= 20% — FIFA has not published per-match policy.'
    AS STRING
  ) AS methodology_note,
  CURRENT_TIMESTAMP() AS staged_at
FROM with_apparent
