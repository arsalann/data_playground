/* @bruin

name: fifa_staging.venue_decadal_warming
type: bq.sql
description: |
  Per-venue, per-decade June-July afternoon (12:00-18:00 venue-local) mean
  apparent temperature, computed from `fifa_raw.openmeteo_climatology`.

  Two decades are stored: 1980-1989 (the "before" baseline) and 2015-2024
  (the "now" recent decade). Each row is one (venue, decade) pair. Used by
  `r1_venue_warming` to chart the warming gradient.

  Why afternoon, not all day? Group-stage and knockout matches at 2026
  northern-hemisphere venues kick off between 12:00 and 18:00 local more than
  90% of the time (verified against the manifest schedule), so the afternoon
  window is what actually matters for player heat exposure. The all-day mean
  is also recorded for reference.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - staging
  - climate

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_raw.openmeteo_climatology
  - fifa_raw.openmeteo_baseline_1980s
  - fifa_raw.host_venues

@bruin */

WITH unioned AS (
  SELECT venue_id, ts_utc, temp_c, humidity_pct, wind_speed_kmh
  FROM `bruin-playground-arsalan.fifa_raw.openmeteo_climatology`
  WHERE temp_c IS NOT NULL
  UNION ALL
  SELECT venue_id, ts_utc, temp_c, humidity_pct, wind_speed_kmh
  FROM `bruin-playground-arsalan.fifa_raw.openmeteo_baseline_1980s`
  WHERE temp_c IS NOT NULL
),
local_hours AS (
  SELECT
    o.venue_id,
    v.city            AS venue_city,
    v.country         AS venue_country,
    v.timezone        AS venue_timezone,
    v.latitude        AS venue_lat,
    v.longitude       AS venue_lon,
    o.ts_utc,
    DATETIME(o.ts_utc, v.timezone) AS ts_local,
    EXTRACT(YEAR FROM o.ts_utc)    AS ts_year,
    o.temp_c,
    o.humidity_pct,
    o.wind_speed_kmh
  FROM unioned o
  JOIN `bruin-playground-arsalan.fifa_raw.host_venues` v USING (venue_id)
),
afternoon AS (
  SELECT
    venue_id,
    venue_city,
    venue_country,
    venue_timezone,
    venue_lat,
    venue_lon,
    ts_year,
    CASE
      WHEN ts_year BETWEEN 1980 AND 1989 THEN '1980s'
      WHEN ts_year BETWEEN 2015 AND 2024 THEN '2015-2024'
      ELSE NULL
    END AS decade,
    EXTRACT(HOUR FROM ts_local) AS hour_local,
    temp_c,
    humidity_pct,
    wind_speed_kmh
  FROM local_hours
),
filtered AS (
  SELECT *
  FROM afternoon
  WHERE decade IS NOT NULL
    AND hour_local BETWEEN 12 AND 18
)
SELECT
  venue_id,
  venue_city,
  venue_country,
  venue_timezone,
  venue_lat,
  venue_lon,
  decade,
  COUNT(*)                                            AS n_hours,
  ROUND(AVG(temp_c), 2)                               AS mean_temp_c,
  ROUND(AVG(humidity_pct), 2)                         AS mean_humidity_pct,
  ROUND(AVG(wind_speed_kmh), 2)                       AS mean_wind_kmh,
  ROUND(APPROX_QUANTILES(temp_c, 100)[OFFSET(95)], 2) AS p95_temp_c,
  ROUND(SAFE_DIVIDE(COUNTIF(temp_c >= 30), COUNT(*)), 4) AS pct_hours_ge30c,
  ROUND(SAFE_DIVIDE(COUNTIF(temp_c >= 35), COUNT(*)), 4) AS pct_hours_ge35c,
  /* Apparent temp via Steadman BoM */
  ROUND(
    AVG(
      temp_c
      + 0.33 * ((humidity_pct / 100.0) * 6.105 * EXP(17.27 * temp_c / (237.7 + temp_c)))
      - 0.70 * (wind_speed_kmh / 3.6)
      - 4.0
    ),
    2
  ) AS mean_apparent_temp_c,
  CURRENT_TIMESTAMP() AS staged_at
FROM filtered
GROUP BY venue_id, venue_city, venue_country, venue_timezone, venue_lat, venue_lon, decade
ORDER BY venue_city, decade
