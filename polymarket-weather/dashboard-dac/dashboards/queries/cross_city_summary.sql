-- Per-city snapshot for the multi-city dashboard's overview table.
-- Combines top_cities_2026 with primary-station anomaly counts and the
-- max absolute peer-z observed at the primary across the 4-month window.
WITH primary_stats AS (
  SELECT
    city,
    COUNT(*) AS primary_anomaly_count,
    ROUND(MAX(ABS(peer_z)), 2) AS max_abs_peer_z,
    ROUND(MAX(peer_residual), 2) AS max_warm_residual,
    ROUND(MIN(peer_residual), 2) AS max_cold_residual
  FROM `bruin-playground-arsalan.polymarket_weather_report.primary_station_anomalies`
  GROUP BY city
),
ingested_primary AS (
  SELECT
    city,
    ANY_VALUE(station_name) AS primary_station_name,
    ANY_VALUE(icao) AS primary_icao
  FROM `bruin-playground-arsalan.polymarket_weather_raw.station_hourly`
  WHERE role = 'primary'
  GROUP BY city
)
SELECT
  tc.rank,
  tc.city,
  ip.primary_station_name,
  tc.primary_icao,
  ROUND(tc.total_event_volume_usd / 1e6, 2) AS volume_musd,
  tc.event_count,
  tc.market_count,
  COALESCE(ps.primary_anomaly_count, 0) AS primary_anomaly_hours,
  COALESCE(ps.max_abs_peer_z, 0)        AS max_abs_peer_z,
  COALESCE(ps.max_warm_residual, 0)     AS max_warm_residual_c,
  COALESCE(ps.max_cold_residual, 0)     AS max_cold_residual_c
FROM `bruin-playground-arsalan.polymarket_weather_staging.top_cities_2026` tc
LEFT JOIN ingested_primary ip USING (city)
LEFT JOIN primary_stats ps USING (city)
WHERE tc.city IN ('Seoul', 'London', 'Toronto')
ORDER BY tc.rank
