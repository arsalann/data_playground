-- Hourly temperature traces for every Toronto Meteostat station + Open-Meteo
-- grid on the city's #1 primary-station anomaly day (Pearson CYYZ, 2026-04-08).
WITH temps AS (
  SELECT
    ts_local,
    ROUND(MAX(IF(source_id = '71624', temp_c, NULL)), 1) AS pearson,
    ROUND(MAX(IF(source_id = '71508', temp_c, NULL)), 1) AS toronto_city,
    ROUND(MAX(IF(source_id = '2XUGG', temp_c, NULL)), 1) AS toronto_city_centre,
    ROUND(MAX(IF(source_id = '71265', temp_c, NULL)), 1) AS toronto_island,
    ROUND(MAX(IF(source_id = '71639', temp_c, NULL)), 1) AS buttonville,
    ROUND(MAX(IF(source_id = 'toronto_centre', temp_c, NULL)), 1) AS open_meteo_grid
  FROM `bruin-playground-arsalan.polymarket_weather_staging.temperature_hourly`
  WHERE city = 'Toronto'
    AND DATE(ts_local) = DATE '2026-04-08'
  GROUP BY ts_local
),
winning AS (
  SELECT m.market_id
  FROM `bruin-playground-arsalan.polymarket_weather_staging.markets_enriched` m
  WHERE m.series_slug = 'toronto-daily-weather'
    AND m.bucket_kind = 'point'
    AND m.end_local_date = DATE '2026-04-08'
    AND m.resolved_yes = TRUE
  LIMIT 1
),
prices_hourly AS (
  SELECT
    DATETIME_TRUNC(DATETIME(p.ts_utc, 'America/Toronto'), HOUR) AS ts_local_hour,
    ARRAY_AGG(p.price ORDER BY p.ts_utc DESC LIMIT 1)[OFFSET(0)] AS yes_price
  FROM `bruin-playground-arsalan.polymarket_weather_staging.prices_enriched` p
  JOIN winning USING (market_id)
  WHERE p.outcome_label = 'Yes'
    AND DATE(p.ts_utc, 'America/Toronto') = DATE '2026-04-08'
  GROUP BY ts_local_hour
)
SELECT
  FORMAT_DATETIME('%H:%M', t.ts_local) AS time_label,
  t.pearson,
  t.toronto_city,
  t.toronto_city_centre,
  t.toronto_island,
  t.buttonville,
  t.open_meteo_grid,
  ROUND(p.yes_price, 4) AS yes_price
FROM temps t
LEFT JOIN prices_hourly p ON DATETIME_TRUNC(t.ts_local, HOUR) = p.ts_local_hour
ORDER BY t.ts_local
