-- Hourly temperature traces for every London Meteostat station + Open-Meteo grid
-- on the city's #1 primary-station anomaly day (London City EGLC, 2026-04-19).
WITH temps AS (
  SELECT
    ts_local,
    ROUND(MAX(IF(source_id = 'EGLC0', temp_c, NULL)), 1) AS london_city,
    ROUND(MAX(IF(source_id = '03779', temp_c, NULL)), 1) AS weather_centre,
    ROUND(MAX(IF(source_id = '03672', temp_c, NULL)), 1) AS northolt,
    ROUND(MAX(IF(source_id = '03772', temp_c, NULL)), 1) AS heathrow,
    ROUND(MAX(IF(source_id = '03781', temp_c, NULL)), 1) AS kenley,
    ROUND(MAX(IF(source_id = 'london_centre', temp_c, NULL)), 1) AS open_meteo_grid
  FROM `bruin-playground-arsalan.polymarket_weather_staging.temperature_hourly`
  WHERE city = 'London'
    AND DATE(ts_local) = DATE '2026-04-19'
  GROUP BY ts_local
),
winning AS (
  SELECT m.market_id
  FROM `bruin-playground-arsalan.polymarket_weather_staging.markets_enriched` m
  WHERE m.series_slug = 'london-daily-weather'
    AND m.bucket_kind = 'point'
    AND m.end_local_date = DATE '2026-04-19'
    AND m.resolved_yes = TRUE
  LIMIT 1
),
prices_hourly AS (
  SELECT
    DATETIME_TRUNC(DATETIME(p.ts_utc, 'Europe/London'), HOUR) AS ts_local_hour,
    ARRAY_AGG(p.price ORDER BY p.ts_utc DESC LIMIT 1)[OFFSET(0)] AS yes_price
  FROM `bruin-playground-arsalan.polymarket_weather_staging.prices_enriched` p
  JOIN winning USING (market_id)
  WHERE p.outcome_label = 'Yes'
    AND DATE(p.ts_utc, 'Europe/London') = DATE '2026-04-19'
  GROUP BY ts_local_hour
)
SELECT
  FORMAT_DATETIME('%H:%M', t.ts_local) AS time_label,
  t.london_city,
  t.weather_centre,
  t.northolt,
  t.heathrow,
  t.kenley,
  t.open_meteo_grid,
  ROUND(p.yes_price, 4) AS yes_price
FROM temps t
LEFT JOIN prices_hourly p ON DATETIME_TRUNC(t.ts_local, HOUR) = p.ts_local_hour
ORDER BY t.ts_local
