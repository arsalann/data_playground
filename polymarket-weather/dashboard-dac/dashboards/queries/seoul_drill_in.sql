-- Hourly temperature traces for every Seoul Meteostat station + Open-Meteo grid
-- on the city's #1 primary-station anomaly day (Incheon RKSI, 2026-02-22).
-- One column per station; the chart's left axis is temperature (°C). Optional
-- Polymarket Yes-price overlay (the day's winning bucket) is joined on the
-- right axis where price ticks exist; null otherwise.
WITH temps AS (
  SELECT
    ts_local,
    ROUND(MAX(IF(source_id = '47113', temp_c, NULL)), 1) AS incheon,
    ROUND(MAX(IF(source_id = '47108', temp_c, NULL)), 1) AS seoul,
    ROUND(MAX(IF(source_id = '47110', temp_c, NULL)), 1) AS kimpo,
    ROUND(MAX(IF(source_id = '47111', temp_c, NULL)), 1) AS seoul_e_ab,
    ROUND(MAX(IF(source_id = 'RKSY0', temp_c, NULL)), 1) AS yongsan,
    ROUND(MAX(IF(source_id = 'seoul_centre', temp_c, NULL)), 1) AS open_meteo_grid
  FROM `bruin-playground-arsalan.polymarket_weather_staging.temperature_hourly`
  WHERE city = 'Seoul'
    AND DATE(ts_local) = DATE '2026-02-22'
  GROUP BY ts_local
),
winning AS (
  SELECT m.market_id
  FROM `bruin-playground-arsalan.polymarket_weather_staging.markets_enriched` m
  WHERE m.series_slug = 'seoul-daily-weather'
    AND m.bucket_kind = 'point'
    AND m.end_local_date = DATE '2026-02-22'
    AND m.resolved_yes = TRUE
  LIMIT 1
),
prices_hourly AS (
  SELECT
    DATETIME_TRUNC(DATETIME(p.ts_utc, 'Asia/Seoul'), HOUR) AS ts_local_hour,
    ARRAY_AGG(p.price ORDER BY p.ts_utc DESC LIMIT 1)[OFFSET(0)] AS yes_price
  FROM `bruin-playground-arsalan.polymarket_weather_staging.prices_enriched` p
  JOIN winning USING (market_id)
  WHERE p.outcome_label = 'Yes'
    AND DATE(p.ts_utc, 'Asia/Seoul') = DATE '2026-02-22'
  GROUP BY ts_local_hour
)
SELECT
  FORMAT_DATETIME('%H:%M', t.ts_local) AS time_label,
  t.incheon,
  t.seoul,
  t.kimpo,
  t.seoul_e_ab,
  t.yongsan,
  t.open_meteo_grid,
  ROUND(p.yes_price, 4) AS yes_price
FROM temps t
LEFT JOIN prices_hourly p ON DATETIME_TRUNC(t.ts_local, HOUR) = p.ts_local_hour
ORDER BY t.ts_local
