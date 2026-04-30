-- Hourly air temperature for the six Paris stations + Open-Meteo grid on
-- 2026-04-15 (event day only — 24 rows). The Polymarket Yes-price for the
-- 22 °C bucket is returned alongside as `yes_price` (0–1 implied probability)
-- and rendered on the chart's RIGHT y-axis via `yRight: [yes_price]` in YAML.
WITH temps AS (
  SELECT
    ts_local_paris,
    ROUND(MAX(IF(source_label = 'Paris-Aeroport Charles De Gaulle', temp_c, NULL)), 1) AS cdg,
    ROUND(MAX(IF(source_label = 'Paris / Le Bourget',               temp_c, NULL)), 1) AS le_bourget,
    ROUND(MAX(IF(source_label = 'Paris-Montsouris',                 temp_c, NULL)), 1) AS montsouris,
    ROUND(MAX(IF(source_label = 'Paris-Orly',                       temp_c, NULL)), 1) AS orly,
    ROUND(MAX(IF(source_label = 'Villacoublay',                     temp_c, NULL)), 1) AS villacoublay,
    ROUND(MAX(IF(source_label = 'Trappes',                          temp_c, NULL)), 1) AS trappes,
    ROUND(MAX(IF(source_label = 'Open-Meteo grid (Paris centre)',   temp_c, NULL)), 1) AS open_meteo_grid
  FROM `bruin-playground-arsalan.polymarket_weather_report.spike_evidence`
  WHERE event_local_date = DATE '2026-04-15'
    AND DATE(ts_local_paris) = DATE '2026-04-15'
  GROUP BY ts_local_paris
),
winning AS (
  SELECT m.market_id
  FROM `bruin-playground-arsalan.polymarket_weather_staging.markets_enriched` m
  JOIN `bruin-playground-arsalan.polymarket_weather_staging.market_resolutions` r
    ON DATE(m.end_date, 'Europe/Paris') = r.event_local_date
   AND m.bucket_value_c = r.winning_bucket_observed
  WHERE r.event_local_date = DATE '2026-04-15'
    AND m.bucket_kind = 'point'
    AND m.series_slug = 'paris-daily-weather'
),
prices_hourly AS (
  SELECT
    DATETIME_TRUNC(p.ts_local_paris, HOUR) AS ts_hour,
    ARRAY_AGG(p.price ORDER BY p.ts_local_paris DESC LIMIT 1)[OFFSET(0)] AS yes_price
  FROM `bruin-playground-arsalan.polymarket_weather_staging.prices_enriched` p
  JOIN winning USING (market_id)
  WHERE p.outcome_label = 'Yes'
    AND DATE(p.ts_local_paris) = DATE '2026-04-15'
  GROUP BY ts_hour
)
SELECT
  FORMAT_TIMESTAMP('%H:%M', t.ts_local_paris) AS time_label,
  t.cdg,
  t.le_bourget,
  t.montsouris,
  t.orly,
  t.villacoublay,
  t.trappes,
  t.open_meteo_grid,
  ROUND(p.yes_price, 4)      AS yes_price
FROM temps t
LEFT JOIN prices_hourly p ON DATETIME(t.ts_local_paris) = p.ts_hour
ORDER BY t.ts_local_paris
