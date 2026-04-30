-- Per (local_date) daily mean peer-residual at each city's PRIMARY station.
-- One line per city across the 120-day window. A persistent non-zero value
-- suggests a calibration drift; isolated spikes match anomaly hours.
WITH per_city_day AS (
  SELECT
    city,
    local_date,
    ROUND(AVG(peer_residual), 3) AS daily_mean_residual
  FROM `bruin-playground-arsalan.polymarket_weather_staging.anomaly_residuals`
  WHERE role = 'primary'
    AND local_date BETWEEN DATE '2026-01-01' AND DATE '2026-04-30'
  GROUP BY city, local_date
)
SELECT
  FORMAT_DATE('%Y-%m-%d', local_date) AS day_label,
  MAX(IF(city = 'Seoul',   daily_mean_residual, NULL)) AS seoul,
  MAX(IF(city = 'London',  daily_mean_residual, NULL)) AS london,
  MAX(IF(city = 'Toronto', daily_mean_residual, NULL)) AS toronto
FROM per_city_day
GROUP BY day_label
ORDER BY day_label
