-- Basic monthly totals with consistent cleaning
SELECT
  date_trunc('month', pickup_time) AS month,
  COUNT(*) AS trips,
  SUM(fare_amount) AS fare_amount_sum,
  SUM(tip_amount) AS tip_amount_sum,
  SUM(total_amount) AS total_amount_sum,
  SUM(tip_amount) / NULLIF(SUM(fare_amount), 0) AS tip_rate_pct
FROM staging.trips_summary
WHERE 1=1
  AND trip_distance BETWEEN 0.05 AND 100
  AND trip_duration_seconds BETWEEN 60 AND 7200
  AND total_amount BETWEEN 0 AND 500
  AND tip_amount >= 0
  AND (trip_distance / NULLIF(trip_duration_seconds, 0)) * 3600.0 BETWEEN 1 AND 80
  AND pickup_borough NOT IN ('Unknown', 'N/A', 'Outside of NYC')
  AND dropoff_borough NOT IN ('Unknown', 'N/A', 'Outside of NYC')
GROUP BY ALL
ORDER BY month DESC;
