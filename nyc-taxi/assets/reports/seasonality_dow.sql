-- Seasonality heatmap: day-of-week x month with tipping and fare intensity
WITH clean AS (
  SELECT
    pickup_time,
    trip_distance,
    trip_duration_seconds,
    total_amount,
    fare_amount,
    tip_amount,
    pickup_borough,
    dropoff_borough,
    (trip_distance / NULLIF(trip_duration_seconds, 0)) * 3600.0 AS speed_mph,
    total_amount / NULLIF(trip_distance, 0) AS fare_per_mile,
    CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount ELSE NULL END AS tip_pct
  FROM staging.trips_summary
  WHERE 1=1
    AND trip_distance BETWEEN 0.05 AND 100
    AND trip_duration_seconds BETWEEN 60 AND 7200
    AND total_amount BETWEEN 0 AND 500
    AND tip_amount >= 0
    AND (trip_distance / NULLIF(trip_duration_seconds, 0)) * 3600.0 BETWEEN 1 AND 80
    AND fare_amount > 0
    AND tip_amount / NULLIF(fare_amount, 0) <= 100 -- cap extreme tip pct outliers
    AND pickup_borough NOT IN ('Unknown', 'N/A', 'Outside of NYC')
    AND dropoff_borough NOT IN ('Unknown', 'N/A', 'Outside of NYC')
)
SELECT
  EXTRACT(dow FROM pickup_time) AS dow,
  EXTRACT(month FROM pickup_time) AS month_num,
  COUNT(*) AS trips,
  AVG(tip_pct) AS avg_tip_pct,
  AVG(CASE WHEN tip_amount = 0 THEN 1 ELSE 0 END) AS zero_tip_rate,
  MEDIAN(fare_per_mile) AS med_fare_per_mile,
  MEDIAN(speed_mph) AS med_speed_mph
FROM clean
GROUP BY 1, 2
ORDER BY 2, 1;
