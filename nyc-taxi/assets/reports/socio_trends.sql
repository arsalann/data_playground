-- Socioeconomic / human-interest signals by borough flows and dayparts
-- Cleaning thresholds aligned with other reports.
WITH clean AS (
  SELECT
    pickup_time,
    pickup_borough,
    dropoff_borough,
    trip_distance,
    trip_duration_seconds,
    total_amount,
    tip_amount,
    fare_amount,
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
, enriched AS (
  SELECT
    pickup_time,
    COALESCE(NULLIF(pickup_borough, ''), 'Unknown') AS pickup_borough,
    COALESCE(NULLIF(dropoff_borough, ''), 'Unknown') AS dropoff_borough,
    trip_distance,
    trip_duration_seconds,
    total_amount,
    tip_amount,
    fare_per_mile,
    tip_pct,
    speed_mph,
    CASE
      WHEN EXTRACT(hour FROM pickup_time) BETWEEN 0 AND 5 THEN 'late_night'
      WHEN EXTRACT(hour FROM pickup_time) BETWEEN 6 AND 9 THEN 'commute_morning'
      WHEN EXTRACT(hour FROM pickup_time) BETWEEN 10 AND 16 THEN 'daytime'
      WHEN EXTRACT(hour FROM pickup_time) BETWEEN 17 AND 20 THEN 'commute_evening'
      ELSE 'late_evening'
    END AS daypart
  FROM clean
)
SELECT
  daypart,
  pickup_borough,
  dropoff_borough,
  COUNT(*) AS trips,
  AVG(tip_pct) AS avg_tip_pct,
  AVG(CASE WHEN tip_amount = 0 THEN 1 ELSE 0 END) AS zero_tip_rate,
  MEDIAN(fare_per_mile) AS med_fare_per_mile,
  MEDIAN(speed_mph) AS med_speed_mph,
  MEDIAN(trip_distance) AS med_distance_mi,
  MEDIAN(total_amount) AS med_total_amount
FROM enriched
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 200 -- stability filter
ORDER BY trips DESC;
