-- Aggregate tip rates by pickup zone for map visualization
WITH clean AS (
    SELECT
        pickup_location_id,
        fare_amount,
        tip_amount,
        tip_amount / NULLIF(fare_amount, 0) AS tip_pct
    FROM staging.trips_summary
    WHERE trip_distance BETWEEN 0.1 AND 100
      AND trip_duration_seconds BETWEEN 60 AND 10800
      AND (trip_distance / NULLIF(trip_duration_seconds / 3600.0, 0)) BETWEEN 1 AND 80
      AND fare_amount BETWEEN 2.50 AND 500
      AND tip_amount >= 0
      AND fare_amount > 0
      AND tip_amount / NULLIF(fare_amount, 0) <= 1.0
      AND pickup_borough NOT IN ('Unknown', 'N/A', 'Outside of NYC')
      AND dropoff_borough NOT IN ('Unknown', 'N/A', 'Outside of NYC')
)
SELECT
    z.location_id,
    z.borough,
    z.zone,
    z.centroid_lat,
    z.centroid_lon,
    COUNT(*) AS trips,
    AVG(c.tip_pct) AS avg_tip_pct,
    SUM(CASE WHEN c.tip_amount = 0 THEN 1 ELSE 0 END)::DOUBLE / COUNT(*) AS zero_tip_rate,
    SUM(c.fare_amount) AS total_fare,
    SUM(c.tip_amount) AS total_tip
FROM clean c
JOIN raw.taxi_zone_geojson z
    ON c.pickup_location_id = z.location_id
GROUP BY z.location_id, z.borough, z.zone, z.centroid_lat, z.centroid_lon
HAVING COUNT(*) >= 100
ORDER BY avg_tip_pct DESC
