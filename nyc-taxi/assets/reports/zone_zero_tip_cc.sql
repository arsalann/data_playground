-- Tip rate distribution for credit card payments by pickup zone
-- Includes multiple threshold counts for slider functionality
WITH clean AS (
    SELECT
        pickup_location_id,
        fare_amount,
        tip_amount,
        tip_amount / NULLIF(fare_amount, 0) AS tip_pct
    FROM staging.trips_summary
    WHERE trip_distance BETWEEN 0.1 AND 100
      AND trip_duration_seconds BETWEEN 60 AND 10800
      AND fare_amount BETWEEN 2.50 AND 500
      AND payment_type = 1  -- Credit card only
      AND pickup_borough NOT IN ('Unknown', 'N/A', 'Outside of NYC')
)
SELECT
    z.location_id,
    z.borough,
    z.zone,
    z.centroid_lat,
    z.centroid_lon,
    COUNT(*) AS cc_trips,
    -- Threshold counts for various tip percentages
    SUM(CASE WHEN c.tip_pct = 0 THEN 1 ELSE 0 END) AS tips_0pct,
    SUM(CASE WHEN c.tip_pct < 0.05 THEN 1 ELSE 0 END) AS tips_under_5pct,
    SUM(CASE WHEN c.tip_pct < 0.10 THEN 1 ELSE 0 END) AS tips_under_10pct,
    SUM(CASE WHEN c.tip_pct < 0.15 THEN 1 ELSE 0 END) AS tips_under_15pct,
    SUM(CASE WHEN c.tip_pct < 0.20 THEN 1 ELSE 0 END) AS tips_under_20pct,
    SUM(CASE WHEN c.tip_pct < 0.25 THEN 1 ELSE 0 END) AS tips_under_25pct,
    AVG(c.tip_pct) AS avg_tip_pct
FROM clean c
JOIN raw.taxi_zone_geojson z
    ON c.pickup_location_id = z.location_id
GROUP BY z.location_id, z.borough, z.zone, z.centroid_lat, z.centroid_lon
HAVING COUNT(*) >= 500
ORDER BY tips_0pct DESC
