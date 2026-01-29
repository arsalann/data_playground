-- Payment type breakdown by pickup zone
WITH clean AS (
    SELECT
        pickup_location_id,
        payment_type,
        fare_amount,
        tip_amount
    FROM staging.trips_summary
    WHERE trip_distance BETWEEN 0.1 AND 100
      AND trip_duration_seconds BETWEEN 60 AND 10800
      AND fare_amount BETWEEN 2.50 AND 500
      AND pickup_borough NOT IN ('Unknown', 'N/A', 'Outside of NYC')
)
SELECT
    z.location_id,
    z.borough,
    z.zone,
    z.centroid_lat,
    z.centroid_lon,
    COUNT(*) AS total_trips,
    SUM(CASE WHEN c.payment_type = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
    SUM(CASE WHEN c.payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips,
    SUM(CASE WHEN c.payment_type NOT IN (1, 2) THEN 1 ELSE 0 END) AS other_trips,
    SUM(CASE WHEN c.payment_type = 1 THEN 1 ELSE 0 END)::DOUBLE / COUNT(*) AS credit_card_pct,
    SUM(CASE WHEN c.payment_type = 2 THEN 1 ELSE 0 END)::DOUBLE / COUNT(*) AS cash_pct,
    -- Tip rate by payment type
    AVG(CASE WHEN c.payment_type = 1 THEN c.tip_amount / NULLIF(c.fare_amount, 0) END) AS credit_card_tip_pct,
    AVG(CASE WHEN c.payment_type = 2 THEN c.tip_amount / NULLIF(c.fare_amount, 0) END) AS cash_tip_pct
FROM clean c
JOIN raw.taxi_zone_geojson z
    ON c.pickup_location_id = z.location_id
GROUP BY z.location_id, z.borough, z.zone, z.centroid_lat, z.centroid_lon
HAVING COUNT(*) >= 1000
ORDER BY cash_pct DESC
