WITH district_totals AS (
    SELECT
        town,
        road_type,
        year,
        total_passages,
        district_total_passages,
        mode_share_pct
    FROM `bruin-playground-arsalan.staging.istanbul_district_summary`
    WHERE year = 2023
),
district_coords AS (
    SELECT
        town,
        AVG(latitude) AS latitude,
        AVG(longitude) AS longitude
    FROM `bruin-playground-arsalan.raw.istanbul_rail_stations`
    WHERE latitude BETWEEN 40 AND 42
      AND longitude BETWEEN 27 AND 30
    GROUP BY town
),
district_wide AS (
    SELECT
        d.town,
        SUM(d.total_passages) AS total_passages,
        SUM(CASE WHEN d.road_type = 'RAYLI' THEN d.total_passages ELSE 0 END) AS rail_passages,
        SUM(CASE WHEN d.road_type = 'OTOYOL' THEN d.total_passages ELSE 0 END) AS bus_passages,
        SUM(CASE WHEN d.road_type = 'DENİZ' THEN d.total_passages ELSE 0 END) AS ferry_passages,
        ROUND(SAFE_DIVIDE(
            SUM(CASE WHEN d.road_type = 'RAYLI' THEN d.total_passages ELSE 0 END),
            SUM(d.total_passages)
        ) * 100, 1) AS rail_share_pct
    FROM district_totals d
    GROUP BY d.town
)
SELECT
    w.*,
    c.latitude,
    c.longitude
FROM district_wide w
LEFT JOIN district_coords c ON w.town = c.town
WHERE w.total_passages > 100000
ORDER BY w.total_passages DESC
