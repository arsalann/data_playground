SELECT
    year,
    SUM(total_journeys) AS total_journeys,
    SUM(unique_passengers) AS unique_passengers,
    COUNT(DISTINCT pier_name) AS active_piers,
    ROUND(SAFE_DIVIDE(SUM(total_journeys), SUM(unique_passengers)), 2) AS trips_per_person
FROM `bruin-playground-arsalan.raw.istanbul_ferry_piers`
GROUP BY 1
ORDER BY 1
