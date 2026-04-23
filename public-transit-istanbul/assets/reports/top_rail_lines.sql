SELECT
    line,
    transaction_year,
    SUM(passage_cnt) AS total_passages,
    COUNT(DISTINCT station_name) AS stations,
    COUNT(DISTINCT town) AS districts
FROM `bruin-playground-arsalan.raw.istanbul_rail_stations`
GROUP BY 1, 2
ORDER BY 2, 3 DESC
