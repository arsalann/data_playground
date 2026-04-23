SELECT
    transaction_year,
    SUM(passage_cnt) AS total_passages,
    SUM(passenger_cnt) AS total_passengers,
    COUNT(DISTINCT station_name) AS active_stations,
    COUNT(DISTINCT line) AS active_lines
FROM `bruin-playground-arsalan.raw.istanbul_rail_stations`
GROUP BY 1
ORDER BY 1
