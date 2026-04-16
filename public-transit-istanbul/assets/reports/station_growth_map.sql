SELECT
    station_name,
    line,
    town,
    transaction_year,
    annual_passages,
    prev_year_passages,
    yoy_growth_pct,
    longitude,
    latitude,
    line_type
FROM `bruin-playground-arsalan.staging.istanbul_station_growth`
WHERE longitude BETWEEN 27 AND 30
  AND latitude BETWEEN 40 AND 42
  AND annual_passages > 0
ORDER BY transaction_year, station_name
