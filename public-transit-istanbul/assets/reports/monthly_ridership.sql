SELECT
    DATE_TRUNC(transition_date, MONTH) AS month_date,
    road_type,
    SUM(total_passages) AS monthly_passages,
    SUM(total_passengers) AS monthly_passengers
FROM `bruin-playground-arsalan.staging.istanbul_daily_ridership`
GROUP BY 1, 2
ORDER BY 1, 2
