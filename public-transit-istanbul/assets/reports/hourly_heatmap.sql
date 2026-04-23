SELECT
    road_type,
    transition_hour,
    day_of_week,
    day_name,
    avg_passages,
    avg_passengers,
    total_passages,
    num_days
FROM `bruin-playground-arsalan.staging.istanbul_hourly_patterns`
ORDER BY road_type, day_of_week, transition_hour
