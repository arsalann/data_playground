SELECT
    LPAD(CAST(hour AS STRING), 2, '0') AS hour_label,
    day_of_week,
    crime_count,
    share_of_week,
    latest_complete_year
FROM report.temporal_heatmap
ORDER BY hour, day_of_week_num
