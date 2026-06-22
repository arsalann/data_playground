SELECT
    occurrence_year,
    assault,
    auto_theft,
    break_and_enter,
    robbery,
    theft_over_5k
FROM report.category_trends
ORDER BY occurrence_year
