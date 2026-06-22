SELECT
    CONCAT(CAST(rank_by_rate AS STRING), '. ', neighbourhood_name) AS neighbourhood_name,
    assault,
    auto_theft,
    break_and_enter,
    robbery,
    theft_over_5k,
    total_crimes,
    latest_complete_year
FROM report.neighbourhood_category_mix
ORDER BY rank_by_rate
