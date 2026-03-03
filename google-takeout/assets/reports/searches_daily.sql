SELECT
    search_date,
    search_count,
    day_of_week,
    day_name,
    week_start,
    month,
    year,
    is_post_chatgpt,
    era
FROM `bruin-playground-arsalan.staging.searches_daily`
ORDER BY search_date
