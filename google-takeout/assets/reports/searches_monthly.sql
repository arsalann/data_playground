WITH monthly AS (
    SELECT
        DATE_TRUNC(search_date, MONTH) AS month,
        SUM(search_count) AS search_count
    FROM `bruin-playground-arsalan.staging.searches_daily`
    GROUP BY 1
)

SELECT
    month,
    search_count,
    EXTRACT(YEAR FROM month) AS year,
    EXTRACT(MONTH FROM month) AS month_num,
    FORMAT_DATE('%b %Y', month) AS month_label,
    month >= '2022-12-01' AS is_post_chatgpt,
    CASE
        WHEN month >= '2022-12-01' THEN 'Post-ChatGPT'
        ELSE 'Pre-ChatGPT'
    END AS era
FROM monthly
ORDER BY month
