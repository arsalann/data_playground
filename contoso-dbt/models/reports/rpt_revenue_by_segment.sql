{{ config(materialized='table') }}

WITH monthly AS (
    SELECT
        year,
        month_number AS month,
        store_country,
        category_name,
        COUNT(DISTINCT order_key) AS order_count,
        SUM(quantity) AS units_sold,
        ROUND(SUM(revenue_usd), 2) AS revenue_usd,
        ROUND(SUM(cost_usd), 2) AS cost_usd,
        ROUND(SUM(profit_usd), 2) AS profit_usd,
        ROUND(AVG(margin_pct), 2) AS avg_margin_pct,
        ROUND(SAFE_DIVIDE(SUM(revenue_usd), NULLIF(COUNT(DISTINCT order_key), 0)), 2) AS avg_order_value
    FROM {{ ref('stg_sales_fact') }}
    GROUP BY 1, 2, 3, 4
)

SELECT
    m.*,
    ROUND(
        SAFE_DIVIDE(
            m.revenue_usd - LAG(m.revenue_usd) OVER (
                PARTITION BY m.store_country, m.category_name
                ORDER BY m.year, m.month
            ),
            NULLIF(LAG(m.revenue_usd) OVER (
                PARTITION BY m.store_country, m.category_name
                ORDER BY m.year, m.month
            ), 0)
        ) * 100,
        2
    ) AS revenue_mom,
    ROUND(
        SAFE_DIVIDE(
            m.revenue_usd - LAG(m.revenue_usd, 12) OVER (
                PARTITION BY m.store_country, m.category_name
                ORDER BY m.year, m.month
            ),
            NULLIF(LAG(m.revenue_usd, 12) OVER (
                PARTITION BY m.store_country, m.category_name
                ORDER BY m.year, m.month
            ), 0)
        ) * 100,
        2
    ) AS revenue_yoy
FROM monthly m
