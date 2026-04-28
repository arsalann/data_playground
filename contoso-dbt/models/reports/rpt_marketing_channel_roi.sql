{{ config(materialized='table') }}

SELECT
    channel,
    EXTRACT(YEAR FROM start_date) AS year,
    EXTRACT(QUARTER FROM start_date) AS quarter,
    COUNT(*) AS campaign_count,
    ROUND(SUM(total_spend), 2) AS total_spend,
    SUM(total_impressions) AS total_impressions,
    SUM(total_clicks) AS total_clicks,
    SUM(total_conversions) AS total_conversions,
    SUM(attributed_orders) AS attributed_orders,
    ROUND(SUM(attributed_revenue_usd), 2) AS attributed_revenue_usd,
    ROUND(SAFE_DIVIDE(SUM(total_clicks), NULLIF(SUM(total_impressions), 0)) * 100, 2) AS avg_ctr,
    ROUND(SAFE_DIVIDE(SUM(total_conversions), NULLIF(SUM(total_clicks), 0)) * 100, 2) AS avg_conversion_rate,
    ROUND(SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(attributed_orders), 0)), 2) AS avg_cpa,
    ROUND(SAFE_DIVIDE(SUM(attributed_revenue_usd), NULLIF(SUM(total_spend), 0)), 2) AS avg_roas,
    CASE
        WHEN SAFE_DIVIDE(SUM(attributed_revenue_usd), NULLIF(SUM(total_spend), 0)) >= 5 THEN 'Excellent'
        WHEN SAFE_DIVIDE(SUM(attributed_revenue_usd), NULLIF(SUM(total_spend), 0)) >= 3 THEN 'Good'
        WHEN SAFE_DIVIDE(SUM(attributed_revenue_usd), NULLIF(SUM(total_spend), 0)) >= 1 THEN 'Fair'
        ELSE 'Poor'
    END AS spend_efficiency
FROM {{ ref('stg_marketing_performance') }}
WHERE total_spend > 0
GROUP BY 1, 2, 3
