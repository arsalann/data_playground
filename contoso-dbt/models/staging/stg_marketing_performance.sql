{{ config(materialized='table') }}

WITH campaigns_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'campaigns') }}
    WHERE campaign_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY campaign_key
        ORDER BY extracted_at DESC
    ) = 1
),

ad_spend_agg AS (
    SELECT
        campaign_key,
        SUM(spend_amount) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(conversions) AS total_conversions
    FROM {{ source('contoso_raw', 'ad_spend_daily') }}
    GROUP BY campaign_key
),

attribution_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'campaign_attribution') }}
    WHERE attribution_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY attribution_key
        ORDER BY extracted_at DESC
    ) = 1
),

sales_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'sales') }}
    WHERE order_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_key, line_number
        ORDER BY extracted_at DESC
    ) = 1
),

attributed_revenue AS (
    SELECT
        a.campaign_key,
        COUNT(DISTINCT a.order_key) AS attributed_orders,
        SUM(s.quantity * s.net_price * s.exchange_rate) AS attributed_revenue_usd
    FROM attribution_deduped a
    INNER JOIN sales_deduped s ON a.order_key = s.order_key
    WHERE a.is_last_touch = TRUE
    GROUP BY a.campaign_key
)

SELECT
    c.campaign_key,
    c.campaign_name,
    c.channel,
    CAST(c.start_date AS DATE) AS start_date,
    CAST(c.end_date AS DATE) AS end_date,
    DATE_DIFF(CAST(c.end_date AS DATE), CAST(c.start_date AS DATE), DAY) AS duration_days,
    c.budget_amount,
    COALESCE(a.total_spend, 0) AS total_spend,
    COALESCE(a.total_impressions, 0) AS total_impressions,
    COALESCE(a.total_clicks, 0) AS total_clicks,
    COALESCE(a.total_conversions, 0) AS total_conversions,
    ROUND(SAFE_DIVIDE(a.total_clicks, NULLIF(a.total_impressions, 0)) * 100, 2) AS ctr,
    ROUND(SAFE_DIVIDE(a.total_conversions, NULLIF(a.total_clicks, 0)) * 100, 2) AS conversion_rate,
    COALESCE(r.attributed_orders, 0) AS attributed_orders,
    ROUND(COALESCE(r.attributed_revenue_usd, 0), 2) AS attributed_revenue_usd,
    ROUND(SAFE_DIVIDE(a.total_spend, NULLIF(r.attributed_orders, 0)), 2) AS cpa,
    ROUND(SAFE_DIVIDE(r.attributed_revenue_usd, NULLIF(a.total_spend, 0)), 2) AS roas
FROM campaigns_deduped c
LEFT JOIN ad_spend_agg a ON c.campaign_key = a.campaign_key
LEFT JOIN attributed_revenue r ON c.campaign_key = r.campaign_key
