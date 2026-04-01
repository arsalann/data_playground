/* @bruin

name: contoso_staging.marketing_performance
type: bq.sql
description: |
  Campaign-level marketing performance summary for Contoso's consumer electronics business.

  This table provides a complete view of marketing campaign effectiveness by combining ad platform
  data (spend, impressions, clicks) with sales attribution to calculate key performance metrics
  like ROAS, CPA, and conversion rates. Uses last-touch attribution to link campaigns to revenue.

  The data integrates campaigns from 5 marketing channels: Paid Search, Social, Display, Email,
  and Referral. Attribution is based on campaign-to-order mapping using last-touch methodology,
  meaning the most recent campaign interaction receives full credit for the order.

  Key transformations:
  - Deduplicates raw data using extracted_at timestamps (most recent wins)
  - Aggregates daily ad spend data to campaign level
  - Joins attributed orders via campaign_attribution table using last-touch logic
  - Calculates derived metrics: CTR (%), conversion rate (%), CPA ($/order), ROAS (revenue/spend)
  - Handles missing attribution gracefully with COALESCE to avoid nulls

  Typical use cases:
  - Marketing channel ROI analysis and budget allocation decisions
  - Campaign performance benchmarking and optimization
  - Attribution modeling and customer journey analysis
  - Executive reporting on marketing effectiveness

  Operational characteristics:
  - Refreshed daily via create+replace strategy
  - Typically contains ~200 campaigns across 5 marketing channels
  - Average campaign duration is 50+ days with campaigns running concurrently
  - Performance varies significantly by channel (Paid Search typically highest volume)
  - Safe division used throughout to handle campaigns with zero attributed orders
  - All monetary values standardized to USD for cross-campaign comparison
connection: bruin-playground-eu
tags:
  - marketing
  - performance
  - attribution
  - staging
  - fact_table
  - daily_refresh

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_raw.campaigns
  - contoso_raw.ad_spend_daily
  - contoso_raw.campaign_attribution
  - contoso_raw.sales


columns:
  - name: campaign_key
    type: INTEGER
    description: Unique campaign identifier, primary key for joins with attribution and ad spend tables
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: campaign_name
    type: VARCHAR
    description: Human-readable campaign name, typically includes brand/product and targeting strategy
    checks:
      - name: not_null
  - name: channel
    type: VARCHAR
    description: Marketing channel (Paid Search, Social, Display, Email, Referral) - standardized enum values
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Paid Search
          - Social
          - Display
          - Email
          - Referral
  - name: start_date
    type: DATE
    description: Campaign launch date, used for performance period analysis
    checks:
      - name: not_null
  - name: end_date
    type: DATE
    description: Campaign end date, may be in future for active campaigns
    checks:
      - name: not_null
  - name: duration_days
    type: INTEGER
    description: Campaign runtime in days, calculated as end_date - start_date
    checks:
      - name: not_null
  - name: budget_amount
    type: DOUBLE
    description: Planned campaign budget in USD, set at campaign creation
    checks:
      - name: not_null
  - name: total_spend
    type: DOUBLE
    description: Actual campaign spend in USD, aggregated from daily ad platform data
    checks:
      - name: not_null
  - name: total_impressions
    type: INTEGER
    description: Total ad impressions served across all campaign placements and days
    checks:
      - name: not_null
  - name: total_clicks
    type: INTEGER
    description: Total ad clicks received across all campaign placements and days
    checks:
      - name: not_null
  - name: total_conversions
    type: INTEGER
    description: Total conversions tracked by ad platforms (may differ from attributed orders due to attribution windows)
    checks:
      - name: not_null
  - name: ctr
    type: DOUBLE
    description: Click-through rate as percentage (clicks/impressions * 100), rounded to 2 decimal places
    checks:
      - name: not_null
  - name: conversion_rate
    type: DOUBLE
    description: Conversion rate as percentage (conversions/clicks * 100), rounded to 2 decimal places
    checks:
      - name: not_null
  - name: attributed_orders
    type: INTEGER
    description: Orders attributed to campaign via last-touch attribution, used for CPA calculation
    checks:
      - name: not_null
  - name: attributed_revenue_usd
    type: NUMERIC
    description: Revenue from attributed orders in USD, calculated as quantity * net_price * exchange_rate
    checks:
      - name: not_null
  - name: cpa
    type: DOUBLE
    description: Cost per acquisition in USD (total_spend / attributed_orders), null-safe calculation
  - name: roas
    type: DOUBLE
    description: Return on ad spend ratio (attributed_revenue / total_spend), key profitability metric

@bruin */

WITH campaigns_deduped AS (
    SELECT *
    FROM contoso_raw.campaigns
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
    FROM contoso_raw.ad_spend_daily
    GROUP BY campaign_key
),

attribution_deduped AS (
    SELECT *
    FROM contoso_raw.campaign_attribution
    WHERE attribution_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY attribution_key
        ORDER BY extracted_at DESC
    ) = 1
),

sales_deduped AS (
    SELECT *
    FROM contoso_raw.sales
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
ORDER BY c.campaign_key
