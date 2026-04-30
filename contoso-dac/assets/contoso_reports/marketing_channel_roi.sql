/* @bruin

name: contoso_reports.marketing_channel_roi
type: duckdb.sql
description: |
  Quarterly marketing channel ROI analysis for Contoso's consumer electronics business.

  This executive-level report aggregates campaign-level performance data into quarterly summaries
  by marketing channel, providing key metrics for budget allocation and channel optimization decisions.
  The analysis covers 5 primary marketing channels: Paid Search, Social, Display, Email, and Referral.

  Key business insights:
  - Paid Search typically drives highest volume but varies in efficiency by quarter
  - Social and Display channels show more consistent ROAS patterns
  - Email marketing generally maintains lower CPA but smaller scale
  - Referral channel often achieves highest ROAS but lowest volume
  - Seasonal patterns emerge in Q4 (holiday shopping) vs Q1 (post-holiday slump)

  Methodology and assumptions:
  - Uses last-touch attribution to credit the final campaign interaction before order
  - All monetary values standardized to USD using daily exchange rates
  - Campaigns with zero spend are excluded to focus on active marketing efforts
  - Average metrics are weighted by spend volume, not simple arithmetic means
  - Safe division prevents errors when campaigns have zero attributed orders

  Data lineage:
  - Aggregates from contoso_staging.marketing_performance (campaign-level data)
  - Upstream data includes campaigns, daily ad spend, attribution mapping, and sales
  - Refreshed daily but shows quarterly snapshots for trend analysis
  - Typically contains ~140 rows covering ~7 years of quarterly channel performance

  Business use cases:
  - Marketing budget allocation across channels for upcoming quarters
  - Channel performance benchmarking and optimization targets
  - Executive reporting on marketing ROI effectiveness
  - Seasonal trend analysis for strategic planning
  - Identification of underperforming channels requiring attention
connection: contoso-duckdb
tags:
  - marketing
  - roi
  - executive_reporting
  - quarterly_summary
  - channel_performance
  - budget_planning
  - consumer_electronics

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_staging.marketing_performance


columns:
  - name: channel
    type: VARCHAR
    description: Marketing channel identifier (Paid Search, Social, Display, Email, Referral) - standardized values across all campaigns
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Paid Search
          - Social
          - Display
          - Email
          - Referral
  - name: year
    type: INTEGER
    description: Calendar year for performance measurement, typically spans 2016-2022 based on campaign activity periods
    primary_key: true
    checks:
      - name: not_null
  - name: quarter
    type: INTEGER
    description: Calendar quarter (1=Jan-Mar, 2=Apr-Jun, 3=Jul-Sep, 4=Oct-Dec) for seasonal performance analysis
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - 1
          - 2
          - 3
          - 4
  - name: campaign_count
    type: INTEGER
    description: Number of distinct campaigns active during the quarter for the channel, indicates campaign diversity and testing volume
    checks:
      - name: not_null
  - name: total_spend
    type: DOUBLE
    description: Aggregate ad spend in USD across all campaigns in the channel-quarter, rounded to 2 decimal places
    checks:
      - name: not_null
  - name: total_impressions
    type: INTEGER
    description: Total ad impressions served across all channel campaigns in the quarter, indicates reach and visibility
    checks:
      - name: not_null
  - name: total_clicks
    type: INTEGER
    description: Total ad clicks received across all channel campaigns in the quarter, indicates engagement level
    checks:
      - name: not_null
  - name: total_conversions
    type: INTEGER
    description: Total conversions tracked by ad platforms across channel campaigns, may differ from attributed orders due to attribution windows
    checks:
      - name: not_null
  - name: attributed_orders
    type: INTEGER
    description: Orders attributed to the channel via last-touch attribution methodology, used for CPA and ROAS calculation
    checks:
      - name: not_null
  - name: attributed_revenue_usd
    type: DOUBLE
    description: Revenue from attributed orders in USD, calculated from order quantities, net prices, and exchange rates
    checks:
      - name: not_null
  - name: avg_ctr
    type: DOUBLE
    description: Average click-through rate as percentage (clicks/impressions * 100), indicates ad creative and targeting effectiveness
    checks:
      - name: not_null
  - name: avg_conversion_rate
    type: DOUBLE
    description: Average conversion rate as percentage (conversions/clicks * 100), indicates landing page and offer effectiveness
    checks:
      - name: not_null
  - name: avg_cpa
    type: DOUBLE
    description: Average cost per acquisition in USD (total_spend / attributed_orders), key efficiency metric for budget optimization
  - name: avg_roas
    type: DOUBLE
    description: Average return on ad spend ratio (attributed_revenue / total_spend), primary profitability metric for channel evaluation
  - name: spend_efficiency
    type: VARCHAR
    description: Categorical efficiency rating based on ROAS thresholds (Excellent ≥5.0, Good ≥3.0, Fair ≥1.0, Poor <1.0)
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Excellent
          - Good
          - Fair
          - Poor

@bruin */

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
    ROUND((SUM(total_clicks)) / (NULLIF(SUM(total_impressions), 0)) * 100, 2) AS avg_ctr,
    ROUND((SUM(total_conversions)) / (NULLIF(SUM(total_clicks), 0)) * 100, 2) AS avg_conversion_rate,
    ROUND((SUM(total_spend)) / (NULLIF(SUM(attributed_orders), 0)), 2) AS avg_cpa,
    ROUND((SUM(attributed_revenue_usd)) / (NULLIF(SUM(total_spend), 0)), 2) AS avg_roas,
    CASE
        WHEN (SUM(attributed_revenue_usd)) / (NULLIF(SUM(total_spend), 0)) >= 5 THEN 'Excellent'
        WHEN (SUM(attributed_revenue_usd)) / (NULLIF(SUM(total_spend), 0)) >= 3 THEN 'Good'
        WHEN (SUM(attributed_revenue_usd)) / (NULLIF(SUM(total_spend), 0)) >= 1 THEN 'Fair'
        ELSE 'Poor'
    END AS spend_efficiency
FROM contoso_staging.marketing_performance
WHERE total_spend > 0
GROUP BY 1, 2, 3
ORDER BY channel, year, quarter
