/* @bruin

name: contoso_reports.revenue_by_segment
type: duckdb.sql
description: |
  Strategic monthly revenue reporting table that segments Contoso's consumer electronics business
  performance across geographic markets and product categories. This executive-level report
  aggregates transactional sales data to provide comprehensive business intelligence for
  sales performance analysis, market trending, and growth tracking.

  The table represents monthly performance metrics for each unique combination of store country
  and product category, creating a multi-dimensional view of business performance. Store countries
  include 8 physical markets (US, UK, Germany, Canada, France, Netherlands, Italy, Australia)
  plus the "Online" channel for e-commerce transactions across all regions.

  Product categories span Contoso's full consumer electronics portfolio across 8 major segments,
  enabling category management insights and cross-category performance comparison. All monetary
  values are normalized to USD using historical exchange rates for consistent global analysis.

  Key analytical capabilities:
  - Revenue and profitability tracking across geographic segments and product categories
  - Month-over-month and year-over-year growth rate analysis with proper seasonality handling
  - Cross-border performance comparison and market share analysis
  - Category-level margin analysis and pricing effectiveness measurement
  - Order velocity and average order value trending for customer behavior insights

  Growth calculations use window functions with proper partitioning by country and category to
  ensure accurate period-over-period comparisons within each segment. MoM calculations handle
  seasonal fluctuations while YoY metrics account for longer-term growth trends and business cycles.

  Typical business use cases:
  - Executive dashboards and monthly business reviews
  - Regional sales performance assessment and territory planning
  - Category management and merchandising strategy decisions
  - Market expansion opportunity identification
  - Sales forecasting and budget planning inputs
  - Competitive positioning analysis across geographic segments

  Data characteristics:
  - 8,352 monthly segment records spanning multiple years of business operations
  - Complete coverage of all active country-category combinations with no gaps
  - All monetary values aggregated and rounded to 2 decimal places for financial precision
  - Growth metrics calculated with safe division to handle edge cases and null values
connection: contoso-duckdb
tags:
  - finance
  - executive_reporting
  - revenue_analysis
  - geographic_segments
  - monthly_aggregation
  - growth_metrics
  - consumer_electronics
  - multi_market
  - business_intelligence

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_staging.sales_fact


columns:
  - name: year
    type: INTEGER
    description: |
      Calendar year of order transactions, serving as the primary temporal dimension
      for annual trend analysis and year-over-year comparisons. Derived from order_date
      in the underlying sales transactions. Forms part of composite key with month
      to uniquely identify reporting periods.
    primary_key: true
    checks:
      - name: not_null
  - name: month
    type: INTEGER
    description: |
      Calendar month number (1-12) of order transactions, enabling monthly trend analysis
      and seasonal pattern identification. Combined with year forms the complete temporal
      dimension for period-based reporting. Essential for month-over-month growth calculations
      and monthly business performance tracking.
    primary_key: true
    checks:
      - name: not_null
  - name: store_country
    type: VARCHAR
    description: |
      Geographic market identifier representing either a physical store country or the
      "Online" e-commerce channel. 9 distinct values: United States, United Kingdom,
      Germany, Canada, France, Netherlands, Italy, Australia, and Online. Online represents
      cross-border e-commerce transactions that cannot be attributed to a specific physical
      market. Critical dimension for geographic performance analysis and market segmentation.
    primary_key: true
    checks:
      - name: not_null
  - name: category_name
    type: VARCHAR
    description: |
      High-level product category classification representing major consumer electronics
      segments in Contoso's product portfolio. 8 distinct categories covering the full
      breadth of product offerings from major appliances to consumer electronics accessories.
      Essential dimension for category management, merchandising decisions, and product
      line performance analysis across different market segments.
    primary_key: true
    checks:
      - name: not_null
  - name: order_count
    type: INTEGER
    description: |
      Total number of distinct customer orders within the month-country-category segment.
      Calculated using COUNT(DISTINCT order_key) to ensure accurate order-level metrics
      regardless of line item complexity. Key performance indicator for customer transaction
      volume and order velocity. Average ~236 orders per segment with high variability
      based on market size and category popularity.
    checks:
      - name: not_null
      - name: positive
  - name: units_sold
    type: INTEGER
    description: |
      Total quantity of individual product units sold across all orders in the segment.
      Calculated as SUM(quantity) from individual line items, representing actual units
      moved through the supply chain. Essential for inventory planning, demand forecasting,
      and unit-based performance analysis. Average ~884 units per segment with significant
      variation by product category and seasonal factors.
    checks:
      - name: not_null
      - name: positive
  - name: revenue_usd
    type: DOUBLE
    description: |
      Total gross revenue generated in US Dollars for the month-country-category segment.
      Aggregated from individual transaction revenue already converted from original
      currencies using historical exchange rates. Represents the primary top-line financial
      performance metric. Values are rounded to 2 decimal places for financial precision.
      All revenue calculations use net pricing that may include promotional discounts.
    checks:
      - name: not_null
  - name: cost_usd
    type: DOUBLE
    description: |
      Total cost of goods sold (COGS) in US Dollars for all units sold in the segment.
      Aggregated from standardized unit costs converted to USD using historical exchange
      rates. Essential for gross margin analysis and profitability assessment across
      geographic and product dimensions. Rounded to 2 decimal places for consistency
      with revenue calculations.
    checks:
      - name: not_null
  - name: profit_usd
    type: DOUBLE
    description: |
      Total gross profit in US Dollars calculated as revenue_usd minus cost_usd for
      the segment. Represents the core profitability metric before operational expenses
      and overheads. Can be negative for promotional periods or loss-leader pricing
      strategies. Critical for margin analysis and profitability optimization across
      segments. Rounded to 2 decimal places for financial precision.
    checks:
      - name: not_null
  - name: avg_margin_pct
    type: DOUBLE
    description: |
      Average gross profit margin as a percentage across all transactions in the segment.
      Calculated as the mean of individual transaction margin percentages (not derived
      from aggregate profit/revenue). Provides insight into pricing effectiveness and
      cost management within each segment. Standard deviation ~1.78% indicates relatively
      consistent margin management across categories. Values rounded to 2 decimal places.
    checks:
      - name: not_null
  - name: avg_order_value
    type: DOUBLE
    description: |
      Average order value in US Dollars calculated as total revenue divided by distinct
      order count. Key customer behavior metric indicating spending patterns and basket
      size effectiveness within each segment. Essential for revenue per customer analysis
      and pricing strategy optimization. Standard deviation ~$974 reflects significant
      variation in customer purchase behavior across segments.
    checks:
      - name: not_null
      - name: positive
  - name: revenue_mom
    type: DOUBLE
    description: |
      Month-over-month revenue growth rate as a percentage, calculated using window functions
      partitioned by store_country and category_name. Compares current month revenue to
      previous month within the same segment. Uses SAFE_DIVIDE to handle edge cases and
      missing prior periods. Null values (72 records) typically occur for the first month
      of new segment combinations. Essential for identifying short-term trends and seasonal patterns.
  - name: revenue_yoy
    type: DOUBLE
    description: |
      Year-over-year revenue growth rate as a percentage, calculated using 12-month lag
      within each country-category partition. Compares current month to the same month
      in the previous year to account for seasonality effects. Higher null count (864 records)
      reflects the natural lag required for annual comparisons in newer segments. Critical
      metric for assessing long-term business growth and market expansion success.

@bruin */

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
        ROUND((SUM(revenue_usd)) / (NULLIF(COUNT(DISTINCT order_key), 0)), 2) AS avg_order_value
    FROM contoso_staging.sales_fact
    GROUP BY 1, 2, 3, 4
)

SELECT
    m.*,
    ROUND(
        (m.revenue_usd - LAG(m.revenue_usd) OVER (
                PARTITION BY m.store_country, m.category_name
                ORDER BY m.year, m.month
            )) / (NULLIF(LAG(m.revenue_usd) OVER (
                PARTITION BY m.store_country, m.category_name
                ORDER BY m.year, m.month
            ), 0)) * 100,
        2
    ) AS revenue_mom,
    ROUND(
        (m.revenue_usd - LAG(m.revenue_usd, 12) OVER (
                PARTITION BY m.store_country, m.category_name
                ORDER BY m.year, m.month
            )) / (NULLIF(LAG(m.revenue_usd, 12) OVER (
                PARTITION BY m.store_country, m.category_name
                ORDER BY m.year, m.month
            ), 0)) * 100,
        2
    ) AS revenue_yoy
FROM monthly m
ORDER BY year, month, store_country, category_name
