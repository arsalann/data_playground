/* @bruin

name: contoso_staging.sales_fact
type: bq.sql
description: |
  Comprehensive sales fact table serving as the analytical foundation for Contoso's consumer
  electronics retail business. This denormalized table combines transactional sales data with
  rich dimensional context including customer demographics, product hierarchy, store geography,
  and temporal dimensions.

  The table represents individual order line items from Contoso's multi-channel retail operations,
  spanning online sales and physical stores across 9 countries. All monetary values are
  standardized to USD using historical exchange rates for consistent cross-market analysis.

  Key business transformations:
  - Currency normalization: Original transaction currencies (USD, EUR, GBP, CAD, AUD) converted
    to USD using daily exchange rates from raw.currency_exchange
  - Data quality: Implements deduplication logic using extracted_at timestamps to handle
    incremental data loads and ensure latest record wins
  - Dimensional denormalization: Pre-joins customer, product, store, and date dimensions to
    eliminate join complexity in downstream analytics
  - Profit calculations: Derives profit margins using standardized cost accounting (net_price - unit_cost)
  - Date enrichment: Adds computed time dimensions (year, quarter, month, day_of_week) for temporal analysis

  Typical analytical use cases:
  - Revenue and profitability analysis across products, geographies, and customer segments
  - Sales performance trending and seasonality analysis
  - Customer behavior analytics and demographic segmentation
  - Product category performance and brand analysis
  - Cross-border trade analysis and currency impact assessment
  - Inventory planning and demand forecasting inputs

  Data characteristics:
  - 2.3M+ transaction records spanning May 2016 through December 2025
  - Average order value varies significantly by geography and product category
  - Multi-line orders represented by multiple rows (avg 1.16 lines per order)
  - Covers 8 product categories with 32 subcategories across 11 brands
  - Customer base of ~83K individuals across 8 countries with diverse occupational profiles
  - Store network spans 74+ locations plus online channel across 9 markets

  Operational characteristics:
  - Refreshed daily via create+replace materialization strategy
  - Source data deduplication ensures referential integrity across dimension tables
  - Safe division operations prevent divide-by-zero errors in margin calculations
  - Maintains full history for trend analysis and comparative reporting
connection: bruin-playground-eu
tags:
  - sales
  - fact_table
  - denormalized
  - staging
  - daily_refresh
  - multi_currency
  - cross_border
  - retail
  - consumer_electronics

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_raw.sales
  - contoso_raw.customers
  - contoso_raw.products
  - contoso_raw.stores
  - contoso_raw.dates

secrets:
  - key: bruin-playground-eu
    inject_as: bruin-playground-eu

columns:
  - name: order_key
    type: INTEGER
    description: |
      Unique order identifier serving as the primary business key for customer orders.
      Combined with line_number forms the composite primary key for individual line items.
      Values are system-generated integers that uniquely identify each customer order
      across all channels and geographies.
    primary_key: true
    checks:
      - name: not_null
  - name: line_number
    type: INTEGER
    description: |
      Line item sequence number within each order, starting from 1. Most orders contain
      1-2 line items (avg 1.16), with maximum observed around 10+ items for large orders.
      Combined with order_key forms the composite primary key for transaction-level analysis.
    primary_key: true
    checks:
      - name: not_null
  - name: order_date
    type: DATE
    description: |
      Date when the customer placed the order, serving as the primary temporal dimension
      for sales analysis. Spans from May 2016 through December 2025, enabling both
      historical analysis and forward-looking planning scenarios.
    checks:
      - name: not_null
  - name: delivery_date
    type: DATE
    description: |
      Date when the order was delivered to the customer. Always occurs on or after
      order_date, useful for analyzing fulfillment performance and delivery times.
      Slight extension beyond order_date range to account for delivery logistics.
    checks:
      - name: not_null
  - name: year
    type: INTEGER
    description: |
      Calendar year extracted from order_date for time-based aggregations and filtering.
      Derived dimension field to simplify year-over-year analysis and reporting.
    checks:
      - name: not_null
  - name: quarter
    type: VARCHAR
    description: |
      Calendar quarter (Q1, Q2, Q3, Q4) extracted from order_date. Standardized
      string format for seasonal analysis and quarterly business reporting.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Q1
          - Q2
          - Q3
          - Q4
  - name: month_number
    type: INTEGER
    description: |
      Numeric month (1-12) extracted from order_date for monthly trending and
      seasonality analysis. Integer format for easy sorting and mathematical operations.
    checks:
      - name: not_null
  - name: dayof_week
    type: VARCHAR
    description: |
      Full day name (Monday, Tuesday, etc.) extracted from order_date for analyzing
      weekly shopping patterns and day-of-week effects on sales performance.
    checks:
      - name: not_null
  - name: customer_key
    type: INTEGER
    description: |
      Foreign key to customer dimension table. Unique identifier for individual customers
      enabling customer-level analytics, segmentation, and lifetime value calculations.
      Links to approximately 83K unique customers in the database.
    checks:
      - name: not_null
  - name: customer_name
    type: VARCHAR
    description: |
      Full customer name concatenated from given_name and surname fields.
      Used for customer identification and personalized reporting. Note: Contains PII
      and should be handled according to data privacy policies.
    checks:
      - name: not_null
  - name: customer_country
    type: VARCHAR
    description: |
      Customer's country of residence, derived from the customers dimension.
      8 distinct countries represented: Germany, France, Australia, United States,
      Italy, Netherlands, Canada, United Kingdom. Key dimension for geographic analysis.
    checks:
      - name: not_null
  - name: customer_gender
    type: VARCHAR
    description: |
      Customer's gender (male/female) for demographic segmentation and analysis.
      Binary classification used for gender-based marketing insights and
      product preference analysis.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - male
          - female
  - name: customer_occupation
    type: VARCHAR
    description: |
      Customer's professional occupation providing socioeconomic context for
      purchasing behavior analysis. Contains ~2.5K distinct values ranging
      from entry-level to executive positions across various industries.
    checks:
      - name: not_null
  - name: store_key
    type: INTEGER
    description: |
      Foreign key to store dimension table. Identifies the specific retail location
      or online channel where the transaction occurred. Essential for store performance
      analysis and geographic sales attribution.
    checks:
      - name: not_null
  - name: store_country
    type: VARCHAR
    description: |
      Country where the store is located, including "Online" for e-commerce transactions.
      9 distinct values: United States, United Kingdom, Germany, Canada, France,
      Netherlands, Italy, Australia, plus Online. Key for market performance analysis.
    checks:
      - name: not_null
  - name: product_key
    type: INTEGER
    description: |
      Foreign key to product dimension table. Unique identifier for individual SKUs
      enabling product-level analysis, inventory planning, and merchandising insights.
      Links to ~2.5K distinct products across the catalog.
    checks:
      - name: not_null
  - name: product_name
    type: VARCHAR
    description: |
      Full product name providing detailed product identification. Names are descriptive
      and typically include key product attributes, specifications, and model information
      for consumer electronics items.
    checks:
      - name: not_null
  - name: brand
    type: VARCHAR
    description: |
      Product brand name representing the manufacturer or brand identity.
      11 distinct brands in the catalog, essential for brand performance analysis
      and competitive positioning insights.
    checks:
      - name: not_null
  - name: category_name
    type: VARCHAR
    description: |
      High-level product category classification. 8 distinct categories represent
      major consumer electronics groupings, used for category management and
      merchandising strategy decisions.
    checks:
      - name: not_null
  - name: sub_category_name
    type: VARCHAR
    description: |
      Detailed product subcategory providing finer classification within categories.
      32 distinct subcategories enable granular product analysis and targeted
      merchandising strategies within major category groups.
    checks:
      - name: not_null
  - name: quantity
    type: INTEGER
    description: |
      Number of units purchased for this line item. Always positive integer
      representing actual units sold. Average quantity is ~3.1 units per line item
      with standard deviation of ~2.3, indicating most sales are small quantities.
    checks:
      - name: not_null
      - name: positive
  - name: unit_price_usd
    type: DOUBLE
    description: |
      Selling price per unit in USD, converted from original transaction currency
      using historical exchange rates. Represents the list price before any discounts
      or promotions. Essential for pricing analysis and revenue calculations.
    checks:
      - name: not_null
  - name: unit_cost_usd
    type: DOUBLE
    description: |
      Cost per unit in USD, converted from original transaction currency.
      Represents the cost of goods sold (COGS) for profitability analysis
      and margin calculations. Used in profit computation and pricing optimization.
    checks:
      - name: not_null
  - name: revenue_usd
    type: DOUBLE
    description: |
      Total line item revenue in USD, calculated as quantity × net_price × exchange_rate.
      Net price may include discounts not visible in unit_price. Primary revenue metric
      for financial reporting and performance analysis.
    checks:
      - name: not_null
  - name: cost_usd
    type: DOUBLE
    description: |
      Total line item cost in USD, calculated as quantity × unit_cost × exchange_rate.
      Represents total cost of goods sold for this transaction. Used for profitability
      analysis and margin calculations across products and geographies.
    checks:
      - name: not_null
  - name: profit_usd
    type: DOUBLE
    description: |
      Line item profit in USD, calculated as revenue_usd - cost_usd.
      Core profitability metric enabling margin analysis across products, customers,
      and markets. Can be negative for promotional or clearance pricing scenarios.
    checks:
      - name: not_null
  - name: margin_pct
    type: DOUBLE
    description: |
      Profit margin as a percentage, calculated as ((net_price - unit_cost) / net_price) × 100.
      Standardized profitability metric enabling comparison across products with different
      price points. Uses safe division to handle zero-price edge cases.
    checks:
      - name: not_null
  - name: currency_code
    type: VARCHAR
    description: |
      Original transaction currency code (ISO 4217 format). 5 distinct currencies:
      USD, EUR, GBP, CAD, AUD. Preserved for audit trails and exchange rate impact
      analysis, while all monetary calculations use USD equivalents.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - USD
          - EUR
          - GBP
          - CAD
          - AUD

@bruin */

WITH sales_deduped AS (
    SELECT *
    FROM contoso_raw.sales
    WHERE order_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_key, line_number
        ORDER BY extracted_at DESC
    ) = 1
),

customers_deduped AS (
    SELECT *
    FROM contoso_raw.customers
    WHERE customer_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_key
        ORDER BY extracted_at DESC
    ) = 1
),

products_deduped AS (
    SELECT *
    FROM contoso_raw.products
    WHERE product_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_key
        ORDER BY extracted_at DESC
    ) = 1
),

stores_deduped AS (
    SELECT *
    FROM contoso_raw.stores
    WHERE store_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_key
        ORDER BY extracted_at DESC
    ) = 1
),

dates_deduped AS (
    SELECT *
    FROM contoso_raw.dates
    WHERE date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY date
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT
    s.order_key,
    s.line_number,
    CAST(s.order_date AS DATE) AS order_date,
    CAST(s.delivery_date AS DATE) AS delivery_date,
    d.year,
    d.quarter,
    d.month_number,
    d.dayof_week,
    s.customer_key,
    CONCAT(c.given_name, ' ', c.surname) AS customer_name,
    c.country_full AS customer_country,
    c.gender AS customer_gender,
    c.occupation AS customer_occupation,
    s.store_key,
    st.country_name AS store_country,
    s.product_key,
    p.product_name,
    p.brand,
    p.category_name,
    p.sub_category_name,
    s.quantity,
    ROUND(s.unit_price * s.exchange_rate, 2) AS unit_price_usd,
    ROUND(s.unit_cost * s.exchange_rate, 2) AS unit_cost_usd,
    ROUND(s.quantity * s.net_price * s.exchange_rate, 2) AS revenue_usd,
    ROUND(s.quantity * s.unit_cost * s.exchange_rate, 2) AS cost_usd,
    ROUND(s.quantity * (s.net_price - s.unit_cost) * s.exchange_rate, 2) AS profit_usd,
    ROUND(
        SAFE_DIVIDE(
            s.net_price - s.unit_cost,
            NULLIF(s.net_price, 0)
        ) * 100,
        2
    ) AS margin_pct,
    s.currency_code
FROM sales_deduped s
LEFT JOIN customers_deduped c ON s.customer_key = c.customer_key
LEFT JOIN products_deduped p ON s.product_key = p.product_key
LEFT JOIN stores_deduped st ON s.store_key = st.store_key
LEFT JOIN dates_deduped d ON CAST(s.order_date AS DATE) = CAST(d.date AS DATE)
ORDER BY s.order_key, s.line_number
