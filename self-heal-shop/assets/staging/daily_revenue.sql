/* @bruin
name: self_heal_test_staging.daily_revenue
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates deduplicated orders to daily revenue by country and product
  category. Handles the product category source column whether it arrives as
  `category` or `product_category`.

depends:
  - self_heal_test_raw.orders
  - self_heal_test_raw.products

materialization:
  type: table
  strategy: create+replace

columns:
  - name: order_date
    type: DATE
    primary_key: true
    nullable: false
  - name: country
    type: VARCHAR
    primary_key: true
    nullable: false
  - name: category
    type: VARCHAR
    primary_key: true
    nullable: false
  - name: order_count
    type: INTEGER
    checks:
      - name: positive
  - name: distinct_users
    type: INTEGER
    checks:
      - name: positive
  - name: revenue_usd
    type: DOUBLE
    checks:
      - name: positive

custom_checks:
  - name: no_partition_has_zero_revenue
    description: |
      Every order_date in the table should have at least one row with revenue > 0.
      A date with all-zero revenue points to a freshness or ingest problem.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT order_date, SUM(revenue_usd) AS total
        FROM self_heal_test_staging.daily_revenue
        GROUP BY 1
        HAVING SUM(revenue_usd) = 0
      )
    value: 0

@bruin */

WITH deduped_orders AS (
    SELECT *
    FROM self_heal_test_raw.orders
    WHERE order_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY created_at DESC, user_id DESC, product_id DESC, country DESC, amount_usd DESC
    ) = 1
),

products AS (
    SELECT
        product_id,
        COALESCE(
            JSON_VALUE(TO_JSON_STRING(product_rows), '$.product_category'),
            JSON_VALUE(TO_JSON_STRING(product_rows), '$.category')
        ) AS category
    FROM self_heal_test_raw.products AS product_rows
    WHERE product_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT
    deduped_orders.order_date,
    deduped_orders.country,
    COALESCE(products.category, 'unknown') AS category,
    COUNT(*) AS order_count,
    COUNT(DISTINCT deduped_orders.user_id) AS distinct_users,
    ROUND(SUM(deduped_orders.amount_usd), 2) AS revenue_usd
FROM deduped_orders
LEFT JOIN products ON deduped_orders.product_id = products.product_id
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 6 DESC
