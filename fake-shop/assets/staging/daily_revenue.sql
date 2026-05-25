/* @bruin
name: staging.daily_revenue
type: duckdb.sql
connection: duckdb-default
description: |
  Aggregates orders to daily revenue by country and product category.
  This is the asset most affected by the injected schema drift on
  raw.products — it joins on `category`, which is renamed after 2026-04-01.

depends:
  - raw.orders
  - raw.products

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
    type: BIGINT
    checks:
      - name: positive
  - name: distinct_users
    type: BIGINT
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
        FROM staging.daily_revenue
        GROUP BY 1
        HAVING SUM(revenue_usd) = 0
      )
    value: 0

@bruin */

SELECT
    o.order_date,
    o.country,
    p.category AS category,
    COUNT(*) AS order_count,
    COUNT(DISTINCT o.user_id) AS distinct_users,
    ROUND(SUM(o.amount_usd), 2) AS revenue_usd
FROM raw.orders o
LEFT JOIN raw.products p ON o.product_id = p.product_id
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 6 DESC
