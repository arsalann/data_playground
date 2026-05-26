/* @bruin
name: self_heal_test_staging.daily_orders
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One row per order_date with counts and revenue. This is the metric surface
  used by anomaly-investigate — the 2026-05-20 spike from country=TR will
  show up here as a daily-total anomaly.

depends:
  - self_heal_test_raw.orders

materialization:
  type: table
  strategy: create+replace

columns:
  - name: order_date
    type: DATE
    primary_key: true
    nullable: false
  - name: order_count
    type: INTEGER
    nullable: false
  - name: distinct_users
    type: INTEGER
    nullable: false
  - name: distinct_countries
    type: INTEGER
    nullable: false
  - name: revenue_usd
    type: DOUBLE
    nullable: false

@bruin */

SELECT
    order_date,
    COUNT(*) AS order_count,
    COUNT(DISTINCT user_id) AS distinct_users,
    COUNT(DISTINCT country) AS distinct_countries,
    ROUND(SUM(amount_usd), 2) AS revenue_usd
FROM self_heal_test_raw.orders
GROUP BY 1
ORDER BY 1 DESC
