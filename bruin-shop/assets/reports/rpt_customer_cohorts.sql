/* @bruin
name: reports.rpt_customer_cohorts
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Calculates simple customer cohort retention and revenue metrics from
  standardized customers and orders.

depends:
  - staging.stg_orders
  - staging.stg_customers

materialization:
  type: table
  strategy: create+replace

columns:
  - name: cohort_month
    type: DATE
    description: Month when customers were first seen
    nullable: false
  - name: cohort_size
    type: INTEGER
    description: Number of customers in the cohort
  - name: months_since_first
    type: INTEGER
    description: Months from first-seen month to order month
  - name: active_customers
    type: INTEGER
    description: Distinct customers active in the cohort month offset
  - name: retention_rate
    type: DOUBLE
    description: Active customers divided by cohort size, as a percentage
  - name: cohort_revenue
    type: DOUBLE
    description: Cohort revenue in USD
  - name: revenue_per_customer
    type: DOUBLE
    description: Cohort revenue divided by cohort size in USD

@bruin */

WITH customer_orders AS (
    SELECT
        o.customer_email,
        DATE_TRUNC(DATE(c.first_seen_at), MONTH) AS cohort_month,
        DATE_TRUNC(DATE(o.order_date), MONTH) AS order_month,
        o.order_total
    FROM staging.stg_orders o
    INNER JOIN staging.stg_customers c
        ON o.customer_email = c.customer_email
    WHERE o.payment_status IN ('paid', 'partially_refunded')
),
cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_email) AS cohort_size
    FROM customer_orders
    GROUP BY cohort_month
)

SELECT
    co.cohort_month,
    cs.cohort_size,
    DATE_DIFF(co.order_month, co.cohort_month, MONTH) AS months_since_first,
    COUNT(DISTINCT co.customer_email) AS active_customers,
    ROUND(SAFE_DIVIDE(COUNT(DISTINCT co.customer_email), cs.cohort_size) * 100, 2) AS retention_rate,
    SUM(co.order_total) AS cohort_revenue,
    ROUND(SAFE_DIVIDE(SUM(co.order_total), cs.cohort_size), 2) AS revenue_per_customer
FROM customer_orders co
INNER JOIN cohort_sizes cs
    ON co.cohort_month = cs.cohort_month
GROUP BY co.cohort_month, cs.cohort_size, months_since_first
ORDER BY co.cohort_month, months_since_first
