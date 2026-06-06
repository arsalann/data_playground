/* @bruin
name: bruin_shop_reports.rpt_daily_kpis
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Combines daily revenue, customer, session, and marketing spend signals into
  one executive KPI table.

depends:
  - bruin_shop_reports.rpt_daily_revenue
  - bruin_shop_staging.stg_customers
  - bruin_shop_staging.stg_orders
  - bruin_shop_staging.stg_web_sessions
  - bruin_shop_staging.stg_marketing_spend

materialization:
  type: table
  strategy: create+replace

columns:
  - name: kpi_date
    type: DATE
    description: KPI reporting date
    primary_key: true
    nullable: false
  - name: net_revenue
    type: DOUBLE
    description: Paid net revenue in USD
  - name: total_orders
    type: INTEGER
    description: Total order count
  - name: paid_orders
    type: INTEGER
    description: Paid order count
  - name: avg_order_value
    type: DOUBLE
    description: Average paid order value in USD
  - name: cancellation_rate
    type: DOUBLE
    description: Cancellation rate as a percentage
  - name: new_customers
    type: INTEGER
    description: Customers whose first seen date matches the order date
  - name: returning_customers
    type: INTEGER
    description: Customers with first seen date before the order date
  - name: sessions
    type: INTEGER
    description: Total web sessions
  - name: new_visitors
    type: INTEGER
    description: Total new web visitors
  - name: conversion_rate
    type: DOUBLE
    description: Purchase-event conversion rate as a percentage
  - name: total_ad_spend
    type: DOUBLE
    description: Total paid media spend in USD
  - name: overall_roas
    type: DOUBLE
    description: Net revenue divided by total paid media spend
  - name: gross_profit
    type: DOUBLE
    description: Net revenue minus COGS and shipping cost in USD
  - name: contribution_profit
    type: DOUBLE
    description: Gross profit minus paid media spend in USD
  - name: gross_margin_pct
    type: DOUBLE
    description: Gross profit divided by net revenue, expressed as a percentage

@bruin */

WITH daily_customers AS (
    SELECT
        DATE(o.order_date) AS order_date,
        COUNT(DISTINCT IF(DATE(c.first_seen_at) = DATE(o.order_date), o.customer_email, NULL)) AS new_customers,
        COUNT(DISTINCT IF(DATE(c.first_seen_at) < DATE(o.order_date), o.customer_email, NULL)) AS returning_customers
    FROM bruin_shop_staging.stg_orders o
    LEFT JOIN bruin_shop_staging.stg_customers c
        ON o.customer_email = c.customer_email
    WHERE o.payment_status IN ('paid', 'partially_refunded')
    GROUP BY DATE(o.order_date)
),
daily_sessions AS (
    SELECT
        session_date,
        SUM(total_sessions) AS sessions,
        SUM(new_users) AS new_visitors,
        SUM(purchase_events) AS purchases
    FROM bruin_shop_staging.stg_web_sessions
    GROUP BY session_date
),
daily_spend AS (
    SELECT
        spend_date,
        SUM(spend) AS total_ad_spend
    FROM bruin_shop_staging.stg_marketing_spend
    WHERE channel IN ('paid_ads', 'paid_search')
    GROUP BY spend_date
)

SELECT
    r.order_date AS kpi_date,
    r.net_revenue,
    r.total_orders,
    r.paid_orders,
    r.avg_order_value,
    r.cancellation_rate,
    COALESCE(dc.new_customers, 0) AS new_customers,
    COALESCE(dc.returning_customers, 0) AS returning_customers,
    COALESCE(ds.sessions, 0) AS sessions,
    COALESCE(ds.new_visitors, 0) AS new_visitors,
    ROUND(SAFE_DIVIDE(COALESCE(ds.purchases, 0), NULLIF(ds.sessions, 0)) * 100, 2) AS conversion_rate,
    COALESCE(sp.total_ad_spend, 0) AS total_ad_spend,
    ROUND(SAFE_DIVIDE(r.net_revenue, NULLIF(sp.total_ad_spend, 0)), 2) AS overall_roas,
    r.gross_profit,
    r.gross_profit - COALESCE(sp.total_ad_spend, 0) AS contribution_profit,
    r.gross_margin_pct
FROM bruin_shop_reports.rpt_daily_revenue r
LEFT JOIN daily_customers dc
    ON r.order_date = dc.order_date
LEFT JOIN daily_sessions ds
    ON r.order_date = ds.session_date
LEFT JOIN daily_spend sp
    ON r.order_date = sp.spend_date
ORDER BY kpi_date DESC
