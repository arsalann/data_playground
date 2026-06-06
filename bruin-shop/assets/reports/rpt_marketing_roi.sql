/* @bruin
name: reports.rpt_marketing_roi
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Calculates daily channel ROI by combining marketing spend, web sessions, and
  exact order revenue/costs by acquisition channel. Synthetic orders are
  generated one-for-one from funnel conversions, so paid-media CPA, ROAS, and
  profit metrics are internally reconciled.

depends:
  - staging.stg_marketing_spend
  - staging.stg_web_sessions
  - staging.stg_orders

materialization:
  type: table
  strategy: create+replace

columns:
  - name: report_date
    type: DATE
    description: Channel ROI reporting date
  - name: channel
    type: VARCHAR
    description: Normalized marketing or traffic channel
    nullable: false
  - name: total_spend
    type: DOUBLE
    description: Channel spend in USD
  - name: total_impressions
    type: INTEGER
    description: Channel impressions or email recipients
  - name: total_clicks
    type: INTEGER
    description: Channel clicks
  - name: total_conversions
    type: INTEGER
    description: Platform-reported conversions
  - name: total_orders
    type: INTEGER
    description: Paid or partially refunded orders attributed to the channel
  - name: sessions
    type: INTEGER
    description: Channel sessions
  - name: new_users
    type: INTEGER
    description: Channel new users
  - name: attributed_revenue
    type: DOUBLE
    description: Paid order revenue attributed to the channel in USD
  - name: total_cogs
    type: DOUBLE
    description: Attributed order cost of goods sold in USD
  - name: total_shipping_cost
    type: DOUBLE
    description: Attributed fulfillment shipping cost in USD
  - name: gross_profit
    type: DOUBLE
    description: Attributed revenue minus COGS and shipping cost in USD
  - name: contribution_profit
    type: DOUBLE
    description: Gross profit minus channel spend in USD
  - name: roas
    type: DOUBLE
    description: Attributed revenue divided by spend
  - name: cost_per_acquisition
    type: DOUBLE
    description: Spend divided by platform-reported conversions in USD
  - name: click_through_rate
    type: DOUBLE
    description: Clicks divided by impressions, expressed as a percentage
  - name: conversion_rate
    type: DOUBLE
    description: Purchase events divided by sessions, expressed as a percentage

@bruin */

WITH channel_spend AS (
    SELECT
        spend_date,
        channel,
        SUM(spend) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(conversions) AS total_conversions
    FROM staging.stg_marketing_spend
    GROUP BY spend_date, channel
),
channel_sessions AS (
    SELECT
        session_date,
        channel,
        SUM(total_sessions) AS sessions,
        SUM(new_users) AS new_users,
        SUM(purchase_events) AS purchases
    FROM staging.stg_web_sessions
    GROUP BY session_date, channel
),
channel_orders AS (
    SELECT
        DATE(order_date) AS order_date,
        source_channel AS channel,
        COUNT(*) AS total_orders,
        SUM(order_total) AS attributed_revenue,
        SUM(COALESCE(cogs_amount, 0)) AS total_cogs,
        SUM(COALESCE(shipping_cost, 0)) AS total_shipping_cost,
        SUM(order_total - COALESCE(cogs_amount, 0) - COALESCE(shipping_cost, 0)) AS gross_profit
    FROM staging.stg_orders
    WHERE payment_status IN ('paid', 'partially_refunded')
    GROUP BY order_date, channel
),
channel_keys AS (
    SELECT spend_date AS report_date, channel FROM channel_spend
    UNION DISTINCT
    SELECT session_date AS report_date, channel FROM channel_sessions
    UNION DISTINCT
    SELECT order_date AS report_date, channel FROM channel_orders
)

SELECT
    ck.report_date,
    ck.channel,
    COALESCE(cs.total_spend, 0) AS total_spend,
    COALESCE(cs.total_impressions, 0) AS total_impressions,
    COALESCE(cs.total_clicks, 0) AS total_clicks,
    COALESCE(cs.total_conversions, sess.purchases, co.total_orders, 0) AS total_conversions,
    COALESCE(co.total_orders, 0) AS total_orders,
    COALESCE(sess.sessions, 0) AS sessions,
    COALESCE(sess.new_users, 0) AS new_users,
    COALESCE(co.attributed_revenue, 0) AS attributed_revenue,
    COALESCE(co.total_cogs, 0) AS total_cogs,
    COALESCE(co.total_shipping_cost, 0) AS total_shipping_cost,
    COALESCE(co.gross_profit, 0) AS gross_profit,
    COALESCE(co.gross_profit, 0) - COALESCE(cs.total_spend, 0) AS contribution_profit,
    ROUND(SAFE_DIVIDE(co.attributed_revenue, NULLIF(cs.total_spend, 0)), 2) AS roas,
    ROUND(SAFE_DIVIDE(cs.total_spend, NULLIF(COALESCE(cs.total_conversions, co.total_orders), 0)), 2) AS cost_per_acquisition,
    ROUND(SAFE_DIVIDE(cs.total_clicks, NULLIF(cs.total_impressions, 0)) * 100, 2) AS click_through_rate,
    ROUND(SAFE_DIVIDE(COALESCE(sess.purchases, co.total_orders), NULLIF(sess.sessions, 0)) * 100, 2) AS conversion_rate
FROM channel_keys ck
LEFT JOIN channel_spend cs
    ON ck.report_date = cs.spend_date
    AND ck.channel = cs.channel
LEFT JOIN channel_sessions sess
    ON ck.report_date = sess.session_date
    AND ck.channel = sess.channel
LEFT JOIN channel_orders co
    ON ck.report_date = co.order_date
    AND ck.channel = co.channel
ORDER BY report_date DESC, total_spend DESC
