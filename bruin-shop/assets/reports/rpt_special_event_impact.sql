/* @bruin
name: bruin_shop_reports.rpt_special_event_impact
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Summarizes the synthetic special events injected into the Bruin Shop fake
  ecommerce data. Metrics combine event metadata, marketing spend, GA4 sessions,
  Shopify-style order outcomes, product context, and a simple immediately prior
  baseline window with the same number of days as the event.

depends:
  - bruin_shop_raw.special_events
  - bruin_shop_staging.stg_marketing_spend
  - bruin_shop_staging.stg_web_sessions
  - bruin_shop_staging.stg_orders
  - bruin_shop_staging.stg_products
  - bruin_shop_reports.rpt_daily_kpis

materialization:
  type: table
  strategy: create+replace

columns:
  - name: event_id
    type: VARCHAR
    description: Stable synthetic event identifier.
    primary_key: true
    nullable: false
  - name: event_name
    type: VARCHAR
    description: Human-readable event name.
  - name: event_type
    type: VARCHAR
    description: Event class used for dashboard filtering.
  - name: event_start_date
    type: DATE
    description: First calendar date affected by the event.
  - name: event_end_date
    type: DATE
    description: Last calendar date affected by the event.
  - name: primary_channel
    type: VARCHAR
    description: Primary affected acquisition channel or all_channels.
  - name: affected_product_id
    type: VARCHAR
    description: Featured or defective product identifier when applicable.
  - name: affected_product_name
    type: VARCHAR
    description: Product display name when the event is product-specific.
  - name: event_days
    type: INTEGER
    description: Number of calendar days in the event window.
  - name: total_spend
    type: DOUBLE
    description: Marketing spend during the event window in USD.
  - name: impressions
    type: INTEGER
    description: Marketing impressions during the event window.
  - name: clicks
    type: INTEGER
    description: Marketing clicks during the event window.
  - name: platform_conversions
    type: INTEGER
    description: Platform-reported conversions during the event window.
  - name: sessions
    type: INTEGER
    description: Web sessions during the event window.
  - name: purchase_events
    type: INTEGER
    description: GA4-style purchase events during the event window.
  - name: orders
    type: INTEGER
    description: Shopify-style orders during the event window.
  - name: paid_orders
    type: INTEGER
    description: Paid or partially refunded orders during the event window.
  - name: refunded_orders
    type: INTEGER
    description: Partially refunded orders during the event window.
  - name: attributed_revenue
    type: DOUBLE
    description: Event-attributed order revenue in USD.
  - name: gross_profit
    type: DOUBLE
    description: Event-attributed gross profit after COGS and shipping in USD.
  - name: contribution_profit
    type: DOUBLE
    description: Gross profit minus event-window marketing spend in USD.
  - name: ctr_pct
    type: DOUBLE
    description: Click-through rate during the event window, expressed as a percentage.
  - name: conversion_rate_pct
    type: DOUBLE
    description: Purchase events divided by sessions during the event window, expressed as a percentage.
  - name: refund_rate_pct
    type: DOUBLE
    description: Partially refunded orders divided by orders during the event window, expressed as a percentage.
  - name: baseline_net_revenue
    type: DOUBLE
    description: Net revenue in the immediately prior same-length baseline window in USD.
  - name: revenue_delta_vs_baseline
    type: DOUBLE
    description: Event-window revenue minus baseline-window net revenue in USD.
  - name: sessions_delta_vs_baseline
    type: INTEGER
    description: Event-window sessions minus baseline-window sessions.
  - name: expected_effect
    type: VARCHAR
    description: Narrative summary of the injected event behavior.

@bruin */

WITH events AS (
    SELECT
        e.*,
        DATE_DIFF(e.event_end_date, e.event_start_date, DAY) + 1 AS event_days,
        p.product_name AS affected_product_name
    FROM bruin_shop_raw.special_events e
    LEFT JOIN bruin_shop_staging.stg_products p
        ON e.affected_product_id = p.product_id
),
marketing AS (
    SELECT
        e.event_id,
        SUM(ms.spend) AS total_spend,
        SUM(ms.impressions) AS impressions,
        SUM(ms.clicks) AS clicks,
        SUM(ms.conversions) AS platform_conversions
    FROM events e
    LEFT JOIN bruin_shop_staging.stg_marketing_spend ms
        ON ms.spend_date BETWEEN e.event_start_date AND e.event_end_date
        AND e.event_type != 'product_defect'
        AND (e.primary_channel = 'all_channels' OR ms.channel = e.primary_channel)
        AND (
            e.event_type NOT IN ('failed_campaign', 'stockout_campaign', 'successful_campaign')
            OR ms.special_event_id = e.event_id
        )
    GROUP BY e.event_id
),
sessions AS (
    SELECT
        e.event_id,
        SUM(ws.total_sessions) AS sessions,
        SUM(ws.purchase_events) AS purchase_events
    FROM events e
    LEFT JOIN bruin_shop_staging.stg_web_sessions ws
        ON ws.session_date BETWEEN e.event_start_date AND e.event_end_date
        AND (e.primary_channel = 'all_channels' OR ws.channel = e.primary_channel)
        AND (
            e.event_type NOT IN ('failed_campaign', 'stockout_campaign', 'successful_campaign')
            OR ws.special_event_id = e.event_id
        )
    GROUP BY e.event_id
),
orders AS (
    SELECT
        e.event_id,
        COUNT(o.order_id) AS orders,
        COUNTIF(o.payment_status IN ('paid', 'partially_refunded')) AS paid_orders,
        COUNTIF(o.payment_status = 'partially_refunded') AS refunded_orders,
        SUM(CASE WHEN o.payment_status IN ('paid', 'partially_refunded') THEN o.order_total ELSE 0 END) AS attributed_revenue,
        SUM(CASE
            WHEN o.payment_status IN ('paid', 'partially_refunded')
                THEN o.order_total - COALESCE(o.cogs_amount, 0) - COALESCE(o.shipping_cost, 0)
            ELSE 0
        END) AS gross_profit
    FROM events e
    LEFT JOIN bruin_shop_staging.stg_orders o
        ON DATE(o.order_date) BETWEEN e.event_start_date AND e.event_end_date
        AND (e.primary_channel = 'all_channels' OR o.source_channel = e.primary_channel)
        AND (e.affected_product_id IS NULL OR o.primary_product_id = e.affected_product_id)
        AND (
            e.event_type NOT IN ('failed_campaign', 'stockout_campaign', 'successful_campaign')
            OR o.special_event_id = e.event_id
        )
    GROUP BY e.event_id
),
baseline AS (
    SELECT
        e.event_id,
        SUM(k.net_revenue) AS baseline_net_revenue,
        SUM(k.sessions) AS baseline_sessions
    FROM events e
    LEFT JOIN bruin_shop_reports.rpt_daily_kpis k
        ON k.kpi_date BETWEEN DATE_SUB(e.event_start_date, INTERVAL e.event_days DAY)
            AND DATE_SUB(e.event_start_date, INTERVAL 1 DAY)
    GROUP BY e.event_id
)

SELECT
    e.event_id,
    e.event_name,
    e.event_type,
    e.event_start_date,
    e.event_end_date,
    e.primary_channel,
    e.affected_product_id,
    e.affected_product_name,
    e.event_days,
    ROUND(COALESCE(m.total_spend, 0), 2) AS total_spend,
    COALESCE(m.impressions, 0) AS impressions,
    COALESCE(m.clicks, 0) AS clicks,
    COALESCE(m.platform_conversions, 0) AS platform_conversions,
    COALESCE(s.sessions, 0) AS sessions,
    COALESCE(s.purchase_events, 0) AS purchase_events,
    COALESCE(o.orders, 0) AS orders,
    COALESCE(o.paid_orders, 0) AS paid_orders,
    COALESCE(o.refunded_orders, 0) AS refunded_orders,
    ROUND(COALESCE(o.attributed_revenue, 0), 2) AS attributed_revenue,
    ROUND(COALESCE(o.gross_profit, 0), 2) AS gross_profit,
    ROUND(COALESCE(o.gross_profit, 0) - COALESCE(m.total_spend, 0), 2) AS contribution_profit,
    ROUND(SAFE_DIVIDE(m.clicks, NULLIF(m.impressions, 0)) * 100, 2) AS ctr_pct,
    ROUND(SAFE_DIVIDE(s.purchase_events, NULLIF(s.sessions, 0)) * 100, 2) AS conversion_rate_pct,
    ROUND(SAFE_DIVIDE(o.refunded_orders, NULLIF(o.orders, 0)) * 100, 2) AS refund_rate_pct,
    ROUND(COALESCE(b.baseline_net_revenue, 0), 2) AS baseline_net_revenue,
    ROUND(COALESCE(o.attributed_revenue, 0) - COALESCE(b.baseline_net_revenue, 0), 2) AS revenue_delta_vs_baseline,
    CAST(COALESCE(s.sessions, 0) - COALESCE(b.baseline_sessions, 0) AS INT64) AS sessions_delta_vs_baseline,
    e.expected_effect
FROM events e
LEFT JOIN marketing m
    USING (event_id)
LEFT JOIN sessions s
    USING (event_id)
LEFT JOIN orders o
    USING (event_id)
LEFT JOIN baseline b
    USING (event_id)
ORDER BY event_start_date
