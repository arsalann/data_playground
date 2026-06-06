/* @bruin
name: reports.rpt_daily_revenue
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates standardized Shopify orders into daily revenue, order-count, AOV,
  discount, tax, item-count, COGS, shipping, gross-profit, and cancellation
  metrics.

depends:
  - staging.stg_orders

materialization:
  type: table
  strategy: create+replace

columns:
  - name: order_date
    type: DATE
    description: Revenue reporting date
    primary_key: true
    nullable: false
  - name: total_orders
    type: INTEGER
    description: Total non-test orders
  - name: paid_orders
    type: INTEGER
    description: Orders with paid or partially refunded payment status
  - name: cancelled_orders
    type: INTEGER
    description: Orders with a cancellation reason
  - name: gross_revenue
    type: DOUBLE
    description: Gross order revenue in USD
  - name: net_revenue
    type: DOUBLE
    description: Paid net order revenue in USD
  - name: total_discounts
    type: DOUBLE
    description: Total discounts in USD
  - name: total_tax
    type: DOUBLE
    description: Total tax in USD
  - name: total_items
    type: INTEGER
    description: Total apparel items sold
  - name: total_cogs
    type: DOUBLE
    description: Total cost of goods sold in USD
  - name: total_shipping_cost
    type: DOUBLE
    description: Total fulfillment shipping cost in USD
  - name: gross_profit
    type: DOUBLE
    description: Net revenue minus COGS and shipping cost in USD
  - name: gross_margin_pct
    type: DOUBLE
    description: Gross profit divided by net revenue, expressed as a percentage
  - name: avg_order_value
    type: DOUBLE
    description: Average paid order value in USD
  - name: cancellation_rate
    type: DOUBLE
    description: Share of orders cancelled, expressed as a percentage

@bruin */

SELECT
    DATE(order_date) AS order_date,
    COUNT(*) AS total_orders,
    COUNTIF(payment_status IN ('paid', 'partially_refunded')) AS paid_orders,
    COUNTIF(cancel_reason IS NOT NULL) AS cancelled_orders,
    SUM(order_total) AS gross_revenue,
    SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN order_total ELSE 0 END) AS net_revenue,
    SUM(COALESCE(discount_amount, 0)) AS total_discounts,
    SUM(COALESCE(tax_amount, 0)) AS total_tax,
    SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN COALESCE(item_count, 0) ELSE 0 END) AS total_items,
    SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN COALESCE(cogs_amount, 0) ELSE 0 END) AS total_cogs,
    SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN COALESCE(shipping_cost, 0) ELSE 0 END) AS total_shipping_cost,
    SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN order_total - COALESCE(cogs_amount, 0) - COALESCE(shipping_cost, 0) ELSE 0 END) AS gross_profit,
    ROUND(SAFE_DIVIDE(
        SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN order_total - COALESCE(cogs_amount, 0) - COALESCE(shipping_cost, 0) ELSE 0 END),
        NULLIF(SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN order_total ELSE 0 END), 0)
    ) * 100, 2) AS gross_margin_pct,
    ROUND(SAFE_DIVIDE(SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN order_total ELSE 0 END), COUNTIF(payment_status IN ('paid', 'partially_refunded'))), 2) AS avg_order_value,
    ROUND(SAFE_DIVIDE(COUNTIF(cancel_reason IS NOT NULL), COUNT(*)) * 100, 2) AS cancellation_rate
FROM staging.stg_orders
GROUP BY DATE(order_date)
ORDER BY order_date
