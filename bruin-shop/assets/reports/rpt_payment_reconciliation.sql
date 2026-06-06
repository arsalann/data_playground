/* @bruin
name: bruin_shop_reports.rpt_payment_reconciliation
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Reconciles daily Shopify-style order payment statuses with synthetic Stripe
  payment intents and refunds. The report is used to verify that successful
  Shopify orders have succeeded Stripe intents, pending/voided orders do not
  create received Stripe amounts, and partially refunded orders have refund
  records.

depends:
  - bruin_shop_staging.stg_orders
  - bruin_shop_raw.stripe_payment_intents
  - bruin_shop_raw.stripe_refunds

materialization:
  type: table
  strategy: create+replace

columns:
  - name: report_date
    type: DATE
    description: Payment reconciliation date.
    primary_key: true
    nullable: false
  - name: shopify_order_attempts
    type: INTEGER
    description: Total Shopify-style order attempts.
  - name: shopify_successful_orders
    type: INTEGER
    description: Shopify-style orders with paid or partially refunded status.
  - name: shopify_partially_refunded_orders
    type: INTEGER
    description: Shopify-style partially refunded orders.
  - name: stripe_payment_intents
    type: INTEGER
    description: Stripe payment-intent count.
  - name: stripe_succeeded_intents
    type: INTEGER
    description: Stripe payment intents with succeeded status.
  - name: stripe_processing_intents
    type: INTEGER
    description: Stripe payment intents with processing status.
  - name: stripe_canceled_intents
    type: INTEGER
    description: Stripe payment intents with canceled status.
  - name: stripe_refunds
    type: INTEGER
    description: Stripe refund record count.
  - name: stripe_gross_amount_usd
    type: DOUBLE
    description: Stripe received amount in USD before refunds.
  - name: stripe_refund_amount_usd
    type: DOUBLE
    description: Stripe refund amount in USD.
  - name: successful_order_gap
    type: INTEGER
    description: Shopify successful orders minus Stripe succeeded intents.
  - name: refund_record_gap
    type: INTEGER
    description: Shopify partially refunded orders minus Stripe refund records.

@bruin */

WITH shopify AS (
    SELECT
        DATE(order_date) AS report_date,
        COUNT(*) AS shopify_order_attempts,
        COUNTIF(payment_status IN ('paid', 'partially_refunded')) AS shopify_successful_orders,
        COUNTIF(payment_status = 'partially_refunded') AS shopify_partially_refunded_orders
    FROM bruin_shop_staging.stg_orders
    GROUP BY report_date
),
stripe AS (
    SELECT
        DATE(created) AS report_date,
        COUNT(*) AS stripe_payment_intents,
        COUNTIF(status = 'succeeded') AS stripe_succeeded_intents,
        COUNTIF(status = 'processing') AS stripe_processing_intents,
        COUNTIF(status = 'canceled') AS stripe_canceled_intents,
        ROUND(SUM(amount_received) / 100.0, 2) AS stripe_gross_amount_usd
    FROM bruin_shop_raw.stripe_payment_intents
    GROUP BY report_date
),
refunds AS (
    SELECT
        DATE(o.order_date) AS report_date,
        COUNT(*) AS stripe_refunds,
        ROUND(SUM(r.amount) / 100.0, 2) AS stripe_refund_amount_usd
    FROM bruin_shop_raw.stripe_refunds r
    INNER JOIN bruin_shop_staging.stg_orders o
        ON r.shopify_order_id = o.order_id
    GROUP BY report_date
)

SELECT
    s.report_date,
    s.shopify_order_attempts,
    s.shopify_successful_orders,
    s.shopify_partially_refunded_orders,
    COALESCE(p.stripe_payment_intents, 0) AS stripe_payment_intents,
    COALESCE(p.stripe_succeeded_intents, 0) AS stripe_succeeded_intents,
    COALESCE(p.stripe_processing_intents, 0) AS stripe_processing_intents,
    COALESCE(p.stripe_canceled_intents, 0) AS stripe_canceled_intents,
    COALESCE(r.stripe_refunds, 0) AS stripe_refunds,
    COALESCE(p.stripe_gross_amount_usd, 0) AS stripe_gross_amount_usd,
    COALESCE(r.stripe_refund_amount_usd, 0) AS stripe_refund_amount_usd,
    s.shopify_successful_orders - COALESCE(p.stripe_succeeded_intents, 0) AS successful_order_gap,
    s.shopify_partially_refunded_orders - COALESCE(r.stripe_refunds, 0) AS refund_record_gap
FROM shopify s
LEFT JOIN stripe p
    ON s.report_date = p.report_date
LEFT JOIN refunds r
    ON s.report_date = r.report_date
ORDER BY report_date DESC
