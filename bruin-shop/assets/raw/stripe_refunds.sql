/* @bruin
name: bruin_shop_raw.stripe_refunds
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake Stripe refunds generated from partially refunded
  Shopify-style orders and linked back to synthetic Stripe payment intents.
  Product-defect orders receive high refund amounts so the Stripe refund layer
  reinforces the same incident visible in Shopify operations.

depends:
  - bruin_shop_raw.shopify_orders
  - bruin_shop_raw.stripe_payment_intents

materialization:
  type: table
  strategy: create+replace

columns:
  - name: id
    type: VARCHAR
    description: Synthetic Stripe refund identifier.
    primary_key: true
    nullable: false
  - name: payment_intent_id
    type: VARCHAR
    description: Synthetic Stripe payment intent receiving the refund.
  - name: shopify_order_id
    type: VARCHAR
    description: Shopify-style order identifier tied to the refund.
  - name: created
    type: TIMESTAMP
    description: Refund creation timestamp.
  - name: amount
    type: INTEGER
    description: Refund amount in cents.
  - name: currency
    type: VARCHAR
    description: Three-letter ISO currency code.
  - name: status
    type: VARCHAR
    description: Stripe-style refund status.
  - name: reason
    type: VARCHAR
    description: Synthetic refund reason.
  - name: source_channel
    type: VARCHAR
    description: Exact normalized acquisition channel from the generated funnel.
  - name: special_event_id
    type: VARCHAR
    description: Synthetic event identifier inherited from the order.
  - name: special_event_type
    type: VARCHAR
    description: Synthetic event type inherited from the order.
  - name: special_event_name
    type: VARCHAR
    description: Human-readable synthetic event name inherited from the order.
  - name: event_phase
    type: VARCHAR
    description: Event phase inherited from the order.

@bruin */

SELECT
    FORMAT('re_%s', SUBSTR(TO_HEX(SHA256(o.id)), 1, 24)) AS id,
    pi.id AS payment_intent_id,
    o.id AS shopify_order_id,
    TIMESTAMP_ADD(o.updated_at, INTERVAL 3 HOUR) AS created,
    CAST(ROUND(
        (o.total_price + o.total_tax)
        * CASE
            WHEN o.special_event_id = 'black_tote_defect_refunds' THEN 0.92
            ELSE 0.34 + ABS(MOD(FARM_FINGERPRINT(CONCAT(o.id, '|refund_pct')), 31)) / 100.0
        END
        * 100
    ) AS INT64) AS amount,
    LOWER(o.currency) AS currency,
    'succeeded' AS status,
    CASE
        WHEN o.special_event_id = 'black_tote_defect_refunds' THEN 'product_defect'
        WHEN ABS(MOD(FARM_FINGERPRINT(CONCAT(o.id, '|refund_reason')), 1000)) < 420 THEN 'requested_by_customer'
        WHEN ABS(MOD(FARM_FINGERPRINT(CONCAT(o.id, '|refund_reason')), 1000)) < 760 THEN 'duplicate'
        ELSE 'fraudulent'
    END AS reason,
    o.source_channel,
    o.special_event_id,
    o.special_event_type,
    o.special_event_name,
    o.event_phase
FROM bruin_shop_raw.shopify_orders o
INNER JOIN bruin_shop_raw.stripe_payment_intents pi
    ON o.id = pi.shopify_order_id
WHERE o.financial_status = 'partially_refunded'
ORDER BY created, shopify_order_id
