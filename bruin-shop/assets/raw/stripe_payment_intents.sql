/* @bruin
name: raw.stripe_payment_intents
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake Stripe payment intents generated from Shopify-style order
  facts. The table lets payment status, amount, customer email, channel,
  geography, and special-event metadata reconcile with Shopify orders without
  relying on external Stripe connector-test data.

depends:
  - raw.shopify_orders

materialization:
  type: table
  strategy: create+replace

columns:
  - name: id
    type: VARCHAR
    description: Synthetic Stripe payment-intent identifier.
    primary_key: true
    nullable: false
  - name: shopify_order_id
    type: VARCHAR
    description: Shopify-style order identifier tied to this payment intent.
  - name: shopify_order_number
    type: INTEGER
    description: Shopify-style order number tied to this payment intent.
  - name: created
    type: TIMESTAMP
    description: Payment intent creation timestamp.
  - name: amount
    type: INTEGER
    description: Intended payment amount in cents, including generated tax.
  - name: amount_received
    type: INTEGER
    description: Received amount in cents. Zero for canceled intents.
  - name: currency
    type: VARCHAR
    description: Three-letter ISO currency code.
  - name: status
    type: VARCHAR
    description: Stripe-style payment-intent status.
  - name: payment_method_type
    type: VARCHAR
    description: Synthetic payment method type.
  - name: customer_email
    type: VARCHAR
    description: Customer email address from the Shopify-style order.
  - name: source_channel
    type: VARCHAR
    description: Exact normalized acquisition channel from the generated funnel.
  - name: city
    type: VARCHAR
    description: Order city.
  - name: state_code
    type: VARCHAR
    description: Two-letter US state or district postal abbreviation.
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
  - name: stripe_fee_amount
    type: INTEGER
    description: Approximate Stripe processing fee in cents for succeeded intents.

@bruin */

SELECT
    FORMAT('pi_%s', SUBSTR(TO_HEX(SHA256(id)), 1, 24)) AS id,
    id AS shopify_order_id,
    order_number AS shopify_order_number,
    created_at AS created,
    CAST(ROUND((total_price + total_tax) * 100) AS INT64) AS amount,
    CASE
        WHEN financial_status IN ('paid', 'partially_refunded')
            THEN CAST(ROUND((total_price + total_tax) * 100) AS INT64)
        ELSE 0
    END AS amount_received,
    LOWER(currency) AS currency,
    CASE
        WHEN financial_status IN ('paid', 'partially_refunded') THEN 'succeeded'
        WHEN financial_status = 'pending' THEN 'processing'
        WHEN financial_status = 'voided' THEN 'canceled'
        ELSE 'requires_payment_method'
    END AS status,
    CASE
        WHEN ABS(MOD(FARM_FINGERPRINT(CONCAT(id, '|payment_method')), 1000)) < 560 THEN 'card'
        WHEN ABS(MOD(FARM_FINGERPRINT(CONCAT(id, '|payment_method')), 1000)) < 780 THEN 'link'
        WHEN ABS(MOD(FARM_FINGERPRINT(CONCAT(id, '|payment_method')), 1000)) < 930 THEN 'shopify_pay'
        ELSE 'paypal'
    END AS payment_method_type,
    email AS customer_email,
    source_channel,
    city,
    state_code,
    special_event_id,
    special_event_type,
    special_event_name,
    event_phase,
    CASE
        WHEN financial_status IN ('paid', 'partially_refunded')
            THEN CAST(ROUND((total_price + total_tax) * 100 * 0.029 + 30) AS INT64)
        ELSE 0
    END AS stripe_fee_amount
FROM raw.shopify_orders
ORDER BY created, shopify_order_id
