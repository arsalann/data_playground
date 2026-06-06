/* @bruin
name: bruin_shop_staging.stg_orders
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deduplicates fake Shopify orders and standardizes payment, fulfillment, and
  revenue, geography, channel-attribution, item-count, COGS, and shipping fields
  for reporting.

depends:
  - bruin_shop_raw.shopify_orders

materialization:
  type: table
  strategy: create+replace

columns:
  - name: order_id
    type: VARCHAR
    description: Shopify order identifier
    primary_key: true
    nullable: false
  - name: order_number
    type: INTEGER
    description: Human-readable Shopify order number
  - name: customer_email
    type: VARCHAR
    description: Customer email address on the order
  - name: order_date
    type: TIMESTAMP
    description: Order creation timestamp in UTC
    nullable: false
  - name: payment_status
    type: VARCHAR
    description: Shopify financial status
  - name: fulfillment_status
    type: VARCHAR
    description: Shopify fulfillment status
  - name: order_total
    type: DOUBLE
    description: Order total in USD
  - name: subtotal
    type: DOUBLE
    description: Order subtotal before tax in USD
  - name: tax_amount
    type: DOUBLE
    description: Tax amount in USD
  - name: discount_amount
    type: DOUBLE
    description: Discount amount in USD
  - name: currency
    type: VARCHAR
    description: ISO 4217 currency code
  - name: cancel_reason
    type: VARCHAR
    description: Cancellation reason when applicable
  - name: cancelled_at
    type: TIMESTAMP
    description: Cancellation timestamp in UTC when applicable
  - name: source_channel
    type: VARCHAR
    description: Exact normalized acquisition channel
  - name: source_platform
    type: VARCHAR
    description: Platform source associated with the order
  - name: campaign_id
    type: VARCHAR
    description: Campaign identifier associated with the order
  - name: campaign_name
    type: VARCHAR
    description: Campaign display name associated with the order
  - name: special_event_id
    type: VARCHAR
    description: Synthetic event identifier when the order belongs to an injected campaign, sitewide incident, or product defect
  - name: special_event_type
    type: VARCHAR
    description: Synthetic event type used for dashboard filtering
  - name: special_event_name
    type: VARCHAR
    description: Human-readable synthetic event name
  - name: event_phase
    type: VARCHAR
    description: Event phase such as launch_push, stockout_waste, outage, recovery, or refund_spike
  - name: city
    type: VARCHAR
    description: Order city
  - name: state_code
    type: VARCHAR
    description: Two-letter US state or district postal abbreviation
  - name: state_name
    type: VARCHAR
    description: Full US state or district name
  - name: item_count
    type: INTEGER
    description: Number of apparel items in the order
  - name: primary_product_id
    type: VARCHAR
    description: Primary product identifier used to price the synthetic basket
  - name: primary_category
    type: VARCHAR
    description: Primary apparel category for the order
  - name: cogs_amount
    type: DOUBLE
    description: Order cost of goods sold in USD
  - name: shipping_cost
    type: DOUBLE
    description: Fulfillment shipping cost in USD

@bruin */

WITH deduped AS (
    SELECT *
    FROM bruin_shop_raw.shopify_orders
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at DESC, created_at DESC, id DESC
    ) = 1
)

SELECT
    id AS order_id,
    order_number,
    email AS customer_email,
    COALESCE(processed_at, created_at) AS order_date,
    financial_status AS payment_status,
    fulfillment_status,
    SAFE_CAST(total_price AS NUMERIC) AS order_total,
    SAFE_CAST(subtotal_price AS NUMERIC) AS subtotal,
    SAFE_CAST(total_tax AS NUMERIC) AS tax_amount,
    SAFE_CAST(total_discounts AS NUMERIC) AS discount_amount,
    currency,
    cancel_reason,
    cancelled_at,
    source_channel,
    source_platform,
    campaign_id,
    campaign_name,
    special_event_id,
    special_event_type,
    special_event_name,
    event_phase,
    city,
    state_code,
    state_name,
    COALESCE(item_count, 1) AS item_count,
    primary_product_id,
    primary_category,
    SAFE_CAST(COALESCE(cogs_amount, 0) AS NUMERIC) AS cogs_amount,
    SAFE_CAST(COALESCE(shipping_cost, 0) AS NUMERIC) AS shipping_cost
FROM deduped
WHERE test IS NOT TRUE
  AND financial_status IS NOT NULL
