/* @bruin
name: staging.stg_customers
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deduplicates fake Shopify customer rows and standardizes customer fields for
  ecommerce reporting. Lifetime order counts and spend are recomputed from the
  generated order facts so customer metrics stay reconciled. First-seen
  timestamps use the first paid or partially refunded order when present, which
  keeps paid customer cohorts anchored to the customer's first completed
  purchase.

depends:
  - raw.shopify_customers
  - raw.shopify_orders

materialization:
  type: table
  strategy: create+replace

columns:
  - name: customer_email
    type: VARCHAR
    description: Customer email address
    primary_key: true
    nullable: false
  - name: shopify_customer_id
    type: VARCHAR
    description: Shopify customer identifier
  - name: first_name
    type: VARCHAR
    description: Customer first name
  - name: last_name
    type: VARCHAR
    description: Customer last name
  - name: shopify_created_at
    type: TIMESTAMP
    description: Original Shopify customer creation timestamp in UTC
  - name: first_seen_at
    type: TIMESTAMP
    description: First paid order timestamp in UTC when available, otherwise first observed customer timestamp
  - name: orders_count
    type: INTEGER
    description: Lifetime order count from Shopify
  - name: shopify_total_spent
    type: DOUBLE
    description: Lifetime spend from Shopify in USD
  - name: customer_tags
    type: VARCHAR
    description: Shopify customer tags
  - name: customer_state
    type: VARCHAR
    description: Shopify customer account state
  - name: city
    type: VARCHAR
    description: Customer city
  - name: state_code
    type: VARCHAR
    description: Two-letter US state or district postal abbreviation
  - name: state_name
    type: VARCHAR
    description: Full US state or district name

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.shopify_customers
    WHERE email IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY email
        ORDER BY updated_at DESC, created_at DESC, id DESC
    ) = 1
),
order_rollup AS (
    SELECT
        email AS customer_email,
        MIN(COALESCE(processed_at, created_at)) AS first_order_at,
        MIN(IF(financial_status IN ('paid', 'partially_refunded'), COALESCE(processed_at, created_at), NULL)) AS first_paid_order_at,
        COUNTIF(financial_status IN ('paid', 'partially_refunded')) AS paid_orders,
        SUM(CASE WHEN financial_status IN ('paid', 'partially_refunded') THEN SAFE_CAST(total_price AS NUMERIC) ELSE 0 END) AS paid_total_spent
    FROM raw.shopify_orders
    WHERE email IS NOT NULL
      AND test IS NOT TRUE
    GROUP BY email
)

SELECT
    email AS customer_email,
    id AS shopify_customer_id,
    first_name,
    last_name,
    created_at AS shopify_created_at,
    COALESCE(o.first_paid_order_at, o.first_order_at, created_at) AS first_seen_at,
    COALESCE(o.paid_orders, orders_count, 0) AS orders_count,
    COALESCE(o.paid_total_spent, SAFE_CAST(COALESCE(CAST(total_spent AS STRING), '0') AS NUMERIC), 0) AS shopify_total_spent,
    tags AS customer_tags,
    state AS customer_state,
    city,
    state_code,
    state_name
FROM deduped
LEFT JOIN order_rollup o
    ON deduped.email = o.customer_email
