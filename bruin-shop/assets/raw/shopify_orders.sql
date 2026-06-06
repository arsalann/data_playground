/* @bruin
name: raw.shopify_orders
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake Shopify-style order facts for a US apparel company. Orders
  are generated one-for-one from `raw.marketing_funnel.conversions`, making
  ad spend, impressions, clicks, web sessions, purchases, order counts, revenue,
  cost of goods sold, shipping cost, and contribution profit internally
  consistent by date, channel, state, and city. Customer assignment uses a
  deterministic lifecycle model: each market accumulates new customers over
  time, and repeat orders draw from already-acquired recent, mid-age, and loyal
  customer cohorts instead of hash collisions across the whole city pool.

depends:
  - raw.us_markets
  - raw.marketing_funnel
  - raw.shopify_products
  - raw.special_events

materialization:
  type: table
  strategy: create+replace

columns:
  - name: id
    type: VARCHAR
    description: Synthetic Shopify order identifier.
    primary_key: true
    nullable: false
  - name: order_number
    type: INTEGER
    description: Human-readable synthetic Shopify order number.
  - name: email
    type: VARCHAR
    description: Customer email address associated with the order.
  - name: created_at
    type: TIMESTAMP
    description: Timestamp when the order was created.
  - name: processed_at
    type: TIMESTAMP
    description: Business timestamp assigned to the order.
  - name: updated_at
    type: TIMESTAMP
    description: Timestamp when the order was last updated.
  - name: financial_status
    type: VARCHAR
    description: Shopify-style financial status.
  - name: fulfillment_status
    type: VARCHAR
    description: Shopify-style fulfillment status.
  - name: total_price
    type: DOUBLE
    description: Merchandise revenue after discounts in USD.
  - name: subtotal_price
    type: DOUBLE
    description: Merchandise subtotal before discounts in USD.
  - name: total_tax
    type: DOUBLE
    description: Estimated order tax amount in USD.
  - name: total_discounts
    type: DOUBLE
    description: Total discount amount in USD.
  - name: currency
    type: VARCHAR
    description: ISO 4217 order currency code.
  - name: cancel_reason
    type: VARCHAR
    description: Cancellation reason when the order was cancelled.
  - name: cancelled_at
    type: TIMESTAMP
    description: Cancellation timestamp when applicable.
  - name: test
    type: BOOLEAN
    description: Whether the order is a test order.
  - name: tags
    type: VARCHAR
    description: Shopify-style order tags.
  - name: source_channel
    type: VARCHAR
    description: Exact normalized acquisition channel from the generated funnel.
  - name: source_platform
    type: VARCHAR
    description: Platform source such as Meta, Google Ads, Klaviyo, organic, or direct.
  - name: campaign_id
    type: VARCHAR
    description: Campaign identifier from the generated funnel.
  - name: campaign_name
    type: VARCHAR
    description: Campaign display name from the generated funnel.
  - name: special_event_id
    type: VARCHAR
    description: Synthetic event identifier when the order belongs to an injected campaign, sitewide incident, or product defect.
  - name: special_event_type
    type: VARCHAR
    description: Synthetic event type used for dashboard filtering.
  - name: special_event_name
    type: VARCHAR
    description: Human-readable synthetic event name.
  - name: event_phase
    type: VARCHAR
    description: Event phase such as launch_push, stockout_waste, outage, recovery, or refund_spike.
  - name: city
    type: VARCHAR
    description: Order city.
  - name: state_code
    type: VARCHAR
    description: Two-letter US state or district postal abbreviation.
  - name: state_name
    type: VARCHAR
    description: Full US state or district name.
  - name: item_count
    type: INTEGER
    description: Number of apparel items in the order, from 1 to 10.
  - name: primary_product_id
    type: VARCHAR
    description: Primary product identifier used to price the synthetic basket.
  - name: primary_category
    type: VARCHAR
    description: Primary apparel category for the order.
  - name: cogs_amount
    type: DOUBLE
    description: Order cost of goods sold in USD.
  - name: shipping_cost
    type: DOUBLE
    description: Fulfillment shipping cost in USD, modeled at roughly $7 per item.

@bruin */

WITH funnel_orders AS (
    SELECT
        mf.*,
        m.demand_weight,
        m.sales_tax_rate,
        CAST(1200 + ROUND(m.demand_weight * 2800) AS INT64) AS customer_count,
        order_index,
        FORMAT('%s|%s|%03d|%05d', FORMAT_DATE('%Y%m%d', mf.activity_date), mf.channel, mf.market_id, order_index) AS order_key
    FROM raw.marketing_funnel mf
    INNER JOIN raw.us_markets m
        ON mf.market_id = m.market_id
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, mf.conversions)) AS order_index
    WHERE mf.conversions > 0
),
enriched AS (
    SELECT
        *,
        ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|items')), 1000)) AS item_rand,
        ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|category')), 1000)) AS category_rand,
        ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|payment')), 1000)) AS payment_rand,
        ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|fulfillment')), 1000)) AS fulfillment_rand,
        ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|customer_new')), 1000)) AS new_customer_rand,
        ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|customer_repeat_segment')), 1000)) AS repeat_segment_rand,
        ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|product')), 30)) + 1 AS product_slot_rand,
        ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|discount')), 1000)) AS discount_rand,
        ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|time')), 86400)) AS base_seconds_after_midnight,
        ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|time_bucket')), 1000)) AS time_bucket_rand,
        ROW_NUMBER() OVER (
            PARTITION BY market_id
            ORDER BY activity_date, channel, order_index
        ) AS market_order_sequence
    FROM funnel_orders
),
customer_intent AS (
    SELECT
        *,
        CASE
            WHEN market_order_sequence = 1 THEN TRUE
            WHEN special_event_type = 'campaign_success'
                AND channel IN ('paid_ads', 'paid_search')
                THEN new_customer_rand < 760
            WHEN special_event_type = 'stockout'
                AND event_phase = 'launch_push'
                THEN new_customer_rand < 700
            WHEN special_event_type = 'site_outage'
                AND event_phase = 'recovery'
                THEN new_customer_rand < 340
            WHEN channel = 'paid_ads' THEN new_customer_rand < 680
            WHEN channel = 'paid_search' THEN new_customer_rand < 620
            WHEN channel = 'organic_search' THEN new_customer_rand < 520
            WHEN channel = 'direct' THEN new_customer_rand < 430
            WHEN channel = 'email' THEN new_customer_rand < 220
            ELSE new_customer_rand < 500
        END AS wants_new_customer
    FROM enriched
),
customer_sequences AS (
    SELECT
        *,
        SUM(IF(wants_new_customer, 1, 0)) OVER (
            PARTITION BY market_id
            ORDER BY market_order_sequence
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS candidate_new_customer_sequence,
        COALESCE(
            SUM(IF(wants_new_customer, 1, 0)) OVER (
                PARTITION BY market_id
                ORDER BY market_order_sequence
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            0
        ) AS prior_candidate_new_customers
    FROM customer_intent
),
customer_pools AS (
    SELECT
        *,
        LEAST(customer_count, GREATEST(prior_candidate_new_customers, 1)) AS returning_pool_size,
        GREATEST(
            1,
            CAST(CEIL(LEAST(customer_count, GREATEST(prior_candidate_new_customers, 1)) * 0.18) AS INT64)
        ) AS recent_pool_size,
        GREATEST(
            1,
            CAST(CEIL(LEAST(customer_count, GREATEST(prior_candidate_new_customers, 1)) * 0.37) AS INT64)
        ) AS mid_pool_size
    FROM customer_sequences
),
customer_assigned AS (
    SELECT
        *,
        CASE
            WHEN wants_new_customer
                AND candidate_new_customer_sequence <= customer_count
                THEN candidate_new_customer_sequence
            WHEN repeat_segment_rand < 450
                THEN GREATEST(
                    1,
                    returning_pool_size - ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|repeat_recent')), recent_pool_size))
                )
            WHEN repeat_segment_rand < 800
                THEN GREATEST(
                    1,
                    returning_pool_size - recent_pool_size - ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|repeat_middle')), mid_pool_size))
                )
            ELSE 1 + ABS(MOD(
                FARM_FINGERPRINT(CONCAT(order_key, '|repeat_loyal')),
                GREATEST(1, returning_pool_size - recent_pool_size - mid_pool_size)
            ))
        END AS customer_index
    FROM customer_pools
),
shaped AS (
    SELECT
        *,
        CASE
            WHEN item_rand < 650 THEN 1
            WHEN item_rand < 850 THEN 2
            WHEN item_rand < 930 THEN 3
            WHEN item_rand < 970 THEN 4
            WHEN item_rand < 985 THEN 5
            WHEN item_rand < 993 THEN 6
            WHEN item_rand < 997 THEN 7
            WHEN item_rand < 999 THEN 8
            ELSE 10
        END AS item_count,
        CASE
            WHEN EXTRACT(MONTH FROM activity_date) IN (5, 6, 7, 8) AND category_rand < 460 THEN 'tshirts'
            WHEN EXTRACT(MONTH FROM activity_date) IN (9, 10, 11) AND category_rand < 430 THEN 'pants'
            WHEN EXTRACT(MONTH FROM activity_date) IN (11, 12) AND category_rand < 380 THEN 'accessories'
            WHEN EXTRACT(MONTH FROM activity_date) IN (3, 4, 8, 9) AND category_rand < 360 THEN 'shoes'
            WHEN category_rand < 280 THEN 'tshirts'
            WHEN category_rand < 540 THEN 'pants'
            WHEN category_rand < 780 THEN 'shoes'
            ELSE 'accessories'
        END AS category_slug,
        CASE
            WHEN channel = 'email' THEN 0.12
            WHEN channel = 'paid_ads' THEN 0.04
            WHEN channel = 'paid_search' THEN 0.05
            WHEN channel = 'direct' THEN 0.03
            ELSE 0.02
        END
        + CASE
            WHEN activity_date BETWEEN DATE '2025-11-27' AND DATE '2025-12-01' THEN 0.10
            WHEN activity_date BETWEEN DATE '2025-12-10' AND DATE '2025-12-20' THEN 0.06
            ELSE 0.00
        END AS discount_rate
    FROM customer_assigned
),
assigned AS (
    SELECT
        *,
        CASE
            WHEN activity_date BETWEEN DATE '2026-03-10' AND DATE '2026-03-17'
                AND channel = 'paid_ads'
                THEN 4
            WHEN activity_date >= DATE '2026-03-18'
                AND category_slug = 'shoes'
                AND product_slot_rand = 4
                THEN 5
            WHEN activity_date BETWEEN DATE '2026-02-20' AND DATE '2026-02-24'
                AND category_slug = 'accessories'
                THEN 9
            ELSE product_slot_rand
        END AS product_slot
    FROM shaped
),
priced AS (
    SELECT
        s.*,
        p.id AS product_id,
        p.product_type,
        SAFE_CAST(JSON_VALUE(p.price_range_v2, '$.minVariantPrice.amount') AS NUMERIC) AS unit_price,
        SAFE_CAST(p.cogs_per_unit AS NUMERIC) AS cogs_per_unit,
        TIMESTAMP_ADD(
            TIMESTAMP(DATETIME(s.activity_date, TIME(0, 0, 0)), 'UTC'),
            INTERVAL
                CASE
                    WHEN s.activity_date = DATE '2026-02-04' AND s.time_bucket_rand < 35
                        THEN 10 * 3600 + MOD(s.base_seconds_after_midnight, 12 * 3600)
                    WHEN s.activity_date = DATE '2026-02-04' AND MOD(s.base_seconds_after_midnight, 12) < 10
                        THEN MOD(s.base_seconds_after_midnight, 10 * 3600)
                    WHEN s.activity_date = DATE '2026-02-04'
                        THEN 22 * 3600 + MOD(s.base_seconds_after_midnight, 2 * 3600)
                    WHEN s.activity_date = DATE '2026-02-05' AND s.time_bucket_rand < 620
                        THEN 8 * 3600 + MOD(s.base_seconds_after_midnight, 12 * 3600)
                    WHEN s.activity_date = DATE '2026-02-05' AND MOD(s.base_seconds_after_midnight, 12) < 8
                        THEN MOD(s.base_seconds_after_midnight, 8 * 3600)
                    WHEN s.activity_date = DATE '2026-02-05'
                        THEN 20 * 3600 + MOD(s.base_seconds_after_midnight, 4 * 3600)
                    ELSE s.base_seconds_after_midnight
                END SECOND
        ) AS order_ts
    FROM assigned s
    INNER JOIN raw.shopify_products p
        ON p.id = FORMAT('prod_%s_%02d', s.category_slug, s.product_slot)
),
amounts AS (
    SELECT
        *,
        ROUND(unit_price * item_count, 2) AS subtotal,
        ROUND(unit_price * item_count * LEAST(discount_rate + discount_rand / 10000.0, 0.30), 2) AS discount_amount,
        ROUND(cogs_per_unit * item_count * (0.97 + ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|cogs')), 7)) / 100.0), 2) AS cogs_amount,
        ROUND(item_count * 7.00, 2) AS shipping_cost
    FROM priced
)

SELECT
    FORMAT('ord_%s_%s_%03d_%05d', FORMAT_DATE('%Y%m%d', activity_date), channel, market_id, order_index) AS id,
    1000000 + ABS(MOD(FARM_FINGERPRINT(order_key), 900000000)) AS order_number,
    FORMAT('apparel-%03d-%06d@example.com', market_id, customer_index) AS email,
    order_ts AS created_at,
    order_ts AS processed_at,
    TIMESTAMP_ADD(order_ts, INTERVAL ABS(MOD(FARM_FINGERPRINT(CONCAT(order_key, '|update')), 4320)) MINUTE) AS updated_at,
    CASE
        WHEN product_id = 'prod_accessories_09'
            AND activity_date BETWEEN DATE '2026-02-20' AND DATE '2026-02-24'
            AND payment_rand < 930
            THEN 'partially_refunded'
        WHEN payment_rand < 932 THEN 'paid'
        WHEN payment_rand < 965 THEN 'partially_refunded'
        WHEN payment_rand < 982 THEN 'pending'
        ELSE 'voided'
    END AS financial_status,
    CASE
        WHEN fulfillment_rand < 820 THEN 'fulfilled'
        WHEN fulfillment_rand < 955 THEN 'partial'
        ELSE 'unfulfilled'
    END AS fulfillment_status,
    CAST(ROUND(subtotal - discount_amount, 2) AS FLOAT64) AS total_price,
    CAST(subtotal AS FLOAT64) AS subtotal_price,
    CAST(ROUND((subtotal - discount_amount) * sales_tax_rate, 2) AS FLOAT64) AS total_tax,
    CAST(discount_amount AS FLOAT64) AS total_discounts,
    'USD' AS currency,
    IF(payment_rand >= 982, 'customer', NULL) AS cancel_reason,
    IF(payment_rand >= 982, TIMESTAMP_ADD(order_ts, INTERVAL 1 DAY), NULL) AS cancelled_at,
    FALSE AS test,
    CONCAT('synthetic,apparel,', channel, ',', LOWER(state_code), ',', category_slug) AS tags,
    channel AS source_channel,
    platform AS source_platform,
    campaign_id,
    campaign_name,
    CASE
        WHEN product_id = 'prod_accessories_09'
            AND activity_date BETWEEN DATE '2026-02-20' AND DATE '2026-02-24'
            THEN 'black_tote_defect_refunds'
        ELSE special_event_id
    END AS special_event_id,
    CASE
        WHEN product_id = 'prod_accessories_09'
            AND activity_date BETWEEN DATE '2026-02-20' AND DATE '2026-02-24'
            THEN 'product_defect'
        ELSE special_event_type
    END AS special_event_type,
    CASE
        WHEN product_id = 'prod_accessories_09'
            AND activity_date BETWEEN DATE '2026-02-20' AND DATE '2026-02-24'
            THEN 'Black Tote Bag defect refund incident'
        ELSE special_event_name
    END AS special_event_name,
    CASE
        WHEN product_id = 'prod_accessories_09'
            AND activity_date BETWEEN DATE '2026-02-20' AND DATE '2026-02-24'
            THEN 'refund_spike'
        ELSE event_phase
    END AS event_phase,
    city,
    state_code,
    state_name,
    item_count,
    product_id AS primary_product_id,
    product_type AS primary_category,
    CAST(cogs_amount AS FLOAT64) AS cogs_amount,
    CAST(shipping_cost AS FLOAT64) AS shipping_cost
FROM amounts
ORDER BY activity_date, market_id, channel, order_index
