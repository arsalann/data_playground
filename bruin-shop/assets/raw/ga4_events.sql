/* @bruin
name: raw.ga4_events
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake GA4 ecommerce and page-path event totals. Session-level
  page activity comes from `raw.ga4_sessions`; checkout, purchase, and order
  confirmation events are reconciled to successful Shopify-style orders so the
  GA4 purchase funnel behaves like a real storefront.

depends:
  - raw.ga4_sessions
  - raw.shopify_orders
  - raw.shopify_products

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date
    type: DATE
    description: Event date.
    primary_key: true
    nullable: false
  - name: event_name
    type: VARCHAR
    description: GA4 event name, such as page_view, view_item, add_to_cart, begin_checkout, add_payment_info, or purchase.
    primary_key: true
  - name: channel
    type: VARCHAR
    description: Normalized acquisition channel.
    primary_key: true
  - name: source
    type: VARCHAR
    description: GA4 traffic source.
    primary_key: true
  - name: medium
    type: VARCHAR
    description: GA4 traffic medium.
    primary_key: true
  - name: state_code
    type: VARCHAR
    description: Two-letter US state or district postal abbreviation.
    primary_key: true
  - name: city
    type: VARCHAR
    description: US city market.
    primary_key: true
  - name: page_path
    type: VARCHAR
    description: Synthetic storefront path where the event happened.
    primary_key: true
  - name: page_title
    type: VARCHAR
    description: Synthetic storefront page title.
  - name: product_id
    type: VARCHAR
    description: Primary product associated with product-detail or cart events when applicable.
  - name: item_category
    type: VARCHAR
    description: Product category associated with item-level events when applicable.
  - name: event_count
    type: INTEGER
    description: Count of events for the date, channel, market, page, and optional product.
  - name: special_event_id
    type: VARCHAR
    description: Synthetic event identifier when the event row belongs to an injected campaign, outage, stockout, or defect incident.
  - name: special_event_type
    type: VARCHAR
    description: Synthetic event type used for dashboard filtering.
  - name: special_event_name
    type: VARCHAR
    description: Human-readable synthetic event name.
  - name: event_phase
    type: VARCHAR
    description: Event phase such as launch_push, stockout_waste, outage, recovery, or refund_spike.

@bruin */

WITH session_rows AS (
    SELECT
        date,
        CASE
            WHEN source IN ('facebook', 'instagram', 'meta') THEN 'paid_ads'
            WHEN medium = 'email' THEN 'email'
            WHEN medium = 'organic' THEN 'organic_search'
            WHEN medium = 'cpc' THEN 'paid_search'
            WHEN source = '(direct)' THEN 'direct'
            ELSE 'other'
        END AS channel,
        source,
        medium,
        state_code,
        city,
        sessions,
        special_event_id,
        special_event_type,
        special_event_name,
        event_phase
    FROM raw.ga4_sessions
),
category_pages AS (
    SELECT *
    FROM UNNEST([
        STRUCT('/collections/t-shirts' AS page_path, 'T-shirts collection' AS page_title, 'T-shirts' AS item_category, 0.24 AS base_share),
        STRUCT('/collections/pants', 'Pants collection', 'Pants', 0.20),
        STRUCT('/collections/shoes', 'Shoes collection', 'Shoes', 0.22),
        STRUCT('/collections/accessories', 'Accessories collection', 'Accessories', 0.18)
    ])
),
session_page_events AS (
    SELECT
        date,
        'page_view' AS event_name,
        channel,
        source,
        medium,
        state_code,
        city,
        page_path,
        page_title,
        CAST(NULL AS STRING) AS product_id,
        item_category,
        event_count,
        special_event_id,
        special_event_type,
        special_event_name,
        event_phase
    FROM (
        SELECT
            date,
            channel,
            source,
            medium,
            state_code,
            city,
            '/' AS page_path,
            'Home' AS page_title,
            CAST(NULL AS STRING) AS item_category,
            CAST(ROUND(sessions * 0.94) AS INT64) AS event_count,
            special_event_id,
            special_event_type,
            special_event_name,
            event_phase
        FROM session_rows

        UNION ALL

        SELECT
            date,
            channel,
            source,
            medium,
            state_code,
            city,
            '/collections/new-arrivals' AS page_path,
            'New arrivals collection' AS page_title,
            CAST(NULL AS STRING) AS item_category,
            CAST(ROUND(sessions * 0.38) AS INT64) AS event_count,
            special_event_id,
            special_event_type,
            special_event_name,
            event_phase
        FROM session_rows

        UNION ALL

        SELECT
            sr.date,
            sr.channel,
            sr.source,
            sr.medium,
            sr.state_code,
            sr.city,
            cp.page_path,
            cp.page_title,
            cp.item_category,
            CAST(ROUND(
                sr.sessions
                * cp.base_share
                * CASE
                    WHEN cp.item_category = 'Shoes'
                        AND sr.special_event_id = 'instagram_trail_shoe_stockout'
                        AND sr.event_phase = 'launch_push'
                        THEN 1.85
                    WHEN cp.item_category = 'Shoes'
                        AND sr.special_event_id = 'instagram_trail_shoe_stockout'
                        AND sr.event_phase = 'stockout_waste'
                        THEN 1.15
                    WHEN cp.item_category = 'Accessories'
                        AND sr.date BETWEEN DATE '2026-02-20' AND DATE '2026-02-24'
                        THEN 1.22
                    ELSE 1.00
                END
            ) AS INT64) AS event_count,
            sr.special_event_id,
            sr.special_event_type,
            sr.special_event_name,
            sr.event_phase
        FROM session_rows sr
        CROSS JOIN category_pages cp
    )
),
order_event_base AS (
    SELECT
        DATE(created_at) AS date,
        source_channel AS channel,
        CASE source_channel
            WHEN 'paid_ads' THEN 'instagram'
            WHEN 'paid_search' THEN 'google'
            WHEN 'email' THEN 'klaviyo'
            WHEN 'organic_search' THEN 'google'
            WHEN 'direct' THEN '(direct)'
            ELSE source_platform
        END AS source,
        CASE source_channel
            WHEN 'paid_ads' THEN 'cpc'
            WHEN 'paid_search' THEN 'cpc'
            WHEN 'email' THEN 'email'
            WHEN 'organic_search' THEN 'organic'
            WHEN 'direct' THEN '(none)'
            ELSE 'referral'
        END AS medium,
        state_code,
        city,
        primary_product_id AS product_id,
        primary_category AS item_category,
        special_event_id,
        special_event_type,
        special_event_name,
        event_phase,
        COUNT(*) AS order_attempts,
        COUNTIF(financial_status IN ('paid', 'partially_refunded')) AS successful_orders,
        COUNTIF(financial_status = 'pending') AS pending_orders,
        COUNTIF(financial_status = 'voided') AS voided_orders
    FROM raw.shopify_orders
    GROUP BY
        date,
        channel,
        source,
        medium,
        state_code,
        city,
        product_id,
        item_category,
        special_event_id,
        special_event_type,
        special_event_name,
        event_phase
),
product_event_counts AS (
    SELECT
        oa.date,
        event_name,
        oa.channel,
        oa.source,
        oa.medium,
        oa.state_code,
        oa.city,
        FORMAT('/products/%s', LOWER(REGEXP_REPLACE(p.title, r'[^a-z0-9]+', '-'))) AS page_path,
        p.title AS page_title,
        oa.product_id,
        oa.item_category,
        event_count,
        oa.special_event_id,
        oa.special_event_type,
        oa.special_event_name,
        oa.event_phase
    FROM order_event_base oa
    INNER JOIN raw.shopify_products p
        ON oa.product_id = p.id
    CROSS JOIN UNNEST([
        STRUCT('view_item' AS event_name, CAST(ROUND((oa.order_attempts + oa.successful_orders) * 3.10) AS INT64) AS event_count),
        STRUCT('add_to_cart', CAST(ROUND((oa.order_attempts + oa.successful_orders) * 1.95) AS INT64)),
        STRUCT('view_cart', CAST(ROUND((oa.order_attempts + oa.successful_orders) * 1.32) AS INT64))
    ])
),
checkout_counts AS (
    SELECT
        date,
        channel,
        source,
        medium,
        state_code,
        city,
        special_event_id,
        special_event_type,
        special_event_name,
        event_phase,
        SUM(order_attempts) AS order_attempts,
        SUM(successful_orders) AS successful_orders,
        SUM(pending_orders) AS pending_orders,
        SUM(voided_orders) AS voided_orders
    FROM order_event_base
    GROUP BY
        date,
        channel,
        source,
        medium,
        state_code,
        city,
        special_event_id,
        special_event_type,
        special_event_name,
        event_phase
),
checkout_events AS (
    SELECT
        date,
        event_name,
        channel,
        source,
        medium,
        state_code,
        city,
        page_path,
        page_title,
        CAST(NULL AS STRING) AS product_id,
        CAST(NULL AS STRING) AS item_category,
        event_count,
        special_event_id,
        special_event_type,
        special_event_name,
        event_phase
    FROM checkout_counts
    CROSS JOIN UNNEST([
        STRUCT('begin_checkout' AS event_name, '/checkout' AS page_path, 'Checkout' AS page_title, CAST(ROUND(order_attempts * 1.18 + pending_orders * 0.65 + voided_orders * 0.35) AS INT64) AS event_count),
        STRUCT('add_shipping_info', '/checkout/shipping', 'Checkout shipping', CAST(ROUND(successful_orders * 1.08 + pending_orders * 0.58 + voided_orders * 0.24) AS INT64)),
        STRUCT('add_payment_info', '/checkout/payment', 'Checkout payment', CAST(ROUND(successful_orders * 1.02 + pending_orders * 0.42 + voided_orders * 0.18) AS INT64)),
        STRUCT('purchase', '/checkout/order-confirmation', 'Order confirmation', successful_orders),
        STRUCT('page_view', '/checkout/order-confirmation', 'Order confirmation', successful_orders)
    ])
)

SELECT *
FROM session_page_events
WHERE event_count > 0

UNION ALL

SELECT *
FROM product_event_counts
WHERE event_count > 0

UNION ALL

SELECT *
FROM checkout_events
WHERE event_count > 0
ORDER BY date, channel, state_code, city, event_name, page_path
