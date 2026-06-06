/* @bruin
name: bruin_shop_raw.shopify_products
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake Shopify-style product catalog for a US apparel company.
  The catalog spans T-shirts, pants, shoes, and accessories with category-
  specific prices, unit costs, vendors, SKUs, and inventory quantities.

depends:
  - bruin_shop_raw.shopify_products_test

materialization:
  type: table
  strategy: create+replace

columns:
  - name: id
    type: VARCHAR
    description: Synthetic Shopify product identifier.
    primary_key: true
    nullable: false
  - name: title
    type: VARCHAR
    description: Product display name.
  - name: product_type
    type: VARCHAR
    description: Apparel merchandising category.
  - name: vendor
    type: VARCHAR
    description: Synthetic vendor or private-label brand.
  - name: status
    type: VARCHAR
    description: Product lifecycle status.
  - name: price_range_v2
    type: VARCHAR
    description: Shopify-style JSON payload containing the product's minimum variant price.
  - name: tags
    type: VARCHAR
    description: Shopify-style product tags.
  - name: created_at
    type: TIMESTAMP
    description: Product creation timestamp in UTC.
  - name: updated_at
    type: TIMESTAMP
    description: Product update timestamp in UTC.
  - name: total_inventory
    type: INTEGER
    description: Current available product inventory units.
  - name: sku
    type: VARCHAR
    description: Synthetic product SKU.
  - name: cogs_per_unit
    type: DOUBLE
    description: Average cost of goods sold per unit in USD.

@bruin */

WITH categories AS (
    SELECT *
    FROM UNNEST([
        STRUCT('tshirts' AS category_slug, 'T-shirts' AS product_type, 'Blue Finch Basics' AS vendor, 16.00 AS base_price, 14.00 AS price_spread, 0.25 AS cogs_rate, ['Classic Tee', 'Relaxed Tee', 'Pocket Tee', 'Rib Crew', 'Heavyweight Tee', 'V-neck Tee'] AS styles),
        STRUCT('pants' AS category_slug, 'Pants' AS product_type, 'Northstar Apparel' AS vendor, 32.00 AS base_price, 22.00 AS price_spread, 0.30 AS cogs_rate, ['Chino Pants', 'Wide-leg Pants', 'Utility Pants', 'Jogger Pants', 'Denim Pants', 'Travel Pants'] AS styles),
        STRUCT('shoes' AS category_slug, 'Shoes' AS product_type, 'Waypoint Footwear' AS vendor, 42.00 AS base_price, 36.00 AS price_spread, 0.32 AS cogs_rate, ['Court Sneakers', 'Runner Sneakers', 'Canvas Slip-ons', 'Trail Shoes', 'Leather Mules', 'Everyday Trainers'] AS styles),
        STRUCT('accessories' AS category_slug, 'Accessories' AS product_type, 'Studio Vale' AS vendor, 10.00 AS base_price, 24.00 AS price_spread, 0.22 AS cogs_rate, ['Canvas Cap', 'Ribbed Socks', 'Tote Bag', 'Crossbody Bag', 'Leather Belt', 'Beanie'] AS styles)
    ])
),
colors AS (
    SELECT *
    FROM UNNEST([
        STRUCT(1 AS color_id, 'Black' AS color_name, 0.00 AS price_premium),
        STRUCT(2 AS color_id, 'White' AS color_name, 0.00 AS price_premium),
        STRUCT(3 AS color_id, 'Navy' AS color_name, 2.00 AS price_premium),
        STRUCT(4 AS color_id, 'Heather Gray' AS color_name, 1.00 AS price_premium),
        STRUCT(5 AS color_id, 'Forest' AS color_name, 3.00 AS price_premium),
        STRUCT(6 AS color_id, 'Clay' AS color_name, 2.00 AS price_premium),
        STRUCT(7 AS color_id, 'Denim' AS color_name, 4.00 AS price_premium),
        STRUCT(8 AS color_id, 'Cream' AS color_name, 1.00 AS price_premium)
    ])
),
product_slots AS (
    SELECT slot
    FROM UNNEST(GENERATE_ARRAY(1, 30)) AS slot
),
products AS (
    SELECT
        FORMAT('prod_%s_%02d', c.category_slug, s.slot) AS id,
        c.category_slug,
        c.product_type,
        c.vendor,
        s.slot,
        color_name,
        c.styles[ORDINAL(1 + MOD(s.slot - 1, ARRAY_LENGTH(c.styles)))] AS style_name,
        ROUND(c.base_price + MOD(s.slot * 7, CAST(c.price_spread AS INT64)) + color_price.price_premium, 2) AS price,
        c.cogs_rate,
        CASE
            WHEN c.category_slug = 'shoes' AND s.slot = 4 THEN 0
            ELSE 550 + ABS(MOD(FARM_FINGERPRINT(FORMAT('%s-%02d-inventory', c.category_slug, s.slot)), 4800))
        END AS inventory_units
    FROM categories c
    CROSS JOIN product_slots s
    INNER JOIN colors color_price
        ON color_price.color_id = 1 + MOD(s.slot - 1, 8)
)

SELECT
    id,
    FORMAT('%s %s', color_name, style_name) AS title,
    product_type,
    vendor,
    'active' AS status,
    TO_JSON_STRING(STRUCT(STRUCT(FORMAT('%.2f', price) AS amount, 'USD' AS currencyCode) AS minVariantPrice)) AS price_range_v2,
    FORMAT('%s,%s,apparel,bruin-test,synthetic', LOWER(REPLACE(product_type, '-', '')), color_name) AS tags,
    TIMESTAMP(DATETIME(DATE_ADD(DATE '2024-08-01', INTERVAL MOD(slot * 11, 120) DAY), TIME(9, 0, 0)), 'UTC') AS created_at,
    TIMESTAMP(DATETIME(DATE '2026-06-05', TIME(12, 0, 0)), 'UTC') AS updated_at,
    inventory_units AS total_inventory,
    FORMAT('APP-%s-%02d', UPPER(category_slug), slot) AS sku,
    ROUND(price * cogs_rate, 2) AS cogs_per_unit
FROM products
ORDER BY product_type, title
