/* @bruin
name: bruin_shop_staging.stg_products
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deduplicates fake Shopify product rows and keeps active product catalog fields
  for ecommerce reporting.

depends:
  - bruin_shop_raw.shopify_products

materialization:
  type: table
  strategy: create+replace

columns:
  - name: product_id
    type: VARCHAR
    description: Shopify product identifier
    primary_key: true
    nullable: false
  - name: product_name
    type: VARCHAR
    description: Product display name
  - name: category
    type: VARCHAR
    description: Product merchandising category
  - name: vendor
    type: VARCHAR
    description: Product vendor name
  - name: product_status
    type: VARCHAR
    description: Product lifecycle status
  - name: price
    type: DOUBLE
    description: Product list price in USD
  - name: inventory_units
    type: INTEGER
    description: Total available product inventory units reported by Shopify
  - name: sku
    type: VARCHAR
    description: Product SKU
  - name: cogs_per_unit
    type: DOUBLE
    description: Average cost of goods sold per unit in USD
  - name: tags
    type: VARCHAR
    description: Shopify product tags
  - name: created_at
    type: TIMESTAMP
    description: Product creation timestamp in UTC
  - name: updated_at
    type: TIMESTAMP
    description: Product update timestamp in UTC

@bruin */

WITH deduped AS (
    SELECT *
    FROM bruin_shop_raw.shopify_products
    WHERE id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at DESC, created_at DESC, id DESC
    ) = 1
)

SELECT
    id AS product_id,
    title AS product_name,
    product_type AS category,
    vendor,
    status AS product_status,
    SAFE_CAST(JSON_VALUE(price_range_v2, '$.minVariantPrice.amount') AS NUMERIC) AS price,
    COALESCE(total_inventory, 0) AS inventory_units,
    sku,
    SAFE_CAST(COALESCE(cogs_per_unit, 0) AS NUMERIC) AS cogs_per_unit,
    tags,
    created_at,
    updated_at
FROM deduped
WHERE LOWER(status) = 'active'
