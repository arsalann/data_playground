/* @bruin
name: bruin_shop_raw.shopify_inventory
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake Shopify-style inventory item rows derived from the
  generated apparel product catalog.

depends:
  - bruin_shop_raw.shopify_products

materialization:
  type: table
  strategy: create+replace

columns:
  - name: id
    type: VARCHAR
    description: Synthetic Shopify inventory item identifier.
    primary_key: true
    nullable: false
  - name: sku
    type: VARCHAR
    description: SKU associated with the inventory item.
  - name: tracked
    type: BOOLEAN
    description: Whether inventory is tracked.
  - name: updated_at
    type: TIMESTAMP
    description: Inventory item update timestamp in UTC.

@bruin */

SELECT
    FORMAT('inv_%s', id) AS id,
    sku,
    TRUE AS tracked,
    updated_at
FROM bruin_shop_raw.shopify_products
