/* @bruin
name: reports.rpt_product_performance
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Exposes active product catalog attributes and primary-product sales context
  for dashboard lookup. Orders do not contain full line-item arrays, so sales
  metrics are attributed to the generated primary product on each order.

depends:
  - staging.stg_products
  - staging.stg_orders

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
  - name: price
    type: DOUBLE
    description: Product list price in USD
  - name: inventory_units
    type: INTEGER
    description: Total available product inventory units reported by Shopify
  - name: inventory_value_usd
    type: DOUBLE
    description: Product inventory value in USD using list price times available units
  - name: product_status
    type: VARCHAR
    description: Product lifecycle status
  - name: cogs_per_unit
    type: DOUBLE
    description: Average cost of goods sold per unit in USD
  - name: paid_orders
    type: INTEGER
    description: Paid or partially refunded orders whose primary product is this product
  - name: items_sold
    type: INTEGER
    description: Total items sold on orders whose primary product is this product
  - name: net_revenue
    type: DOUBLE
    description: Net revenue attributed to this primary product in USD
  - name: gross_profit
    type: DOUBLE
    description: Net revenue minus COGS and shipping for orders attributed to this product in USD
  - name: created_at
    type: TIMESTAMP
    description: Product creation timestamp in UTC
  - name: updated_at
    type: TIMESTAMP
    description: Product update timestamp in UTC

@bruin */

WITH product_orders AS (
    SELECT
        primary_product_id AS product_id,
        COUNT(*) AS paid_orders,
        SUM(item_count) AS items_sold,
        SUM(order_total) AS net_revenue,
        SUM(order_total - COALESCE(cogs_amount, 0) - COALESCE(shipping_cost, 0)) AS gross_profit
    FROM staging.stg_orders
    WHERE payment_status IN ('paid', 'partially_refunded')
    GROUP BY primary_product_id
)

SELECT
    product_id,
    product_name,
    category,
    vendor,
    price,
    inventory_units,
    ROUND(COALESCE(inventory_units, 0) * COALESCE(price, 0), 2) AS inventory_value_usd,
    product_status,
    cogs_per_unit,
    COALESCE(po.paid_orders, 0) AS paid_orders,
    COALESCE(po.items_sold, 0) AS items_sold,
    COALESCE(po.net_revenue, 0) AS net_revenue,
    COALESCE(po.gross_profit, 0) AS gross_profit,
    created_at,
    updated_at
FROM staging.stg_products
LEFT JOIN product_orders po
    USING (product_id)
ORDER BY product_name
