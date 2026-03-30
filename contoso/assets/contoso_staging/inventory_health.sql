/* @bruin

name: contoso_staging.inventory_health
type: bq.sql
description: |
  Current inventory health snapshot across Contoso's global consumer electronics retail network.

  Provides the most recent inventory position for every active store-product combination,
  enabling proactive stock management and preventing stockouts. This staging table combines
  the latest inventory snapshots with product and store dimensions to create a comprehensive
  view of inventory health across all 9 countries of operation.

  Key business logic includes automated stockout risk classification where inventory levels
  are categorized as Critical (zero stock), Low Stock (below reorder point), Adequate
  (1-3x reorder point), or Overstocked (>3x reorder point). This classification system
  enables automated alerts and purchasing decisions.

  The dataset reflects Contoso's omnichannel strategy with both physical store inventory
  and 'Online' inventory for direct-to-consumer fulfillment. Reorder points are set at
  the product level but may vary by geographic region based on supplier lead times and
  demand patterns.

  Critical for operations, purchasing, and finance teams to prevent stockouts (revenue loss),
  reduce carrying costs (working capital optimization), and maintain service level agreements
  with customers. Updated daily to support rapid response to inventory movements.
connection: bruin-playground-eu
tags:
  - operations
  - inventory_management
  - fact_table
  - staging
  - daily_refresh
  - stockout_prevention
  - working_capital

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_raw.inventory_snapshots
  - contoso_raw.products
  - contoso_raw.stores

secrets:
  - key: bruin-playground-eu
    inject_as: bruin-playground-eu

columns:
  - name: store_key
    type: INTEGER
    description: |
      Foreign key to stores dimension table. Links to both physical retail locations
      across 9 countries (US, UK, Germany, France, Italy, Canada, Australia, Netherlands)
      and the 'Online' virtual store for direct-to-consumer fulfillment. Average store
      carries ~200 distinct products with seasonal variations.
    primary_key: true
  - name: product_key
    type: INTEGER
    description: |
      Foreign key to products dimension table. References Contoso's catalog of ~2,500
      consumer electronics SKUs across 8 major categories (Computers, Cell phones,
      TV and Video, Audio, Cameras and camcorders, Games and Toys, Home Appliances,
      Music Movies and Audio Books). Product keys are stable identifiers used across
      all operational systems.
    primary_key: true
  - name: snapshot_date
    type: DATE
    description: |
      Date of the most recent inventory count for this store-product combination.
      Represents the effective date of the inventory position, not the system
      extraction time. Typically aligns with daily close-of-business inventory
      reconciliation. May vary slightly across stores due to timezone differences
      and operational schedules.
  - name: store_country
    type: VARCHAR
    description: |
      Geographic market for this inventory position. Nine countries plus 'Online'
      for digital fulfillment center. Country assignment determines local currency
      pricing, supplier relationships, and regulatory compliance requirements.
      'Online' represents centralized e-commerce inventory serving all markets.
  - name: product_name
    type: VARCHAR
    description: |
      Full product name as displayed to customers and in operational systems.
      Standardized naming convention includes brand, model, and key specifications.
      Names average ~42 characters and are unique across the product catalog.
      Essential for human-readable reporting and troubleshooting.
  - name: category_name
    type: VARCHAR
    description: |
      High-level product category grouping for inventory management and reporting.
      Eight categories representing Contoso's major business segments. Category
      assignment determines buyer responsibility, supplier relationships, seasonal
      planning cycles, and margin expectations. Categories have distinct inventory
      turnover patterns and reorder strategies.
  - name: quantity_on_hand
    type: INTEGER
    description: |
      Current physical inventory count in sellable units. Reflects most recent
      physical or cycle count, adjusted for known sales and receipts since counting.
      Includes only saleable inventory (excludes damaged, reserved, or in-transit
      goods). Zero values indicate complete stockout requiring immediate attention.
      Average on-hand quantities ~250 units per store-product combination.
  - name: reorder_point
    type: INTEGER
    description: |
      Minimum inventory level triggering automatic replenishment orders. Set based
      on historical demand patterns, supplier lead times, and desired service levels.
      Calculated to maintain 95% in-stock rate while minimizing carrying costs.
      Typically represents 2-4 weeks of expected demand. Reorder points average
      ~55 units and are reviewed quarterly for optimization.
  - name: quantity_on_order
    type: INTEGER
    description: |
      Units currently on order from suppliers but not yet received. Includes purchase
      orders issued but goods in-transit, at receiving, or pending quality inspection.
      Essential for calculating total available inventory (on-hand + on-order) to
      avoid over-ordering. Zero values are normal for items with adequate stock levels.
      Average on-order quantities ~18 units, concentrated in fast-moving products.
  - name: is_below_reorder
    type: BOOLEAN
    description: |
      Binary flag indicating whether current inventory has fallen below the reorder
      point threshold. TRUE values (10.7% of records) trigger automated purchasing
      workflows and alert operations managers. Critical for preventing stockouts
      and maintaining customer service levels. Used in executive dashboards and
      automated reporting systems.
  - name: stockout_risk
    type: VARCHAR
    description: |-
      Four-tier risk classification for inventory management prioritization:

      - 'Critical': Zero inventory, immediate stockout risk requiring expedited ordering
      - 'Low Stock': Below reorder point, standard replenishment process triggered
      - 'Adequate': 1-3x reorder point, healthy stock levels for normal operations
      - 'Overstocked': >3x reorder point, potential excess inventory requiring review

      Classification drives automated alerts, purchasing priorities, and working capital
      management. Critical items receive daily monitoring, while overstocked items
      trigger markdown consideration to optimize cash flow.

@bruin */

WITH snapshots_deduped AS (
    SELECT *
    FROM contoso_raw.inventory_snapshots
    WHERE snapshot_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY snapshot_key
        ORDER BY extracted_at DESC
    ) = 1
),

latest_snapshots AS (
    SELECT *
    FROM snapshots_deduped
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_key, product_key
        ORDER BY snapshot_date DESC
    ) = 1
),

products_deduped AS (
    SELECT *
    FROM contoso_raw.products
    WHERE product_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_key
        ORDER BY extracted_at DESC
    ) = 1
),

stores_deduped AS (
    SELECT *
    FROM contoso_raw.stores
    WHERE store_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_key
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT
    ls.store_key,
    ls.product_key,
    CAST(ls.snapshot_date AS DATE) AS snapshot_date,
    st.country_name AS store_country,
    p.product_name,
    p.category_name,
    ls.quantity_on_hand,
    ls.reorder_point,
    ls.quantity_on_order,
    ls.quantity_on_hand < ls.reorder_point AS is_below_reorder,
    CASE
        WHEN ls.quantity_on_hand = 0 THEN 'Critical'
        WHEN ls.quantity_on_hand < ls.reorder_point THEN 'Low Stock'
        WHEN ls.quantity_on_hand < ls.reorder_point * 3 THEN 'Adequate'
        ELSE 'Overstocked'
    END AS stockout_risk
FROM latest_snapshots ls
LEFT JOIN products_deduped p ON ls.product_key = p.product_key
LEFT JOIN stores_deduped st ON ls.store_key = st.store_key
ORDER BY ls.store_key, ls.product_key
