/* @bruin

name: contoso_staging.shipping_performance
type: bq.sql
description: |
  Comprehensive shipping performance metrics for Contoso consumer electronics retailer operations team.

  This staging table analyzes delivery performance across four major carriers (FedEx, UPS, DHL, USPS)
  and nine fulfillment regions including online fulfillment. Combines order, shipment, and store data
  to compute end-to-end logistics KPIs including dispatch timing, transit performance, and delivery outcomes.

  Key business insights: Tracks realistic shipping patterns with 0-3 day dispatch times, 2-14 day
  transit periods, and realistic cost distribution ($5-45 USD). Supports carrier SLA monitoring,
  cost optimization analysis, and geographic fulfillment performance evaluation.

  Data quality: Uses extracted_at timestamps for reliable deduplication across source tables.
  Includes 93% delivered rate reflecting realistic logistics success patterns. Store_country
  includes "Online" indicating direct-to-consumer fulfillment model alongside physical stores.

  Downstream usage: Feeds delivery SLA reports, carrier performance dashboards, and operations
  cost analysis. Critical for identifying shipping bottlenecks and optimizing fulfillment strategies.
connection: bruin-playground-eu
tags:
  - operations
  - logistics
  - shipping
  - carrier_performance
  - fulfillment
  - staging
  - fact_table
  - daily_batch

materialization:
  type: table
  strategy: create+replace

depends:
  - contoso_raw.shipments
  - contoso_raw.orders
  - contoso_raw.stores

secrets:
  - key: bruin-playground-eu
    inject_as: bruin-playground-eu

columns:
  - name: shipment_key
    type: INTEGER
    description: Unique shipment identifier (sequential 1 to N) - primary key for tracking individual shipments
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: order_key
    type: INTEGER
    description: Foreign key to orders table (1:1 relationship) - links shipment to originating customer order
    checks:
      - name: not_null
  - name: order_date
    type: DATE
    description: Date when customer placed the order - baseline for measuring total fulfillment lead time
    checks:
      - name: not_null
  - name: ship_date
    type: DATE
    description: Date shipment was dispatched from fulfillment center - typically 0-3 days after order date
    checks:
      - name: not_null
  - name: delivery_date
    type: DATE
    description: Actual date customer received the shipment - final milestone for delivery SLA measurement
    checks:
      - name: not_null
  - name: carrier
    type: VARCHAR
    description: Shipping carrier name (FedEx, UPS, DHL, USPS) - distribution weighted toward premium carriers
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - FedEx
          - UPS
          - DHL
          - USPS
  - name: shipment_status
    type: VARCHAR
    description: Final shipment outcome - 93% delivered success rate with realistic return and loss patterns
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Delivered
          - Returned
          - Lost
  - name: ship_cost
    type: DOUBLE
    description: Shipping cost in USD charged to customer - ranges $5-45 based on realistic fulfillment pricing
    checks:
      - name: not_null
      - name: positive
  - name: store_country
    type: VARCHAR
    description: Country of fulfillment location from stores table - includes "Online" for direct-ship fulfillment
    checks:
      - name: not_null
  - name: days_to_ship
    type: INTEGER
    description: Processing time metric - days from order placement to dispatch (0-3 day range typical)
    checks:
      - name: not_null
      - name: positive
  - name: days_in_transit
    type: INTEGER
    description: Transit time metric - days from dispatch to delivery (2-14 day range typical)
    checks:
      - name: not_null
      - name: positive
  - name: total_lead_days
    type: INTEGER
    description: End-to-end fulfillment time - total days from order to delivery (sum of processing + transit)
    checks:
      - name: not_null
      - name: positive
  - name: year
    type: INTEGER
    description: Order year extracted from order_date - enables year-over-year performance trending
    checks:
      - name: not_null
  - name: month
    type: INTEGER
    description: Order month (1-12) extracted from order_date - enables seasonal performance analysis
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - 1
          - 2
          - 3
          - 4
          - 5
          - 6
          - 7
          - 8
          - 9
          - 10
          - 11
          - 12

@bruin */

WITH shipments_deduped AS (
    SELECT *
    FROM contoso_raw.shipments
    WHERE shipment_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY shipment_key
        ORDER BY extracted_at DESC
    ) = 1
),

orders_deduped AS (
    SELECT *
    FROM contoso_raw.orders
    WHERE order_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_key
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
    sh.shipment_key,
    sh.order_key,
    CAST(o.order_date AS DATE) AS order_date,
    CAST(sh.ship_date AS DATE) AS ship_date,
    CAST(sh.delivery_date AS DATE) AS delivery_date,
    sh.carrier,
    sh.shipment_status,
    sh.ship_cost,
    st.country_name AS store_country,
    DATE_DIFF(CAST(sh.ship_date AS DATE), CAST(o.order_date AS DATE), DAY) AS days_to_ship,
    DATE_DIFF(CAST(sh.delivery_date AS DATE), CAST(sh.ship_date AS DATE), DAY) AS days_in_transit,
    DATE_DIFF(CAST(sh.delivery_date AS DATE), CAST(o.order_date AS DATE), DAY) AS total_lead_days,
    EXTRACT(YEAR FROM o.order_date) AS year,
    EXTRACT(MONTH FROM o.order_date) AS month
FROM shipments_deduped sh
LEFT JOIN orders_deduped o ON sh.order_key = o.order_key
LEFT JOIN stores_deduped st ON sh.store_key = st.store_key
ORDER BY sh.shipment_key
