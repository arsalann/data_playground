{{ config(materialized='table') }}

WITH shipments_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'shipments') }}
    WHERE shipment_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY shipment_key
        ORDER BY extracted_at DESC
    ) = 1
),

orders_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'orders') }}
    WHERE order_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_key
        ORDER BY extracted_at DESC
    ) = 1
),

stores_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'stores') }}
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
