{{ config(materialized='table') }}

WITH snapshots_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'inventory_snapshots') }}
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
    FROM {{ source('contoso_raw', 'products') }}
    WHERE product_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_key
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
