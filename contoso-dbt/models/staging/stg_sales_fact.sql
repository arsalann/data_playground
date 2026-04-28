{{ config(materialized='table') }}

WITH sales_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'sales') }}
    WHERE order_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_key, line_number
        ORDER BY extracted_at DESC
    ) = 1
),

customers_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'customers') }}
    WHERE customer_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_key
        ORDER BY extracted_at DESC
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
),

dates_deduped AS (
    SELECT *
    FROM {{ source('contoso_raw', 'dates') }}
    WHERE date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY date
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT
    s.order_key,
    s.line_number,
    CAST(s.order_date AS DATE) AS order_date,
    CAST(s.delivery_date AS DATE) AS delivery_date,
    d.year,
    d.quarter,
    d.month_number,
    d.dayof_week,
    s.customer_key,
    CONCAT(c.given_name, ' ', c.surname) AS customer_name,
    c.country_full AS customer_country,
    c.gender AS customer_gender,
    c.occupation AS customer_occupation,
    s.store_key,
    st.country_name AS store_country,
    s.product_key,
    p.product_name,
    p.brand,
    p.category_name,
    p.sub_category_name,
    s.quantity,
    ROUND(s.unit_price * s.exchange_rate, 2) AS unit_price_usd,
    ROUND(s.unit_cost * s.exchange_rate, 2) AS unit_cost_usd,
    ROUND(s.quantity * s.net_price * s.exchange_rate, 2) AS revenue_usd,
    ROUND(s.quantity * s.unit_cost * s.exchange_rate, 2) AS cost_usd,
    ROUND(s.quantity * (s.net_price - s.unit_cost) * s.exchange_rate, 2) AS profit_usd,
    ROUND(
        SAFE_DIVIDE(
            s.net_price - s.unit_cost,
            NULLIF(s.net_price, 0)
        ) * 100,
        2
    ) AS margin_pct,
    s.currency_code
FROM sales_deduped s
LEFT JOIN customers_deduped c ON s.customer_key = c.customer_key
LEFT JOIN products_deduped p ON s.product_key = p.product_key
LEFT JOIN stores_deduped st ON s.store_key = st.store_key
LEFT JOIN dates_deduped d ON CAST(s.order_date AS DATE) = CAST(d.date AS DATE)
