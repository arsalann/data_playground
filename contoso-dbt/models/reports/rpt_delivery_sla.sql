{{ config(materialized='table') }}

SELECT
    year,
    month,
    carrier,
    store_country,
    COUNT(*) AS shipment_count,
    COUNTIF(shipment_status = 'Delivered') AS delivered_count,
    COUNTIF(shipment_status = 'Returned') AS returned_count,
    COUNTIF(shipment_status = 'Lost') AS lost_count,
    ROUND(COUNTIF(shipment_status = 'Delivered') / COUNT(*) * 100, 2) AS delivery_rate,
    ROUND(AVG(days_to_ship), 2) AS avg_days_to_ship,
    ROUND(AVG(days_in_transit), 2) AS avg_days_in_transit,
    ROUND(AVG(total_lead_days), 2) AS avg_total_lead_days,
    CAST(APPROX_QUANTILES(total_lead_days, 100)[OFFSET(95)] AS INTEGER) AS p95_lead_days,
    ROUND(SUM(ship_cost), 2) AS total_ship_cost,
    ROUND(AVG(ship_cost), 2) AS avg_cost_per_shipment
FROM {{ ref('stg_shipping_performance') }}
GROUP BY 1, 2, 3, 4
