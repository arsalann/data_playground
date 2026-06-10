WITH filtered_orders AS (
  SELECT *
  FROM `bruin-playground-arsalan.bruin_shop_staging.stg_orders`
  WHERE DATE(order_date) BETWEEN COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01')
      AND COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.end }}', '') AS DATE), DATE '2026-06-08')
    AND (
      '{{ filters.channel }}' = 'All channels'
      OR source_channel = CASE '{{ filters.channel }}'
        WHEN 'Paid social' THEN 'paid_ads'
        WHEN 'Paid search' THEN 'paid_search'
        WHEN 'Email' THEN 'email'
        WHEN 'Organic search' THEN 'organic_search'
        WHEN 'Direct' THEN 'direct'
      END
    )
    AND (
      '{{ filters.category }}' = 'All categories'
      OR primary_category = '{{ filters.category }}'
    )
    AND (
      '{{ filters.event_type }}' = 'All event types'
      OR special_event_type = CASE '{{ filters.event_type }}'
        WHEN 'Failed campaign' THEN 'failed_campaign'
        WHEN 'Stockout campaign' THEN 'stockout_campaign'
        WHEN 'Website outage' THEN 'website_outage'
        WHEN 'Successful campaign' THEN 'successful_campaign'
        WHEN 'Product defect' THEN 'product_defect'
      END
    )
),
total_orders AS (
  SELECT COUNT(*) AS all_orders
  FROM filtered_orders
),
exceptions AS (
  SELECT
    'Partial fulfillments' AS exception_type,
    COUNTIF(fulfillment_status = 'partial') AS orders,
    ROUND(SUM(IF(fulfillment_status = 'partial', order_total, 0)), 2) AS revenue_exposure_usd
  FROM filtered_orders
  UNION ALL
  SELECT
    'Partial refunds' AS exception_type,
    COUNTIF(payment_status = 'partially_refunded') AS orders,
    ROUND(SUM(IF(payment_status = 'partially_refunded', order_total, 0)), 2) AS revenue_exposure_usd
  FROM filtered_orders
  UNION ALL
  SELECT
    'Unfulfilled orders' AS exception_type,
    COUNTIF(fulfillment_status = 'unfulfilled') AS orders,
    ROUND(SUM(IF(fulfillment_status = 'unfulfilled', order_total, 0)), 2) AS revenue_exposure_usd
  FROM filtered_orders
  UNION ALL
  SELECT
    'Voided orders' AS exception_type,
    COUNTIF(payment_status = 'voided') AS orders,
    ROUND(SUM(IF(payment_status = 'voided', order_total, 0)), 2) AS revenue_exposure_usd
  FROM filtered_orders
  UNION ALL
  SELECT
    'Pending payments' AS exception_type,
    COUNTIF(payment_status = 'pending') AS orders,
    ROUND(SUM(IF(payment_status = 'pending', order_total, 0)), 2) AS revenue_exposure_usd
  FROM filtered_orders
)

SELECT
  exception_type,
  orders,
  ROUND(SAFE_DIVIDE(orders, NULLIF(all_orders, 0)) * 100, 2) AS order_share_pct,
  revenue_exposure_usd
FROM exceptions
CROSS JOIN total_orders
ORDER BY orders DESC
