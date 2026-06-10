WITH filtered_orders AS (
  SELECT *
  FROM `bruin-playground-arsalan.bruin_shop_staging.stg_orders`
  WHERE DATE(order_date) BETWEEN COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01')
      AND COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.end }}', '') AS DATE), DATE '2026-06-08')
    AND (
      '{{ filters.payment_status }}' = 'All payment statuses'
      OR payment_status = CASE '{{ filters.payment_status }}'
        WHEN 'Paid' THEN 'paid'
        WHEN 'Partially refunded' THEN 'partially_refunded'
        WHEN 'Pending' THEN 'pending'
        WHEN 'Voided' THEN 'voided'
      END
    )
    AND (
      '{{ filters.fulfillment_status }}' = 'All fulfillment statuses'
      OR fulfillment_status = CASE '{{ filters.fulfillment_status }}'
        WHEN 'Fulfilled' THEN 'fulfilled'
        WHEN 'Partial' THEN 'partial'
        WHEN 'Unfulfilled' THEN 'unfulfilled'
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
)

SELECT
  CASE payment_status
    WHEN 'paid' THEN 'Paid'
    WHEN 'partially_refunded' THEN 'Partially refunded'
    WHEN 'voided' THEN 'Voided'
    ELSE INITCAP(REPLACE(payment_status, '_', ' '))
  END AS payment_status,
  CASE fulfillment_status
    WHEN 'fulfilled' THEN 'Fulfilled'
    WHEN 'partial' THEN 'Partial'
    ELSE INITCAP(REPLACE(COALESCE(fulfillment_status, 'unknown'), '_', ' '))
  END AS fulfillment_status,
  COUNT(*) AS orders,
  ROUND(SUM(order_total), 2) AS gross_revenue_usd,
  ROUND(AVG(order_total), 2) AS avg_order_value_usd
FROM filtered_orders
GROUP BY payment_status, fulfillment_status
ORDER BY orders DESC
