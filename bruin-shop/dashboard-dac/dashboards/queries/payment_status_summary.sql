WITH filtered_orders AS (
  SELECT *
  FROM `bruin-playground-arsalan.staging.stg_orders`
  WHERE DATE(order_date) BETWEEN COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01')
      AND COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.end }}', '') AS DATE), DATE '2026-06-05')
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
)

SELECT
  CASE payment_status
    WHEN 'paid' THEN 'Paid'
    WHEN 'partially_refunded' THEN 'Partially refunded'
    WHEN 'voided' THEN 'Voided'
    ELSE INITCAP(REPLACE(payment_status, '_', ' '))
  END AS payment_status,
  COUNT(*) AS orders,
  ROUND(SUM(order_total), 2) AS gross_revenue_usd,
  ROUND(AVG(order_total), 2) AS avg_order_value_usd,
  ROUND(SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()) * 100, 2) AS order_share_pct
FROM filtered_orders
GROUP BY payment_status
ORDER BY orders DESC
