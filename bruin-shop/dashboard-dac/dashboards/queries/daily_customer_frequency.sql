WITH daily AS (
  SELECT
    DATE(order_date) AS order_day,
    COUNT(DISTINCT customer_email) AS ordering_customers,
    COUNT(*) AS paid_orders,
    ROUND(SAFE_DIVIDE(COUNT(*), NULLIF(COUNT(DISTINCT customer_email), 0)), 2) AS orders_per_customer
  FROM `bruin-playground-arsalan.staging.stg_orders`
  WHERE payment_status IN ('paid', 'partially_refunded')
    AND DATE(order_date) BETWEEN COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01')
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
  GROUP BY order_day
)

SELECT
  FORMAT_DATE('%b %d', order_day) AS day_label,
  ordering_customers,
  paid_orders,
  orders_per_customer
FROM daily
ORDER BY order_day
