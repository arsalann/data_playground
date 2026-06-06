WITH selected_customer_orders AS (
  SELECT
    customer_email,
    COUNT(*) AS selected_orders,
    SUM(order_total) AS selected_spend
  FROM `bruin-playground-arsalan.bruin_shop_staging.stg_orders`
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
  GROUP BY customer_email
)

SELECT
  CAST(selected_orders AS STRING) AS lifetime_orders,
  COUNT(*) AS customers,
  ROUND(SUM(selected_spend), 2) AS lifetime_spend_usd,
  ROUND(AVG(selected_spend), 2) AS avg_lifetime_spend_usd
FROM selected_customer_orders
GROUP BY selected_orders
ORDER BY selected_orders
