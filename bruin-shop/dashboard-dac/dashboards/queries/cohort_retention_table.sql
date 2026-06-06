WITH customer_orders AS (
  SELECT
    o.customer_email,
    DATE_TRUNC(DATE(c.first_seen_at), MONTH) AS cohort_month,
    DATE_TRUNC(DATE(o.order_date), MONTH) AS order_month,
    o.order_total
  FROM `bruin-playground-arsalan.staging.stg_orders` o
  INNER JOIN `bruin-playground-arsalan.staging.stg_customers` c
    ON o.customer_email = c.customer_email
  WHERE o.payment_status IN ('paid', 'partially_refunded')
    AND DATE(o.order_date) BETWEEN COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01')
      AND COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.end }}', '') AS DATE), DATE '2026-06-05')
    AND (
      '{{ filters.channel }}' = 'All channels'
      OR o.source_channel = CASE '{{ filters.channel }}'
        WHEN 'Paid social' THEN 'paid_ads'
        WHEN 'Paid search' THEN 'paid_search'
        WHEN 'Email' THEN 'email'
        WHEN 'Organic search' THEN 'organic_search'
        WHEN 'Direct' THEN 'direct'
      END
    )
    AND (
      '{{ filters.category }}' = 'All categories'
      OR o.primary_category = '{{ filters.category }}'
    )
),
cohort_sizes AS (
  SELECT
    cohort_month,
    COUNT(DISTINCT customer_email) AS cohort_size
  FROM customer_orders
  GROUP BY cohort_month
)

SELECT
  FORMAT_DATE('%Y-%m', co.cohort_month) AS cohort_month,
  cs.cohort_size,
  DATE_DIFF(co.order_month, co.cohort_month, MONTH) AS months_since_first,
  COUNT(DISTINCT co.customer_email) AS active_customers,
  ROUND(SAFE_DIVIDE(COUNT(DISTINCT co.customer_email), cs.cohort_size) * 100, 2) AS retention_rate,
  ROUND(SUM(co.order_total), 2) AS cohort_revenue_usd,
  ROUND(SAFE_DIVIDE(SUM(co.order_total), cs.cohort_size), 2) AS revenue_per_customer_usd
FROM customer_orders co
INNER JOIN cohort_sizes cs
  ON co.cohort_month = cs.cohort_month
GROUP BY co.cohort_month, cs.cohort_size, months_since_first
ORDER BY co.cohort_month, months_since_first
