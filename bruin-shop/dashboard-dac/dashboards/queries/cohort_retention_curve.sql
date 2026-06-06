WITH customer_orders AS (
  SELECT
    o.customer_email,
    DATE_TRUNC(DATE(c.first_seen_at), MONTH) AS cohort_month,
    DATE_TRUNC(DATE(o.order_date), MONTH) AS order_month
  FROM `bruin-playground-arsalan.bruin_shop_staging.stg_orders` o
  INNER JOIN `bruin-playground-arsalan.bruin_shop_staging.stg_customers` c
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
),
cohort_retention AS (
  SELECT
    co.cohort_month,
    DATE_DIFF(co.order_month, co.cohort_month, MONTH) AS months_since_first,
    SAFE_DIVIDE(COUNT(DISTINCT co.customer_email), cs.cohort_size) * 100 AS retention_rate_pct
  FROM customer_orders co
  INNER JOIN cohort_sizes cs
    ON co.cohort_month = cs.cohort_month
  GROUP BY co.cohort_month, cs.cohort_size, months_since_first
)

SELECT
  months_since_first,
  ROUND(AVG(retention_rate_pct), 2) AS avg_retention_rate_pct,
  ROUND(APPROX_QUANTILES(retention_rate_pct, 4)[OFFSET(1)], 2) AS lower_quartile_retention_pct,
  ROUND(APPROX_QUANTILES(retention_rate_pct, 4)[OFFSET(3)], 2) AS upper_quartile_retention_pct
FROM cohort_retention
WHERE months_since_first BETWEEN 0 AND 12
GROUP BY months_since_first
ORDER BY months_since_first
