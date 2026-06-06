SELECT
  p.product_name,
  p.category,
  COUNT(*) AS orders,
  COUNTIF(o.payment_status = 'partially_refunded') AS refunded_orders,
  ROUND(SAFE_DIVIDE(COUNTIF(o.payment_status = 'partially_refunded'), COUNT(*)) * 100, 2) AS refund_rate_pct,
  ROUND(SUM(o.order_total), 2) AS gross_revenue_usd
FROM `bruin-playground-arsalan.staging.stg_orders` o
LEFT JOIN `bruin-playground-arsalan.staging.stg_products` p
  ON o.primary_product_id = p.product_id
WHERE DATE(o.order_date) BETWEEN DATE '2026-02-20' AND DATE '2026-02-24'
GROUP BY p.product_name, p.category
HAVING orders >= 10
ORDER BY refund_rate_pct DESC, orders DESC
LIMIT 12
