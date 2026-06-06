SELECT
  category,
  COUNT(*) AS products,
  SUM(paid_orders) AS paid_orders,
  ROUND(SUM(net_revenue), 2) AS net_revenue_usd,
  ROUND(SUM(gross_profit), 2) AS gross_profit_usd,
  ROUND(SAFE_DIVIDE(SUM(gross_profit), NULLIF(SUM(net_revenue), 0)) * 100, 2) AS gross_margin_pct,
  SUM(COALESCE(inventory_units, 0)) AS inventory_units,
  ROUND(SUM(COALESCE(inventory_value_usd, 0)), 2) AS inventory_value_usd
FROM `bruin-playground-arsalan.bruin_shop_reports.rpt_product_performance`
WHERE '{{ filters.category }}' = 'All categories'
  OR category = '{{ filters.category }}'
GROUP BY category
ORDER BY gross_profit_usd DESC
