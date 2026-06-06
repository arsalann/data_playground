SELECT
  p.category,
  COUNT(*) AS products,
  ROUND(AVG(p.price), 2) AS avg_price_usd,
  SUM(p.paid_orders) AS paid_orders,
  SUM(p.items_sold) AS items_sold,
  ROUND(SUM(p.net_revenue), 2) AS net_revenue_usd,
  ROUND(SUM(p.gross_profit), 2) AS gross_profit_usd,
  SUM(COALESCE(p.inventory_units, 0)) AS available_units,
  ROUND(SUM(COALESCE(p.inventory_value_usd, 0)), 2) AS inventory_value_usd
FROM `bruin-playground-arsalan.bruin_shop_reports.rpt_product_performance` p
WHERE '{{ filters.category }}' = 'All categories'
  OR p.category = '{{ filters.category }}'
GROUP BY p.category
ORDER BY inventory_value_usd DESC
