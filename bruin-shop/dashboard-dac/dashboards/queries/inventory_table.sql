SELECT
  p.product_name,
  p.category,
  p.vendor,
  p.price AS price_usd,
  p.cogs_per_unit AS cogs_per_unit_usd,
  p.paid_orders,
  p.items_sold,
  p.net_revenue AS net_revenue_usd,
  p.gross_profit AS gross_profit_usd,
  COALESCE(p.inventory_units, 0) AS available_units,
  COALESCE(p.inventory_value_usd, 0) AS inventory_value_usd,
  p.product_status
FROM `bruin-playground-arsalan.reports.rpt_product_performance` p
WHERE '{{ filters.category }}' = 'All categories'
  OR p.category = '{{ filters.category }}'
ORDER BY net_revenue_usd DESC, inventory_value_usd DESC, p.product_name
