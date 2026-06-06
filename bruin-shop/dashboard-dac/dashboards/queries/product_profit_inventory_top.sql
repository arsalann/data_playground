WITH product_rollup AS (
  SELECT
    product_name,
    category,
    SUM(gross_profit) AS gross_profit_usd,
    SUM(COALESCE(inventory_value_usd, 0)) AS inventory_value_usd
  FROM `bruin-playground-arsalan.bruin_shop_reports.rpt_product_performance`
  WHERE (
    '{{ filters.category }}' = 'All categories'
    OR category = '{{ filters.category }}'
  )
  GROUP BY product_name, category
)

SELECT
  CASE
    WHEN product_name = 'Forest Leather Mules' THEN 'Forest Mules'
    WHEN product_name = 'Clay Everyday Trainers' THEN 'Clay Trainers'
    WHEN product_name = 'Denim Canvas Slip-ons' THEN 'Denim Slip-ons'
    WHEN product_name = 'White Trail Shoes' THEN 'White Trails'
    WHEN product_name = 'Heather Gray Runner Sneakers' THEN 'Gray Runners'
    WHEN product_name = 'Clay Runner Sneakers' THEN 'Clay Runners'
    WHEN product_name = 'Black Canvas Slip-ons' THEN 'Black Slip-ons'
    WHEN product_name = 'Navy Court Sneakers' THEN 'Navy Courts'
    ELSE REGEXP_REPLACE(product_name, r'^(Heather Gray|Black|Cream|Forest|Clay|White|Navy) ', '')
  END AS product_label,
  product_name,
  category,
  ROUND(gross_profit_usd, 2) AS gross_profit_usd,
  ROUND(inventory_value_usd, 2) AS inventory_value_usd,
  ROUND(SAFE_DIVIDE(gross_profit_usd, NULLIF(inventory_value_usd, 0)), 2) AS profit_to_inventory_ratio
FROM product_rollup
WHERE inventory_value_usd > 0
ORDER BY profit_to_inventory_ratio DESC
LIMIT 8
