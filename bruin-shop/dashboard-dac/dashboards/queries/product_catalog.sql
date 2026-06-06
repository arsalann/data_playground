SELECT
  product_name,
  category,
  vendor,
  price,
  product_status
FROM `bruin-playground-arsalan.bruin_shop_reports.rpt_product_performance`
ORDER BY price DESC, product_name
