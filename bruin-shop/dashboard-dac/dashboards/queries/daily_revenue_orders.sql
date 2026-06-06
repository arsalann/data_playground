SELECT
  FORMAT_DATE('%b %d', kpi_date) AS day_label,
  net_revenue,
  total_orders
FROM `bruin-playground-arsalan.bruin_shop_reports.rpt_daily_kpis`
ORDER BY kpi_date
