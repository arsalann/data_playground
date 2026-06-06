SELECT
  CASE channel
    WHEN 'paid_ads' THEN 'Paid social'
    WHEN 'paid_search' THEN 'Paid search'
    WHEN 'organic_search' THEN 'Organic search'
    WHEN 'email' THEN 'Email'
    WHEN 'direct' THEN 'Direct'
    ELSE INITCAP(REPLACE(channel, '_', ' '))
  END AS channel,
  ROUND(SUM(total_spend), 2) AS spend_usd,
  ROUND(SUM(attributed_revenue), 2) AS attributed_revenue_usd,
  ROUND(SUM(gross_profit), 2) AS gross_profit_usd,
  ROUND(SUM(contribution_profit), 2) AS contribution_profit_usd,
  ROUND(SAFE_DIVIDE(SUM(attributed_revenue), NULLIF(SUM(total_spend), 0)), 2) AS roas,
  ROUND(SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(total_orders), 0)), 2) AS cpa_usd,
  SUM(total_orders) AS orders,
  SUM(sessions) AS sessions
FROM `bruin-playground-arsalan.bruin_shop_reports.rpt_marketing_roi`
WHERE report_date BETWEEN COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01')
    AND COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.end }}', '') AS DATE), DATE '2026-06-05')
  AND (
    '{{ filters.channel }}' = 'All channels'
    OR channel = CASE '{{ filters.channel }}'
      WHEN 'Paid social' THEN 'paid_ads'
      WHEN 'Paid search' THEN 'paid_search'
      WHEN 'Email' THEN 'email'
      WHEN 'Organic search' THEN 'organic_search'
      WHEN 'Direct' THEN 'direct'
    END
  )
GROUP BY channel
ORDER BY contribution_profit_usd DESC
