SELECT
  FORMAT_DATE('%b %d', report_date) AS day_label,
  ROUND(SUM(IF(channel = 'paid_search', attributed_revenue, 0)), 2) AS paid_search_revenue_usd,
  ROUND(SUM(IF(channel = 'paid_ads', attributed_revenue, 0)), 2) AS paid_social_revenue_usd,
  ROUND(SAFE_DIVIDE(
    SUM(IF(channel = 'paid_search', attributed_revenue, 0)),
    NULLIF(SUM(IF(channel = 'paid_search', total_spend, 0)), 0)
  ), 2) AS paid_search_roas,
  ROUND(SAFE_DIVIDE(
    SUM(IF(channel = 'paid_ads', attributed_revenue, 0)),
    NULLIF(SUM(IF(channel = 'paid_ads', total_spend, 0)), 0)
  ), 2) AS paid_social_roas
FROM `bruin-playground-arsalan.reports.rpt_marketing_roi`
WHERE channel IN ('paid_search', 'paid_ads')
  AND report_date BETWEEN COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01')
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
GROUP BY report_date
ORDER BY report_date
