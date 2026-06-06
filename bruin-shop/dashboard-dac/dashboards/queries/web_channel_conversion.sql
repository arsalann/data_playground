SELECT
  CASE channel
    WHEN 'paid_search' THEN 'Paid search'
    WHEN 'paid_ads' THEN 'Paid social'
    WHEN 'organic_search' THEN 'Organic search'
    WHEN 'direct' THEN 'Direct'
    WHEN 'email' THEN 'Email'
    ELSE INITCAP(REPLACE(channel, '_', ' '))
  END AS channel,
  SUM(total_sessions) AS sessions,
  SUM(engaged_sessions) AS engaged_sessions,
  SUM(purchase_events) AS purchases,
  ROUND(SAFE_DIVIDE(SUM(engaged_sessions), NULLIF(SUM(total_sessions), 0)) * 100, 2) AS engagement_rate_pct,
  ROUND(SAFE_DIVIDE(SUM(purchase_events), NULLIF(SUM(total_sessions), 0)) * 100, 2) AS conversion_rate_pct
FROM `bruin-playground-arsalan.bruin_shop_staging.stg_web_sessions`
WHERE session_date BETWEEN COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01')
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
ORDER BY conversion_rate_pct DESC
