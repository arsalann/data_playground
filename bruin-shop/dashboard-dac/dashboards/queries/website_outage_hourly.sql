SELECT
  FORMAT_TIMESTAMP('%b %d %H:%M', activity_hour) AS hour_label,
  SUM(sessions) AS sessions,
  SUM(purchase_events) AS purchase_events
FROM `bruin-playground-arsalan.bruin_shop_raw.ga4_hourly_sessions`
WHERE session_date BETWEEN DATE '2026-02-04' AND DATE '2026-02-05'
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
  AND '{{ filters.event_type }}' IN ('All event types', 'Website outage')
GROUP BY activity_hour
ORDER BY activity_hour
