WITH date_bounds AS (
  SELECT
    COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01') AS start_date,
    COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.end }}', '') AS DATE), DATE '2026-06-08') AS end_date
),
selected_events AS (
  SELECT event_start_date, event_end_date
  FROM `bruin-playground-arsalan.bruin_shop_raw.special_events`
  WHERE '{{ filters.event_type }}' = 'All event types'
    OR event_type = CASE '{{ filters.event_type }}'
      WHEN 'Failed campaign' THEN 'failed_campaign'
      WHEN 'Stockout campaign' THEN 'stockout_campaign'
      WHEN 'Website outage' THEN 'website_outage'
      WHEN 'Successful campaign' THEN 'successful_campaign'
      WHEN 'Product defect' THEN 'product_defect'
    END
)

SELECT
  FORMAT_DATE('%b %d', report_date) AS day_label,
  ROUND(SAFE_DIVIDE(SUM(total_clicks), NULLIF(SUM(total_impressions), 0)) * 100, 2) AS click_through_rate_pct,
  ROUND(SAFE_DIVIDE(SUM(total_conversions), NULLIF(SUM(total_clicks), 0)) * 100, 2) AS click_to_order_rate_pct,
  ROUND(SAFE_DIVIDE(SUM(attributed_revenue), NULLIF(SUM(total_spend), 0)), 2) AS roas
FROM `bruin-playground-arsalan.bruin_shop_reports.rpt_marketing_roi`
CROSS JOIN date_bounds db
WHERE channel IN ('paid_ads', 'paid_search')
  AND report_date BETWEEN db.start_date AND db.end_date
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
  AND (
    '{{ filters.event_type }}' = 'All event types'
    OR EXISTS (
      SELECT 1
      FROM selected_events se
      WHERE report_date BETWEEN se.event_start_date AND se.event_end_date
    )
  )
GROUP BY report_date
ORDER BY report_date
