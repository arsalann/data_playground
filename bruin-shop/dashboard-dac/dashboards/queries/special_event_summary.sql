WITH date_bounds AS (
  SELECT
    COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01') AS start_date,
    COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.end }}', '') AS DATE), DATE '2026-06-05') AS end_date
)

SELECT
  event_name,
  CASE event_type
    WHEN 'failed_campaign' THEN 'Failed campaign'
    WHEN 'stockout_campaign' THEN 'Stockout campaign'
    WHEN 'website_outage' THEN 'Website outage'
    WHEN 'successful_campaign' THEN 'Successful campaign'
    WHEN 'product_defect' THEN 'Product defect'
  END AS event_type,
  FORMAT_DATE('%Y-%m-%d', event_start_date) AS start_date,
  FORMAT_DATE('%Y-%m-%d', event_end_date) AS end_date,
  CASE primary_channel
    WHEN 'paid_ads' THEN 'Paid social'
    WHEN 'paid_search' THEN 'Paid search'
    WHEN 'all_channels' THEN 'All channels'
    ELSE INITCAP(REPLACE(primary_channel, '_', ' '))
  END AS channel,
  COALESCE(affected_product_name, affected_product_id, '-') AS affected_product,
  ROUND(total_spend, 2) AS spend_usd,
  impressions,
  clicks,
  platform_conversions,
  sessions,
  orders,
  paid_orders,
  refunded_orders,
  ROUND(attributed_revenue, 2) AS revenue_usd,
  ROUND(contribution_profit, 2) AS contribution_profit_usd,
  ctr_pct,
  conversion_rate_pct,
  refund_rate_pct,
  ROUND(revenue_delta_vs_baseline, 2) AS revenue_delta_vs_baseline_usd,
  sessions_delta_vs_baseline,
  expected_effect
FROM `bruin-playground-arsalan.reports.rpt_special_event_impact`
CROSS JOIN date_bounds db
WHERE event_start_date <= db.end_date
  AND event_end_date >= db.start_date
  AND (
    '{{ filters.event_type }}' = 'All event types'
    OR event_type = CASE '{{ filters.event_type }}'
      WHEN 'Failed campaign' THEN 'failed_campaign'
      WHEN 'Stockout campaign' THEN 'stockout_campaign'
      WHEN 'Website outage' THEN 'website_outage'
      WHEN 'Successful campaign' THEN 'successful_campaign'
      WHEN 'Product defect' THEN 'product_defect'
    END
  )
  AND (
    '{{ filters.channel }}' = 'All channels'
    OR primary_channel = 'all_channels'
    OR primary_channel = CASE '{{ filters.channel }}'
      WHEN 'Paid social' THEN 'paid_ads'
      WHEN 'Paid search' THEN 'paid_search'
      WHEN 'Email' THEN 'email'
      WHEN 'Organic search' THEN 'organic_search'
      WHEN 'Direct' THEN 'direct'
    END
  )
ORDER BY start_date
