WITH date_bounds AS (
  SELECT
    COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01') AS start_date,
    COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.end }}', '') AS DATE), DATE '2026-06-05') AS end_date
)

SELECT
  CASE
    WHEN event_name = 'Google search broad-match test failure' THEN 'Failed search test'
    WHEN event_name = 'Black Tote Bag defect refund incident' THEN 'Tote defect'
    WHEN event_name = 'Website checkout outage and recovery' THEN 'Checkout outage'
    WHEN event_name = 'Google Memorial Day search campaign win' THEN 'Search campaign win'
    WHEN event_name = 'Instagram trail-shoe launch and stockout' THEN 'Trail-shoe stockout'
    WHEN event_name = 'Instagram spring outfit campaign win' THEN 'Spring social win'
    ELSE event_name
  END AS event_label,
  CASE event_type
    WHEN 'failed_campaign' THEN 'Failed campaign'
    WHEN 'stockout_campaign' THEN 'Stockout campaign'
    WHEN 'website_outage' THEN 'Website outage'
    WHEN 'successful_campaign' THEN 'Successful campaign'
    WHEN 'product_defect' THEN 'Product defect'
  END AS event_type,
  ROUND(revenue_delta_vs_baseline, 2) AS revenue_delta_usd,
  ROUND(contribution_profit, 2) AS contribution_profit_usd,
  ROUND(total_spend, 2) AS spend_usd,
  paid_orders,
  refunded_orders,
  sessions_delta_vs_baseline,
  ROUND(COALESCE(refund_rate_pct, 0), 2) AS refund_rate_pct
FROM `bruin-playground-arsalan.bruin_shop_reports.rpt_special_event_impact`
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
ORDER BY revenue_delta_usd
