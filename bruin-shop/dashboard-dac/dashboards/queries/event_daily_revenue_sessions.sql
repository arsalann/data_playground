WITH date_bounds AS (
  SELECT
    COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01') AS start_date,
    COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.end }}', '') AS DATE), DATE '2026-06-05') AS end_date
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
),
orders AS (
  SELECT
    DATE(order_date) AS activity_date,
    ROUND(SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN order_total ELSE 0 END), 2) AS net_revenue_usd,
    COUNTIF(payment_status IN ('paid', 'partially_refunded')) AS paid_orders
  FROM `bruin-playground-arsalan.bruin_shop_staging.stg_orders`
  WHERE '{{ filters.channel }}' = 'All channels'
    OR source_channel = CASE '{{ filters.channel }}'
      WHEN 'Paid social' THEN 'paid_ads'
      WHEN 'Paid search' THEN 'paid_search'
      WHEN 'Email' THEN 'email'
      WHEN 'Organic search' THEN 'organic_search'
      WHEN 'Direct' THEN 'direct'
    END
  GROUP BY activity_date
),
sessions AS (
  SELECT
    session_date AS activity_date,
    SUM(total_sessions) AS sessions,
    SUM(purchase_events) AS purchase_events
  FROM `bruin-playground-arsalan.bruin_shop_staging.stg_web_sessions`
  WHERE '{{ filters.channel }}' = 'All channels'
    OR channel = CASE '{{ filters.channel }}'
      WHEN 'Paid social' THEN 'paid_ads'
      WHEN 'Paid search' THEN 'paid_search'
      WHEN 'Email' THEN 'email'
      WHEN 'Organic search' THEN 'organic_search'
      WHEN 'Direct' THEN 'direct'
    END
  GROUP BY activity_date
),
spend AS (
  SELECT
    spend_date AS activity_date,
    ROUND(SUM(spend), 2) AS total_spend_usd
  FROM `bruin-playground-arsalan.bruin_shop_staging.stg_marketing_spend`
  WHERE '{{ filters.channel }}' = 'All channels'
    OR channel = CASE '{{ filters.channel }}'
      WHEN 'Paid social' THEN 'paid_ads'
      WHEN 'Paid search' THEN 'paid_search'
      WHEN 'Email' THEN 'email'
      WHEN 'Organic search' THEN 'organic_search'
      WHEN 'Direct' THEN 'direct'
    END
  GROUP BY activity_date
),
keys AS (
  SELECT activity_date FROM orders
  UNION DISTINCT
  SELECT activity_date FROM sessions
  UNION DISTINCT
  SELECT activity_date FROM spend
)

SELECT
  FORMAT_DATE('%b %d', k.activity_date) AS day_label,
  COALESCE(o.net_revenue_usd, 0) AS net_revenue_usd,
  COALESCE(o.paid_orders, 0) AS paid_orders,
  COALESCE(s.sessions, 0) AS sessions,
  COALESCE(s.purchase_events, 0) AS purchase_events,
  COALESCE(sp.total_spend_usd, 0) AS total_spend_usd
FROM keys k
CROSS JOIN date_bounds db
LEFT JOIN orders o USING (activity_date)
LEFT JOIN sessions s USING (activity_date)
LEFT JOIN spend sp USING (activity_date)
WHERE k.activity_date BETWEEN db.start_date AND db.end_date
  AND (
    '{{ filters.event_type }}' = 'All event types'
    OR EXISTS (
      SELECT 1
      FROM selected_events se
      WHERE k.activity_date BETWEEN se.event_start_date AND se.event_end_date
    )
  )
ORDER BY k.activity_date
