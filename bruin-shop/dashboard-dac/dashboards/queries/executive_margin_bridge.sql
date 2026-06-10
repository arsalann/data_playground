WITH filtered_orders AS (
  SELECT *
  FROM `bruin-playground-arsalan.bruin_shop_staging.stg_orders`
  WHERE DATE(order_date) BETWEEN COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01')
      AND COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.end }}', '') AS DATE), DATE '2026-06-08')
    AND (
      '{{ filters.channel }}' = 'All channels'
      OR source_channel = CASE '{{ filters.channel }}'
        WHEN 'Paid social' THEN 'paid_ads'
        WHEN 'Paid search' THEN 'paid_search'
        WHEN 'Email' THEN 'email'
        WHEN 'Organic search' THEN 'organic_search'
        WHEN 'Direct' THEN 'direct'
      END
    )
    AND (
      '{{ filters.category }}' = 'All categories'
      OR primary_category = '{{ filters.category }}'
    )
    AND (
      '{{ filters.event_type }}' = 'All event types'
      OR special_event_type = CASE '{{ filters.event_type }}'
        WHEN 'Failed campaign' THEN 'failed_campaign'
        WHEN 'Stockout campaign' THEN 'stockout_campaign'
        WHEN 'Website outage' THEN 'website_outage'
        WHEN 'Successful campaign' THEN 'successful_campaign'
        WHEN 'Product defect' THEN 'product_defect'
      END
    )
),
order_totals AS (
  SELECT
    ROUND(SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN order_total ELSE 0 END), 2) AS net_revenue_usd,
    ROUND(SUM(CASE
      WHEN payment_status IN ('paid', 'partially_refunded')
        THEN order_total - COALESCE(cogs_amount, 0) - COALESCE(shipping_cost, 0)
      ELSE 0
    END), 2) AS gross_profit_usd
  FROM filtered_orders
),
media_spend AS (
  SELECT
    ROUND(SUM(total_spend), 2) AS spend_usd
  FROM `bruin-playground-arsalan.bruin_shop_reports.rpt_marketing_roi`
  WHERE report_date BETWEEN COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01')
      AND COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.end }}', '') AS DATE), DATE '2026-06-08')
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
)

SELECT 'Net revenue' AS stage, net_revenue_usd AS amount_usd, 1 AS sort_order
FROM order_totals
UNION ALL
SELECT 'Gross profit' AS stage, gross_profit_usd AS amount_usd, 2 AS sort_order
FROM order_totals
UNION ALL
SELECT 'Contribution profit' AS stage, gross_profit_usd - COALESCE(spend_usd, 0) AS amount_usd, 3 AS sort_order
FROM order_totals
CROSS JOIN media_spend
ORDER BY sort_order
