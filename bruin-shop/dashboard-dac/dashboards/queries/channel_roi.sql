WITH filtered_spend AS (
  SELECT
    spend_date,
    channel,
    SUM(spend) AS total_spend
  FROM `bruin-playground-arsalan.bruin_shop_staging.stg_marketing_spend`
  WHERE spend_date BETWEEN COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01')
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
  GROUP BY spend_date, channel
),
filtered_orders AS (
  SELECT
    DATE(order_date) AS order_date,
    source_channel AS channel,
    SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN order_total ELSE 0 END) AS attributed_revenue,
    SUM(CASE
      WHEN payment_status IN ('paid', 'partially_refunded')
        THEN order_total - COALESCE(cogs_amount, 0) - COALESCE(shipping_cost, 0)
      ELSE 0
    END) AS gross_profit
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
      '{{ filters.event_type }}' = 'All event types'
      OR special_event_type = CASE '{{ filters.event_type }}'
        WHEN 'Failed campaign' THEN 'failed_campaign'
        WHEN 'Stockout campaign' THEN 'stockout_campaign'
        WHEN 'Website outage' THEN 'website_outage'
        WHEN 'Successful campaign' THEN 'successful_campaign'
        WHEN 'Product defect' THEN 'product_defect'
      END
    )
  GROUP BY order_date, channel
),
keys AS (
  SELECT spend_date AS report_date, channel FROM filtered_spend
  UNION DISTINCT
  SELECT order_date AS report_date, channel FROM filtered_orders
)

SELECT
  CASE k.channel
    WHEN 'paid_search' THEN 'Paid search'
    WHEN 'paid_ads' THEN 'Paid social'
    WHEN 'email' THEN 'Email'
    ELSE INITCAP(REPLACE(k.channel, '_', ' '))
  END AS channel,
  ROUND(SUM(COALESCE(fs.total_spend, 0)), 2) AS total_spend_usd,
  ROUND(SUM(COALESCE(fo.attributed_revenue, 0)), 2) AS attributed_revenue_usd,
  ROUND(SUM(COALESCE(fo.gross_profit, 0)) - SUM(COALESCE(fs.total_spend, 0)), 2) AS contribution_profit_usd,
  ROUND(SAFE_DIVIDE(SUM(COALESCE(fo.attributed_revenue, 0)), NULLIF(SUM(COALESCE(fs.total_spend, 0)), 0)), 2) AS roas
FROM keys k
LEFT JOIN filtered_spend fs
  ON k.report_date = fs.spend_date
  AND k.channel = fs.channel
LEFT JOIN filtered_orders fo
  ON k.report_date = fo.order_date
  AND k.channel = fo.channel
GROUP BY channel
ORDER BY total_spend_usd DESC, attributed_revenue_usd DESC
