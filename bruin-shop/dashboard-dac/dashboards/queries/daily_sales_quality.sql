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

daily AS (
  SELECT
    DATE(order_date) AS order_day,
    ROUND(SUM(order_total), 2) AS gross_revenue_usd,
    ROUND(SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN order_total ELSE 0 END), 2) AS net_revenue_usd,
    ROUND(SUM(COALESCE(discount_amount, 0)), 2) AS discount_usd,
    ROUND(SUM(CASE
      WHEN payment_status IN ('paid', 'partially_refunded')
        THEN order_total - COALESCE(cogs_amount, 0) - COALESCE(shipping_cost, 0)
      ELSE 0
    END), 2) AS gross_profit_usd,
    ROUND(SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN COALESCE(cogs_amount, 0) ELSE 0 END), 2) AS cogs_usd,
    ROUND(SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN COALESCE(shipping_cost, 0) ELSE 0 END), 2) AS shipping_cost_usd,
    ROUND(SAFE_DIVIDE(
      SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN order_total ELSE 0 END),
      NULLIF(COUNTIF(payment_status IN ('paid', 'partially_refunded')), 0)
    ), 2) AS avg_order_value_usd,
    ROUND(SAFE_DIVIDE(
      SUM(CASE
        WHEN payment_status IN ('paid', 'partially_refunded')
          THEN order_total - COALESCE(cogs_amount, 0) - COALESCE(shipping_cost, 0)
        ELSE 0
      END),
      NULLIF(SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN order_total ELSE 0 END), 0)
    ) * 100, 2) AS gross_margin_pct,
    COUNTIF(payment_status IN ('paid', 'partially_refunded')) AS paid_orders,
    SUM(CASE WHEN payment_status IN ('paid', 'partially_refunded') THEN COALESCE(item_count, 0) ELSE 0 END) AS total_items,
    COUNTIF(cancel_reason IS NOT NULL) AS cancelled_orders,
    ROUND(SAFE_DIVIDE(COUNTIF(cancel_reason IS NOT NULL), COUNT(*)) * 100, 2) AS cancellation_rate
  FROM filtered_orders
  GROUP BY order_day
)

SELECT
  FORMAT_DATE('%b %d', order_day) AS day_label,
  gross_revenue_usd,
  net_revenue_usd,
  discount_usd,
  gross_profit_usd,
  cogs_usd,
  shipping_cost_usd,
  avg_order_value_usd,
  gross_margin_pct,
  paid_orders,
  total_items,
  cancelled_orders,
  cancellation_rate
FROM daily
ORDER BY order_day
