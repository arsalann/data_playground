WITH market_funnel AS (
  SELECT *
  FROM (
    WITH paid_social AS (
      SELECT
        spend_date,
        state_code,
        city,
        SUM(spend) AS spend,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(conversions) AS platform_conversions
      FROM `bruin-playground-arsalan.bruin_shop_staging.stg_marketing_spend`
      WHERE channel = 'paid_ads'
        AND spend_date BETWEEN DATE '2025-05-01' AND DATE '2025-05-31'
      GROUP BY spend_date, state_code, city
    ),
    orders AS (
      SELECT
        DATE(order_date) AS order_date,
        state_code,
        city,
        COUNT(*) AS orders,
        SUM(order_total) AS revenue,
        SUM(cogs_amount) AS cogs,
        SUM(shipping_cost) AS shipping_cost,
        SUM(order_total - cogs_amount - shipping_cost) AS gross_profit
      FROM `bruin-playground-arsalan.bruin_shop_staging.stg_orders`
      WHERE source_channel = 'paid_ads'
        AND payment_status IN ('paid', 'partially_refunded')
        AND DATE(order_date) BETWEEN DATE '2025-05-01' AND DATE '2025-05-31'
      GROUP BY order_date, state_code, city
    )
    SELECT
      FORMAT('%s, %s', ps.city, ps.state_code) AS market,
      ROUND(SUM(ps.spend), 2) AS spend_usd,
      ROUND(SUM(o.revenue), 2) AS revenue_usd,
      ROUND(SUM(o.gross_profit) - SUM(ps.spend), 2) AS contribution_profit_usd
    FROM paid_social ps
    LEFT JOIN orders o
      ON ps.spend_date = o.order_date
      AND ps.state_code = o.state_code
      AND ps.city = o.city
    GROUP BY market
  )
  ORDER BY spend_usd DESC
  LIMIT 12
)

SELECT
  market,
  spend_usd,
  revenue_usd,
  contribution_profit_usd,
  ROUND(SAFE_DIVIDE(contribution_profit_usd, NULLIF(spend_usd, 0)), 2) AS profit_per_spend_dollar
FROM market_funnel
ORDER BY contribution_profit_usd DESC
