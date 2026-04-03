/* @bruin
name: staging.polymarket_financial_overlay
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Cross-pipeline overlay: joins daily Polymarket prediction probabilities with
  oil prices (from hormuz-effect pipeline) and energy stock prices (from
  stock-market pipeline). Enables correlation analysis between prediction
  market sentiment and real financial market movements.

  Cross-pipeline references:
    - staging.hormuz_prices_wide (hormuz-effect pipeline)
    - stock_market_staging.prices_daily (stock-market pipeline)

depends:
  - raw.polymarket_price_history
  - staging.polymarket_markets_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: trade_date
    type: DATE
    description: Trading date
    primary_key: true
    nullable: false
  - name: question
    type: VARCHAR
    description: Polymarket prediction question
    primary_key: true
    nullable: false
  - name: topic
    type: VARCHAR
    description: Derived topic category from polymarket_markets_enriched
  - name: probability_pct
    type: DOUBLE
    description: Daily average implied probability (0-100)
  - name: brent_crude_usd
    type: DOUBLE
    description: Brent crude oil price (USD/barrel) from FRED
  - name: wti_crude_usd
    type: DOUBLE
    description: WTI crude oil price (USD/barrel) from FRED
  - name: natural_gas_usd
    type: DOUBLE
    description: Henry Hub natural gas price (USD/MMBtu) from FRED
  - name: yield_curve
    type: DOUBLE
    description: 10Y minus 2Y Treasury spread (percent) from FRED
  - name: consumer_sentiment
    type: DOUBLE
    description: U of Michigan Consumer Sentiment Index from FRED
  - name: xom_close
    type: DOUBLE
    description: ExxonMobil adjusted close price (USD)
  - name: cvx_close
    type: DOUBLE
    description: Chevron adjusted close price (USD)

@bruin */

WITH poly_daily AS (
    SELECT
        DATE(ph.timestamp) AS trade_date,
        ph.question,
        ph.condition_id,
        ROUND(AVG(ph.price) * 100, 2) AS probability_pct
    FROM raw.polymarket_price_history ph
    WHERE ph.outcome_label = 'Yes'
    GROUP BY DATE(ph.timestamp), ph.question, ph.condition_id
),

market_topics AS (
    SELECT DISTINCT question, topic
    FROM staging.polymarket_markets_enriched
),

oil_and_macro AS (
    SELECT
        observation_date AS trade_date,
        brent_crude_usd,
        wti_crude_usd,
        natural_gas_usd,
        yield_curve,
        consumer_sentiment
    FROM `bruin-playground-arsalan`.staging.hormuz_prices_wide
    WHERE observation_date >= '2025-12-01'
),

energy_stocks AS (
    SELECT
        date AS trade_date,
        MAX(CASE WHEN ticker = 'XOM' THEN adj_close END) AS xom_close,
        MAX(CASE WHEN ticker = 'CVX' THEN adj_close END) AS cvx_close
    FROM `bruin-playground-arsalan`.stock_market_staging.prices_daily
    WHERE ticker IN ('XOM', 'CVX')
      AND date >= '2025-12-01'
    GROUP BY date
)

SELECT
    p.trade_date,
    p.question,
    COALESCE(m.topic, 'Other') AS topic,
    p.probability_pct,
    o.brent_crude_usd,
    o.wti_crude_usd,
    o.natural_gas_usd,
    o.yield_curve,
    o.consumer_sentiment,
    s.xom_close,
    s.cvx_close
FROM poly_daily p
LEFT JOIN market_topics m ON p.question = m.question
LEFT JOIN oil_and_macro o ON p.trade_date = o.trade_date
LEFT JOIN energy_stocks s ON p.trade_date = s.trade_date
WHERE p.trade_date >= '2025-12-01'
ORDER BY p.trade_date, p.question
