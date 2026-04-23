/* @bruin
name: epias_staging.h3_hormuz_risk_mcp
type: bq.sql
connection: bruin-playground-arsalan
description: |
  H3: "Geopolitical risk in the Strait of Hormuz is priced into Turkey's electricity"
  Joins Polymarket Iran conflict prediction probabilities with EPIAS MCP prices
  and Brent oil prices to test whether geopolitical risk leads price movements.

  Cross-pipeline references:
    - polymarket-insights: raw.polymarket_price_history, staging.polymarket_markets_enriched
    - hormuz-effect: staging.hormuz_prices_wide

depends:
  - epias_staging.epias_market_prices_daily

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date
    type: DATE
    description: Calendar date
    primary_key: true
    nullable: false
  - name: iran_conflict_prob
    type: DOUBLE
    description: Average daily probability across Iran conflict markets (0-1)
  - name: iran_prob_7d_avg
    type: DOUBLE
    description: 7-day rolling average of Iran conflict probability
  - name: iran_prob_7d_change
    type: DOUBLE
    description: 7-day change in Iran conflict probability (pp)
  - name: brent_usd
    type: DOUBLE
    description: Brent crude oil price (USD/barrel)
  - name: nat_gas_usd
    type: DOUBLE
    description: Henry Hub natural gas price (USD/MMBtu)
  - name: mcp_avg_try
    type: DOUBLE
    description: Average daily MCP in Turkish Lira
  - name: mcp_avg_eur
    type: DOUBLE
    description: Average daily MCP in Euros
  - name: mcp_eur_7d_avg
    type: DOUBLE
    description: 7-day rolling average MCP in Euros
  - name: spread_avg
    type: DOUBLE
    description: Average daily SMP-MCP spread

@bruin */

WITH iran_questions AS (
    SELECT DISTINCT question
    FROM `bruin-playground-arsalan`.staging.polymarket_markets_enriched
    WHERE topic = 'Iran Conflict'
),

poly_daily AS (
    SELECT
        DATE(ph.timestamp) AS date,
        AVG(ph.price) AS iran_conflict_prob
    FROM `bruin-playground-arsalan`.raw.polymarket_price_history ph
    INNER JOIN iran_questions iq ON ph.question = iq.question
    WHERE ph.outcome_label = 'Yes'
    GROUP BY 1
),

poly_enriched AS (
    SELECT
        date,
        ROUND(iran_conflict_prob, 4) AS iran_conflict_prob,
        ROUND(AVG(iran_conflict_prob) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 4) AS iran_prob_7d_avg,
        ROUND(
            iran_conflict_prob - LAG(iran_conflict_prob, 7) OVER (ORDER BY date),
            4
        ) AS iran_prob_7d_change
    FROM poly_daily
),

oil_prices AS (
    SELECT
        observation_date AS date,
        brent_crude_usd AS brent_usd,
        natural_gas_usd AS nat_gas_usd
    FROM `bruin-playground-arsalan`.staging.hormuz_prices_wide
    WHERE observation_date >= '2024-01-01'
),

mcp AS (
    SELECT
        date,
        mcp_avg AS mcp_avg_try,
        mcp_avg_eur,
        spread_avg,
        ROUND(AVG(mcp_avg_eur) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 4) AS mcp_eur_7d_avg
    FROM epias_staging.epias_market_prices_daily
    WHERE date >= '2024-01-01'
)

SELECT
    m.date,
    p.iran_conflict_prob,
    p.iran_prob_7d_avg,
    p.iran_prob_7d_change,
    o.brent_usd,
    o.nat_gas_usd,
    m.mcp_avg_try,
    m.mcp_avg_eur,
    m.mcp_eur_7d_avg,
    m.spread_avg

FROM mcp m
LEFT JOIN poly_enriched p ON m.date = p.date
LEFT JOIN oil_prices o ON m.date = o.date
WHERE m.date < DATE_TRUNC(CURRENT_DATE(), MONTH)
ORDER BY m.date
