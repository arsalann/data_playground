/* @bruin
name: staging.polymarket_biggest_movers
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Identifies the biggest price movers on Polymarket across different
  time horizons. Captures markets with the largest absolute price changes
  in the last day, week, and month, along with volume and topic context.

depends:
  - staging.polymarket_markets_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: market_id
    type: VARCHAR
    description: Unique market identifier
    primary_key: true
    nullable: false
  - name: question
    type: VARCHAR
    description: The prediction question
  - name: topic
    type: VARCHAR
    description: Derived topic category
  - name: implied_probability_pct
    type: DOUBLE
    description: Current implied probability (0-100)
  - name: price_change_1d
    type: DOUBLE
    description: Price change in the last 24 hours
  - name: price_change_1w
    type: DOUBLE
    description: Price change in the last 7 days
  - name: price_change_1m
    type: DOUBLE
    description: Price change in the last 30 days
  - name: abs_change_1d
    type: DOUBLE
    description: Absolute price change in the last 24 hours
  - name: abs_change_1w
    type: DOUBLE
    description: Absolute price change in the last 7 days
  - name: abs_change_1m
    type: DOUBLE
    description: Absolute price change in the last 30 days
  - name: volume_total
    type: DOUBLE
    description: Total trading volume in USD
  - name: volume_24h
    type: DOUBLE
    description: 24-hour trading volume in USD
  - name: is_active
    type: BOOLEAN
    description: Whether the market is still active
  - name: mover_rank_1d
    type: INTEGER
    description: Rank by absolute 1-day price change
  - name: mover_rank_1w
    type: INTEGER
    description: Rank by absolute 7-day price change
  - name: mover_rank_1m
    type: INTEGER
    description: Rank by absolute 30-day price change

@bruin */

SELECT
    market_id,
    question,
    topic,
    implied_probability_pct,
    price_change_1d,
    price_change_1w,
    price_change_1m,
    ABS(price_change_1d) AS abs_change_1d,
    ABS(price_change_1w) AS abs_change_1w,
    ABS(price_change_1m) AS abs_change_1m,
    volume_total,
    volume_24h,
    is_active,
    ROW_NUMBER() OVER (ORDER BY ABS(price_change_1d) DESC) AS mover_rank_1d,
    ROW_NUMBER() OVER (ORDER BY ABS(price_change_1w) DESC) AS mover_rank_1w,
    ROW_NUMBER() OVER (ORDER BY ABS(price_change_1m) DESC) AS mover_rank_1m
FROM staging.polymarket_markets_enriched
WHERE volume_total > 100000
ORDER BY ABS(price_change_1d) DESC
