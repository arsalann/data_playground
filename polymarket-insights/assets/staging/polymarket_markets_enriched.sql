/* @bruin
name: staging.polymarket_markets_enriched
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Transforms raw Polymarket market data into an analysis-ready table.
  Deduplicates appended data, parses JSON outcome/price fields,
  categorizes markets by topic, and computes derived metrics like
  implied probability and volume rankings.

depends:
  - raw.polymarket_markets

materialization:
  type: table
  strategy: create+replace

columns:
  - name: market_id
    type: VARCHAR
    description: Unique Polymarket market identifier
    primary_key: true
    nullable: false
  - name: question
    type: VARCHAR
    description: The prediction question being traded on
  - name: slug
    type: VARCHAR
    description: URL-friendly market identifier
  - name: category
    type: VARCHAR
    description: Original Polymarket category
  - name: topic
    type: VARCHAR
    description: Derived topic category based on question content
  - name: end_date
    type: TIMESTAMP
    description: Market resolution/expiry date
  - name: is_active
    type: BOOLEAN
    description: Whether the market is currently active
  - name: is_closed
    type: BOOLEAN
    description: Whether the market has been resolved
  - name: is_resolved_yes
    type: BOOLEAN
    description: Whether the market resolved YES (price near 1.0)
  - name: implied_probability_pct
    type: DOUBLE
    description: Current implied probability as percentage (0-100)
  - name: volume_total
    type: DOUBLE
    description: Total all-time trading volume in USD
  - name: volume_24h
    type: DOUBLE
    description: Trading volume in the last 24 hours in USD
  - name: volume_1w
    type: DOUBLE
    description: Trading volume in the last 7 days in USD
  - name: volume_1m
    type: DOUBLE
    description: Trading volume in the last 30 days in USD
  - name: liquidity
    type: DOUBLE
    description: Current liquidity in USD
  - name: last_trade_price
    type: DOUBLE
    description: Price of the most recent trade
  - name: price_change_1d
    type: DOUBLE
    description: Price change over the last 24 hours
  - name: price_change_1w
    type: DOUBLE
    description: Price change over the last 7 days
  - name: price_change_1m
    type: DOUBLE
    description: Price change over the last 30 days
  - name: spread
    type: DOUBLE
    description: Current bid-ask spread
  - name: created_at
    type: TIMESTAMP
    description: When the market was created
  - name: volume_rank
    type: INTEGER
    description: Rank by total volume (1 = highest)
  - name: volume_24h_rank
    type: INTEGER
    description: Rank by 24h volume (1 = highest)

@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.polymarket_markets
    WHERE market_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY extracted_at DESC) = 1
),

enriched AS (
    SELECT
        market_id,
        question,
        slug,
        COALESCE(NULLIF(category, ''), 'Uncategorized') AS category,
        end_date,
        active AS is_active,
        closed AS is_closed,
        COALESCE(volume_total, 0) AS volume_total,
        COALESCE(volume_24h, 0) AS volume_24h,
        COALESCE(volume_1w, 0) AS volume_1w,
        COALESCE(volume_1m, 0) AS volume_1m,
        COALESCE(liquidity, 0) AS liquidity,
        last_trade_price,
        COALESCE(price_change_1d, 0) AS price_change_1d,
        COALESCE(price_change_1w, 0) AS price_change_1w,
        COALESCE(price_change_1m, 0) AS price_change_1m,
        COALESCE(spread, 0) AS spread,
        created_at,
        event_slug,
        description,

        -- Implied probability from last trade price (0-100%)
        ROUND(COALESCE(last_trade_price, 0) * 100, 1) AS implied_probability_pct,

        -- Resolved YES if closed and price near 1.0
        CASE
            WHEN closed = TRUE AND last_trade_price >= 0.95 THEN TRUE
            ELSE FALSE
        END AS is_resolved_yes,

        -- Topic classification based on question keywords
        CASE
            WHEN LOWER(question) LIKE '%iran%' OR LOWER(question) LIKE '%tehran%' OR LOWER(question) LIKE '%khamenei%' OR LOWER(question) LIKE '%fordow%' THEN 'Iran Conflict'
            WHEN LOWER(question) LIKE '%trump%' OR LOWER(question) LIKE '%doge%' OR LOWER(question) LIKE '%maga%' THEN 'Trump & US Politics'
            WHEN LOWER(question) LIKE '%russia%' OR LOWER(question) LIKE '%ukraine%' OR LOWER(question) LIKE '%zelensky%' OR LOWER(question) LIKE '%putin%' THEN 'Russia-Ukraine'
            WHEN LOWER(question) LIKE '%china%' OR LOWER(question) LIKE '%xi jinping%' OR LOWER(question) LIKE '%taiwan%' THEN 'China & Taiwan'
            WHEN LOWER(question) LIKE '%bitcoin%' OR LOWER(question) LIKE '%btc%' OR LOWER(question) LIKE '%ethereum%' OR LOWER(question) LIKE '%eth %' OR LOWER(question) LIKE '%solana%' OR LOWER(question) LIKE '%crypto%' THEN 'Crypto'
            WHEN LOWER(question) LIKE '%recession%' OR LOWER(question) LIKE '%s&p%' OR LOWER(question) LIKE '%nasdaq%' OR LOWER(question) LIKE '%fed %' OR LOWER(question) LIKE '%tariff%' OR LOWER(question) LIKE '%interest rate%' THEN 'Economy & Markets'
            WHEN LOWER(question) LIKE '%ai %' OR LOWER(question) LIKE '%openai%' OR LOWER(question) LIKE '%chatgpt%' OR LOWER(question) LIKE '%artificial intelligence%' THEN 'AI & Tech'
            WHEN LOWER(question) LIKE '%nba%' OR LOWER(question) LIKE '%nfl%' OR LOWER(question) LIKE '%premier league%' OR LOWER(question) LIKE '%champions league%' OR LOWER(question) LIKE '%super bowl%' OR LOWER(question) LIKE '%world cup%' OR LOWER(question) LIKE '%ufc%' OR LOWER(question) LIKE '%boxing%' THEN 'Sports'
            WHEN LOWER(question) LIKE '%epstein%' OR LOWER(question) LIKE '%jfk%' OR LOWER(question) LIKE '%alien%' OR LOWER(question) LIKE '%ufo%' OR LOWER(question) LIKE '%uap%' THEN 'Conspiracies & Disclosure'
            WHEN LOWER(question) LIKE '%venezuela%' OR LOWER(question) LIKE '%greenland%' OR LOWER(question) LIKE '%mexico%' OR LOWER(question) LIKE '%canada%' OR LOWER(question) LIKE '%nato%' THEN 'US Foreign Policy'
            WHEN LOWER(question) LIKE '%oil%' OR LOWER(question) LIKE '%crude%' OR LOWER(question) LIKE '%energy%' OR LOWER(question) LIKE '%gas %' THEN 'Energy & Oil'
            WHEN LOWER(question) LIKE '%elon%' OR LOWER(question) LIKE '%musk%' OR LOWER(question) LIKE '%tesla%' OR LOWER(question) LIKE '%spacex%' THEN 'Elon Musk'
            WHEN LOWER(question) LIKE '%election%' OR LOWER(question) LIKE '%governor%' OR LOWER(question) LIKE '%senate%' OR LOWER(question) LIKE '%congress%' OR LOWER(question) LIKE '%democrat%' OR LOWER(question) LIKE '%republican%' THEN 'Elections'
            WHEN LOWER(question) LIKE '%israel%' OR LOWER(question) LIKE '%gaza%' OR LOWER(question) LIKE '%netanyahu%' OR LOWER(question) LIKE '%hamas%' OR LOWER(question) LIKE '%hezbollah%' THEN 'Israel & Middle East'
            ELSE 'Other'
        END AS topic

    FROM deduped
)

SELECT
    e.*,
    ROW_NUMBER() OVER (ORDER BY volume_total DESC) AS volume_rank,
    ROW_NUMBER() OVER (ORDER BY volume_24h DESC) AS volume_24h_rank
FROM enriched e
ORDER BY volume_total DESC
