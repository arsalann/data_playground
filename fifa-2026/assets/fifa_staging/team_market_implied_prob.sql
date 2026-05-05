/* @bruin

name: fifa_staging.team_market_implied_prob
type: bq.sql
description: |
  Latest implied probability per team for the Polymarket "FIFA 2026 Winner"
  market. One row per qualified team.

  Method:
    1. Filter `polymarket_fifa_markets` to events whose series/event/title
       reference the FIFA-2026 winner market, take the most recent snapshot.
    2. Map each market's `question` (e.g. "Will Argentina win the 2026 FIFA
       World Cup?") to a fifa_code via a lookup against qualified_teams.name.
    3. Use the first element of `outcome_prices` (a JSON array `[YES, NO]`)
       as the latest yes_price.
    4. Normalise prices to sum to 1 across the field (naive vig removal).

  Note: this build sources prices from the Gamma `outcome_prices` snapshot
  rather than the CLOB price-history feed. Snapshot freshness equals
  `extracted_at` on the markets table. CLOB tick history is stubbed in
  `polymarket_fifa_clob` (see asset doc); when that asset is re-enabled,
  swap the price source back to per-token CLOB ticks for `last_trade_at`
  and a 24-hour volume rolling proxy.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - staging
  - prediction_markets

materialization:
  type: table
  strategy: create+replace

depends:
  - fifa_raw.polymarket_fifa_markets
  - fifa_raw.qualified_teams

@bruin */

WITH latest_market_snap AS (
  SELECT MAX(extracted_at) AS snap_ts
  FROM `bruin-playground-arsalan.fifa_raw.polymarket_fifa_markets`
),
winner_markets AS (
  SELECT
    m.market_id,
    m.condition_id,
    m.question,
    m.volume,
    m.outcome_prices,
    m.extracted_at
  FROM `bruin-playground-arsalan.fifa_raw.polymarket_fifa_markets` m, latest_market_snap l
  WHERE m.extracted_at = l.snap_ts
    AND m.outcome_prices IS NOT NULL
    AND LOWER(m.event_title) LIKE '%2026%'
    AND (
      LOWER(m.event_slug)  LIKE '%winner%'
      OR LOWER(m.series_slug) LIKE '%winner%'
      OR LOWER(m.event_title) LIKE '%winner%'
    )
),
team_match AS (
  SELECT
    w.market_id,
    w.condition_id,
    w.question,
    w.volume,
    w.outcome_prices,
    w.extracted_at,
    t.fifa_code
  FROM winner_markets w
  JOIN `bruin-playground-arsalan.fifa_raw.qualified_teams` t
    ON LOWER(w.question) LIKE CONCAT('%', LOWER(t.name), '%')
),
priced AS (
  SELECT
    fifa_code,
    market_id,
    volume,
    extracted_at,
    SAFE_CAST(JSON_VALUE_ARRAY(outcome_prices)[SAFE_OFFSET(0)] AS FLOAT64) AS yes_price
  FROM team_match
),
best_per_team AS (
  /* If the team matches multiple winner-style markets, pick the highest-volume one. */
  SELECT
    fifa_code,
    market_id,
    yes_price,
    volume,
    extracted_at,
    ROW_NUMBER() OVER (PARTITION BY fifa_code ORDER BY volume DESC NULLS LAST) AS rn
  FROM priced
  WHERE yes_price IS NOT NULL
),
normalised AS (
  SELECT
    fifa_code,
    yes_price,
    volume,
    extracted_at,
    SUM(yes_price) OVER ()                          AS sum_yes,
    yes_price / NULLIF(SUM(yes_price) OVER (), 0)   AS market_implied_prob
  FROM best_per_team
  WHERE rn = 1
)
SELECT
  fifa_code,
  yes_price,
  market_implied_prob,
  extracted_at                                            AS last_trade_at,
  volume                                                  AS market_volume,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), extracted_at, HOUR) AS hours_since_last_trade,
  CASE
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), extracted_at, DAY) > 7 THEN TRUE
    ELSE FALSE
  END                                                     AS is_stale,
  CURRENT_TIMESTAMP() AS staged_at
FROM normalised
