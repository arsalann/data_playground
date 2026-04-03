/* @bruin
name: staging.polymarket_topic_summary
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates Polymarket data by topic to show where the money flows.
  Computes total volume, market counts, resolution rates, and
  average implied probabilities per topic category.

depends:
  - staging.polymarket_markets_enriched

materialization:
  type: table
  strategy: create+replace

columns:
  - name: topic
    type: VARCHAR
    description: Derived topic category
    primary_key: true
    nullable: false
  - name: market_count
    type: INTEGER
    description: Total number of markets in this topic
  - name: active_count
    type: INTEGER
    description: Number of currently active (unresolved) markets
  - name: resolved_yes_count
    type: INTEGER
    description: Number of markets that resolved YES
  - name: resolved_no_count
    type: INTEGER
    description: Number of markets that resolved NO
  - name: yes_rate_pct
    type: DOUBLE
    description: Percentage of resolved markets that resolved YES
  - name: total_volume
    type: DOUBLE
    description: Total all-time trading volume in USD across all markets
  - name: total_volume_24h
    type: DOUBLE
    description: Total 24h trading volume across all markets
  - name: total_volume_1w
    type: DOUBLE
    description: Total 7-day trading volume across all markets
  - name: avg_implied_probability
    type: DOUBLE
    description: Average implied probability across active markets
  - name: top_market_question
    type: VARCHAR
    description: The highest-volume market question in this topic
  - name: top_market_volume
    type: DOUBLE
    description: Volume of the highest-volume market
  - name: volume_share_pct
    type: DOUBLE
    description: This topic's share of total platform volume

@bruin */

WITH topic_stats AS (
    SELECT
        topic,
        COUNT(*) AS market_count,
        COUNTIF(is_active = TRUE AND is_closed = FALSE) AS active_count,
        COUNTIF(is_resolved_yes = TRUE) AS resolved_yes_count,
        COUNTIF(is_closed = TRUE AND is_resolved_yes = FALSE) AS resolved_no_count,
        SUM(volume_total) AS total_volume,
        SUM(volume_24h) AS total_volume_24h,
        SUM(volume_1w) AS total_volume_1w,
        AVG(CASE WHEN is_active = TRUE AND is_closed = FALSE THEN implied_probability_pct END) AS avg_implied_probability
    FROM staging.polymarket_markets_enriched
    GROUP BY topic
),

top_markets AS (
    SELECT
        topic,
        question AS top_market_question,
        volume_total AS top_market_volume,
        ROW_NUMBER() OVER (PARTITION BY topic ORDER BY volume_total DESC) AS rn
    FROM staging.polymarket_markets_enriched
),

platform_total AS (
    SELECT SUM(volume_total) AS total_platform_volume
    FROM staging.polymarket_markets_enriched
)

SELECT
    t.topic,
    t.market_count,
    t.active_count,
    t.resolved_yes_count,
    t.resolved_no_count,
    ROUND(
        t.resolved_yes_count / NULLIF(t.resolved_yes_count + t.resolved_no_count, 0) * 100, 1
    ) AS yes_rate_pct,
    ROUND(t.total_volume, 0) AS total_volume,
    ROUND(t.total_volume_24h, 0) AS total_volume_24h,
    ROUND(t.total_volume_1w, 0) AS total_volume_1w,
    ROUND(t.avg_implied_probability, 1) AS avg_implied_probability,
    m.top_market_question,
    ROUND(m.top_market_volume, 0) AS top_market_volume,
    ROUND(t.total_volume / NULLIF(p.total_platform_volume, 0) * 100, 1) AS volume_share_pct
FROM topic_stats t
LEFT JOIN top_markets m ON t.topic = m.topic AND m.rn = 1
CROSS JOIN platform_total p
ORDER BY t.total_volume DESC
