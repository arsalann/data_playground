/* @bruin
name: staging.polymarket_headline_events
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Curates the most significant resolved and active Polymarket events.
  Identifies markets that represent major world events based on volume,
  resolution status, and topic relevance. Groups related markets by
  event slug to show the full narrative arc of each story.

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
  - name: is_active
    type: BOOLEAN
    description: Whether the market is still active
  - name: is_closed
    type: BOOLEAN
    description: Whether the market has resolved
  - name: is_resolved_yes
    type: BOOLEAN
    description: Whether resolved YES
  - name: outcome_label
    type: VARCHAR
    description: Human-readable outcome (YES/NO/ACTIVE)
  - name: implied_probability_pct
    type: DOUBLE
    description: Current implied probability
  - name: volume_total
    type: DOUBLE
    description: Total volume in USD
  - name: volume_24h
    type: DOUBLE
    description: 24h volume in USD
  - name: event_slug
    type: VARCHAR
    description: Parent event grouping
  - name: end_date
    type: TIMESTAMP
    description: Resolution date
  - name: headline_tier
    type: VARCHAR
    description: Significance tier (Tier 1 = >$50M, Tier 2 = >$10M, Tier 3 = >$1M)

@bruin */

SELECT
    market_id,
    question,
    topic,
    is_active,
    is_closed,
    is_resolved_yes,
    CASE
        WHEN is_closed = TRUE AND is_resolved_yes = TRUE THEN 'Resolved YES'
        WHEN is_closed = TRUE AND is_resolved_yes = FALSE THEN 'Resolved NO'
        ELSE CONCAT('Active (', CAST(implied_probability_pct AS STRING), '%)')
    END AS outcome_label,
    implied_probability_pct,
    volume_total,
    volume_24h,
    event_slug,
    end_date,
    CASE
        WHEN volume_total >= 50000000 THEN 'Tier 1: Mega ($50M+)'
        WHEN volume_total >= 10000000 THEN 'Tier 2: Major ($10M+)'
        WHEN volume_total >= 1000000 THEN 'Tier 3: Notable ($1M+)'
        ELSE 'Tier 4: Minor (<$1M)'
    END AS headline_tier
FROM staging.polymarket_markets_enriched
WHERE volume_total >= 1000000
ORDER BY volume_total DESC
