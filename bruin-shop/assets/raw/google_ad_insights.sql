/* @bruin
name: bruin_shop_raw.google_ad_insights
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake Google Ads campaign performance rows generated from the
  shared apparel marketing funnel.

depends:
  - bruin_shop_raw.marketing_funnel

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date
    type: DATE
    description: Campaign performance date
    primary_key: true
    nullable: false
  - name: campaign_id
    type: VARCHAR
    description: Google Ads campaign identifier
    primary_key: true
  - name: state_code
    type: VARCHAR
    description: Two-letter US state or district postal abbreviation
    primary_key: true
  - name: city
    type: VARCHAR
    description: US city market
    primary_key: true
  - name: campaign_name
    type: VARCHAR
    description: Google Ads campaign display name
  - name: special_event_id
    type: VARCHAR
    description: Synthetic event identifier when the row belongs to an injected campaign incident.
  - name: special_event_type
    type: VARCHAR
    description: Synthetic event type used for dashboard filtering.
  - name: special_event_name
    type: VARCHAR
    description: Human-readable synthetic event name.
  - name: event_phase
    type: VARCHAR
    description: Event phase such as campaign, outage, or recovery.
  - name: state_name
    type: VARCHAR
    description: Full US state or district name
  - name: spend
    type: DOUBLE
    description: Daily campaign spend in USD
  - name: impressions
    type: INTEGER
    description: Daily impressions
  - name: clicks
    type: INTEGER
    description: Daily clicks
  - name: conversions
    type: INTEGER
    description: Daily reported conversions

@bruin */

SELECT
    activity_date AS date,
    campaign_id,
    state_code,
    city,
    campaign_name,
    special_event_id,
    special_event_type,
    special_event_name,
    event_phase,
    state_name,
    spend,
    impressions,
    clicks,
    conversions
FROM bruin_shop_raw.marketing_funnel
WHERE channel = 'paid_search'
ORDER BY date, state_code, city
