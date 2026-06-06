/* @bruin
name: staging.stg_marketing_spend
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Combines fake Meta Ads, Google Ads, and Klaviyo campaign activity into a
  common daily channel-performance table with city and state geography.

depends:
  - raw.facebook_ad_insights
  - raw.google_ad_insights
  - raw.klaviyo_campaigns
  - raw.klaviyo_metrics

materialization:
  type: table
  strategy: create+replace

columns:
  - name: spend_date
    type: DATE
    description: Daily marketing activity date
    nullable: false
  - name: channel
    type: VARCHAR
    description: Normalized marketing channel
  - name: campaign_name
    type: VARCHAR
    description: Campaign display name
  - name: special_event_id
    type: VARCHAR
    description: Synthetic event identifier when the marketing row belongs to an injected campaign incident.
  - name: special_event_type
    type: VARCHAR
    description: Synthetic event type used for dashboard filtering.
  - name: special_event_name
    type: VARCHAR
    description: Human-readable synthetic event name.
  - name: event_phase
    type: VARCHAR
    description: Event phase such as launch_push, stockout_waste, campaign, outage, or recovery.
  - name: state_code
    type: VARCHAR
    description: Two-letter US state or district postal abbreviation
  - name: state_name
    type: VARCHAR
    description: Full US state or district name
  - name: city
    type: VARCHAR
    description: US city market
  - name: spend
    type: DOUBLE
    description: Daily spend in USD
  - name: impressions
    type: INTEGER
    description: Daily impressions or recipients
  - name: clicks
    type: INTEGER
    description: Daily clicks
  - name: conversions
    type: INTEGER
    description: Daily platform-reported conversions

@bruin */

WITH facebook AS (
    SELECT
        DATE(date_start) AS spend_date,
        'paid_ads' AS channel,
        campaign_name,
        special_event_id,
        special_event_type,
        special_event_name,
        event_phase,
        state_code,
        state_name,
        city,
        CAST(COALESCE(spend, 0) AS NUMERIC) AS spend,
        CAST(COALESCE(impressions, 0) AS INT64) AS impressions,
        CAST(COALESCE(clicks, 0) AS INT64) AS clicks,
        CAST(COALESCE(conversions, 0) AS INT64) AS conversions
    FROM raw.facebook_ad_insights
),
google AS (
    SELECT
        DATE(date) AS spend_date,
        'paid_search' AS channel,
        campaign_name,
        special_event_id,
        special_event_type,
        special_event_name,
        event_phase,
        state_code,
        state_name,
        city,
        CAST(COALESCE(spend, 0) AS NUMERIC) AS spend,
        CAST(COALESCE(impressions, 0) AS INT64) AS impressions,
        CAST(COALESCE(clicks, 0) AS INT64) AS clicks,
        CAST(COALESCE(conversions, 0) AS INT64) AS conversions
    FROM raw.google_ad_insights
),
klaviyo AS (
    SELECT
        DATE(kc.send_time) AS spend_date,
        'email' AS channel,
        kc.name AS campaign_name,
        CAST(NULL AS STRING) AS special_event_id,
        CAST(NULL AS STRING) AS special_event_type,
        CAST(NULL AS STRING) AS special_event_name,
        CAST(NULL AS STRING) AS event_phase,
        CAST(NULL AS STRING) AS state_code,
        CAST(NULL AS STRING) AS state_name,
        CAST(NULL AS STRING) AS city,
        CAST(0 AS NUMERIC) AS spend,
        CAST(COALESCE(kc.num_recipients, 0) AS INT64) AS impressions,
        CAST(COALESCE(km.click_count, 0) AS INT64) AS clicks,
        CAST(COALESCE(km.conversion_count, 0) AS INT64) AS conversions
    FROM raw.klaviyo_campaigns kc
    LEFT JOIN raw.klaviyo_metrics km
        ON kc.id = km.campaign_id
    WHERE kc.send_time IS NOT NULL
)

SELECT * FROM facebook
UNION ALL
SELECT * FROM google
UNION ALL
SELECT * FROM klaviyo
