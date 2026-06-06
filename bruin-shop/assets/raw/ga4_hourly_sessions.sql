/* @bruin
name: raw.ga4_hourly_sessions
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake GA4 hourly session totals derived from the shared apparel
  marketing funnel. The table exists to show sub-day incidents, especially the
  2026-02-04 12-hour checkout outage where traffic drops close to zero before
  a small return-to-shop bump.

depends:
  - raw.ga4_sessions
  - raw.shopify_orders

materialization:
  type: table
  strategy: create+replace

columns:
  - name: activity_hour
    type: TIMESTAMP
    description: UTC hour bucket for synthetic web traffic.
    primary_key: true
    nullable: false
  - name: session_date
    type: DATE
    description: UTC session date.
    primary_key: true
  - name: hour_of_day
    type: INTEGER
    description: UTC hour of day from 0 to 23.
    primary_key: true
  - name: channel
    type: VARCHAR
    description: Normalized traffic channel.
    primary_key: true
  - name: sessions
    type: INTEGER
    description: Synthetic hourly sessions.
  - name: purchase_events
    type: INTEGER
    description: Synthetic hourly purchase events.
  - name: special_event_id
    type: VARCHAR
    description: Synthetic event identifier when the hour belongs to an injected incident.
  - name: special_event_type
    type: VARCHAR
    description: Synthetic event type used for dashboard filtering.
  - name: special_event_name
    type: VARCHAR
    description: Human-readable synthetic event name.
  - name: event_phase
    type: VARCHAR
    description: Event phase such as outage or recovery.

@bruin */

WITH daily AS (
    SELECT
        date AS session_date,
        CASE
            WHEN source IN ('facebook', 'instagram', 'meta') THEN 'paid_ads'
            WHEN medium = 'email' THEN 'email'
            WHEN medium = 'organic' THEN 'organic_search'
            WHEN medium = 'cpc' THEN 'paid_search'
            WHEN source = '(direct)' THEN 'direct'
            ELSE 'other'
        END AS channel,
        SUM(sessions) AS daily_sessions,
        ANY_VALUE(special_event_id) AS special_event_id,
        ANY_VALUE(special_event_type) AS special_event_type,
        ANY_VALUE(special_event_name) AS special_event_name,
        ANY_VALUE(event_phase) AS event_phase
    FROM raw.ga4_sessions
    GROUP BY session_date, channel
),
hours AS (
    SELECT hour_of_day
    FROM UNNEST(GENERATE_ARRAY(0, 23)) AS hour_of_day
),
hourly_weights AS (
    SELECT
        d.*,
        h.hour_of_day,
        CASE
            WHEN h.hour_of_day BETWEEN 0 AND 4 THEN 0.32
            WHEN h.hour_of_day BETWEEN 5 AND 7 THEN 0.58
            WHEN h.hour_of_day BETWEEN 8 AND 10 THEN 0.92
            WHEN h.hour_of_day BETWEEN 11 AND 15 THEN 1.18
            WHEN h.hour_of_day BETWEEN 16 AND 20 THEN 1.32
            ELSE 0.86
        END
        * CASE
            WHEN d.session_date = DATE '2026-02-04' AND h.hour_of_day BETWEEN 10 AND 21 THEN 0.025
            WHEN d.session_date = DATE '2026-02-05' AND h.hour_of_day BETWEEN 8 AND 19 THEN 1.24
            ELSE 1.00
        END AS adjusted_weight
    FROM daily d
    CROSS JOIN hours h
),
distributed AS (
    SELECT
        *,
        SAFE_DIVIDE(adjusted_weight, SUM(adjusted_weight) OVER (PARTITION BY session_date, channel)) AS hour_share
    FROM hourly_weights
),
hourly_orders AS (
    SELECT
        DATE(created_at) AS session_date,
        source_channel AS channel,
        EXTRACT(HOUR FROM created_at) AS hour_of_day,
        COUNTIF(financial_status IN ('paid', 'partially_refunded')) AS purchase_events
    FROM raw.shopify_orders
    GROUP BY session_date, channel, hour_of_day
)

SELECT
    TIMESTAMP(DATETIME(session_date, TIME(hour_of_day, 0, 0)), 'UTC') AS activity_hour,
    session_date,
    hour_of_day,
    channel,
    CAST(ROUND(daily_sessions * hour_share) AS INT64) AS sessions,
    COALESCE(ho.purchase_events, 0) AS purchase_events,
    special_event_id,
    special_event_type,
    special_event_name,
    event_phase
FROM distributed
LEFT JOIN hourly_orders ho
    USING (session_date, channel, hour_of_day)
ORDER BY activity_hour, channel
