/* @bruin
name: bruin_shop_staging.stg_web_sessions
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Normalizes fake GA4 session rows into ecommerce channel groups while
  preserving exact purchase-event counts from the generated funnel.

depends:
  - bruin_shop_raw.ga4_sessions

materialization:
  type: table
  strategy: create+replace

columns:
  - name: session_date
    type: DATE
    description: Session date
    nullable: false
  - name: total_sessions
    type: INTEGER
    description: Sessions attributed to the channel
  - name: new_users
    type: INTEGER
    description: New users attributed to the channel
  - name: engaged_sessions
    type: INTEGER
    description: Engaged sessions attributed to the channel
  - name: purchase_events
    type: INTEGER
    description: Purchase events observed for the channel and market
  - name: channel
    type: VARCHAR
    description: Normalized traffic channel
  - name: special_event_id
    type: VARCHAR
    description: Synthetic event identifier when the session row belongs to an injected campaign or sitewide incident.
  - name: special_event_type
    type: VARCHAR
    description: Synthetic event type used for dashboard filtering.
  - name: special_event_name
    type: VARCHAR
    description: Human-readable synthetic event name.
  - name: event_phase
    type: VARCHAR
    description: Event phase such as launch_push, stockout_waste, outage, or recovery.
  - name: state_code
    type: VARCHAR
    description: Two-letter US state or district postal abbreviation
  - name: state_name
    type: VARCHAR
    description: Full US state or district name
  - name: city
    type: VARCHAR
    description: US city market

@bruin */

SELECT
    DATE(s.date) AS session_date,
    CAST(COALESCE(s.sessions, 0) AS INT64) AS total_sessions,
    CAST(COALESCE(s.new_users, 0) AS INT64) AS new_users,
    CAST(COALESCE(s.engaged_sessions, 0) AS INT64) AS engaged_sessions,
    CAST(COALESCE(s.purchase_events, 0) AS INT64) AS purchase_events,
    CASE
        WHEN s.source IN ('facebook', 'instagram', 'meta') THEN 'paid_ads'
        WHEN s.medium = 'email' THEN 'email'
        WHEN s.medium = 'organic' THEN 'organic_search'
        WHEN s.medium = 'cpc' THEN 'paid_search'
        WHEN s.source = '(direct)' THEN 'direct'
        ELSE 'other'
    END AS channel,
    s.special_event_id,
    s.special_event_type,
    s.special_event_name,
    s.event_phase,
    s.state_code,
    s.state_name,
    s.city
FROM bruin_shop_raw.ga4_sessions s
