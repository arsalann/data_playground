/* @bruin
name: bruin_shop_raw.ga4_sessions
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake GA4 sessions generated from the shared apparel marketing
  funnel. Purchase events are carried at channel and city level so downstream
  attribution matches the synthetic order facts exactly.

depends:
  - bruin_shop_raw.marketing_funnel
  - bruin_shop_raw.shopify_orders

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date
    type: DATE
    description: Session date
    primary_key: true
    nullable: false
  - name: source
    type: VARCHAR
    description: GA4 traffic source
    primary_key: true
  - name: medium
    type: VARCHAR
    description: GA4 traffic medium
    primary_key: true
  - name: state_code
    type: VARCHAR
    description: Two-letter US state or district postal abbreviation
    primary_key: true
  - name: city
    type: VARCHAR
    description: US city market
    primary_key: true
  - name: sessions
    type: INTEGER
    description: Number of sessions
  - name: new_users
    type: INTEGER
    description: Number of new users
  - name: engaged_sessions
    type: INTEGER
    description: Number of engaged sessions
  - name: purchase_events
    type: INTEGER
    description: Number of purchase events for the channel and market
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
  - name: state_name
    type: VARCHAR
    description: Full US state or district name

@bruin */

WITH paid_orders AS (
    SELECT
        DATE(created_at) AS order_date,
        source_channel AS channel,
        state_code,
        city,
        COUNTIF(financial_status IN ('paid', 'partially_refunded')) AS paid_order_count
    FROM bruin_shop_raw.shopify_orders
    GROUP BY order_date, channel, state_code, city
)

SELECT
    mf.activity_date AS date,
    mf.source,
    mf.medium,
    mf.state_code,
    mf.city,
    mf.sessions,
    mf.new_users,
    mf.engaged_sessions,
    COALESCE(po.paid_order_count, 0) AS purchase_events,
    mf.special_event_id,
    mf.special_event_type,
    mf.special_event_name,
    mf.event_phase,
    mf.state_name
FROM bruin_shop_raw.marketing_funnel mf
LEFT JOIN paid_orders po
    ON mf.activity_date = po.order_date
    AND mf.channel = po.channel
    AND mf.state_code = po.state_code
    AND mf.city = po.city
ORDER BY date, source, medium, state_code, city
