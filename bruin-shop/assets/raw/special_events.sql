/* @bruin
name: bruin_shop_raw.special_events
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic catalog of synthetic business incidents and campaign events
  injected into the Bruin Shop fake ecommerce model. These rows define the
  generated event windows used by marketing, GA4, Shopify orders, and DAC
  event-analysis dashboards.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: event_id
    type: VARCHAR
    description: Stable synthetic event identifier.
    primary_key: true
    nullable: false
  - name: event_name
    type: VARCHAR
    description: Human-readable event name.
  - name: event_type
    type: VARCHAR
    description: Event class used for dashboard filtering.
  - name: event_start_date
    type: DATE
    description: First calendar date affected by the event.
  - name: event_end_date
    type: DATE
    description: Last calendar date affected by the event.
  - name: primary_channel
    type: VARCHAR
    description: Primary affected acquisition channel, or all_channels for sitewide events.
  - name: platform
    type: VARCHAR
    description: Primary source platform affected by the event.
  - name: campaign_id
    type: VARCHAR
    description: Synthetic campaign identifier used during campaign events.
  - name: affected_product_id
    type: VARCHAR
    description: Featured or defective product identifier when the event is product-specific.
  - name: expected_effect
    type: VARCHAR
    description: Narrative summary of how the event is injected into the fake data.

@bruin */

SELECT *
FROM UNNEST([
    STRUCT(
        'google_search_ctr_failure' AS event_id,
        'Google search broad-match test failure' AS event_name,
        'failed_campaign' AS event_type,
        DATE '2026-01-12' AS event_start_date,
        DATE '2026-01-18' AS event_end_date,
        'paid_search' AS primary_channel,
        'Google Ads' AS platform,
        'google_search_broad_match_failure' AS campaign_id,
        CAST(NULL AS STRING) AS affected_product_id,
        'Spend rises about 8% for one week while CTR and click-to-order conversion collapse.' AS expected_effect
    ),
    STRUCT(
        'website_outage_12h',
        'Website checkout outage and recovery',
        'website_outage',
        DATE '2026-02-04',
        DATE '2026-02-05',
        'all_channels',
        'GA4',
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        'A 12-hour outage pushes hourly web traffic close to zero, suppresses same-day purchases, then creates a modest next-day return bump.'
    ),
    STRUCT(
        'black_tote_defect_refunds',
        'Black Tote Bag defect refund incident',
        'product_defect',
        DATE '2026-02-20',
        DATE '2026-02-24',
        'all_channels',
        'Shopify',
        CAST(NULL AS STRING),
        'prod_accessories_09',
        'Orders for the affected tote bag are marked partially refunded at a rate above 90%.'
    ),
    STRUCT(
        'instagram_trail_shoe_stockout',
        'Instagram trail-shoe launch and stockout',
        'stockout_campaign',
        DATE '2026-03-10',
        DATE '2026-03-21',
        'paid_ads',
        'Meta',
        'instagram_trail_shoe_launch',
        'prod_shoes_04',
        'Instagram spend and demand surge for a featured shoe, then the product runs out of stock for the final four campaign days while spend continues.'
    ),
    STRUCT(
        'instagram_spring_outfit_win',
        'Instagram spring outfit campaign win',
        'successful_campaign',
        DATE '2026-04-08',
        DATE '2026-04-14',
        'paid_ads',
        'Meta',
        'instagram_spring_outfit_win',
        CAST(NULL AS STRING),
        'Paid social CTR and conversion improve enough to create a short, profitable revenue bump.'
    ),
    STRUCT(
        'google_memorial_day_win',
        'Google Memorial Day search campaign win',
        'successful_campaign',
        DATE '2026-05-11',
        DATE '2026-05-17',
        'paid_search',
        'Google Ads',
        'google_search_memorial_day_win',
        CAST(NULL AS STRING),
        'Paid search CTR and conversion improve during a short seasonal campaign, lifting revenue and ROAS.'
    ),
    STRUCT(
        'google_summer_sale_search',
        'Google Search - Summer Sale',
        'successful_campaign',
        DATE '2026-06-07',
        DATE '2026-06-08',
        'paid_search',
        'Google Ads',
        'google_search_summer_sale',
        CAST(NULL AS STRING),
        'Short paid-search summer sale campaign with elevated seasonal demand and order volume.'
    )
])
