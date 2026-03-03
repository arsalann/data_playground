/* @bruin
name: ga_sample.staging_ga
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Flattened and deduplicated Google Analytics 360 sessions.
  Parses the raw date string into a DATE, unnests device/geoNetwork/totals/trafficSource
  RECORDs into flat columns, deduplicates by session key (fullVisitorId + visitId),
  and adds temporal dimension columns.

depends:
  - ga_sample.raw_ga

materialization:
  type: table
  strategy: create+replace

columns:
  - name: full_visitor_id
    type: VARCHAR
    description: Unique identifier for each visitor across all sessions
    primary_key: true
    nullable: false
  - name: visit_id
    type: INTEGER
    description: Unique identifier for each visit/session
    primary_key: true
    nullable: false
  - name: session_date
    type: DATE
    description: Date of the session
    nullable: false
  - name: visit_start_time
    type: TIMESTAMP
    description: Timestamp when the visit started
    nullable: false
  - name: visit_number
    type: INTEGER
    description: Sequential visit number for the visitor (1 = first visit)
    nullable: false
  - name: channel_grouping
    type: VARCHAR
    description: Marketing channel (Organic Search, Paid Search, Direct, Social, Referral, Affiliates, Display, Other)
    nullable: false
  - name: browser
    type: VARCHAR
    description: Browser used for the session
  - name: operating_system
    type: VARCHAR
    description: Operating system of the device
  - name: is_mobile
    type: BOOLEAN
    description: Whether the session was on a mobile device
  - name: device_category
    type: VARCHAR
    description: Device type (desktop, mobile, tablet)
  - name: continent
    type: VARCHAR
    description: Continent of the visitor
  - name: sub_continent
    type: VARCHAR
    description: Sub-continent of the visitor
  - name: country
    type: VARCHAR
    description: Country of the visitor
  - name: source
    type: VARCHAR
    description: Traffic source (e.g. google, direct, facebook.com)
  - name: medium
    type: VARCHAR
    description: Traffic medium (e.g. organic, cpc, referral, none)
  - name: campaign
    type: VARCHAR
    description: Campaign name if applicable
  - name: is_true_direct
    type: BOOLEAN
    description: Whether the session was true direct traffic (no prior campaign within timeout)
  - name: hits
    type: INTEGER
    description: Total number of hits (interactions) in the session
  - name: pageviews
    type: INTEGER
    description: Total pageviews in the session
  - name: bounces
    type: INTEGER
    description: 1 if the session was a bounce, 0 otherwise
  - name: new_visits
    type: INTEGER
    description: 1 if this was the visitor's first session, 0 otherwise
  - name: time_on_site_seconds
    type: INTEGER
    description: Total session duration in seconds (0 for bounces)
  - name: transactions
    type: INTEGER
    description: Number of e-commerce transactions in the session
  - name: total_transaction_revenue
    type: DOUBLE
    description: Total transaction revenue in USD (original value divided by 1e6)
  - name: session_year
    type: INTEGER
    description: Year of the session date
  - name: session_month
    type: INTEGER
    description: Month of the session date (1-12)
  - name: session_quarter
    type: INTEGER
    description: Quarter of the session date (1-4)
  - name: session_day_of_week
    type: INTEGER
    description: Day of week (1=Sunday, 7=Saturday)
  - name: session_day_name
    type: VARCHAR
    description: Human-readable day name (Monday, Tuesday, etc.)
  - name: is_weekend
    type: BOOLEAN
    description: Whether the session occurred on a weekend (Saturday or Sunday)
  - name: is_new_visitor
    type: BOOLEAN
    description: Whether this was the visitor's first-ever session

@bruin */

WITH deduped AS (
    SELECT *
    FROM `bruin-playground-arsalan.ga_sample.raw_ga`
    WHERE fullVisitorId IS NOT NULL
      AND visitId IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fullVisitorId, visitId
        ORDER BY visitStartTime DESC
    ) = 1
)

SELECT
    fullVisitorId                                         AS full_visitor_id,
    visitId                                               AS visit_id,
    PARSE_DATE('%Y%m%d', date)                            AS session_date,
    TIMESTAMP_SECONDS(visitStartTime)                     AS visit_start_time,
    visitNumber                                           AS visit_number,
    channelGrouping                                       AS channel_grouping,

    device.browser                                        AS browser,
    device.operatingSystem                                AS operating_system,
    device.isMobile                                       AS is_mobile,
    device.deviceCategory                                 AS device_category,

    geoNetwork.continent                                  AS continent,
    geoNetwork.subContinent                               AS sub_continent,
    geoNetwork.country                                    AS country,

    trafficSource.source                                  AS source,
    trafficSource.medium                                  AS medium,
    trafficSource.campaign                                AS campaign,
    COALESCE(trafficSource.isTrueDirect, FALSE)           AS is_true_direct,

    COALESCE(totals.hits, 0)                              AS hits,
    COALESCE(totals.pageviews, 0)                         AS pageviews,
    COALESCE(totals.bounces, 0)                           AS bounces,
    COALESCE(totals.newVisits, 0)                         AS new_visits,
    COALESCE(totals.timeOnSite, 0)                        AS time_on_site_seconds,
    COALESCE(totals.transactions, 0)                      AS transactions,
    COALESCE(totals.totalTransactionRevenue, 0) / 1e6     AS total_transaction_revenue,

    EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date))         AS session_year,
    EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', date))        AS session_month,
    EXTRACT(QUARTER FROM PARSE_DATE('%Y%m%d', date))      AS session_quarter,
    EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date))    AS session_day_of_week,
    FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', date))         AS session_day_name,
    EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date)) IN (1, 7) AS is_weekend,
    visitNumber = 1                                       AS is_new_visitor

FROM deduped
ORDER BY session_date, full_visitor_id
