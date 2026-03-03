/* @bruin
name: ga_sample.report_ga_daily
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Daily aggregate report of Google Analytics 360 sessions.
  Summarizes sessions, users, pageviews, bounce rate, avg session duration,
  revenue, and transactions broken down by date, channel, device category,
  country, and temporal dimensions.

depends:
  - ga_sample.staging_ga

materialization:
  type: table
  strategy: create+replace

columns:
  - name: session_date
    type: DATE
    description: Date of the aggregated sessions
    primary_key: true
    nullable: false
  - name: channel_grouping
    type: VARCHAR
    description: Marketing channel grouping
    primary_key: true
    nullable: false
  - name: device_category
    type: VARCHAR
    description: Device type (desktop, mobile, tablet)
    primary_key: true
    nullable: false
  - name: country
    type: VARCHAR
    description: Visitor country
    primary_key: true
    nullable: false
  - name: session_year
    type: INTEGER
    description: Year of the session date
  - name: session_month
    type: INTEGER
    description: Month of the session date (1-12)
  - name: session_quarter
    type: INTEGER
    description: Quarter of the session date (1-4)
  - name: session_day_name
    type: VARCHAR
    description: Human-readable day name (Monday, Tuesday, etc.)
  - name: is_weekend
    type: BOOLEAN
    description: Whether the date falls on a weekend
  - name: sessions
    type: INTEGER
    description: Total number of sessions
  - name: users
    type: INTEGER
    description: Count of distinct visitors
  - name: new_users
    type: INTEGER
    description: Number of sessions from first-time visitors
  - name: returning_users
    type: INTEGER
    description: Number of sessions from returning visitors
  - name: pageviews
    type: INTEGER
    description: Total pageviews across all sessions
  - name: hits
    type: INTEGER
    description: Total hits (interactions) across all sessions
  - name: bounces
    type: INTEGER
    description: Number of single-page sessions (bounces)
  - name: bounce_rate
    type: DOUBLE
    description: Percentage of sessions that were bounces (0-100)
  - name: avg_session_duration_seconds
    type: DOUBLE
    description: Average session duration in seconds
  - name: avg_pageviews_per_session
    type: DOUBLE
    description: Average number of pageviews per session
  - name: transactions
    type: INTEGER
    description: Total e-commerce transactions
  - name: total_revenue_usd
    type: DOUBLE
    description: Total transaction revenue in USD
  - name: conversion_rate
    type: DOUBLE
    description: Percentage of sessions with at least one transaction (0-100)

@bruin */

SELECT
    session_date,
    channel_grouping,
    device_category,
    country,

    session_year,
    session_month,
    session_quarter,
    session_day_name,
    is_weekend,

    COUNT(*)                                                            AS sessions,
    COUNT(DISTINCT full_visitor_id)                                     AS users,
    COUNTIF(is_new_visitor)                                             AS new_users,
    COUNTIF(NOT is_new_visitor)                                         AS returning_users,
    SUM(pageviews)                                                      AS pageviews,
    SUM(hits)                                                           AS hits,
    SUM(bounces)                                                        AS bounces,
    ROUND(SAFE_DIVIDE(SUM(bounces), COUNT(*)) * 100, 2)                 AS bounce_rate,
    ROUND(SAFE_DIVIDE(SUM(time_on_site_seconds), COUNT(*)), 2)          AS avg_session_duration_seconds,
    ROUND(SAFE_DIVIDE(SUM(pageviews), COUNT(*)), 2)                     AS avg_pageviews_per_session,
    SUM(transactions)                                                   AS transactions,
    ROUND(SUM(total_transaction_revenue), 2)                            AS total_revenue_usd,
    ROUND(SAFE_DIVIDE(COUNTIF(transactions > 0), COUNT(*)) * 100, 2)    AS conversion_rate

FROM `bruin-playground-arsalan.ga_sample.staging_ga`

GROUP BY
    session_date,
    channel_grouping,
    device_category,
    country,
    session_year,
    session_month,
    session_quarter,
    session_day_name,
    is_weekend

ORDER BY session_date, sessions DESC
