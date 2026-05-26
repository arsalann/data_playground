/* @bruin
name: self_heal_test_staging.daily_pageviews
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Daily pageview totals with country and browser breakdowns. This is the
  metric surface used by anomaly-investigate — the 2026-05-18 country=ID
  spike and the 2026-05-16 Arc-browser emergence both show up here.

depends:
  - self_heal_test_raw.pageviews

materialization:
  type: table
  strategy: create+replace

columns:
  - name: event_date
    type: DATE
    primary_key: true
    nullable: false
  - name: country
    type: VARCHAR
    primary_key: true
    nullable: false
  - name: browser
    type: VARCHAR
    primary_key: true
    nullable: false
  - name: pageviews
    type: INTEGER
    nullable: false
  - name: distinct_sessions
    type: INTEGER
    nullable: false
  - name: distinct_users
    type: INTEGER
    nullable: false

custom_checks:
  - name: known_browsers_only
    description: |
      Browser values should be limited to the historically-known set
      (Chrome, Safari, Firefox, Edge). A new value indicates a new
      segment that downstream code may not handle.
      Failure starting 2026-05-16 is expected (Arc emergence).
    query: |
      SELECT COUNT(*)
      FROM self_heal_test_staging.daily_pageviews
      WHERE browser NOT IN ('Chrome', 'Safari', 'Firefox', 'Edge')
    value: 0

@bruin */

SELECT
    event_date,
    country,
    browser,
    COUNT(*) AS pageviews,
    COUNT(DISTINCT session_id) AS distinct_sessions,
    COUNT(DISTINCT user_id) AS distinct_users
FROM self_heal_test_raw.pageviews
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
