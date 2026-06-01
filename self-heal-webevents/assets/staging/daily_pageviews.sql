/* @bruin
name: self_heal_test_staging.daily_pageviews
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Daily pageview totals with country and browser breakdowns. This is the
  metric surface used by anomaly-investigate — the 2026-05-18 country=ID
  spike and the 2026-05-16 Arc-browser emergence both show up here. Raw
  event duplicates from repeated ingestions are removed using the latest
  inserted_at timestamp before aggregation.

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
      Browser values should be limited to the supported set
      (Chrome, Safari, Firefox, Edge, Arc). A new value indicates a
      segment that downstream code may not handle.
    query: |
      SELECT COUNT(*)
      FROM self_heal_test_staging.daily_pageviews
      WHERE browser NOT IN ('Chrome', 'Safari', 'Firefox', 'Edge', 'Arc')
    value: 0

@bruin */

WITH raw_with_inserted_at AS (
    SELECT
        session_id,
        user_id,
        country,
        browser,
        device,
        page_path,
        event_time,
        event_date,
        SAFE_CAST(JSON_VALUE(TO_JSON_STRING(raw_pageviews), '$.inserted_at') AS TIMESTAMP) AS inserted_at
    FROM self_heal_test_raw.pageviews AS raw_pageviews
),

deduped AS (
    SELECT
        session_id,
        user_id,
        country,
        browser,
        device,
        page_path,
        event_time,
        event_date
    FROM raw_with_inserted_at
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY session_id, user_id, country, browser, device, page_path, event_time, event_date
        ORDER BY inserted_at DESC NULLS LAST
    ) = 1
)

SELECT
    event_date,
    country,
    browser,
    COUNT(*) AS pageviews,
    COUNT(DISTINCT session_id) AS distinct_sessions,
    COUNT(DISTINCT user_id) AS distinct_users
FROM deduped
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
