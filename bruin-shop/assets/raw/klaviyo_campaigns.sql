/* @bruin
name: raw.klaviyo_campaigns
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake daily Klaviyo campaign rows derived from the shared
  apparel marketing funnel.

depends:
  - raw.marketing_funnel

materialization:
  type: table
  strategy: create+replace

columns:
  - name: id
    type: VARCHAR
    description: Klaviyo campaign identifier
    primary_key: true
    nullable: false
  - name: name
    type: VARCHAR
    description: Campaign display name
  - name: send_time
    type: TIMESTAMP
    description: Campaign send timestamp in UTC
  - name: num_recipients
    type: INTEGER
    description: Number of campaign recipients

@bruin */

SELECT
    FORMAT('kl_%s', FORMAT_DATE('%Y%m%d', activity_date)) AS id,
    FORMAT('Lifecycle apparel offer %s', FORMAT_DATE('%b %d, %Y', activity_date)) AS name,
    TIMESTAMP(DATETIME(activity_date, TIME(16, 0, 0)), 'UTC') AS send_time,
    SUM(impressions) AS num_recipients
FROM raw.marketing_funnel
WHERE channel = 'email'
GROUP BY activity_date
ORDER BY send_time
