/* @bruin
name: raw.google_campaigns
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake Google Ads campaign metadata.

depends:
  - raw.marketing_funnel

materialization:
  type: table
  strategy: create+replace

columns:
  - name: campaign_id
    type: VARCHAR
    description: Google Ads campaign identifier
    primary_key: true
    nullable: false
  - name: campaign_name
    type: VARCHAR
    description: Google Ads campaign display name
  - name: status
    type: VARCHAR
    description: Campaign delivery status

@bruin */

SELECT DISTINCT
    campaign_id,
    campaign_name,
    'ENABLED' AS status
FROM raw.marketing_funnel
WHERE channel = 'paid_search'
ORDER BY campaign_id
