/* @bruin
name: bruin_shop_raw.facebook_campaigns
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake Meta/Facebook Ads campaign metadata.

depends:
  - bruin_shop_raw.marketing_funnel

materialization:
  type: table
  strategy: create+replace

columns:
  - name: campaign_id
    type: VARCHAR
    description: Advertising campaign identifier
    primary_key: true
    nullable: false
  - name: campaign_name
    type: VARCHAR
    description: Advertising campaign display name
  - name: status
    type: VARCHAR
    description: Campaign delivery status

@bruin */

SELECT DISTINCT
    campaign_id,
    campaign_name,
    'ACTIVE' AS status
FROM bruin_shop_raw.marketing_funnel
WHERE channel = 'paid_ads'
ORDER BY campaign_id
