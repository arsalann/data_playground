/* @bruin
name: raw.klaviyo_metrics
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake Klaviyo campaign metric rows derived from the shared
  apparel marketing funnel.

depends:
  - raw.marketing_funnel

materialization:
  type: table
  strategy: create+replace

columns:
  - name: campaign_id
    type: VARCHAR
    description: Klaviyo campaign identifier
    primary_key: true
    nullable: false
  - name: click_count
    type: INTEGER
    description: Campaign click count
  - name: conversion_count
    type: INTEGER
    description: Campaign conversion count

@bruin */

SELECT
    FORMAT('kl_%s', FORMAT_DATE('%Y%m%d', activity_date)) AS campaign_id,
    SUM(clicks) AS click_count,
    SUM(conversions) AS conversion_count
FROM raw.marketing_funnel
WHERE channel = 'email'
GROUP BY activity_date
ORDER BY campaign_id
