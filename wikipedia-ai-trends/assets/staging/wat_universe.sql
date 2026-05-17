/* @bruin

name: staging.wat_universe
type: bq.sql
description: |
  Cleaned Vital Articles Level 4 universe with stable subject + sub-subject
  category labels. One row per article.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.wat_vital_articles

columns:
  - name: article_title
    type: STRING
    primary_key: true
  - name: subject
    type: STRING
  - name: sub_subject
    type: STRING

@bruin */

SELECT
    article_title,
    subject,
    COALESCE(sub_subject, 'Other') AS sub_subject
FROM `bruin-playground-arsalan.raw.wat_vital_articles`
QUALIFY ROW_NUMBER() OVER (PARTITION BY article_title ORDER BY ingested_at DESC) = 1
