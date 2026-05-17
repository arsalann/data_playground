/* @bruin

name: report.wat_subcategory_metrics
type: bq.sql
description: |
  Sub-subject breakdown of AI reference prevalence at the latest snapshot.
  One row per (subject, sub_subject). Used by the treemap / stacked-bar
  widget that drills into surprising sub-domains.

  Filtered to sub-subjects with ≥5 articles to suppress noisy small buckets.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.wat_ai_reference_counts

columns:
  - name: subject
    type: STRING
    primary_key: true
  - name: sub_subject
    type: STRING
    primary_key: true
  - name: snapshot_date
    type: DATE
    primary_key: true
  - name: n_articles
    type: INT64
  - name: articles_with_ai
    type: INT64
  - name: share_with_ai
    type: FLOAT64
  - name: mean_ai_refs_per_article
    type: FLOAT64

@bruin */

WITH latest AS (
    SELECT MAX(snapshot_date) AS d FROM `bruin-playground-arsalan.staging.wat_ai_reference_counts`
)

SELECT
    subject,
    sub_subject,
    snapshot_date,
    COUNT(*) AS n_articles,
    COUNTIF(ai_ref_count > 0) AS articles_with_ai,
    SAFE_DIVIDE(COUNTIF(ai_ref_count > 0), COUNT(*)) AS share_with_ai,
    SAFE_DIVIDE(SUM(ai_ref_count), COUNT(*)) AS mean_ai_refs_per_article
FROM `bruin-playground-arsalan.staging.wat_ai_reference_counts`, latest
WHERE snapshot_date = latest.d
GROUP BY subject, sub_subject, snapshot_date
HAVING COUNT(*) >= 5
ORDER BY subject, share_with_ai DESC
