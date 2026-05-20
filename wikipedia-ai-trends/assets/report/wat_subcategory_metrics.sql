/* @bruin

name: report.wat_subcategory_metrics
type: bq.sql
description: |
  Sub-subject breakdown of AI reference prevalence at the latest snapshot.
  One row per (cohort, subject, sub_subject). Used by the treemap /
  stacked-bar widget that drills into surprising sub-domains.

  Two cohorts mirror those in `wat_category_metrics`:
    cohort = 'all'      — every article that existed at the latest snapshot.
    cohort = 'balanced' — only articles that already existed at the earliest
                          snapshot in the dataset (fixed panel).

  Filtered to sub-subjects with ≥5 articles (within the cohort) to suppress
  noisy small buckets.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.wat_ai_reference_counts

columns:
  - name: cohort
    type: STRING
    primary_key: true
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

WITH base AS (
    SELECT * FROM `bruin-playground-arsalan.staging.wat_ai_reference_counts`
),

snap_bounds AS (
    SELECT
        MIN(snapshot_date) AS earliest,
        MAX(snapshot_date) AS latest
    FROM base
),

balanced_cohort AS (
    SELECT DISTINCT b.article_title
    FROM base b, snap_bounds s
    WHERE b.snapshot_date = s.earliest
),

latest_all AS (
    SELECT
        'all' AS cohort,
        b.subject,
        b.sub_subject,
        b.snapshot_date,
        b.article_title,
        b.ai_ref_count
    FROM base b, snap_bounds s
    WHERE b.snapshot_date = s.latest
),

latest_balanced AS (
    SELECT
        'balanced' AS cohort,
        b.subject,
        b.sub_subject,
        b.snapshot_date,
        b.article_title,
        b.ai_ref_count
    FROM base b
    INNER JOIN balanced_cohort c USING (article_title), snap_bounds s
    WHERE b.snapshot_date = s.latest
),

unioned AS (
    SELECT * FROM latest_all
    UNION ALL
    SELECT * FROM latest_balanced
)

SELECT
    cohort,
    subject,
    sub_subject,
    snapshot_date,
    COUNT(*) AS n_articles,
    COUNTIF(ai_ref_count > 0) AS articles_with_ai,
    SAFE_DIVIDE(COUNTIF(ai_ref_count > 0), COUNT(*)) AS share_with_ai,
    SAFE_DIVIDE(SUM(ai_ref_count), COUNT(*)) AS mean_ai_refs_per_article
FROM unioned
GROUP BY cohort, subject, sub_subject, snapshot_date
HAVING COUNT(*) >= 5
ORDER BY cohort, subject, share_with_ai DESC
