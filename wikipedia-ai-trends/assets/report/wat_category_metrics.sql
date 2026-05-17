/* @bruin

name: report.wat_category_metrics
type: bq.sql
description: |
  Long-format category × snapshot_date aggregate. One row per
  (subject, snapshot_date). Foundation for prevalence-by-category bars,
  multi-line trend charts, slope/dumbbell growth, and surprise rankings.

  Metrics:
    n_articles            — articles in this subject that existed at the snapshot
    articles_with_ai      — count of those articles with ≥1 AI reference
    share_with_ai         — articles_with_ai / n_articles  (the headline metric)
    sum_ai_refs           — total AI references across the subject
    sum_total_refs        — total wikilinks across the subject
    link_share_ai         — sum_ai_refs / sum_total_refs   (intensity measure)
    mean_ai_refs_per_article — sum_ai_refs / n_articles
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
  - name: snapshot_date
    type: DATE
    primary_key: true
  - name: n_articles
    type: INT64
  - name: articles_with_ai
    type: INT64
  - name: share_with_ai
    type: FLOAT64
  - name: sum_ai_refs
    type: INT64
  - name: sum_total_refs
    type: INT64
  - name: link_share_ai
    type: FLOAT64
  - name: mean_ai_refs_per_article
    type: FLOAT64

@bruin */

SELECT
    subject,
    snapshot_date,
    COUNT(*) AS n_articles,
    COUNTIF(ai_ref_count > 0) AS articles_with_ai,
    SAFE_DIVIDE(COUNTIF(ai_ref_count > 0), COUNT(*)) AS share_with_ai,
    SUM(ai_ref_count) AS sum_ai_refs,
    SUM(total_refs) AS sum_total_refs,
    SAFE_DIVIDE(SUM(ai_ref_count), NULLIF(SUM(total_refs), 0)) AS link_share_ai,
    SAFE_DIVIDE(SUM(ai_ref_count), COUNT(*)) AS mean_ai_refs_per_article
FROM `bruin-playground-arsalan.staging.wat_ai_reference_counts`
GROUP BY subject, snapshot_date
ORDER BY subject, snapshot_date
