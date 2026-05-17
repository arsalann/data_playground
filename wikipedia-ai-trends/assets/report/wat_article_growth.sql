/* @bruin

name: report.wat_article_growth
type: bq.sql
description: |
  Per-article growth in AI references between the earliest snapshot where
  the article existed and the most recent snapshot. One row per article
  that has ANY AI reference in the most recent snapshot.

  Used by the "top growing articles" table and to power surprise-ranking
  drill-downs in the dashboard.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - staging.wat_ai_reference_counts

columns:
  - name: article_title
    type: STRING
    primary_key: true
  - name: subject
    type: STRING
  - name: sub_subject
    type: STRING
  - name: earliest_snapshot
    type: DATE
  - name: earliest_ai_refs
    type: INT64
  - name: latest_snapshot
    type: DATE
  - name: latest_ai_refs
    type: INT64
  - name: latest_total_refs
    type: INT64
  - name: latest_ai_ref_share
    type: FLOAT64
  - name: ai_refs_added
    type: INT64

@bruin */

WITH first_last AS (
    SELECT
        article_title,
        subject,
        sub_subject,
        MIN(snapshot_date) AS earliest_snapshot,
        MAX(snapshot_date) AS latest_snapshot
    FROM `bruin-playground-arsalan.staging.wat_ai_reference_counts`
    GROUP BY article_title, subject, sub_subject
),

earliest AS (
    SELECT r.article_title, r.ai_ref_count AS earliest_ai_refs
    FROM `bruin-playground-arsalan.staging.wat_ai_reference_counts` r
    INNER JOIN first_last f
        ON r.article_title = f.article_title
        AND r.snapshot_date = f.earliest_snapshot
),

latest AS (
    SELECT
        r.article_title,
        r.ai_ref_count AS latest_ai_refs,
        r.total_refs AS latest_total_refs,
        r.ai_ref_share AS latest_ai_ref_share
    FROM `bruin-playground-arsalan.staging.wat_ai_reference_counts` r
    INNER JOIN first_last f
        ON r.article_title = f.article_title
        AND r.snapshot_date = f.latest_snapshot
)

SELECT
    f.article_title,
    f.subject,
    f.sub_subject,
    f.earliest_snapshot,
    e.earliest_ai_refs,
    f.latest_snapshot,
    l.latest_ai_refs,
    l.latest_total_refs,
    l.latest_ai_ref_share,
    l.latest_ai_refs - e.earliest_ai_refs AS ai_refs_added
FROM first_last f
LEFT JOIN earliest e USING (article_title)
LEFT JOIN latest l USING (article_title)
WHERE l.latest_ai_refs > 0
