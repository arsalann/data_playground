/* @bruin
name: staging.stackoverflow_tag_trends
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Transforms raw monthly tag counts into peak-normalized trend data.
  Combines tag data from the BigQuery public dataset and the Stack Exchange
  API, preferring BQ data for months where both exist. Each tag's monthly
  question count is expressed as a percentage of its all-time peak month,
  making it easy to compare the relative trajectory of different technology
  communities on the same scale.

depends:
  - raw.stackoverflow_tags_monthly
  - raw.stackoverflow_tags_api_monthly

materialization:
  type: table
  strategy: create+replace

columns:
  - name: month
    type: DATE
    description: First day of the month
    primary_key: true
    nullable: false
  - name: tag
    type: VARCHAR
    description: Programming language or technology tag
    primary_key: true
    nullable: false
  - name: question_count
    type: INTEGER
    description: Number of questions with this tag in the month
  - name: peak_count
    type: INTEGER
    description: All-time peak monthly question count for this tag
  - name: pct_of_peak
    type: DOUBLE
    description: Question count as percentage of the tag's all-time peak month
  - name: era
    type: VARCHAR
    description: |
      Activity era label: Growth (2008-2014), Plateau (2015-2022),
      or Post-ChatGPT (2023+)
  - name: is_post_chatgpt
    type: BOOLEAN
    description: True if month is December 2022 or later

@bruin */

WITH bq_tags AS (
    SELECT CAST(month AS DATE) AS month, tag, question_count
    FROM raw.stackoverflow_tags_monthly
),

api_tags_deduped AS (
    SELECT *
    FROM raw.stackoverflow_tags_api_monthly
    QUALIFY ROW_NUMBER() OVER (PARTITION BY month, tag ORDER BY extracted_at DESC) = 1
),

api_tags AS (
    SELECT CAST(month AS DATE) AS month, tag, question_count
    FROM api_tags_deduped
    WHERE (CAST(month AS DATE), tag) NOT IN (
        SELECT (CAST(month AS DATE), tag) FROM raw.stackoverflow_tags_monthly
    )
),

all_tags AS (
    SELECT * FROM bq_tags
    UNION ALL
    SELECT * FROM api_tags
),

tag_peaks AS (
    SELECT
        tag,
        MAX(question_count) AS peak_count
    FROM all_tags
    GROUP BY 1
)

SELECT
    t.month,
    t.tag,
    t.question_count,
    tp.peak_count,
    ROUND(t.question_count / NULLIF(tp.peak_count, 0) * 100, 1) AS pct_of_peak,

    CASE
        WHEN EXTRACT(YEAR FROM t.month) <= 2014 THEN 'Growth (2008-2014)'
        WHEN t.month < '2022-12-01' THEN 'Plateau (2015-2022)'
        ELSE 'Post-ChatGPT (2023+)'
    END AS era,

    t.month >= '2022-12-01' AS is_post_chatgpt

FROM all_tags t
INNER JOIN tag_peaks tp ON t.tag = tp.tag
ORDER BY t.month, t.question_count DESC
