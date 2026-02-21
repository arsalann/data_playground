/* @bruin
name: staging.stackoverflow_tag_trends
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Transforms raw monthly tag counts into peak-normalized trend data.
  Each tag's monthly question count is expressed as a percentage of its
  all-time peak month, making it easy to compare the relative decline
  of different technology communities on the same scale.

depends:
  - raw.stackoverflow_tags_monthly

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

WITH tag_peaks AS (
    SELECT
        tag,
        MAX(question_count) AS peak_count
    FROM raw.stackoverflow_tags_monthly
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

FROM raw.stackoverflow_tags_monthly t
INNER JOIN tag_peaks tp ON t.tag = tp.tag
ORDER BY t.month, t.question_count DESC
