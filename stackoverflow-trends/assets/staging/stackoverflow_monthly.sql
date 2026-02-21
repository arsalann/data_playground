/* @bruin
name: staging.stackoverflow_monthly
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Transforms raw monthly Stack Overflow question aggregates into an analysis-ready
  table. Combines data from two sources: the BigQuery public dataset (2008-Sept 2022)
  and the Stack Exchange API (Oct 2022-present). Adds temporal dimensions,
  answer/acceptance rates, era labels, year-over-year change percentages, and peak
  comparison metrics.

depends:
  - raw.stackoverflow_questions_monthly
  - raw.stackoverflow_api_monthly

materialization:
  type: table
  strategy: create+replace

columns:
  - name: month
    type: DATE
    description: First day of the month
    primary_key: true
    nullable: false
  - name: year
    type: INTEGER
    description: Year extracted from month
  - name: quarter
    type: INTEGER
    description: Quarter of the year (1-4)
  - name: question_count
    type: INTEGER
    description: Total questions posted this month
  - name: unique_askers
    type: INTEGER
    description: Distinct users who posted questions
  - name: avg_score
    type: DOUBLE
    description: Average question score (upvotes minus downvotes)
  - name: avg_views
    type: DOUBLE
    description: Average view count per question
  - name: avg_answer_count
    type: DOUBLE
    description: Average answers received per question
  - name: answered_count
    type: INTEGER
    description: Questions that received at least one answer
  - name: accepted_count
    type: INTEGER
    description: Questions with an accepted answer
  - name: answer_rate_pct
    type: DOUBLE
    description: Percentage of questions that received at least one answer
  - name: acceptance_rate_pct
    type: DOUBLE
    description: Percentage of questions with an accepted answer
  - name: era
    type: VARCHAR
    description: |
      Activity era label: Growth (2008-2014), Plateau (2015-2022),
      or Post-ChatGPT (2023+)
  - name: is_post_chatgpt
    type: BOOLEAN
    description: True if month is December 2022 or later
  - name: yoy_change_pct
    type: DOUBLE
    description: Year-over-year percentage change in question count
  - name: pct_of_peak
    type: DOUBLE
    description: Question count as a percentage of the all-time peak month

@bruin */

WITH combined AS (
    SELECT
        CAST(month AS DATE) AS month,
        question_count,
        unique_askers,
        avg_score,
        avg_views,
        avg_answer_count,
        answered_count,
        accepted_count
    FROM raw.stackoverflow_questions_monthly
    WHERE month IS NOT NULL

    UNION ALL

    SELECT
        CAST(month AS DATE) AS month,
        question_count,
        CAST(NULL AS INT64) AS unique_askers,
        CAST(NULL AS FLOAT64) AS avg_score,
        CAST(NULL AS FLOAT64) AS avg_views,
        CAST(NULL AS FLOAT64) AS avg_answer_count,
        CAST(NULL AS INT64) AS answered_count,
        CAST(NULL AS INT64) AS accepted_count
    FROM raw.stackoverflow_api_monthly
    WHERE month IS NOT NULL
),

peak AS (
    SELECT MAX(question_count) AS peak_count FROM combined
)

SELECT
    c.month,
    EXTRACT(YEAR FROM c.month) AS year,
    EXTRACT(QUARTER FROM c.month) AS quarter,

    c.question_count,
    c.unique_askers,
    ROUND(c.avg_score, 2) AS avg_score,
    ROUND(c.avg_views, 0) AS avg_views,
    ROUND(c.avg_answer_count, 2) AS avg_answer_count,
    c.answered_count,
    c.accepted_count,

    ROUND(c.answered_count / NULLIF(c.question_count, 0) * 100, 1) AS answer_rate_pct,
    ROUND(c.accepted_count / NULLIF(c.question_count, 0) * 100, 1) AS acceptance_rate_pct,

    CASE
        WHEN EXTRACT(YEAR FROM c.month) <= 2014 THEN 'Growth (2008-2014)'
        WHEN c.month < '2022-12-01' THEN 'Plateau (2015-2022)'
        ELSE 'Post-ChatGPT (2023+)'
    END AS era,

    c.month >= '2022-12-01' AS is_post_chatgpt,

    ROUND(
        (c.question_count - LAG(c.question_count, 12) OVER (ORDER BY c.month))
        / NULLIF(LAG(c.question_count, 12) OVER (ORDER BY c.month), 0) * 100,
        1
    ) AS yoy_change_pct,

    ROUND(c.question_count / NULLIF(p.peak_count, 0) * 100, 1) AS pct_of_peak

FROM combined c
CROSS JOIN peak p
ORDER BY c.month
