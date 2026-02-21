/* @bruin
name: raw.stackoverflow_questions_monthly
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates Stack Overflow question data by month from the BigQuery public dataset.
  Captures monthly question volume, unique askers, scoring, view counts, and answer
  metrics to track the platform's activity over time.

  Data source: bigquery-public-data.stackoverflow.posts_questions
  License: CC BY-SA 4.0

materialization:
  type: table
  strategy: create+replace

columns:
  - name: month
    type: DATE
    description: First day of the month (truncated from creation_date)
    primary_key: true
  - name: question_count
    type: INTEGER
    description: Total number of questions posted in this month
  - name: unique_askers
    type: INTEGER
    description: Count of distinct users who posted at least one question
  - name: avg_score
    type: DOUBLE
    description: Average score (upvotes minus downvotes) across all questions
  - name: avg_views
    type: DOUBLE
    description: Average view count per question
  - name: avg_answer_count
    type: DOUBLE
    description: Average number of answers received per question
  - name: answered_count
    type: INTEGER
    description: Number of questions that received at least one answer
  - name: accepted_count
    type: INTEGER
    description: Number of questions with an accepted answer
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was materialized

@bruin */

SELECT
    DATE_TRUNC(creation_date, MONTH) AS month,
    COUNT(*) AS question_count,
    COUNT(DISTINCT owner_user_id) AS unique_askers,
    AVG(score) AS avg_score,
    AVG(view_count) AS avg_views,
    AVG(answer_count) AS avg_answer_count,
    COUNTIF(answer_count > 0) AS answered_count,
    COUNTIF(accepted_answer_id IS NOT NULL) AS accepted_count,
    CURRENT_TIMESTAMP() AS extracted_at
FROM `bigquery-public-data.stackoverflow.posts_questions`
WHERE creation_date IS NOT NULL
GROUP BY 1
ORDER BY 1
