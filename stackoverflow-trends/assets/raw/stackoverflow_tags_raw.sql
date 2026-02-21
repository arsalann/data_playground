/* @bruin
name: raw.stackoverflow_tags_monthly
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Aggregates Stack Overflow question counts by month for the top 15 all-time tags.
  Tags are extracted from the pipe-delimited format used in the public dataset
  (e.g. python|pandas|dataframe). Only the top 15 tags by total question count
  are included to keep the query efficient and the dashboard focused.

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
  - name: tag
    type: VARCHAR
    description: Programming language or technology tag (e.g. python, javascript)
  - name: question_count
    type: INTEGER
    description: Number of questions tagged with this tag in the given month
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was materialized

@bruin */

WITH top_tags AS (
    SELECT tag, COUNT(*) AS total
    FROM `bigquery-public-data.stackoverflow.posts_questions`,
        UNNEST(SPLIT(tags, '|')) AS tag
    WHERE creation_date IS NOT NULL
      AND tag != ''
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 15
)

SELECT
    DATE_TRUNC(q.creation_date, MONTH) AS month,
    tag,
    COUNT(*) AS question_count,
    CURRENT_TIMESTAMP() AS extracted_at
FROM `bigquery-public-data.stackoverflow.posts_questions` q,
    UNNEST(SPLIT(q.tags, '|')) AS tag
INNER JOIN top_tags tt USING (tag)
WHERE q.creation_date IS NOT NULL
  AND tag != ''
GROUP BY 1, 2
ORDER BY 1, 3 DESC
