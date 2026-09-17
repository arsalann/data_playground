/* @bruin

name: imdb_mutual_staging.title_pairs_shared_cast
type: bq.sql
description: |
  One row per unordered pair of titles that share at least two billed actors.
  Built by self-joining title_cast on the person id with tconst_a < tconst_b so
  each pair appears once. shared_actor_count is the number of distinct actors
  the two titles have in common; shared_actors lists their names.

  This is the raw overlap graph BEFORE any "directly related" filtering; the
  report layer adds the franchise / sequel exclusion.

materialization:
  type: table
  strategy: create+replace

depends:
  - imdb_mutual_staging.title_cast

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: tconst_a
    type: STRING
    description: First title id (lexicographically smaller)
    checks:
      - name: not_null
  - name: tconst_b
    type: STRING
    description: Second title id (lexicographically larger)
    checks:
      - name: not_null
  - name: shared_actor_count
    type: INTEGER
    description: Number of distinct billed actors common to both titles
    checks:
      - name: positive
  - name: shared_actors
    type: STRING
    description: Comma-separated names of the shared actors

@bruin */

WITH cast_clean AS (
  SELECT DISTINCT tconst, nconst, actor_name
  FROM `bruin-playground-arsalan.imdb_mutual_staging.title_cast`
)

SELECT
  a.tconst AS tconst_a,
  b.tconst AS tconst_b,
  COUNT(DISTINCT a.nconst) AS shared_actor_count,
  STRING_AGG(DISTINCT a.actor_name, ', ' ORDER BY a.actor_name) AS shared_actors
FROM cast_clean a
JOIN cast_clean b
  ON a.nconst = b.nconst
  AND a.tconst < b.tconst
GROUP BY tconst_a, tconst_b
HAVING shared_actor_count >= 2
