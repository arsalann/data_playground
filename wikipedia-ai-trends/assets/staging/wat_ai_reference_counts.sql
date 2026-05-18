/* @bruin

name: staging.wat_ai_reference_counts
type: bq.sql
description: |
  For each (universe article × snapshot date), counts AI references and total
  references. AI references = wikilinks that match an AI target's canonical
  title or any of its redirect aliases.

  Dedupes against `append`-strategy duplicates in the raw snapshot table by
  keeping the most recently-fetched row per (article, snapshot_date).

  Joined to the universe table so each row carries subject + sub_subject for
  category-level aggregation downstream.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.wat_article_snapshots
  - staging.wat_universe
  - staging.wat_ai_articles

columns:
  - name: article_title
    type: STRING
    primary_key: true
  - name: snapshot_date
    type: DATE
    primary_key: true
  - name: subject
    type: STRING
  - name: sub_subject
    type: STRING
  - name: universe_tier
    type: STRING
  - name: revision_id
    type: INT64
  - name: revision_timestamp
    type: TIMESTAMP
  - name: total_refs
    type: INT64
  - name: ai_ref_count
    type: INT64
  - name: ai_ref_share
    type: FLOAT64
  - name: ai_targets
    type: STRING

@bruin */

WITH snapshots AS (
    SELECT *
    FROM `bruin-playground-arsalan.raw.wat_article_snapshots`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY article_title, snapshot_date
        ORDER BY fetched_at DESC
    ) = 1
),

ai_hits AS (
    SELECT
        s.article_title,
        s.snapshot_date,
        ai.canonical_title AS ai_target
    FROM snapshots s,
         UNNEST(JSON_EXTRACT_STRING_ARRAY(s.wikilinks)) AS link
    INNER JOIN `bruin-playground-arsalan.staging.wat_ai_articles` ai
        ON ai.alias_title = link
),

per_snapshot AS (
    SELECT
        article_title,
        snapshot_date,
        COUNT(DISTINCT ai_target) AS ai_ref_count,
        STRING_AGG(DISTINCT ai_target, '|' ORDER BY ai_target) AS ai_targets
    FROM ai_hits
    GROUP BY article_title, snapshot_date
)

SELECT
    s.article_title,
    s.snapshot_date,
    u.subject,
    u.sub_subject,
    u.universe_tier,
    s.revision_id,
    s.revision_timestamp,
    s.wikilinks_count AS total_refs,
    COALESCE(p.ai_ref_count, 0) AS ai_ref_count,
    SAFE_DIVIDE(COALESCE(p.ai_ref_count, 0), NULLIF(s.wikilinks_count, 0)) AS ai_ref_share,
    p.ai_targets
FROM snapshots s
INNER JOIN `bruin-playground-arsalan.staging.wat_universe` u
    ON u.article_title = s.article_title
LEFT JOIN per_snapshot p
    ON p.article_title = s.article_title
    AND p.snapshot_date = s.snapshot_date
WHERE s.revision_id IS NOT NULL  -- only periods where the article existed
