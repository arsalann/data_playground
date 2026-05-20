/* @bruin

name: staging.wat_ai_articles
type: bq.sql
description: |
  AI target articles expanded to one row per (canonical_title, alias).
  Each canonical article contributes itself plus every redirect alias that
  resolves to it. The dashboard joins universe-article wikilinks against the
  `alias_title` column so we catch both direct links and redirect-spelled
  links in old revisions.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.wat_ai_seed_articles

columns:
  - name: canonical_title
    type: STRING
  - name: alias_title
    type: STRING
    primary_key: true
  - name: category
    type: STRING
  - name: created_at
    type: TIMESTAMP

@bruin */

WITH src AS (
    SELECT
        canonical_title,
        category,
        created_at,
        redirect_aliases
    FROM `bruin-playground-arsalan.raw.wat_ai_seed_articles`
),

canonical_rows AS (
    SELECT canonical_title, canonical_title AS alias_title, category, created_at FROM src
),

alias_rows AS (
    SELECT
        s.canonical_title,
        alias_title,
        s.category,
        s.created_at
    FROM src s,
         UNNEST(SPLIT(s.redirect_aliases, '|')) AS alias_title
    WHERE s.redirect_aliases IS NOT NULL
      AND s.redirect_aliases != ''
      AND alias_title != ''
)

SELECT canonical_title, alias_title, category, created_at
FROM (
    SELECT * FROM canonical_rows
    UNION ALL
    SELECT * FROM alias_rows
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY alias_title ORDER BY canonical_title) = 1
