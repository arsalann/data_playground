/* @bruin

name: staging.wat_universe
type: bq.sql
description: |
  Cleaned article universe with stable subject + sub-subject labels and a
  universe_tier marker. One row per article. Union of:

    tier = 'vital_l4'             — Wikipedia Vital Articles / Level 4
                                    (~9,907 articles, the originally-tracked
                                    curated universe).
    tier = 'wikiproject_extended' — articles scraped from WikiProject
                                    assessment categories (Companies, Brands,
                                    Computing, Internet culture, Business) at
                                    Top + High importance. Adds named-entity
                                    companies and tech/internet topics that
                                    are systematically under-represented in
                                    Vital-L4.

  Vital takes priority on collision: an article that appears in both is kept
  under tier = 'vital_l4' with the Vital subject/sub_subject.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.wat_vital_articles
  - raw.wat_wikiproject_articles

columns:
  - name: article_title
    type: STRING
    primary_key: true
  - name: subject
    type: STRING
  - name: sub_subject
    type: STRING
  - name: universe_tier
    type: STRING

@bruin */

WITH vital AS (
    SELECT
        article_title,
        subject,
        COALESCE(sub_subject, 'Other') AS sub_subject,
        'vital_l4' AS universe_tier
    FROM `bruin-playground-arsalan.raw.wat_vital_articles`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY article_title ORDER BY ingested_at DESC) = 1
),

extended AS (
    SELECT
        article_title,
        subject,
        sub_subject,
        universe_tier
    FROM `bruin-playground-arsalan.raw.wat_wikiproject_articles`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY article_title ORDER BY ingested_at DESC) = 1
)

SELECT * FROM vital
UNION ALL
SELECT * FROM extended
WHERE article_title NOT IN (SELECT article_title FROM vital)
