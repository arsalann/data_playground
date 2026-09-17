/* @bruin

name: imdb_mutual_staging.title_cast
type: bq.sql
description: |
  One row per (title, actor): the deduplicated billed acting credits restricted
  to the popular movie / TV-series universe. Inner-joins raw acting credits to
  the popular-title master, so any credit whose title is not a movie /
  tvSeries / tvMiniSeries above the vote gate is dropped here.

materialization:
  type: table
  strategy: create+replace

depends:
  - imdb_mutual_raw.imdb_cast
  - imdb_mutual_raw.imdb_titles

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: tconst
    type: STRING
    description: IMDb title identifier
    checks:
      - name: not_null
  - name: primary_title
    type: STRING
    description: Title display name
  - name: title_type
    type: STRING
    description: movie, tvSeries or tvMiniSeries
  - name: start_year
    type: INTEGER
    description: Release / series-start year
  - name: genres
    type: STRING
    description: Comma-separated IMDb genres
  - name: num_votes
    type: INTEGER
    description: IMDb vote count for the title
  - name: average_rating
    type: FLOAT64
    description: IMDb average rating for the title
  - name: nconst
    type: STRING
    description: IMDb person identifier
    checks:
      - name: not_null
  - name: actor_name
    type: STRING
    description: Actor display name

@bruin */

SELECT DISTINCT
  c.tconst,
  t.primary_title,
  t.title_type,
  t.start_year,
  t.genres,
  t.num_votes,
  t.average_rating,
  c.nconst,
  c.actor_name
FROM `bruin-playground-arsalan.imdb_mutual_raw.imdb_cast` c
JOIN `bruin-playground-arsalan.imdb_mutual_raw.imdb_titles` t
  ON c.tconst = t.tconst
WHERE c.category IN ('actor', 'actress')
  AND c.actor_name IS NOT NULL
