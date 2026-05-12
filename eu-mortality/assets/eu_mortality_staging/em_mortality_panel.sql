/* @bruin

name: eu_mortality_staging.em_mortality_panel
type: bq.sql
description: |
  Weekly NUTS3 mortality panel for the EU-27, 2015-2025.

  Cleaned and EU-27-filtered version of raw Eurostat DEMO_R_MWK3_T. Adds the
  ISO-week-start date for joining against weekly temperature aggregates, and
  attaches NUTS3 dimension columns for downstream filtering.

  Provisional observations (obs_status = 'p') are retained, since the most recent
  weeks are typically provisional and are essential to the heat-attribution story
  for the most recent summers.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_mortality_raw.eurostat_weekly_deaths
  - eu_mortality_staging.em_nuts3_dim

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 region code.
    primary_key: true
    checks:
      - name: not_null
  - name: iso_year
    type: INTEGER
    description: ISO 8601 calendar year.
    primary_key: true
  - name: iso_week
    type: INTEGER
    description: ISO 8601 week number (1-53).
    primary_key: true
  - name: week_start_date
    type: DATE
    description: Monday of the ISO week.
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code (from NUTS3 dim).
  - name: country_name_en
    type: VARCHAR
    description: English country name.
  - name: deaths_total
    type: DOUBLE
    description: Total weekly deaths.
  - name: obs_status
    type: VARCHAR
    description: Eurostat status flag, e.g. 'p' provisional.
  - name: is_provisional
    type: BOOLEAN
    description: TRUE if obs_status indicates provisional / estimated.

@bruin */

SELECT
    r.nuts_id,
    r.iso_year,
    r.iso_week,
    DATE_TRUNC(PARSE_DATE('%G-W%V-%u',
        CONCAT(CAST(r.iso_year AS STRING), '-W',
               FORMAT('%02d', r.iso_week), '-1')), WEEK(MONDAY))
        AS week_start_date,
    d.country_code,
    d.country_name_en,
    r.deaths_total,
    r.obs_status,
    r.obs_status IN ('p', 'e') AS is_provisional
FROM `bruin-playground-arsalan.eu_mortality_raw.eurostat_weekly_deaths` r
JOIN `bruin-playground-arsalan.eu_mortality_staging.em_nuts3_dim` d
    USING (nuts_id)
WHERE r.iso_year BETWEEN 2015 AND 2025
  AND r.iso_week BETWEEN 1 AND 53
  AND r.deaths_total IS NOT NULL
ORDER BY r.nuts_id, r.iso_year, r.iso_week
