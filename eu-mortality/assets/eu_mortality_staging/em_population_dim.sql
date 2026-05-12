/* @bruin

name: eu_mortality_staging.em_population_dim
type: bq.sql
description: |
  Annual NUTS3 × year population dimension with derived vulnerable-population
  aggregates.

  Built from raw Eurostat DEMO_R_PJANGRP3 (sex=Total). Adds:
    - pop_total: full population (TOTAL age class).
    - pop_65plus: sum of Y65-69 + Y70-74 + Y75-79 + Y_GE80.
    - share_65plus: pop_65plus / pop_total.

  Filtered to EU-27 NUTS3 codes via the em_nuts3_dim anchor table (the raw
  Eurostat extract contains additional non-EU27 NUTS codes returned by the
  REST API).
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_mortality_raw.eurostat_population
  - eu_mortality_staging.em_nuts3_dim

tags:
  - eu-27
  - mortality
  - staging
  - dimension
  - population

columns:
  - name: nuts_id
    type: VARCHAR
    description: NUTS3 region code (EU-27 only).
    primary_key: true
    checks:
      - name: not_null
  - name: ref_year
    type: INTEGER
    description: Reference year (1 January).
    primary_key: true
    checks:
      - name: not_null
  - name: pop_total
    type: DOUBLE
    description: Total population (sex=Total, age=TOTAL).
  - name: pop_65plus
    type: DOUBLE
    description: Population aged 65 and over (sum of Y65-69 + Y70-74 + Y75-79 + Y_GE80).
  - name: share_65plus
    type: DOUBLE
    description: pop_65plus / pop_total, 0 to 1.

@bruin */

WITH src AS (
    SELECT
        nuts_id,
        ref_year,
        age_group,
        population
    FROM `bruin-playground-arsalan.eu_mortality_raw.eurostat_population`
    WHERE nuts_id IN (
        SELECT nuts_id FROM `bruin-playground-arsalan.eu_mortality_staging.em_nuts3_dim`
    )
),

pivoted AS (
    SELECT
        nuts_id,
        ref_year,
        MAX(IF(age_group = 'TOTAL', population, NULL)) AS pop_total,
        SUM(IF(age_group IN ('Y65-69', 'Y70-74', 'Y75-79', 'Y_GE80'), population, 0)) AS pop_65plus
    FROM src
    GROUP BY nuts_id, ref_year
)

SELECT
    nuts_id,
    ref_year,
    pop_total,
    pop_65plus,
    SAFE_DIVIDE(pop_65plus, pop_total) AS share_65plus
FROM pivoted
WHERE pop_total IS NOT NULL AND pop_total > 0
ORDER BY nuts_id, ref_year
