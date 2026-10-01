/* @bruin
name: report.de_state_indicator_snapshot
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Dashboard-ready snapshot: one row per mapped indicator x state for the indicator's fixed
  reference year (staging.de_indicator_catalog.reference_year), with the Germany value,
  the difference to Germany, the rank among the 16 states, and the state's map label
  and label anchor (the dashboard joins state geometry by nuts1_id). Only indicators flagged `in_dashboard` are included, and the custom check
  enforces that each has all 16 states in its reference year.

depends:
  - staging.de_state_indicators
  - staging.de_indicator_catalog
  - staging.de_states

materialization:
  type: table
  strategy: create+replace

columns:
  - name: indicator_id
    type: VARCHAR
    description: Indicator identifier.
    primary_key: true
    nullable: false
  - name: ags_code
    type: VARCHAR
    description: State code AGS 01-16.
    primary_key: true
    nullable: false
  - name: nuts1_id
    type: VARCHAR
    description: NUTS 1 code of the state (join key to the map vertices).
  - name: state_name
    type: VARCHAR
    description: Official German state name.
  - name: state_abbr
    type: VARCHAR
    description: Two-letter state abbreviation.
  - name: region_group
    type: VARCHAR
    description: West, East, or Berlin.
  - name: category
    type: VARCHAR
    description: Economy, Society, or Latest available.
  - name: short_label
    type: VARCHAR
    description: Short English label without unit.
  - name: indicator_label
    type: VARCHAR
    description: English indicator label with unit.
  - name: unit_label
    type: VARCHAR
    description: Unit of `value`.
  - name: reference_year
    type: INTEGER
    description: Reference year of the value.
  - name: reference_period
    type: VARCHAR
    description: Reference period as published (e.g. 31 Dec 2025, April 2025, 2023-2025).
  - name: value
    type: DOUBLE
    description: Indicator value for the state, in `unit_label`.
    checks:
      - name: not_null
  - name: value_label
    type: VARCHAR
    description: Value rounded to the indicator's label decimals, as text for map and bar labels.
  - name: map_label
    type: VARCHAR
    description: Two-line map label, state abbreviation and rounded value.
  - name: germany_value
    type: DOUBLE
    description: Indicator value for Germany as a whole in the same reference year.
  - name: germany_value_label
    type: VARCHAR
    description: Germany value rounded to the indicator's label decimals, as text for tooltips.
  - name: diff_to_germany_label
    type: VARCHAR
    description: Signed difference to Germany (e.g. +3.2), rounded to the indicator's label decimals.
  - name: diff_to_germany
    type: DOUBLE
    description: State value minus Germany value, in `unit_label`.
  - name: pct_of_germany
    type: DOUBLE
    description: State value as a percentage of the Germany value (100 = national level).
  - name: rank_desc
    type: INTEGER
    description: Rank among the 16 states, 1 = highest value.
  - name: label_lon
    type: DOUBLE
    description: Longitude of the state's map label anchor.
  - name: label_lat
    type: DOUBLE
    description: Latitude of the state's map label anchor.

custom_checks:
  - name: sixteen states per indicator in its reference year
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT indicator_id, COUNT(DISTINCT ags_code) AS n_states, COUNT(DISTINCT reference_year) AS n_years
        FROM report.de_state_indicator_snapshot
        GROUP BY indicator_id
      )
      WHERE n_states != 16 OR n_years != 1
    value: 0

@bruin */

WITH catalog AS (
    SELECT *
    FROM staging.de_indicator_catalog
    WHERE in_dashboard
),

values_at_reference AS (
    SELECT
        i.*,
        c.category,
        c.short_label,
        c.indicator_label,
        c.unit_label,
        c.label_decimals,
        c.reference_year
    FROM staging.de_state_indicators AS i
    INNER JOIN catalog AS c
        ON c.indicator_id = i.indicator_id AND c.reference_year = i.year
),

germany AS (
    SELECT indicator_id, value AS germany_value
    FROM values_at_reference
    WHERE region_code = 'DG'
)

SELECT
    v.indicator_id,
    s.ags_code,
    s.nuts1_id,
    s.state_name,
    s.state_abbr,
    s.region_group,
    v.category,
    v.short_label,
    v.indicator_label,
    v.unit_label,
    v.reference_year,
    v.reference_period,
    v.value,
    FORMAT('%\'.*f', v.label_decimals, v.value) AS value_label,
    CONCAT(s.state_abbr, '\n', FORMAT('%\'.*f', v.label_decimals, v.value)) AS map_label,
    g.germany_value,
    FORMAT('%\'.*f', v.label_decimals, g.germany_value) AS germany_value_label,
    v.value - g.germany_value AS diff_to_germany,
    CONCAT(IF(v.value >= g.germany_value, '+', ''), FORMAT('%\'.*f', v.label_decimals, v.value - g.germany_value)) AS diff_to_germany_label,
    100 * v.value / NULLIF(g.germany_value, 0) AS pct_of_germany,
    CAST(RANK() OVER (PARTITION BY v.indicator_id ORDER BY v.value DESC) AS INT64) AS rank_desc,
    s.label_lon,
    s.label_lat
FROM values_at_reference AS v
INNER JOIN staging.de_states AS s
    ON s.ags_code = v.region_code
LEFT JOIN germany AS g
    ON g.indicator_id = v.indicator_id
ORDER BY v.indicator_id, rank_desc
