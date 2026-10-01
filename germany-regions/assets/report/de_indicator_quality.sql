/* @bruin
name: report.de_indicator_quality
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Data-quality overview per indicator: the reference year used on the dashboard, the
  latest year available in the warehouse, how many of the 16 states have a value in the
  reference year, whether a Germany value exists, and the source. Feeds the
  "Data quality" table of the dashboard so readers can see freshness and completeness.

depends:
  - staging.de_state_indicators
  - staging.de_indicator_catalog

materialization:
  type: table
  strategy: create+replace

columns:
  - name: indicator_id
    type: VARCHAR
    description: Indicator identifier.
    primary_key: true
    nullable: false
  - name: display_order
    type: INTEGER
    description: Dashboard order.
  - name: category
    type: VARCHAR
    description: Economy, Society, Latest available, or Supporting.
  - name: indicator_label
    type: VARCHAR
    description: English indicator label with unit.
  - name: reference_period_label
    type: VARCHAR
    description: Reference period used on the dashboard.
  - name: latest_year_available
    type: INTEGER
    description: Most recent year with at least one state value in the warehouse.
  - name: states_in_reference_year
    type: INTEGER
    description: Number of states (of 16) with a value in the reference year.
  - name: has_germany_value
    type: BOOLEAN
    description: TRUE if a Germany-level value exists for the reference year.
  - name: coverage_status
    type: VARCHAR
    description: Complete (16 of 16 states) or Incomplete.
  - name: source_name
    type: VARCHAR
    description: Publisher of the data.
  - name: source_table
    type: VARCHAR
    description: Source table or dataset code.
  - name: license
    type: VARCHAR
    description: Licence of the source data.
  - name: in_dashboard
    type: BOOLEAN
    description: TRUE if mapped on the dashboard.
  - name: exclusion_reason
    type: VARCHAR
    description: Why the indicator is not mapped (NULL when mapped).

@bruin */

WITH stats AS (
    SELECT
        c.indicator_id,
        MAX(IF(i.region_code != 'DG', i.year, NULL)) AS latest_year_available,
        COUNT(DISTINCT IF(i.region_code != 'DG' AND i.year = c.reference_year, i.region_code, NULL)) AS states_in_reference_year,
        LOGICAL_OR(i.region_code = 'DG' AND i.year = c.reference_year) AS has_germany_value
    FROM staging.de_indicator_catalog AS c
    LEFT JOIN staging.de_state_indicators AS i
        ON i.indicator_id = c.indicator_id
    GROUP BY c.indicator_id
)

SELECT
    c.indicator_id,
    c.display_order,
    c.category,
    c.indicator_label,
    c.reference_period_label,
    s.latest_year_available,
    CAST(s.states_in_reference_year AS INT64) AS states_in_reference_year,
    COALESCE(s.has_germany_value, FALSE) AS has_germany_value,
    IF(s.states_in_reference_year = 16, 'Complete', 'Incomplete') AS coverage_status,
    c.source_name,
    c.source_table,
    c.license,
    c.in_dashboard,
    c.exclusion_reason
FROM staging.de_indicator_catalog AS c
INNER JOIN stats AS s USING (indicator_id)
ORDER BY c.display_order
