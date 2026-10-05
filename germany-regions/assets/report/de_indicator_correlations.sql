/* @bruin
name: report.de_indicator_correlations
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Pearson correlation between every pair of mapped 2025 indicators across the 16 states
  (full matrix including the diagonal, so the dashboard heatmap needs no pivoting).
  Only indicators whose dashboard reference year is 2025 are included, so no pair mixes
  reference years. With n = 16 states, |r| below about 0.5 is not distinguishable from
  zero at the 5% level; state-level correlations describe states, not individuals.

depends:
  - report.de_state_indicator_snapshot
  - staging.de_indicator_catalog

materialization:
  type: table
  strategy: create+replace

columns:
  - name: indicator_x
    type: VARCHAR
    description: Indicator identifier on the x axis.
    primary_key: true
    nullable: false
  - name: indicator_y
    type: VARCHAR
    description: Indicator identifier on the y axis.
    primary_key: true
    nullable: false
  - name: label_x
    type: VARCHAR
    description: Short label of indicator_x.
  - name: label_y
    type: VARCHAR
    description: Short label of indicator_y.
  - name: order_x
    type: INTEGER
    description: Display order of indicator_x.
  - name: order_y
    type: INTEGER
    description: Display order of indicator_y.
  - name: pearson_r
    type: DOUBLE
    description: Pearson correlation coefficient across the 16 states (-1 to 1).
  - name: r_label
    type: VARCHAR
    description: pearson_r rounded to two decimals, as text.
  - name: n_states
    type: INTEGER
    description: Number of states in the correlation (16).

@bruin */

WITH base AS (
    SELECT s.indicator_id, s.short_label, s.ags_code, s.value, c.display_order
    FROM report.de_state_indicator_snapshot AS s
    INNER JOIN staging.de_indicator_catalog AS c USING (indicator_id)
    WHERE s.reference_year = 2025
)

SELECT
    x.indicator_id AS indicator_x,
    y.indicator_id AS indicator_y,
    ANY_VALUE(x.short_label) AS label_x,
    ANY_VALUE(y.short_label) AS label_y,
    ANY_VALUE(x.display_order) AS order_x,
    ANY_VALUE(y.display_order) AS order_y,
    CORR(x.value, y.value) AS pearson_r,
    FORMAT('%.2f', CORR(x.value, y.value)) AS r_label,
    COUNT(*) AS n_states
FROM base AS x
INNER JOIN base AS y USING (ags_code)
GROUP BY indicator_x, indicator_y
ORDER BY order_x, order_y
