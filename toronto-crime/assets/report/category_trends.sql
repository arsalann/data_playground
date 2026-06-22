/* @bruin
name: report.category_trends
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Annual CSI category counts in wide format for DAC line charts.

depends:
  - staging.crime_events

materialization:
  type: table
  strategy: create+replace

columns:
  - name: occurrence_year
    type: INTEGER
    description: Occurrence year.
    primary_key: true
  - name: assault
    type: INTEGER
    description: Assault CSI rows.
  - name: auto_theft
    type: INTEGER
    description: Auto Theft CSI rows.
  - name: break_and_enter
    type: INTEGER
    description: Break and Enter CSI rows.
  - name: robbery
    type: INTEGER
    description: Robbery CSI rows.
  - name: theft_over_5k
    type: INTEGER
    description: Theft Over $5k CSI rows.
  - name: total_crimes
    type: INTEGER
    description: Total CSI rows.

@bruin */

SELECT
    occurrence_year,
    COUNTIF(csi_category = 'Assault') AS assault,
    COUNTIF(csi_category = 'Auto Theft') AS auto_theft,
    COUNTIF(csi_category = 'Break and Enter') AS break_and_enter,
    COUNTIF(csi_category = 'Robbery') AS robbery,
    COUNTIF(csi_category = 'Theft Over $5k') AS theft_over_5k,
    COUNT(*) AS total_crimes
FROM staging.crime_events
GROUP BY 1
ORDER BY occurrence_year
