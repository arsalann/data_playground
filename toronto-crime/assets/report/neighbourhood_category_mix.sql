/* @bruin
name: report.neighbourhood_category_mix
type: bq.sql
connection: bruin-playground-arsalan
description: |
  CSI category mix for the ten highest-rate neighbourhoods in the latest
  complete year.

depends:
  - staging.crime_events
  - report.neighbourhood_rankings

materialization:
  type: table
  strategy: create+replace

columns:
  - name: neighbourhood_name
    type: VARCHAR
    description: Neighbourhood name.
    primary_key: true
  - name: rank_by_rate
    type: INTEGER
    description: Rank by CSI rows per 1,000 residents.
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
    description: Total CSI rows for the neighbourhood.
  - name: latest_complete_year
    type: INTEGER
    description: Latest year treated as complete for yearly comparisons.

@bruin */

WITH top_neighbourhoods AS (
    SELECT *
    FROM report.neighbourhood_rankings
    WHERE rank_by_rate <= 10
),

category_counts AS (
    SELECT
        n.neighbourhood_name,
        n.rank_by_rate,
        e.csi_category,
        COUNT(*) AS crime_count,
        n.latest_complete_year
    FROM top_neighbourhoods AS n
    JOIN staging.crime_events AS e
        ON n.neighbourhood_id = e.neighbourhood_id
       AND e.occurrence_year = n.latest_complete_year
       AND e.neighbourhood_model = 158
    GROUP BY 1, 2, 3, 5
)

SELECT
    neighbourhood_name,
    rank_by_rate,
    SUM(IF(csi_category = 'Assault', crime_count, 0)) AS assault,
    SUM(IF(csi_category = 'Auto Theft', crime_count, 0)) AS auto_theft,
    SUM(IF(csi_category = 'Break and Enter', crime_count, 0)) AS break_and_enter,
    SUM(IF(csi_category = 'Robbery', crime_count, 0)) AS robbery,
    SUM(IF(csi_category = 'Theft Over $5k', crime_count, 0)) AS theft_over_5k,
    SUM(crime_count) AS total_crimes,
    latest_complete_year
FROM category_counts
GROUP BY 1, 2, 9
ORDER BY rank_by_rate
