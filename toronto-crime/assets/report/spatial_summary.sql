/* @bruin
name: report.spatial_summary
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Default coordinate-radius summary around Toronto City Hall using BigQuery
  geography distance. The default radius is 0.833 km, approximately a
  10-minute walk at 5 km/h. Locations are privacy-offset and approximate.

depends:
  - staging.crime_events

materialization:
  type: table
  strategy: create+replace

columns:
  - name: center_label
    type: VARCHAR
    description: Documented point used for the radius summary.
    primary_key: true
  - name: latitude
    type: DOUBLE
    description: Center latitude in WGS84 decimal degrees.
  - name: longitude
    type: DOUBLE
    description: Center longitude in WGS84 decimal degrees.
  - name: radius_km
    type: DOUBLE
    description: Radius in kilometres.
  - name: latest_complete_year
    type: INTEGER
    description: Latest year treated as complete for yearly comparisons.
  - name: nearby_csi_rows
    type: INTEGER
    description: CSI rows within the radius in the latest complete year.
  - name: nearby_distinct_events
    type: INTEGER
    description: Distinct event identifiers within the radius in the latest complete year.

@bruin */

WITH parameters AS (
    SELECT
        'Toronto City Hall' AS center_label,
        43.6535 AS latitude,
        -79.3839 AS longitude,
        0.833 AS radius_km
),

latest_year AS (
    SELECT COALESCE(
        MAX(IF(occurrence_year < EXTRACT(YEAR FROM CURRENT_DATE()), occurrence_year, NULL)),
        MAX(occurrence_year)
    ) AS latest_complete_year
    FROM staging.crime_events
)

SELECT
    p.center_label,
    p.latitude,
    p.longitude,
    p.radius_km,
    ly.latest_complete_year,
    COUNT(*) AS nearby_csi_rows,
    COUNT(DISTINCT e.event_unique_id) AS nearby_distinct_events
FROM parameters AS p
CROSS JOIN latest_year AS ly
LEFT JOIN staging.crime_events AS e
    ON e.occurrence_year = ly.latest_complete_year
   AND e.valid_toronto_coordinate
   AND ST_DISTANCE(e.crime_point, ST_GEOGPOINT(p.longitude, p.latitude)) <= p.radius_km * 1000
GROUP BY 1, 2, 3, 4, 5
