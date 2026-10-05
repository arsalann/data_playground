/* @bruin
name: staging.fire_detections
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deduplicated NASA FIRMS MODIS fire detections with quality flags.
  - Deduplicates appended raw rows on detection_id (latest extracted_at wins).
  - Flags FIRMS low-confidence detections (confidence < 30 %).
  - Flags persistent non-vegetation heat sources. The Near Real-Time stream has no
    fire-type field, so any 0.1 degree cell where the Standard Processing archive
    labels a detection as a volcano, static land source (gas flares, industry) or
    offshore platform (type 1-3) is masked for the whole season.
  - Flags persistent hot spots: any 0.02 degree (~2 km) cell with detections on 10 or
    more distinct days within the same summer season.
  Static-source cells are pooled across all loaded years (gas flares persist), so
  past-year SP types also improve the mask for NRT data. Vegetation fires move; gas flares, kilns and
    industrial sites recur in place (masks ~26k extra NRT flare detections, e.g. Basra).
  Downstream "fire" counts use is_vegetation_fire = TRUE.

depends:
  - raw.firms_modis_fires

materialization:
  type: table
  strategy: create+replace

columns:
  - name: detection_id
    type: VARCHAR
    description: Natural key (latitude_longitude_date_time_satellite).
    primary_key: true
    nullable: false
    checks:
      - name: unique
      - name: not_null
  - name: latitude
    type: DOUBLE
    description: Fire pixel centre latitude (WGS84 degrees).
  - name: longitude
    type: DOUBLE
    description: Fire pixel centre longitude (WGS84 degrees).
  - name: geog
    type: GEOGRAPHY
    description: Fire pixel centre as a BigQuery GEOGRAPHY point.
  - name: acq_date
    type: DATE
    description: Overpass date (UTC).
    checks:
      - name: not_null
  - name: season_year
    type: INTEGER
    description: Calendar year of acq_date (one summer season per year).
  - name: week_start
    type: DATE
    description: Monday of the ISO week containing acq_date.
  - name: satellite
    type: VARCHAR
    description: Terra or Aqua.
  - name: confidence
    type: INTEGER
    description: FIRMS detection confidence (0-100 %).
  - name: confidence_class
    type: VARCHAR
    description: FIRMS confidence class - low (< 30), nominal (30-79), high (>= 80).
  - name: frp_mw
    type: DOUBLE
    description: Fire Radiative Power (MW).
  - name: daynight
    type: VARCHAR
    description: D = day overpass, N = night overpass.
  - name: data_source
    type: VARCHAR
    description: MODIS_SP (Standard Processing) or MODIS_NRT (Near Real-Time).
  - name: is_static_source_cell
    type: BOOLEAN
    description: TRUE if the 0.1 degree cell contains an SP detection typed as volcano, static land source or offshore.
  - name: is_persistent_cell
    type: BOOLEAN
    description: TRUE if the 0.02 degree cell has detections on >= 10 distinct days in the same season.
  - name: is_vegetation_fire
    type: BOOLEAN
    description: TRUE when confidence >= 30 and the cell is neither a static-source nor a persistent cell - the definition used for fire exposure.
@bruin */

WITH deduped AS (
    SELECT *
    FROM raw.firms_modis_fires
    WHERE detection_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY detection_id ORDER BY extracted_at DESC) = 1
),

static_cells AS (
    SELECT DISTINCT
        ROUND(latitude, 1) AS lat_cell,
        ROUND(longitude, 1) AS lon_cell
    FROM deduped
    WHERE fire_type IN (1, 2, 3)
),

persistent_cells AS (
    SELECT
        EXTRACT(YEAR FROM acq_date) AS season_year,
        ROUND(latitude / 0.02) * 0.02 AS lat_cell,
        ROUND(longitude / 0.02) * 0.02 AS lon_cell
    FROM deduped
    GROUP BY 1, 2, 3
    HAVING COUNT(DISTINCT acq_date) >= 10
)

SELECT
    d.detection_id,
    d.latitude,
    d.longitude,
    ST_GEOGPOINT(d.longitude, d.latitude) AS geog,
    d.acq_date,
    EXTRACT(YEAR FROM d.acq_date) AS season_year,
    DATE_TRUNC(d.acq_date, ISOWEEK) AS week_start,
    d.satellite,
    d.confidence,
    CASE
        WHEN d.confidence < 30 THEN 'low'
        WHEN d.confidence < 80 THEN 'nominal'
        ELSE 'high'
    END AS confidence_class,
    d.frp AS frp_mw,
    d.daynight,
    d.data_source,
    s.lat_cell IS NOT NULL AS is_static_source_cell,
    p.lat_cell IS NOT NULL AS is_persistent_cell,
    (d.confidence >= 30 AND s.lat_cell IS NULL AND p.lat_cell IS NULL) AS is_vegetation_fire
FROM deduped AS d
LEFT JOIN static_cells AS s
    ON ROUND(d.latitude, 1) = s.lat_cell
    AND ROUND(d.longitude, 1) = s.lon_cell
LEFT JOIN persistent_cells AS p
    ON EXTRACT(YEAR FROM d.acq_date) = p.season_year
    AND ROUND(d.latitude / 0.02) * 0.02 = p.lat_cell
    AND ROUND(d.longitude / 0.02) * 0.02 = p.lon_cell
