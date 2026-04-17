/* @bruin
name: marts.hk_transit_mart_mtr_stations
type: bq.sql
connection: bruin-playground-arsalan
description: |
  MTR station and line reference table for dashboarding.
  Aggregates station data to show which lines serve each station,
  the number of lines per station (interchange indicator), and coordinates.

  Note: MTR does not publish GTFS data, so trip-level analysis (headway,
  crowding) is not possible for heavy rail lines. This table provides
  station-level reference data only.

depends:
  - staging.hk_transit_mtr_stations

materialization:
  type: table
  strategy: create+replace

columns:
  - name: station_id
    type: VARCHAR
    description: Unique station identifier
    primary_key: true
    nullable: false
  - name: station_name_en
    type: VARCHAR
    description: Station name in English
  - name: station_name_tc
    type: VARCHAR
    description: Station name in Traditional Chinese
  - name: station_type
    type: VARCHAR
    description: Heavy Rail or Light Rail
  - name: lines_served
    type: VARCHAR
    description: Comma-separated list of line codes serving this station
  - name: line_count
    type: INTEGER
    description: Number of lines serving this station (interchange indicator)
  - name: is_interchange
    type: BOOLEAN
    description: True if station is served by more than one line
  - name: latitude
    type: DOUBLE
    description: Station latitude in WGS84 (where available)
  - name: longitude
    type: DOUBLE
    description: Station longitude in WGS84 (where available)

@bruin */

SELECT
    station_id,
    ANY_VALUE(station_name_en) AS station_name_en,
    ANY_VALUE(station_name_tc) AS station_name_tc,
    ANY_VALUE(station_type) AS station_type,
    STRING_AGG(DISTINCT line_code, ', ' ORDER BY line_code) AS lines_served,
    COUNT(DISTINCT line_code) AS line_count,
    COUNT(DISTINCT line_code) > 1 AS is_interchange,
    ANY_VALUE(latitude) AS latitude,
    ANY_VALUE(longitude) AS longitude
FROM staging.hk_transit_mtr_stations
GROUP BY station_id
ORDER BY line_count DESC, station_name_en
