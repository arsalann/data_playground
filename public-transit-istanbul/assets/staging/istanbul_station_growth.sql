/* @bruin
name: staging.istanbul_station_growth
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Rail station ridership with year-over-year growth rates.
  Joins station ridership data with GeoJSON coordinates for mapping.
  Calculates annual totals and growth rates per station.
  Used for the metro expansion impact analysis.

depends:
  - raw.istanbul_rail_stations
  - raw.istanbul_geo_stations

materialization:
  type: table
  strategy: create+replace

columns:
  - name: station_name
    type: VARCHAR
    description: Name of the rail station
    primary_key: true
  - name: line
    type: VARCHAR
    description: Rail line identifier
    primary_key: true
  - name: transaction_year
    type: INTEGER
    description: Calendar year
    primary_key: true
  - name: town
    type: VARCHAR
    description: Istanbul district
  - name: longitude
    type: DOUBLE
    description: Station longitude (WGS84)
  - name: latitude
    type: DOUBLE
    description: Station latitude (WGS84)
  - name: annual_passages
    type: INTEGER
    description: Total Istanbulkart tap-ins at the station for the year
  - name: annual_passengers
    type: INTEGER
    description: Total unique passengers at the station for the year
  - name: prev_year_passages
    type: INTEGER
    description: Previous year total passages (for growth calculation)
  - name: yoy_growth_pct
    type: DOUBLE
    description: Year-over-year growth percentage in passages
  - name: line_type
    type: VARCHAR
    description: Type of rail system from GeoJSON (Metro, Tramvay, etc.)

@bruin */

WITH station_deduped AS (
    SELECT *
    FROM raw.istanbul_rail_stations
    WHERE station_name IS NOT NULL
      AND station_name != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY transaction_year, transaction_month, transaction_day, line, station_name, station_number
        ORDER BY extracted_at DESC
    ) = 1
),
annual_totals AS (
    SELECT
        station_name,
        line,
        transaction_year,
        MAX(town) AS town,
        MAX(longitude) AS longitude,
        MAX(latitude) AS latitude,
        SUM(COALESCE(passage_cnt, 0)) AS annual_passages,
        SUM(COALESCE(passenger_cnt, 0)) AS annual_passengers
    FROM station_deduped
    GROUP BY station_name, line, transaction_year
),
with_growth AS (
    SELECT
        a.*,
        LAG(a.annual_passages) OVER (
            PARTITION BY a.station_name, a.line
            ORDER BY a.transaction_year
        ) AS prev_year_passages,
        ROUND(
            SAFE_DIVIDE(
                a.annual_passages - LAG(a.annual_passages) OVER (
                    PARTITION BY a.station_name, a.line ORDER BY a.transaction_year
                ),
                LAG(a.annual_passages) OVER (
                    PARTITION BY a.station_name, a.line ORDER BY a.transaction_year
                )
            ) * 100,
            2
        ) AS yoy_growth_pct
    FROM annual_totals a
),
geo AS (
    SELECT
        station_name AS geo_station_name,
        line_name AS geo_line_name,
        line_type,
        longitude AS geo_longitude,
        latitude AS geo_latitude
    FROM raw.istanbul_geo_stations
    WHERE station_name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY station_name, line_name
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT
    g.station_name,
    g.line,
    g.transaction_year,
    g.town,
    COALESCE(g.longitude, geo.geo_longitude) AS longitude,
    COALESCE(g.latitude, geo.geo_latitude) AS latitude,
    g.annual_passages,
    g.annual_passengers,
    g.prev_year_passages,
    g.yoy_growth_pct,
    geo.line_type
FROM with_growth g
LEFT JOIN geo
    ON g.station_name = geo.geo_station_name
ORDER BY g.line, g.station_name, g.transaction_year
