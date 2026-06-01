/* @bruin
name: self_heal_test_staging.hourly_sensor_stats
type: bq.sql
connection: bruin-playground-arsalan
description: |
  One row per (sensor, hour) with reading and a quality-window flag.
  Reads from the cleaned sensor readings table so physically-impossible
  temperature values are dropped before downstream aggregation.

depends:
  - self_heal_test_staging.valid_sensor_readings

materialization:
  type: table
  strategy: create+replace

columns:
  - name: sensor_id
    type: VARCHAR
    primary_key: true
    nullable: false
  - name: hour
    type: TIMESTAMP
    primary_key: true
    nullable: false
  - name: temperature_c
    type: DOUBLE
    nullable: false
  - name: humidity_pct
    type: DOUBLE
    nullable: false
  - name: battery_pct
    type: DOUBLE
    nullable: false
  - name: ingest_lag_minutes
    type: DOUBLE
    description: Minutes between reading_time and created_at; > 60 = late.
    nullable: false

custom_checks:
  - name: ingest_lag_under_60_min
    description: |
      Every sensor reading should arrive within 60 minutes.
      Failure on 2026-05-22 is expected (late-arriving injection).
    query: |
      SELECT COUNT(*)
      FROM self_heal_test_staging.hourly_sensor_stats
      WHERE ingest_lag_minutes > 60
    value: 0

@bruin */

SELECT
    sensor_id,
    reading_time AS hour,
    temperature_c,
    humidity_pct,
    battery_pct,
    TIMESTAMP_DIFF(created_at, reading_time, SECOND) / 60.0 AS ingest_lag_minutes
FROM self_heal_test_staging.valid_sensor_readings
ORDER BY hour DESC, sensor_id
