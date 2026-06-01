/* @bruin
name: self_heal_test_staging.valid_sensor_readings
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Clean sensor readings for downstream assets. Deduplicates the append-only raw
  table by sensor and reading timestamp, keeping the latest duplicate by the
  inserted timestamp (`created_at`), then drops physically impossible
  temperature readings so bad source records do not block derived tables.

depends:
  - self_heal_test_raw.sensor_readings

materialization:
  type: table
  strategy: create+replace

columns:
  - name: sensor_id
    type: VARCHAR
    description: Sensor hardware identifier
    primary_key: true
    nullable: false
  - name: reading_time
    type: TIMESTAMP
    description: UTC time the sensor took the reading
    primary_key: true
    nullable: false
  - name: temperature_c
    type: DOUBLE
    description: Valid temperature in degrees Celsius, constrained to -50..70
    nullable: false
  - name: humidity_pct
    type: DOUBLE
    description: Relative humidity percentage, 0-100
    nullable: false
  - name: battery_pct
    type: DOUBLE
    description: Battery level percentage, 0-100
    nullable: false
  - name: created_at
    type: TIMESTAMP
    description: UTC time the reading was written to the ingest table
    nullable: false

custom_checks:
  - name: temperature_in_physical_range
    description: Cleaned readings must contain only physically plausible temperatures.
    query: |
      SELECT COUNT(*)
      FROM self_heal_test_staging.valid_sensor_readings
      WHERE temperature_c < -50 OR temperature_c > 70
    value: 0

@bruin */

WITH deduped AS (
    SELECT
        sensor_id,
        reading_time,
        temperature_c,
        humidity_pct,
        battery_pct,
        created_at
    FROM self_heal_test_raw.sensor_readings
    WHERE sensor_id IS NOT NULL
      AND reading_time IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sensor_id, reading_time
        ORDER BY created_at DESC, temperature_c DESC, humidity_pct DESC, battery_pct DESC
    ) = 1
)

SELECT
    sensor_id,
    reading_time,
    temperature_c,
    humidity_pct,
    battery_pct,
    created_at
FROM deduped
WHERE temperature_c BETWEEN -50 AND 70
ORDER BY reading_time DESC, sensor_id
