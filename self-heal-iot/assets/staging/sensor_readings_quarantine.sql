/* @bruin
name: self_heal_test_staging.sensor_readings_quarantine
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Quarantined sensor readings that are intentionally dropped from the cleaned
  downstream dataset. Keeps rejected source records auditable without allowing
  physically impossible values into reporting tables.

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
  - name: failure_reason
    type: VARCHAR
    description: Reason the reading was quarantined
    primary_key: true
    nullable: false
  - name: temperature_c
    type: DOUBLE
    description: Rejected temperature in degrees Celsius
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
    'temperature_out_of_physical_range' AS failure_reason,
    temperature_c,
    humidity_pct,
    battery_pct,
    created_at
FROM deduped
WHERE temperature_c < -50 OR temperature_c > 70
ORDER BY reading_time DESC, sensor_id
