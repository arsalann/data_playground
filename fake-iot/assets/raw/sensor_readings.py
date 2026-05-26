"""@bruin
name: self_heal_test_raw.sensor_readings
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Generates deterministic fake IoT sensor readings (temperature, humidity,
  battery). Twelve sensors emit one reading per hour. Same date inputs always
  produce the same data.

  Injected issues:

  1. QUALITY-FAIL — impossible_values
     On 2026-04-10, sensor S07 reports temperature_c = 999.0 for every hour.
     A range check (temp between -50 and 70) catches this. Routes to
     data-quality-investigate; mode = source-bug.

  2. SCHEMA-DRIFT — type_narrowed
     Before 2026-03-01 temperature_c is float (e.g. 22.473).
     From 2026-03-01 onward, the source rounds to int (e.g. 22.0) — declared
     dtype is still DOUBLE, so the column technically validates, but a
     downstream "precision check" sees the distribution change. Routes to
     schema-drift-check; type-narrowed → escalate.

  3. LATE-ARRIVING — backdated_writes
     Hourly readings for 2026-05-10 are emitted with a created_at timestamp
     of 2026-05-11 (one full day late). A staleness check on `created_at`
     vs `reading_time` should flag this.

materialization:
  type: table
  strategy: append
  incremental_key: reading_time

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
    description: Temperature in degrees Celsius
    nullable: false
  - name: humidity_pct
    type: DOUBLE
    description: Relative humidity, 0-100
    nullable: false
  - name: battery_pct
    type: DOUBLE
    description: Battery level, 0-100
    nullable: false
  - name: created_at
    type: TIMESTAMP
    description: UTC time the reading was written to the ingest table
    nullable: false

custom_checks:
  - name: temperature_in_physical_range
    description: |
      Temperature readings outside -50..70 degC are sensor malfunctions.
      Failure on 2026-04-10 is expected (impossible_values injection).
    query: |
      SELECT COUNT(*)
      FROM self_heal_test_raw.sensor_readings
      WHERE temperature_c < -50 OR temperature_c > 70
    value: 0

  - name: readings_arrive_within_one_hour
    description: |
      created_at should be within 1 hour of reading_time. Larger gaps point
      to late-arriving data. Failure on 2026-05-10 is expected (late-arriving
      injection).
    query: |
      SELECT COUNT(*)
      FROM self_heal_test_raw.sensor_readings
      WHERE created_at > TIMESTAMP_ADD(reading_time, INTERVAL 1 HOUR)
    value: 0

@bruin"""

import hashlib
import os
import random
from datetime import date, datetime, timedelta

import pandas as pd

SENSOR_IDS = [f"S{i:02d}" for i in range(1, 13)]
TYPE_NARROW_DATE = date(2026, 3, 1)
IMPOSSIBLE_DATE = date(2026, 4, 10)
LATE_DATE = date(2026, 5, 10)


def seed_for(d: date, hour: int) -> int:
    return int(hashlib.sha256(f"{d.isoformat()}T{hour:02d}".encode()).hexdigest()[:8], 16)


def generate_day(d: date) -> pd.DataFrame:
    type_narrow = d >= TYPE_NARROW_DATE
    is_impossible = d == IMPOSSIBLE_DATE
    is_late = d == LATE_DATE

    rows = []
    for hour in range(24):
        rng = random.Random(seed_for(d, hour))
        reading_time = datetime.combine(d, datetime.min.time()) + timedelta(hours=hour)

        if is_late:
            created_at = reading_time + timedelta(days=1)
        else:
            created_at = reading_time + timedelta(minutes=rng.randint(0, 10))

        for sensor_id in SENSOR_IDS:
            if is_impossible and sensor_id == "S07":
                temp = 999.0
            else:
                base = 18.0 + 8.0 * rng.random()
                temp = round(base) if type_narrow else round(base, 3)

            rows.append({
                "sensor_id": sensor_id,
                "reading_time": reading_time,
                "temperature_c": float(temp),
                "humidity_pct": round(30 + 50 * rng.random(), 1),
                "battery_pct": round(40 + 60 * rng.random(), 1),
                "created_at": created_at,
            })

    return pd.DataFrame(rows)


def materialize():
    start_str = os.environ.get("BRUIN_START_DATE", "2026-01-01")
    end_str = os.environ.get("BRUIN_END_DATE", date.today().isoformat())

    start = date.fromisoformat(start_str[:10])
    end = date.fromisoformat(end_str[:10])

    frames = []
    current = start
    while current <= end:
        frames.append(generate_day(current))
        current += timedelta(days=1)

    df = pd.concat(frames, ignore_index=True)
    for column in ("reading_time", "created_at"):
        df[column] = pd.to_datetime(df[column]).astype("datetime64[us]")
    print(f"[fake-iot] generated {len(df):,} readings across {df['reading_time'].dt.date.nunique()} days")
    return df
