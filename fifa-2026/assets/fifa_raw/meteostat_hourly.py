"""@bruin

name: fifa_raw.meteostat_hourly
description: |
  Hourly METAR observations from Meteostat at the primary ICAO of each
  FIFA-2026 host venue, for the same June-July climatology window (2010-2024)
  as `openmeteo_climatology`. Used downstream by `match_climatology` as a
  cross-check on the Open-Meteo gridded reanalysis.

  Limitation: METAR is sampled at the top of every UTC hour, so sub-hour
  temperature spikes are invisible — same caveat as the polymarket-weather
  Paris analysis. This source is labelled `source='meteostat'` in staging and
  is never aggregated alongside Open-Meteo grid rows.

  Status: deferred in this build. Meteostat 2.x reorganised its API and
  ICAO-based station lookup needs reworking; the H1 hypothesis still works
  using only Open-Meteo ERA5 reanalysis. This asset materialises an empty
  table with the expected schema so `match_climatology` can LEFT JOIN safely.

  Source: Meteostat hourly() API.
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - weather
  - metar
  - external_source
  - raw_data

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: venue_id
    type: VARCHAR
  - name: ts_utc
    type: TIMESTAMP
  - name: icao
    type: VARCHAR
  - name: temp_c
    type: DOUBLE
  - name: dew_point_c
    type: DOUBLE
  - name: humidity_pct
    type: DOUBLE
  - name: wind_speed_kmh
    type: DOUBLE
  - name: precipitation_mm
    type: DOUBLE
  - name: extracted_at
    type: TIMESTAMP

@bruin"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def materialize():
    logger.info("meteostat_hourly is currently a stub — H1 uses Open-Meteo ERA5 only")
    return pd.DataFrame({
        "venue_id":        pd.Series(dtype="string"),
        "ts_utc":          pd.Series(dtype="datetime64[ns, UTC]"),
        "icao":            pd.Series(dtype="string"),
        "temp_c":          pd.Series(dtype="float64"),
        "dew_point_c":     pd.Series(dtype="float64"),
        "humidity_pct":    pd.Series(dtype="float64"),
        "wind_speed_kmh":  pd.Series(dtype="float64"),
        "precipitation_mm": pd.Series(dtype="float64"),
        "extracted_at":    pd.Series(dtype="datetime64[ns, UTC]"),
    })
