"""@bruin
name: raw.de_eurostat_nuts1
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Eurostat regional indicators for the 16 German NUTS 1 regions (= Bundeslaender)
  and Germany as a whole, from the Eurostat dissemination API (JSON-stat 2.0):
  https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/<dataset>

  These indicators come from EU-harmonised household surveys that Destatis does not
  publish at state level in GENESIS:
    - lfst_r_lfe2emprt: employment rate, age 20-64, both sexes (Labour Force Survey), %
    - edat_lfse_04: population aged 25-64 with tertiary education (ISCED 5-8), %
    - ilc_li41: at-risk-of-poverty rate (EU-SILC, 60% of national median), %

  The fetch window is the last four years up to the year of BRUIN_END_DATE.
  License: CC BY 4.0, (c) European Union, Eurostat.

materialization:
  type: table
  strategy: append

columns:
  - name: dataset_code
    type: VARCHAR
    description: Eurostat dataset code.
    primary_key: true
  - name: geo
    type: VARCHAR
    description: NUTS code (DE1-DEG for the states, DE for Germany).
    primary_key: true
  - name: geo_label
    type: VARCHAR
    description: Eurostat label of the NUTS region (English).
  - name: year
    type: INTEGER
    description: Reference year.
    primary_key: true
  - name: value
    type: DOUBLE
    description: Indicator value in percent.
  - name: status_flag
    type: VARCHAR
    description: Eurostat observation flag (b = break in series, u = low reliability, p = provisional, e = estimated); NULL if none.
  - name: dataset_updated
    type: TIMESTAMP
    description: Last update timestamp of the dataset as reported by Eurostat.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of ingestion.

@bruin"""

import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

EUROSTAT_API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
GEOS = ["DE"] + [f"DE{c}" for c in "123456789ABCDEFG"]
DATASETS = {
    "lfst_r_lfe2emprt": {"sex": "T", "age": "Y20-64", "unit": "PC"},
    "edat_lfse_04": {"sex": "T", "age": "Y25-64", "isced11": "ED5-8", "unit": "PC"},
    "ilc_li41": {"unit": "PC"},
}
MAX_RETRIES = 5
LOOKBACK_YEARS = 3


def fetch(dataset: str, filters: dict, years: list[int]) -> dict:
    params = [("format", "JSON"), ("lang", "EN")]
    params += list(filters.items())
    params += [("geo", g) for g in GEOS]
    params += [("time", str(y)) for y in years]
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(EUROSTAT_API + dataset, params=params, timeout=180)
            if r.status_code in (429, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            wait = 2 ** attempt * 3
            logger.warning("%s: %s on attempt %d; retry in %ds", dataset, e, attempt, wait)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch {dataset}")


def parse(dataset: str, payload: dict) -> pd.DataFrame:
    """Decode JSON-stat 2.0 flat value index into geo/year rows (all other dims are fixed to one value)."""
    ids, size = payload.get("id", []), payload.get("size", [])
    values, status = payload.get("value", {}), payload.get("status", {})
    if not values:
        return pd.DataFrame()
    dims = payload["dimension"]
    inv = {d: {idx: code for code, idx in dims[d]["category"]["index"].items()} for d in ids}
    strides = [1] * len(size)
    for i in range(len(size) - 2, -1, -1):
        strides[i] = strides[i + 1] * size[i + 1]
    geo_i, time_i = ids.index("geo"), ids.index("time")
    rows = []
    for key, v in values.items():
        flat, coords = int(key), []
        for s in strides:
            coords.append(flat // s)
            flat %= s
        geo = inv["geo"][coords[geo_i]]
        rows.append({
            "dataset_code": dataset,
            "geo": geo,
            "geo_label": dims["geo"]["category"]["label"].get(geo),
            "year": int(inv["time"][coords[time_i]]),
            "value": float(v) if v is not None else None,
            "status_flag": status.get(key),
        })
    df = pd.DataFrame(rows)
    df["dataset_updated"] = pd.to_datetime(payload.get("updated"), utc=True)
    return df


def materialize():
    end_date = os.environ.get("BRUIN_END_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    start_date = os.environ.get("BRUIN_START_DATE", end_date)
    end_year = int(end_date[:4])
    start_year = min(int(start_date[:4]), end_year - LOOKBACK_YEARS)
    years = list(range(start_year, end_year + 1))
    logger.info("Eurostat window: %d-%d", start_year, end_year)

    frames = []
    for dataset, filters in DATASETS.items():
        df = parse(dataset, fetch(dataset, filters, years))
        if df.empty:
            logger.warning("%s: no values returned", dataset)
            continue
        for year, g in df.groupby("year"):
            logger.info("%s %d: %d/17 regions with values", dataset, year, g["value"].notna().sum())
        frames.append(df)
        time.sleep(0.5)

    if not frames:
        raise RuntimeError("No Eurostat data returned")
    out = pd.concat(frames, ignore_index=True)
    out["extracted_at"] = datetime.now(timezone.utc)
    return out
