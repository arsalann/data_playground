"""@bruin
name: raw.epias_dpp_first_version
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches day-ahead production program (FDPP - first version) data by source
  type from the EPIAS Transparency Platform. This represents what power plants
  planned to produce the day before, enabling forecast vs actual comparisons.

  Data source: https://seffaflik.epias.com.tr/electricity-service
  Endpoint: POST /v1/generation/data/dpp-first-version

secrets:
  - key: epias_username
  - key: epias_password

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date
    type: TIMESTAMP
    description: Date and hour of the planned generation (Turkish time, +03:00)
    primary_key: true
  - name: natural_gas
    type: DOUBLE
    description: Planned generation from natural gas plants (MWh)
  - name: wind
    type: DOUBLE
    description: Planned generation from wind plants (MWh)
  - name: lignite
    type: DOUBLE
    description: Planned generation from lignite coal plants (MWh)
  - name: hard_coal
    type: DOUBLE
    description: Planned generation from imported hard coal plants (MWh)
  - name: fuel_oil
    type: DOUBLE
    description: Planned generation from fuel oil plants (MWh)
  - name: geothermal
    type: DOUBLE
    description: Planned generation from geothermal plants (MWh)
  - name: dammed_hydro
    type: DOUBLE
    description: Planned generation from dammed hydroelectric plants (MWh)
  - name: river
    type: DOUBLE
    description: Planned generation from run-of-river hydroelectric plants (MWh)
  - name: solar
    type: DOUBLE
    description: Planned generation from solar plants (MWh)
  - name: biomass
    type: DOUBLE
    description: Planned generation from biomass plants (MWh)
  - name: naphta
    type: DOUBLE
    description: Planned generation from naphta plants (MWh)
  - name: import_export
    type: DOUBLE
    description: Planned net import/export balance (MWh, positive = import)
  - name: total
    type: DOUBLE
    description: Total planned generation across all sources (MWh)
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched from the API

@bruin"""

import logging
import os
import time
from datetime import datetime, timedelta
from urllib.parse import quote

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://seffaflik.epias.com.tr/electricity-service"
AUTH_URL = "https://giris.epias.com.tr/cas/v1/tickets"

FIELD_MAP = {
    "naturalGas": "natural_gas",
    "wind": "wind",
    "lignite": "lignite",
    "importCoal": "hard_coal",
    "fueloil": "fuel_oil",
    "geothermal": "geothermal",
    "dammedHydro": "dammed_hydro",
    "river": "river",
    "sun": "solar",
    "biomass": "biomass",
    "naphta": "naphta",
    "importExport": "import_export",
    "total": "total",
}


def get_tgt():
    username = os.environ["epias_username"]
    password = os.environ["epias_password"]
    encoded_body = f"username={quote(username, safe='')}&password={quote(password, safe='')}"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/plain",
    }
    resp = requests.post(AUTH_URL, data=encoded_body, headers=headers, timeout=30)
    if resp.status_code != 200:
        logger.error("Auth failed (%d): %s", resp.status_code, resp.text[:300])
    resp.raise_for_status()
    tgt = resp.text.strip()
    if tgt.startswith("<"):
        raise RuntimeError(f"Auth returned HTML instead of TGT: {tgt[:200]}")
    logger.info("TGT obtained (length=%d)", len(tgt))
    return tgt


def epias_post(endpoint, body, tgt):
    url = f"{BASE_URL}{endpoint}"
    headers = {"TGT": tgt, "Content-Type": "application/json", "Accept": "application/json"}
    for attempt in range(5):
        try:
            resp = requests.post(url, json=body, headers=headers, timeout=60)
            if resp.status_code in (429, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("HTTP %d, retrying in %ds...", resp.status_code, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            wait = 10 * (attempt + 1)
            logger.warning("Timeout on attempt %d, retrying in %ds...", attempt + 1, wait)
            time.sleep(wait)
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            logger.warning("Connection error on attempt %d, retrying in %ds...", attempt + 1, wait)
            time.sleep(wait)
    raise RuntimeError(f"Failed after 5 retries: {endpoint}")


def fmt_date(dt):
    return dt.strftime("%Y-%m-%dT00:00:00+03:00")


def parse_items(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "hourlyGenerations", "content", "dppList"):
            if key in data and isinstance(data[key], list):
                return data[key]
        body = data.get("body", {})
        if isinstance(body, dict):
            for key in ("hourlyGenerations", "content", "items", "dppList"):
                if key in body and isinstance(body[key], list):
                    return body[key]
        if isinstance(body, list):
            return body
    return []


def fetch_dpp(tgt, start_date, end_date):
    all_rows = []
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=30), end_date)
        body = {
            "startDate": fmt_date(current),
            "endDate": fmt_date(chunk_end),
            "region": "TR1",
        }
        logger.info("Fetching dpp-first-version %s to %s", current.date(), chunk_end.date())
        data = epias_post("/v1/generation/data/dpp-first-version", body, tgt)
        items = parse_items(data)

        for item in items:
            row = {"date": item.get("date", item.get("tarih"))}
            for api_field, col_name in FIELD_MAP.items():
                row[col_name] = item.get(api_field, 0.0)
            all_rows.append(row)

        logger.info("  Got %d records", len(items))
        current = chunk_end
        time.sleep(0.5)

    return all_rows


def materialize():
    start_str = os.environ.get("BRUIN_START_DATE", "2025-03-01")
    end_str = os.environ.get("BRUIN_END_DATE", "2026-03-03")
    logger.info("Interval: %s to %s", start_str, end_str)

    start_date = datetime.strptime(start_str[:10], "%Y-%m-%d")
    end_date = datetime.strptime(end_str[:10], "%Y-%m-%d") + timedelta(days=1)

    tgt = get_tgt()
    rows = fetch_dpp(tgt, start_date, end_date)

    if not rows:
        raise RuntimeError("No data fetched from dpp-first-version endpoint")

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["extracted_at"] = datetime.now()

    logger.info("Total records: %d", len(df))
    logger.info("Date range: %s to %s", df["date"].min(), df["date"].max())
    return df
