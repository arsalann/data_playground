"""@bruin
name: raw.de_genesis_tables
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Destatis GENESIS-Online tables for the 16 German states (Bundeslaender) plus the
  matching Germany-level tables, pulled through the GENESIS REST API 2020
  (https://genesis.destatis.de/genesisWS/rest/2020/data/tablefile, flat CSV "ffcsv").

  One row per published cell, in long format: table, time, up to four classifying
  variables (the first is usually the state, DLAND), the value variable, and the value.
  Values keep the German decimal comma in `value_raw`; `value` is the parsed number and
  is NULL where Destatis publishes a special sign ("-", ".", "/", "x", "...").

  Tables (all Bundeslaender unless noted):
    12411-0010 population on 31.12., 12411-0014 population by nationality, sex and age,
    11111-0001 land area, 12612-0104 total fertility rate (12612-0009 Germany),
    12711-0020 migration across state borders, 13211-0007 registered unemployment
    (13211-0001 Germany), 61511-0050 building land prices (61511-0010 Germany),
    62361-0051 gross hourly earnings April (62361-0046 Germany), 62361-0050 low-wage jobs
    April, 12621-0004 life expectancy at birth (12621-0002 Germany).

  The fetch window is the last four calendar years up to the year of BRUIN_END_DATE,
  so a default run still returns the latest complete reference year.
  License: Datenlizenz Deutschland - Namensnennung - Version 2.0 (dl-de/by-2-0),
  source: Statistisches Bundesamt (Destatis).

secrets:
  - key: destatos-genesis-germany-key
    inject_as: GENESIS_API_TOKEN

materialization:
  type: table
  strategy: append

columns:
  - name: table_code
    type: VARCHAR
    description: GENESIS table code, e.g. 12411-0010.
    primary_key: true
  - name: table_label
    type: VARCHAR
    description: GENESIS statistic label (German), e.g. "Fortschreibung des Bevoelkerungsstandes".
  - name: time_code
    type: VARCHAR
    description: GENESIS time variable code (JAHR = year, STAG = reference day, MONAT = reference month).
  - name: time_value
    type: VARCHAR
    description: Raw GENESIS time value, e.g. 2025, 2025-12-31, 2025-04P1M (April 2025), 2023-P2Y (period 2023-2025).
    primary_key: true
  - name: region_code
    type: VARCHAR
    description: Official state code (AGS 01-16) from the DLAND variable, or DG for Germany-level tables.
    primary_key: true
  - name: region_name
    type: VARCHAR
    description: State name as published by Destatis (German), or Deutschland.
  - name: dim1_code
    type: VARCHAR
    description: Variable code of the first non-regional classifying variable (e.g. NAT, GES, ALTX20); NULL if none.
  - name: dim1_value_code
    type: VARCHAR
    description: Attribute code for dim1 (e.g. NATA for foreigners); empty string if no dim1.
    primary_key: true
  - name: dim1_value_label
    type: VARCHAR
    description: Attribute label for dim1 (German).
  - name: dim2_code
    type: VARCHAR
    description: Variable code of the second non-regional classifying variable; NULL if none.
  - name: dim2_value_code
    type: VARCHAR
    description: Attribute code for dim2; empty string if no dim2.
    primary_key: true
  - name: dim2_value_label
    type: VARCHAR
    description: Attribute label for dim2 (German).
  - name: dim3_code
    type: VARCHAR
    description: Variable code of the third non-regional classifying variable; NULL if none.
  - name: dim3_value_code
    type: VARCHAR
    description: Attribute code for dim3; empty string if no dim3.
    primary_key: true
  - name: dim3_value_label
    type: VARCHAR
    description: Attribute label for dim3 (German).
  - name: value_variable_code
    type: VARCHAR
    description: GENESIS value variable code, e.g. BEVSTD (population), ERW032 (unemployment rate).
    primary_key: true
  - name: value_variable_label
    type: VARCHAR
    description: GENESIS value variable label (German).
  - name: value_unit
    type: VARCHAR
    description: Unit as published (Anzahl, Prozent, EUR, EUR/qm, qkm, Jahre, 1000, Tsd. EUR).
  - name: value_raw
    type: VARCHAR
    description: Cell value exactly as published, German number format, including special signs.
  - name: value
    type: DOUBLE
    description: Parsed numeric value in `value_unit`; NULL when the cell is a special sign (not available, secret, not meaningful).
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of ingestion.

@bruin"""

import csv
import io
import logging
import os
import time
import zipfile
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

GENESIS_URL = "https://genesis.destatis.de/genesisWS/rest/2020/data/tablefile"
MAX_RETRIES = 5
LOOKBACK_YEARS = 3

# Tables are fetched whole for the year window; staging picks the cells it needs.
TABLES = [
    "12411-0010",  # Bevoelkerung: Bundeslaender, Stichtag
    "12411-0014",  # Bevoelkerung: Bundeslaender, Stichtag, Nationalitaet, Geschlecht, Altersjahre
    "11111-0001",  # Gebietsflaeche: Bundeslaender, Stichtag
    "12612-0104",  # Zusammengefasste Geburtenziffern: Bundeslaender, Jahre
    "12612-0009",  # Zusammengefasste Geburtenziffern: Deutschland, Jahre
    "12711-0020",  # Gesamtwanderungen ueber die Grenzen der Bundeslaender: Bundeslaender, Jahre
    "13211-0007",  # Arbeitslose, Arbeitslosenquoten: Bundeslaender, Jahre
    "13211-0001",  # Arbeitslose, Arbeitslosenquoten: Deutschland, Jahre
    "61511-0050",  # Kaufwerte fuer Bauland: Bundeslaender, Jahre
    "61511-0010",  # Kaufwerte fuer Bauland: Deutschland, Jahre
    "62361-0051",  # Bruttostundenverdienste: Bundeslaender, Stichmonat April
    "62361-0046",  # Bruttostundenverdienste: Deutschland, Stichmonat April
    "62361-0050",  # Beschaeftigungsverhaeltnisse mit Niedriglohn: Bundeslaender, Stichmonat April
    "12621-0004",  # Lebenserwartung bei Geburt: Bundeslaender, 3-Jahres-Zeitraum
    "12621-0002",  # Lebenserwartung: Deutschland, 3-Jahres-Zeitraum, Vollendetes Alter
]

SPECIAL_SIGNS = {"-", ".", "/", "x", "...", "–", ""}
REGION_VARIABLES = {"DLAND", "DINSG"}


def parse_number(raw: str):
    raw = (raw or "").strip()
    if raw in SPECIAL_SIGNS:
        return None
    try:
        return float(raw.replace(".", "").replace(",", ".")) if "," in raw else float(raw)
    except ValueError:
        return None


class TableTooLarge(Exception):
    pass


def fetch_table(token: str, code: str, start_year: int, end_year: int) -> str | None:
    headers = {
        "username": token,
        "password": "",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    body = {
        "name": code,
        "area": "all",
        "compress": "false",
        "transpose": "false",
        "startyear": str(start_year),
        "endyear": str(end_year),
        "format": "ffcsv",
        "language": "de",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(GENESIS_URL, headers=headers, data=body, timeout=300)
        except requests.RequestException as e:
            wait = 2 ** attempt
            logger.warning("%s: network error on attempt %d (%s); retry in %ds", code, attempt, e, wait)
            time.sleep(wait)
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            wait = 2 ** attempt * 5
            logger.warning("%s: HTTP %d on attempt %d; retry in %ds", code, r.status_code, attempt, wait)
            time.sleep(wait)
            continue
        if r.status_code != 200:
            logger.warning("%s: HTTP %d, skipping", code, r.status_code)
            return None
        content_type = r.headers.get("content-type", "")
        if "zip" in content_type or r.content[:2] == b"PK":
            z = zipfile.ZipFile(io.BytesIO(r.content))
            return z.read(z.namelist()[0]).decode("utf-8-sig")
        if "json" in content_type:
            # GENESIS reports "no data for this selection" or job-mode status as JSON.
            status = r.json().get("Status", {})
            if "zu gross" in (status.get("Content") or ""):
                raise TableTooLarge(code)
            logger.warning("%s: no table returned (%s)", code, status.get("Content"))
            return None
        return r.content.decode("utf-8-sig")
    logger.warning("%s: giving up after %d attempts", code, MAX_RETRIES)
    return None


def flatten(code: str, text: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    n_vars = sum(1 for f in (reader.fieldnames or []) if f.endswith("_variable_code") and f[0].isdigit())
    rows = []
    for rec in reader:
        region_code, region_name, dims = "DG", "Deutschland", []
        for i in range(1, n_vars + 1):
            var = rec.get(f"{i}_variable_code")
            attr_code = rec.get(f"{i}_variable_attribute_code") or ""
            attr_label = rec.get(f"{i}_variable_attribute_label") or ""
            if var in REGION_VARIABLES:
                region_code, region_name = attr_code, attr_label
            else:
                dims.append((var, attr_code, attr_label))
        if len(dims) > 3:
            raise ValueError(f"{code}: more than three non-regional variables, extend the schema")
        dims += [(None, "", None)] * (3 - len(dims))
        row = {
            "table_code": code,
            "table_label": rec.get("statistics_label"),
            "time_code": rec.get("time_code"),
            "time_value": rec.get("time"),
            "region_code": region_code,
            "region_name": region_name,
            "value_variable_code": rec.get("value_variable_code"),
            "value_variable_label": rec.get("value_variable_label"),
            "value_unit": rec.get("value_unit"),
            "value_raw": rec.get("value"),
            "value": parse_number(rec.get("value")),
        }
        for n, (var, attr_code, attr_label) in enumerate(dims, start=1):
            row[f"dim{n}_code"] = var
            row[f"dim{n}_value_code"] = attr_code
            row[f"dim{n}_value_label"] = attr_label
        rows.append(row)
    return rows


def materialize():
    token = os.environ["GENESIS_API_TOKEN"]
    end_date = os.environ.get("BRUIN_END_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    start_date = os.environ.get("BRUIN_START_DATE", end_date)
    end_year = int(end_date[:4])
    start_year = min(int(start_date[:4]), end_year - LOOKBACK_YEARS)
    table_filter = os.environ.get("GENESIS_TABLES")
    tables = [t for t in TABLES if not table_filter or t in table_filter.split(",")]
    logger.info("Fetching %d GENESIS tables for %d-%d", len(tables), start_year, end_year)

    all_rows = []
    for code in tables:
        try:
            texts = [fetch_table(token, code, start_year, end_year)]
        except TableTooLarge:
            # Large tables must be requested one year at a time in synchronous mode.
            logger.info("%s: too large for one request, fetching year by year", code)
            texts = []
            for year in range(start_year, end_year + 1):
                texts.append(fetch_table(token, code, year, year))
                time.sleep(0.5)
        rows = [row for text in texts if text is not None for row in flatten(code, text)]
        if not rows:
            continue
        years = sorted({r["time_value"] for r in rows})
        logger.info("%s: %d cells, periods %s", code, len(rows), ", ".join(years))
        all_rows.extend(rows)
        time.sleep(0.5)

    if not all_rows:
        raise RuntimeError("No GENESIS tables returned; check the API token and the year window")

    df = pd.DataFrame(all_rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Total rows: %d from %d tables", len(df), df["table_code"].nunique())
    return df
