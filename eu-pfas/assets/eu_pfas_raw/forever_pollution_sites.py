"""@bruin

name: eu_pfas_raw.forever_pollution_sites
description: |
  PFAS contamination sites across Europe -- the canonical aggregation from the
  Forever Pollution Project consortium (Le Monde + 17 partners, 2023), maintained
  and extended by the PFAS Data Hub at CNRS since 2024.

  Source: https://pdh.cnrs.fr/download/full.parquet (~191 MB, last updated 2025).

  Each row is a site record with:
    - category: Known PFAS user / Presumptive contamination / Detected contamination /
      Manufacturer.
    - lat, lon: WGS84.
    - country (filter to EU-27 keeps the EU-only scope).
    - type: Industrial site / Military / Airport / Wastewater / etc.
    - sector: NACE sector where available.
    - source_type, dataset_id, dataset_name: provenance.
    - pfas_sum, unit: aggregated PFAS concentration (single value where reported).
    - matrix: water / soil / sediment / biota / unknown.
    - date, year: measurement date.
    - source_url: hyperlink to authority data.

  This raw asset preserves every site without dedup; staging will resolve the
  one-site-many-measurements case.

  License: CC BY-SA 4.0 (per PFAS Data Hub terms of use).
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: site_uid
    type: VARCHAR
    description: Synthetic per-row hash (used as primary key; PFAS Data Hub does not provide stable site IDs).
    primary_key: true
    checks:
      - name: not_null
  - name: category
    type: VARCHAR
    description: PFAS Data Hub site category (e.g., "Known PFAS user", "Presumptive contamination", "Detected contamination").
  - name: site_name
    type: VARCHAR
    description: Site name as reported by the original source.
  - name: city
    type: VARCHAR
    description: City / locality.
  - name: country
    type: VARCHAR
    description: Country name in English.
  - name: site_type
    type: VARCHAR
    description: Site classification (Industrial site, Military, Airport, etc.).
  - name: sector
    type: VARCHAR
    description: NACE sector descriptor.
  - name: source_type
    type: VARCHAR
    description: Provenance category (Authorities, Company, Press, Whistle-blower, etc.).
  - name: source_url
    type: VARCHAR
    description: Original authority / press URL.
  - name: dataset_id
    type: VARCHAR
    description: Upstream PDH dataset identifier.
  - name: dataset_name
    type: VARCHAR
    description: Human-readable upstream dataset name.
  - name: matrix
    type: VARCHAR
    description: Sampling matrix (water, soil, sediment, biota, unknown).
  - name: pfas_sum
    type: DOUBLE
    description: Aggregated PFAS concentration where reported (single value; unit column gives the unit).
  - name: unit
    type: VARCHAR
    description: Unit of pfas_sum (typically ng/L for water, ng/g for solids).
  - name: measure_date
    type: DATE
    description: Date of measurement where reported.
  - name: measure_year
    type: INTEGER
    description: Year of measurement where reported.
  - name: lat
    type: DOUBLE
    description: Latitude (WGS84).
  - name: lon
    type: DOUBLE
    description: Longitude (WGS84).
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of ingestion.

@bruin"""

import hashlib
import logging
import os
import tempfile
import time
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

PARQUET_URL = "https://pdh.cnrs.fr/download/full.parquet"
MAX_RETRIES = 4

EU27_COUNTRIES = {
    "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czechia",
    "Czech Republic", "Denmark", "Estonia", "Finland", "France", "Germany",
    "Greece", "Hungary", "Ireland", "Italy", "Latvia", "Lithuania",
    "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal", "Romania",
    "Slovakia", "Slovenia", "Spain", "Sweden",
}


def download_parquet(url: str) -> str:
    for attempt in range(MAX_RETRIES):
        try:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
            logger.info("Downloading PFAS parquet from %s", url)
            with requests.get(url, stream=True, timeout=300) as r:
                if r.status_code in (429, 502, 503):
                    wait = 20 * (attempt + 1)
                    logger.warning("HTTP %d, retry in %ds", r.status_code, wait)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                bytes_written = 0
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        tmp.write(chunk)
                        bytes_written += len(chunk)
                tmp.close()
                logger.info("Wrote %.1f MB to %s", bytes_written / 1e6, tmp.name)
                return tmp.name
        except requests.RequestException as e:
            wait = 30 * (attempt + 1)
            logger.warning("Download attempt %d/%d failed: %s", attempt + 1, MAX_RETRIES, e)
            time.sleep(wait)
    raise RuntimeError(f"Failed to download {url} after {MAX_RETRIES} attempts")


def _hash_row(row) -> str:
    s = "|".join([
        str(row.get("lat", "")),
        str(row.get("lon", "")),
        str(row.get("name", "")),
        str(row.get("dataset_id", "")),
        str(row.get("date", "")),
        str(row.get("matrix", "")),
        str(row.get("pfas_sum", "")),
    ])
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


def materialize():
    parquet_path = download_parquet(PARQUET_URL)
    logger.info("Reading parquet into DataFrame")
    df = pd.read_parquet(parquet_path)
    logger.info("Raw rows: %d, cols=%s", len(df), list(df.columns))

    df = df[df["country"].isin(EU27_COUNTRIES)].copy()
    logger.info("After EU-27 country filter: %d rows", len(df))

    df = df[df["lat"].notna() & df["lon"].notna()].copy()
    logger.info("After lat/lon filter: %d rows", len(df))

    df["site_uid"] = df.apply(_hash_row, axis=1)

    df["measure_date"] = pd.to_datetime(df.get("date"), errors="coerce").dt.date
    df["measure_year"] = pd.to_numeric(df.get("year"), errors="coerce").astype("Int64")
    df["pfas_sum"] = pd.to_numeric(df.get("pfas_sum"), errors="coerce")

    out = pd.DataFrame({
        "site_uid": df["site_uid"],
        "category": df.get("category"),
        "site_name": df.get("name"),
        "city": df.get("city"),
        "country": df.get("country"),
        "site_type": df.get("type"),
        "sector": df.get("sector"),
        "source_type": df.get("source_type"),
        "source_url": df.get("source_url"),
        "dataset_id": df.get("dataset_id"),
        "dataset_name": df.get("dataset_name"),
        "matrix": df.get("matrix"),
        "pfas_sum": df["pfas_sum"],
        "unit": df.get("unit"),
        "measure_date": df["measure_date"],
        "measure_year": df["measure_year"].astype("float"),
        "lat": df["lat"].astype(float),
        "lon": df["lon"].astype(float),
    })

    out = out.drop_duplicates(subset=["site_uid"], keep="first")
    out["extracted_at"] = datetime.now(timezone.utc)

    n_with_measurement = out["pfas_sum"].notna().sum()
    by_country = out.groupby("country").size().sort_values(ascending=False)
    logger.info("Final rows: %d, with PFAS measurement: %d", len(out), n_with_measurement)
    logger.info("Top countries: %s", by_country.head(8).to_dict())

    os.unlink(parquet_path)
    return out
