"""@bruin

name: eu_pfas_raw.forever_pollution_sites
description: |
  PFAS contamination sites across Europe from the Forever Pollution Project consortium
  (Le Monde + 17 partners, 2023), maintained and extended by the PFAS Data Hub at CNRS.

  Contains 820K+ georeferenced contamination records spanning military sites, industrial
  facilities, airports, firefighting training areas, and environmental sampling locations
  across all EU-27 member states. Data combines investigative journalism, government
  monitoring, scientific research, and open-source intelligence.

  **Data source**: https://pdh.cnrs.fr/download/full.parquet (~191 MB)
  **Coverage**: 2003-2026, with ~99% of measurements from 2015-2025
  **Refresh**: Updated weekly via pipeline schedule
  **Geographic scope**: EU-27 only (filtered from global dataset)

  **Data characteristics**:
  - ~50% of sites lack city names (rural/unnamed sampling points)
  - ~99% lack sector classification (mainly non-industrial sites)
  - PFAS concentrations span 6+ orders of magnitude (ng/L to mg/L scales)
  - 27 countries represented with comprehensive coverage

  **Deduplication note**: This raw asset preserves all source records including
  multiple measurements per site. Staging layer implements spatial and temporal
  deduplication for analysis-ready datasets.

  **License**: CC BY-SA 4.0 (PFAS Data Hub terms of use)
  **Update frequency**: Weekly refresh from upstream parquet source
connection: bruin-playground-arsalan
tags:
  - eu-27
  - pfas
  - raw
  - forever-pollution-project
  - sites
  - external_source
  - geospatial
  - cnrs
  - environmental_monitoring

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: site_uid
    type: STRING
    description: Synthetic per-row hash (16-character SHA1 prefix) used as primary key since PFAS Data Hub does not provide stable site identifiers.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: category
    type: STRING
    description: PFAS Data Hub site category indicating contamination status.
    checks:
      - name: accepted_values
        value:
          - Known PFAS user
          - Presumptive contamination
          - Detected contamination
          - Manufacturer
  - name: site_name
    type: STRING
    description: Site name as reported by the original source (facility name, location description, or sampling point identifier).
  - name: city
    type: STRING
    description: City or locality name (null for ~50% of records, particularly rural or unnamed sampling locations).
  - name: country
    type: STRING
    description: Country name in English (EU-27 member states only due to upstream filtering).
    checks:
      - name: not_null
  - name: site_type
    type: STRING
    description: Site classification indicating the type of facility or location.
    checks:
      - name: accepted_values
        value:
          - Waste management site
          - Military site
          - Firefighting incident / training
          - PFAS production facility
          - Industrial site
          - Airport
          - Sampling location
  - name: sector
    type: STRING
    description: NACE sector descriptor where available (null for ~99% of records, primarily available for industrial sites).
  - name: source_type
    type: STRING
    description: Data provenance category indicating how the contamination was identified.
    checks:
      - name: accepted_values
        value:
          - OSINT
          - Scientific article
          - Authorities
          - Company
  - name: source_url
    type: STRING
    description: Original authority, press, or research URL providing evidence of contamination.
  - name: dataset_id
    type: INT64
    description: Upstream PDH dataset identifier (numeric key linking to dataset metadata).
    checks:
      - name: not_null
  - name: dataset_name
    type: STRING
    description: Human-readable upstream dataset name identifying the data source or study.
    checks:
      - name: not_null
  - name: matrix
    type: STRING
    description: Environmental sampling matrix (water, soil, sediment, biota, unknown) where PFAS concentration was measured.
  - name: pfas_sum
    type: FLOAT64
    description: Aggregated PFAS concentration in the specified unit (varies widely from 0 to 800M+ due to different measurement scales and matrices).
    checks:
      - name: non_negative
  - name: unit
    type: STRING
    description: Unit of pfas_sum measurement (typically ng/L for water samples, ng/g for solid matrices).
  - name: measure_date
    type: DATE
    description: Date of PFAS measurement or sampling (spans 2005-2026, with ~2% null values).
  - name: measure_year
    type: FLOAT64
    description: Year extracted from measure_date for temporal analysis (2003-2026 range).
  - name: lat
    type: FLOAT64
    description: Latitude coordinate in WGS84 decimal degrees (required for all records).
    checks:
      - name: not_null
      - name: min
        value: -90
      - name: max
        value: 90
  - name: lon
    type: FLOAT64
    description: Longitude coordinate in WGS84 decimal degrees (required for all records).
    checks:
      - name: not_null
      - name: min
        value: -180
      - name: max
        value: 180
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of data ingestion from PFAS Data Hub.
    checks:
      - name: not_null
  - name: index_level_0
    type: INT64
    description: Pandas DataFrame index from source data (preserved for debugging, not analytically meaningful).

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
