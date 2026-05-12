"""@bruin

name: eu_pfas_raw.manufacturer_facilities
description: |
  Hand-curated seed of known PFAS-relevant manufacturing facilities operating in
  the EU-27. Companies covered: 3M, Solvay, Chemours, Daikin, Arkema, AGC, Bayer
  (Currenta), Saint-Gobain Performance Plastics, Miteni (defunct, source of the
  Veneto catastrophe).

  Source: seed/manufacturer_facilities.yml (in-repo seed, sibling of assets/).

  Why a seed and not a scraping job: this is a small, slow-changing list curated
  from press coverage, ICIJ Toxic Bonds / Forever Lobbying reporting, and EPA /
  ECHA filings. The seed is checked in so every measurement is reproducible and
  reviewable.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace
image: python:3.11

tags:
  - eu-27
  - pfas
  - raw
  - seed
  - manufacturer

columns:
  - name: facility_name
    type: VARCHAR
    description: Facility name as commonly cited.
    primary_key: true
    checks:
      - name: not_null
  - name: company
    type: VARCHAR
    description: Parent company / operator.
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2.
  - name: city
    type: VARCHAR
    description: City / locality.
  - name: sector
    type: VARCHAR
    description: Sector descriptor (PFAS production, fluoropolymer manufacture, etc.).
  - name: operating_status
    type: VARCHAR
    description: Operating status -- active, production_paused_YYYY, or closed_YYYY.
  - name: notes
    type: VARCHAR
    description: Free-text provenance / context.
  - name: lat
    type: DOUBLE
    description: Latitude.
  - name: lon
    type: DOUBLE
    description: Longitude.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of ingestion.

@bruin"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SEED = Path(__file__).resolve().parents[2] / "seed" / "manufacturer_facilities.yml"


def materialize():
    with open(SEED) as f:
        manifest = yaml.safe_load(f)
    rows = []
    for f_ in manifest["facilities"]:
        rows.append({
            "facility_name": f_["name"],
            "company": f_.get("company"),
            "country_code": f_["country"],
            "city": f_.get("city"),
            "sector": f_.get("sector"),
            "operating_status": f_.get("operating_status"),
            "notes": f_.get("notes"),
            "lat": float(f_["lat"]),
            "lon": float(f_["lon"]),
        })
    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Manufacturer facilities seed loaded: %d rows", len(df))
    return df
