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

  This asset serves as the authoritative registry of major PFAS production sources
  for spatial correlation analysis with contamination patterns and health outcomes.
  Facilities are georeferenced to enable distance-based exposure modeling and
  regulatory compliance tracking under the EU Drinking Water Directive 2020/2184
  (0.5 µg/L total PFAS threshold effective January 2026).

  Notable environmental incidents represented:
    - Miteni Trissino: Source of 2013 Veneto PFAS catastrophe (~300k people exposed)
    - 3M Antwerp/Zwijndrecht: Belgian PFAS scandal, production halted 2024
    - Lyon-Sud hotspot: Arkema Pierre-Benite + Daikin cluster contamination
    - Chemours Dordrecht: GenX emissions, groundwater exceedances documented

  Coverage spans active facilities, recently closed operations, and production-paused
  sites where legacy contamination persists. Operational status changes are tracked
  to correlate facility shutdowns with contamination remediation efforts.
connection: bruin-playground-arsalan
tags:
  - eu-27
  - pfas
  - raw
  - seed
  - manufacturer
  - forever-chemicals
  - industrial-sources
  - environmental-health
  - spatial-reference
  - regulatory-compliance

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: facility_name
    type: VARCHAR
    description: Official facility name as commonly cited in regulatory filings and press coverage. Serves as natural business key for linking contamination incidents to source facilities.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: company
    type: VARCHAR
    description: Parent company or facility operator. Major PFAS manufacturers include chemical giants with multiple European facilities (3M, Solvay) and specialized fluorochemical producers.
    checks:
      - name: not_null
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-2 country code. Limited to EU-27 member states for regulatory consistency under EU chemical regulations.
    checks:
      - name: not_null
  - name: city
    type: VARCHAR
    description: City or locality where facility is located. Used for administrative boundary linkage and population exposure modeling within urban areas.
    checks:
      - name: not_null
  - name: sector
    type: VARCHAR
    description: Industrial sector description including specific PFAS-related activities (PFAS production, fluoropolymer manufacture, specialty chemicals, etc.). Enables sector-specific contamination risk assessment.
    checks:
      - name: not_null
  - name: operating_status
    type: VARCHAR
    description: Current operational status with closure/pause dates where applicable. Format active | production_paused_YYYY | closed_YYYY. Critical for temporal correlation with contamination remediation timelines.
    checks:
      - name: not_null
  - name: notes
    type: VARCHAR
    description: Free-text provenance and regulatory context including contamination incident details, enforcement actions, and source attribution (ICIJ reporting, EPA/ECHA filings, press coverage).
    checks:
      - name: not_null
  - name: lat
    type: DOUBLE
    description: Latitude coordinate in decimal degrees (WGS84). Used for spatial joins with contamination monitoring sites and NUTS3 administrative boundaries for exposure assessment.
    checks:
      - name: not_null
  - name: lon
    type: DOUBLE
    description: Longitude coordinate in decimal degrees (WGS84). European facilities span approximately 4.3°E to 12.7°E covering major industrial basins from Netherlands to northern Italy.
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp of data ingestion. Since this is a hand-curated seed dataset, all rows share the same extraction timestamp representing the pipeline run time.
    checks:
      - name: not_null

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
