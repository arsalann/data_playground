"""@bruin

name: raw.pc_un_wpp_population
description: |
  UN World Population Prospects 2024 — population by broad age group for every year
  1950-2100, medium-variant projections.

  Download source: United Nations, Department of Economic and Social Affairs, Population
  Division (2024). World Population Prospects 2024, Online Edition.
  https://population.un.org/wpp/

  Released July 11, 2024. Medium variant is UN's central projection.

  The source CSV is filtered in Python to the 38 OECD member countries (by ISO-3 code)
  to keep the warehouse table small. Every country-year yields one row with total
  population, population aged 0-14, 15-64, and 65+, plus the old-age dependency ratio
  computed on ingest.

  Methodology: same for every country — single UN-DESA model applied uniformly to all
  237 WPP locations. This is the apples-to-apples demographic foundation for the
  pension-crisis pipeline.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace
image: python:3.11

columns:
  - name: iso3_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code as reported by UN WPP.
    primary_key: true
  - name: year
    type: INTEGER
    description: Calendar year of the estimate (1950-2024) or projection (2025-2100).
    primary_key: true
  - name: country_name
    type: VARCHAR
    description: UN-reported country name (may differ slightly from OECD/WB spellings).
  - name: pop_total
    type: DOUBLE
    description: Total population mid-year, in thousands (UN WPP unit).
  - name: pop_0_14
    type: DOUBLE
    description: Population aged 0-14 mid-year, in thousands.
  - name: pop_15_64
    type: DOUBLE
    description: Working-age population 15-64 mid-year, in thousands.
  - name: pop_65plus
    type: DOUBLE
    description: Population aged 65 and over mid-year, in thousands.
  - name: old_age_dep_ratio
    type: DOUBLE
    description: Old-age dependency ratio = pop_65plus / pop_15_64 * 100.
  - name: variant
    type: VARCHAR
    description: WPP variant — always "Medium" for this asset.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this snapshot was ingested.

@bruin"""

import gzip
import io
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

WPP_URL = os.environ.get(
    "WPP_POPULATION_URL",
    "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_PopulationByAge5GroupSex_Medium.csv.gz",
)

OECD_ISO3 = {
    "AUS", "AUT", "BEL", "CAN", "CHL", "COL", "CRI", "CZE", "DNK", "EST",
    "FIN", "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN",
    "KOR", "LVA", "LTU", "LUX", "MEX", "NLD", "NZL", "NOR", "POL", "PRT",
    "SVK", "SVN", "ESP", "SWE", "CHE", "TUR", "GBR", "USA",
}

YOUNG_BANDS = {"0-4", "5-9", "10-14"}
WORKING_BANDS = {"15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49", "50-54", "55-59", "60-64"}
OLD_BANDS = {"65-69", "70-74", "75-79", "80-84", "85-89", "90-94", "95-99", "100+"}


def download_csv(url: str) -> bytes:
    logger.info("Downloading %s", url)
    resp = requests.get(url, timeout=300, stream=True)
    resp.raise_for_status()
    data = resp.content
    logger.info("Downloaded %d bytes", len(data))
    return data


def load_dataframe(raw: bytes) -> pd.DataFrame:
    if WPP_URL.endswith(".gz"):
        buf = io.BytesIO(gzip.decompress(raw))
    else:
        buf = io.BytesIO(raw)
    df = pd.read_csv(buf, low_memory=False)
    logger.info("Loaded %d rows from WPP CSV", len(df))
    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    # WPP 2024 columns: LocID, Location, ISO3_code, ISO2_code, Variant, Time, AgeGrpStart, AgeGrpSpan, AgeGrp, PopMale, PopFemale, PopTotal
    required = {"ISO3_code", "Location", "Variant", "Time", "AgeGrp", "PopTotal"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"WPP CSV missing expected columns: {missing}. Got: {list(df.columns)}")

    df = df[df["ISO3_code"].isin(OECD_ISO3)].copy()
    logger.info("Filtered to %d OECD rows", len(df))

    def band(age_grp: str) -> str:
        if age_grp in YOUNG_BANDS:
            return "young"
        if age_grp in WORKING_BANDS:
            return "working"
        if age_grp in OLD_BANDS:
            return "old"
        return "other"

    df["band"] = df["AgeGrp"].astype(str).map(band)
    pivoted = (
        df.pivot_table(
            index=["ISO3_code", "Location", "Variant", "Time"],
            columns="band",
            values="PopTotal",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reset_index()
    )

    for col in ("young", "working", "old"):
        if col not in pivoted.columns:
            pivoted[col] = 0.0

    pivoted["pop_total"] = pivoted[["young", "working", "old"]].sum(axis=1)
    pivoted["old_age_dep_ratio"] = (pivoted["old"] / pivoted["working"].replace(0, pd.NA)) * 100.0

    out = pd.DataFrame({
        "iso3_code": pivoted["ISO3_code"],
        "year": pivoted["Time"].astype(int),
        "country_name": pivoted["Location"],
        "pop_total": pivoted["pop_total"].astype(float),
        "pop_0_14": pivoted["young"].astype(float),
        "pop_15_64": pivoted["working"].astype(float),
        "pop_65plus": pivoted["old"].astype(float),
        "old_age_dep_ratio": pivoted["old_age_dep_ratio"].astype(float),
        "variant": pivoted["Variant"],
    })
    return out


def materialize():
    raw = download_csv(WPP_URL)
    df = load_dataframe(raw)
    out = aggregate(df)
    out["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Emitting %d rows, %d countries, years %d-%d",
        len(out), out["iso3_code"].nunique(), out["year"].min(), out["year"].max(),
    )
    return out
