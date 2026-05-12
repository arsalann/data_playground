"""@bruin

name: raw.pc_un_wpp_life_expectancy
description: |
  UN World Population Prospects 2024 — abridged life table (both sexes, medium variant).
  Used for life expectancy at exact age 65, which is the key pension-planning metric:
  how many years does the average 65-year-old have left.

  Download source: United Nations, Department of Economic and Social Affairs, Population
  Division (2024). World Population Prospects 2024, Life Tables (Abridged).
  https://population.un.org/wpp/Download/Standard/Mortality/

  Released July 11, 2024. Abridged life tables use 5-year age groups from age 0 to 100+.
  This asset extracts only the ex (life expectancy at exact age) series for age 65,
  filtered to the 38 OECD countries, for every year 1950-2100.

  Same UN-DESA methodology applied to all countries — apples-to-apples.
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
    description: Calendar year (1950-2100).
    primary_key: true
  - name: country_name
    type: VARCHAR
    description: UN-reported country name.
  - name: life_expectancy_at_65
    type: DOUBLE
    description: Remaining life expectancy in years for a person aged exactly 65 (both sexes).
  - name: variant
    type: VARCHAR
    description: WPP variant — always "Medium".
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

WPP_URLS = [
    u for u in os.environ.get(
        "WPP_LIFETABLE_URLS",
        "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Abridged_Medium_1950-2023.csv.gz,"
        "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Abridged_Medium_2024-2100.csv.gz",
    ).split(",") if u.strip()
]

OECD_ISO3 = {
    "AUS", "AUT", "BEL", "CAN", "CHL", "COL", "CRI", "CZE", "DNK", "EST",
    "FIN", "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN",
    "KOR", "LVA", "LTU", "LUX", "MEX", "NLD", "NZL", "NOR", "POL", "PRT",
    "SVK", "SVN", "ESP", "SWE", "CHE", "TUR", "GBR", "USA",
}


def download_csv(url: str) -> bytes:
    logger.info("Downloading %s", url)
    resp = requests.get(url, timeout=300, stream=True)
    resp.raise_for_status()
    data = resp.content
    logger.info("Downloaded %d bytes", len(data))
    return data


def load_and_filter(url: str, raw: bytes) -> pd.DataFrame:
    """Stream-parse the CSV in chunks, keeping only OECD age-65 rows to bound memory."""
    if url.endswith(".gz"):
        buf = io.BytesIO(gzip.decompress(raw))
    else:
        buf = io.BytesIO(raw)

    kept = []
    required = {"ISO3_code", "Location", "Variant", "Time", "AgeGrpStart", "ex"}
    for chunk in pd.read_csv(buf, low_memory=False, chunksize=200_000):
        missing = required - set(chunk.columns)
        if missing:
            raise RuntimeError(f"WPP life table missing expected columns: {missing}. Got: {list(chunk.columns)}")
        # Abridged file has Sex column with Male/Female/Both — keep Both only.
        sex_col = chunk["Sex"] if "Sex" in chunk.columns else None
        mask = (chunk["ISO3_code"].isin(OECD_ISO3)) & (chunk["AgeGrpStart"] == 65)
        if sex_col is not None:
            mask &= sex_col.isin({"Both", "Total"})
        kept.append(chunk.loc[mask])
    df = pd.concat(kept, ignore_index=True) if kept else pd.DataFrame()
    logger.info("Kept %d OECD age-65 rows from %s", len(df), url)
    return df


def extract_e65(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "iso3_code": df["ISO3_code"],
        "year": df["Time"].astype(int),
        "country_name": df["Location"],
        "life_expectancy_at_65": df["ex"].astype(float),
        "variant": df["Variant"],
    })
    return out


def materialize():
    frames = []
    for url in WPP_URLS:
        raw = download_csv(url)
        frames.append(load_and_filter(url, raw))
    combined = pd.concat(frames, ignore_index=True)
    out = extract_e65(combined)
    out = out.drop_duplicates(subset=["iso3_code", "year"]).reset_index(drop=True)
    out["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Emitting %d rows, %d countries, years %d-%d",
        len(out), out["iso3_code"].nunique(), out["year"].min(), out["year"].max(),
    )
    return out
