"""@bruin
name: raw.madrid_terraces
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests the licensed-terrace file of the Censo de locales y actividades published by
  the Ayuntamiento de Madrid. One row per authorised terrace, joinable to a premise on
  `id_local`.

  Terraces are an annotation layer, not a score input. They exist because no other city in
  this project publishes licensed outdoor seating area and capacity per premise, and because
  in Madrid a bar's terrace is often a larger revenue surface than its interior. The
  `Superficie_ES` / `Superficie_RA` split distinguishes the seasonal authorisation from the
  year-round one.

  Source: https://datos.madrid.es/dataset/200085-0-censo-locales
  Licence: CC BY 4.0 (Ayuntamiento de Madrid)

materialization:
  type: table
  strategy: append

columns:
  - name: id_terraza
    type: VARCHAR
    description: Municipal terrace identifier.
    primary_key: true
  - name: id_local
    type: VARCHAR
    description: Municipal premise identifier the terrace belongs to, joins to raw.madrid_premises.
  - name: district_name
    type: VARCHAR
    description: Madrid district name.
  - name: barrio_name
    type: VARCHAR
    description: Barrio name.
  - name: terrace_period_desc
    type: VARCHAR
    description: Authorisation period label, distinguishing seasonal from year-round terraces.
  - name: terrace_situation_desc
    type: VARCHAR
    description: Terrace status label, for example whether the authorisation is in force.
  - name: terrace_location_desc
    type: VARCHAR
    description: Where the terrace sits relative to the premise, for example on the pavement or in a parking bay.
  - name: surface_seasonal_m2
    type: DOUBLE
    description: Authorised terrace surface in square metres under the seasonal regime ("Superficie_ES").
  - name: surface_annual_m2
    type: DOUBLE
    description: Authorised terrace surface in square metres under the year-round regime ("Superficie_RA").
  - name: tables_seasonal
    type: INTEGER
    description: Authorised table count under the seasonal regime.
  - name: tables_annual
    type: INTEGER
    description: Authorised table count under the year-round regime.
  - name: chairs_seasonal
    type: INTEGER
    description: Authorised chair count under the seasonal regime, a direct outdoor seating capacity.
  - name: chairs_annual
    type: INTEGER
    description: Authorised chair count under the year-round regime.
  - name: signage_name
    type: VARCHAR
    description: Trading name displayed on the premise ("rotulo"), often blank.
  - name: source_load_date
    type: VARCHAR
    description: Publisher's own load date for the row ("fx_carga").
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted.

@bruin"""

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

SOURCE_URL = (
    "https://datos.madrid.es/dataset/200085-0-censo-locales/resource/"
    "200085-6-censo-locales/download/200085-6-censo-locales.csv"
)
REQUEST_TIMEOUT = 600

USE_COLUMNS = [
    "id_terraza",
    "id_local",
    "desc_distrito_local",
    "desc_barrio_local",
    "desc_periodo_terraza",
    "desc_situacion_terraza",
    "desc_ubicacion_terraza",
    "Superficie_ES",
    "Superficie_RA",
    "mesas_es",
    "mesas_ra",
    "sillas_es",
    "sillas_ra",
    "rotulo",
    "fx_carga",
]

RENAMES = {
    "desc_distrito_local": "district_name",
    "desc_barrio_local": "barrio_name",
    "desc_periodo_terraza": "terrace_period_desc",
    "desc_situacion_terraza": "terrace_situation_desc",
    "desc_ubicacion_terraza": "terrace_location_desc",
    "Superficie_ES": "surface_seasonal_m2",
    "Superficie_RA": "surface_annual_m2",
    "mesas_es": "tables_seasonal",
    "mesas_ra": "tables_annual",
    "sillas_es": "chairs_seasonal",
    "sillas_ra": "chairs_annual",
    "rotulo": "signage_name",
    "fx_carga": "source_load_date",
}

NUMERIC_COLUMNS = ["surface_seasonal_m2", "surface_annual_m2"]
INTEGER_COLUMNS = ["tables_seasonal", "tables_annual", "chairs_seasonal", "chairs_annual"]


def download_csv(url: str) -> bytes:
    logger.info("Downloading %s", url)
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    logger.info("Downloaded %.1f MB", len(response.content) / 1e6)
    return response.content


def materialize() -> pd.DataFrame:
    raw = download_csv(SOURCE_URL)

    df = pd.read_csv(
        io.BytesIO(raw),
        sep=";",
        encoding="utf-8-sig",
        usecols=USE_COLUMNS,
        dtype=str,
        low_memory=False,
    )
    logger.info("Parsed %d terrace rows", len(df))

    df = df.rename(columns=RENAMES)

    for column in NUMERIC_COLUMNS:
        # Surface columns use a period decimal separator, for example "7.92" for a
        # three-table terrace. Do not strip periods as thousands separators.
        df[column] = pd.to_numeric(df[column], errors="coerce")

    for column in INTEGER_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].str.strip().replace({"": None})

    logger.info("Distinct premises with a terrace: %d", df["id_local"].nunique())
    for column in NUMERIC_COLUMNS:
        series = df[column].dropna()
        if series.empty:
            continue
        logger.info(
            "%s: n=%d, median %.1f m2, p90 %.1f m2, max %.1f m2",
            column,
            len(series),
            series.median(),
            series.quantile(0.90),
            series.max(),
        )
    logger.info(
        "Outdoor seats (chairs_seasonal): n=%d, median %.0f, max %.0f",
        int(df["chairs_seasonal"].notna().sum()),
        df["chairs_seasonal"].dropna().median() if df["chairs_seasonal"].notna().any() else 0,
        df["chairs_seasonal"].dropna().max() if df["chairs_seasonal"].notna().any() else 0,
    )
    logger.info(
        "Terrace status breakdown: %s",
        df["terrace_situation_desc"].value_counts(dropna=False).head(6).to_dict(),
    )

    df["extracted_at"] = datetime.now(timezone.utc)

    ordered = [
        "id_terraza",
        "id_local",
        "district_name",
        "barrio_name",
        "terrace_period_desc",
        "terrace_situation_desc",
        "terrace_location_desc",
        "surface_seasonal_m2",
        "surface_annual_m2",
        "tables_seasonal",
        "tables_annual",
        "chairs_seasonal",
        "chairs_annual",
        "signage_name",
        "source_load_date",
        "extracted_at",
    ]
    df = df[ordered]

    logger.info("Returning %d rows", len(df))
    return df
