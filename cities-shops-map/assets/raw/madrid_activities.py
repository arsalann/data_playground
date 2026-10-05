"""@bruin
name: raw.madrid_activities
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests the activity-level file of the Censo de locales y actividades published by the
  Ayuntamiento de Madrid. One row per (premise, economic activity) pair, so a premise
  holding several licensed activities appears more than once.

  This file carries the CNAE-derived activity taxonomy that makes Madrid the highest-scoring
  source in this project: `desc_epigrafe` resolves to 455 distinct classes, separating
  BAR CON COCINA from BAR SIN COCINA from TABERNA from CAFETERIA, and splitting bakeries by
  whether they have an on-site oven.

  Source: https://datos.madrid.es/dataset/200085-0-censo-locales
  Licence: CC BY 4.0 (Ayuntamiento de Madrid)

materialization:
  type: table
  strategy: append

columns:
  - name: id_local
    type: VARCHAR
    description: Municipal premise identifier, joins to raw.madrid_premises.
    primary_key: true
  - name: epigrafe_id
    type: VARCHAR
    description: Activity class code ("epigrafe"), the finest level of the municipal taxonomy.
    primary_key: true
  - name: epigrafe_desc
    type: VARCHAR
    description: Activity class label, for example "BAR CON COCINA" or "CAFETERIA".
  - name: division_id
    type: VARCHAR
    description: CNAE division code containing the activity class.
  - name: division_desc
    type: VARCHAR
    description: CNAE division label.
  - name: section_id
    type: VARCHAR
    description: CNAE section code containing the division.
  - name: section_desc
    type: VARCHAR
    description: CNAE section label.
  - name: situation_desc
    type: VARCHAR
    description: Premise status label as carried on the activity row, for example "Abierto".
  - name: district_name
    type: VARCHAR
    description: Madrid district name, denormalized onto the activity row by the publisher.
  - name: barrio_name
    type: VARCHAR
    description: Barrio name, denormalized onto the activity row by the publisher.
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
    "200085-5-censo-locales/download/200085-5-censo-locales.csv"
)
REQUEST_TIMEOUT = 600

USE_COLUMNS = [
    "id_local",
    "id_epigrafe",
    "desc_epigrafe",
    "id_division",
    "desc_division",
    "id_seccion",
    "desc_seccion",
    "desc_situacion_local",
    "desc_distrito_local",
    "desc_barrio_local",
    "rotulo",
    "fx_carga",
]

RENAMES = {
    "id_epigrafe": "epigrafe_id",
    "desc_epigrafe": "epigrafe_desc",
    "id_division": "division_id",
    "desc_division": "division_desc",
    "id_seccion": "section_id",
    "desc_seccion": "section_desc",
    "desc_situacion_local": "situation_desc",
    "desc_distrito_local": "district_name",
    "desc_barrio_local": "barrio_name",
    "rotulo": "signage_name",
    "fx_carga": "source_load_date",
}


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
    logger.info("Parsed %d activity rows", len(df))

    df = df.rename(columns=RENAMES)

    for column in df.columns:
        df[column] = df[column].str.strip().replace({"": None})

    logger.info("Distinct premises: %d", df["id_local"].nunique())
    logger.info("Distinct epigrafe classes: %d", df["epigrafe_desc"].nunique())

    is_open = df["situation_desc"].str.upper() == "ABIERTO"
    logger.info("Open activity rows: %d", int(is_open.sum()))
    logger.info(
        "Top open classes: %s",
        df.loc[is_open, "epigrafe_desc"].value_counts().head(15).to_dict(),
    )

    # Composite key must be unique for the declared primary key to hold.
    duplicates = int(df.duplicated(subset=["id_local", "epigrafe_id"]).sum())
    if duplicates:
        logger.warning(
            "%d rows share an (id_local, epigrafe_id) pair; keeping the first of each",
            duplicates,
        )
        df = df.drop_duplicates(subset=["id_local", "epigrafe_id"], keep="first")

    df["extracted_at"] = datetime.now(timezone.utc)

    ordered = [
        "id_local",
        "epigrafe_id",
        "epigrafe_desc",
        "division_id",
        "division_desc",
        "section_id",
        "section_desc",
        "situation_desc",
        "district_name",
        "barrio_name",
        "signage_name",
        "source_load_date",
        "extracted_at",
    ]
    df = df[ordered]

    logger.info("Returning %d rows", len(df))
    return df
