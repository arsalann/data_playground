"""@bruin
name: raw.cdmx_denue_units
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests the Ciudad de Mexico extract of INEGI's Directorio Estadistico Nacional de
  Unidades Economicas (DENUE), the national statistical business directory. One row per
  fixed economic establishment across all 16 alcaldias.

  DENUE is the only source in this project produced by a national statistics institute
  rather than a municipality, and it is the only one where every row carries coordinates.
  Activity is classified with 6-digit SCIAN codes, which separate bars from nightclubs from
  cafeterias without ambiguity.

  Two implementation notes:
  - The archive holds three CSVs. The data is `conjunto_de_datos/denue_inegi_09_.csv`; the
    first member alphabetically is the data dictionary, not the data.
  - The data file is latin-1 encoded, not UTF-8.

  Source: https://www.inegi.org.mx/app/mapa/denue/
  Bulk file: https://www.inegi.org.mx/contenidos/masiva/denue/denue_09_csv.zip
  Licence: INEGI open terms of use

materialization:
  type: table
  strategy: append

columns:
  - name: denue_id
    type: VARCHAR
    description: DENUE establishment identifier.
    primary_key: true
  - name: clee
    type: VARCHAR
    description: Clave Unica de Establecimiento, INEGI's stable composite establishment key.
  - name: establishment_name
    type: VARCHAR
    description: Trading name of the establishment.
  - name: legal_name
    type: VARCHAR
    description: Registered legal name, often blank for sole traders.
  - name: scian_code
    type: VARCHAR
    description: 6-digit SCIAN activity code, for example 722412 for bars and cantinas.
  - name: scian_label
    type: VARCHAR
    description: SCIAN activity label in Spanish.
  - name: employee_band
    type: VARCHAR
    description: Employee-count band as published, for example "0 a 5 personas". DENUE never publishes exact headcount.
  - name: street_name
    type: VARCHAR
    description: Street type and name concatenated.
  - name: street_number
    type: VARCHAR
    description: Exterior street number with any letter suffix.
  - name: settlement_name
    type: VARCHAR
    description: Colonia or other settlement name.
  - name: postal_code
    type: VARCHAR
    description: Postal code.
  - name: alcaldia_code
    type: VARCHAR
    description: INEGI municipality code within entity 09, 001-017.
  - name: alcaldia_name
    type: VARCHAR
    description: Alcaldia name.
  - name: ageb
    type: VARCHAR
    description: Area Geoestadistica Basica identifier, INEGI's small-area statistical unit.
  - name: manzana
    type: VARCHAR
    description: City block identifier within the AGEB, the finest statistical unit published.
  - name: unit_type
    type: VARCHAR
    description: Establishment type. "Fijo" means a fixed premise; DENUE excludes street and informal commerce.
  - name: lon
    type: DOUBLE
    description: Longitude in decimal degrees, WGS84, as published.
  - name: lat
    type: DOUBLE
    description: Latitude in decimal degrees, WGS84, as published.
  - name: registered_month
    type: VARCHAR
    description: Year and month the establishment entered the directory ("fecha_alta"), YYYY-MM.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted.

@bruin"""

import io
import logging
import os
import zipfile
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SOURCE_URL = "https://www.inegi.org.mx/contenidos/masiva/denue/denue_09_csv.zip"
REQUEST_TIMEOUT = 900
DATA_MEMBER_PREFIX = "conjunto_de_datos/"
SOURCE_ENCODING = "latin-1"

USE_COLUMNS = [
    "id",
    "clee",
    "nom_estab",
    "raz_social",
    "codigo_act",
    "nombre_act",
    "per_ocu",
    "tipo_vial",
    "nom_vial",
    "numero_ext",
    "letra_ext",
    "nomb_asent",
    "cod_postal",
    "cve_mun",
    "municipio",
    "ageb",
    "manzana",
    "tipoUniEco",
    "latitud",
    "longitud",
    "fecha_alta",
]

RENAMES = {
    "id": "denue_id",
    "nom_estab": "establishment_name",
    "raz_social": "legal_name",
    "codigo_act": "scian_code",
    "nombre_act": "scian_label",
    "per_ocu": "employee_band",
    "nomb_asent": "settlement_name",
    "cod_postal": "postal_code",
    "cve_mun": "alcaldia_code",
    "municipio": "alcaldia_name",
    "tipoUniEco": "unit_type",
    "latitud": "lat",
    "longitud": "lon",
    "fecha_alta": "registered_month",
}

# Ciudad de Mexico bounding box, used only to flag out-of-range coordinates.
CDMX_BBOX = (-99.40, 19.00, -98.90, 19.65)


def download_archive(url: str) -> zipfile.ZipFile:
    logger.info("Downloading %s", url)
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    logger.info("Downloaded %.1f MB", len(response.content) / 1e6)
    return zipfile.ZipFile(io.BytesIO(response.content))


def find_data_member(archive: zipfile.ZipFile) -> str:
    candidates = [
        name
        for name in archive.namelist()
        if name.startswith(DATA_MEMBER_PREFIX) and name.lower().endswith(".csv")
    ]
    if not candidates:
        raise ValueError(
            f"No CSV under {DATA_MEMBER_PREFIX!r} in the archive; members: {archive.namelist()}"
        )
    member = max(candidates, key=lambda name: archive.getinfo(name).file_size)
    logger.info(
        "Reading %s (%.1f MB uncompressed)", member, archive.getinfo(member).file_size / 1e6
    )
    return member


def materialize() -> pd.DataFrame:
    archive = download_archive(SOURCE_URL)
    member = find_data_member(archive)

    with archive.open(member) as handle:
        df = pd.read_csv(
            handle,
            encoding=SOURCE_ENCODING,
            usecols=USE_COLUMNS,
            dtype=str,
            low_memory=False,
        )
    logger.info("Parsed %d establishment rows", len(df))

    df = df.rename(columns=RENAMES)

    df["street_name"] = (
        df["tipo_vial"].fillna("").str.strip() + " " + df["nom_vial"].fillna("").str.strip()
    ).str.strip()
    df["street_number"] = (
        df["numero_ext"].fillna("").str.strip() + df["letra_ext"].fillna("").str.strip()
    ).str.strip()
    df = df.drop(columns=["tipo_vial", "nom_vial", "numero_ext", "letra_ext"])

    for column in ("lon", "lat"):
        df[column] = pd.to_numeric(df[column], errors="coerce")

    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].str.strip().replace({"": None})

    geocoded = int((df["lon"].notna() & df["lat"].notna()).sum())
    logger.info(
        "Geocoding: %d of %d rows carry lon/lat (%.2f%%)",
        geocoded,
        len(df),
        100.0 * geocoded / max(len(df), 1),
    )
    min_lon, min_lat, max_lon, max_lat = CDMX_BBOX
    inside = df["lon"].between(min_lon, max_lon) & df["lat"].between(min_lat, max_lat)
    outside = int((df["lon"].notna() & ~inside).sum())
    if outside:
        logger.warning("%d rows carry coordinates outside the CDMX bounding box", outside)
    logger.info(
        "Bounding box: lon %.4f..%.4f, lat %.4f..%.4f",
        df["lon"].min(),
        df["lon"].max(),
        df["lat"].min(),
        df["lat"].max(),
    )

    logger.info("Alcaldias covered: %d", df["alcaldia_name"].nunique())
    logger.info("Distinct SCIAN codes: %d", df["scian_code"].nunique())
    for code in ("722412", "722411", "722515", "465312"):
        logger.info(
            "SCIAN %s: %d establishments", code, int((df["scian_code"] == code).sum())
        )
    logger.info(
        "Unit types: %s", df["unit_type"].value_counts(dropna=False).head(5).to_dict()
    )

    df["extracted_at"] = datetime.now(timezone.utc)

    ordered = [
        "denue_id",
        "clee",
        "establishment_name",
        "legal_name",
        "scian_code",
        "scian_label",
        "employee_band",
        "street_name",
        "street_number",
        "settlement_name",
        "postal_code",
        "alcaldia_code",
        "alcaldia_name",
        "ageb",
        "manzana",
        "unit_type",
        "lon",
        "lat",
        "registered_month",
        "extracted_at",
    ]
    df = df[ordered]

    logger.info("Returning %d rows", len(df))
    return df
