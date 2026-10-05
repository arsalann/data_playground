"""@bruin
name: raw.madrid_premises
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests the premise-level ("locales") file of the Censo de locales y actividades
  published by the Ayuntamiento de Madrid. One row per registered commercial premise
  in the municipality of Madrid, whether currently open, closed or vacant.

  Coordinates arrive as EPSG:25830 (ETRS89 / UTM zone 30N) easting/northing. Both the
  native coordinates and a WGS84 lon/lat pair reprojected with pyproj are stored, so
  downstream BigQuery GEOGRAPHY functions can be used without re-deriving geometry.

  Source: https://datos.madrid.es/dataset/200085-0-censo-locales
  Licence: CC BY 4.0 (Ayuntamiento de Madrid)

  Note: the resource URL is hardcoded because the portal's CKAN package_show endpoint
  returns 404 for this dataset. The file is UTF-8 with a BOM and semicolon-delimited.

materialization:
  type: table
  strategy: append

columns:
  - name: id_local
    type: VARCHAR
    description: Municipal premise identifier, unique per premise.
    primary_key: true
  - name: district_id
    type: VARCHAR
    description: Madrid district code, 01-21.
  - name: district_name
    type: VARCHAR
    description: Madrid district name.
  - name: barrio_id
    type: VARCHAR
    description: Barrio code within the district.
  - name: barrio_name
    type: VARCHAR
    description: Barrio name.
  - name: census_section_id
    type: VARCHAR
    description: INE census section code for the premise, the smallest statistical unit available.
  - name: coord_x_25830
    type: DOUBLE
    description: Easting in metres, EPSG:25830 (ETRS89 / UTM 30N), as published.
  - name: coord_y_25830
    type: DOUBLE
    description: Northing in metres, EPSG:25830 (ETRS89 / UTM 30N), as published.
  - name: lon
    type: DOUBLE
    description: Longitude in decimal degrees, WGS84, reprojected from EPSG:25830.
  - name: lat
    type: DOUBLE
    description: Latitude in decimal degrees, WGS84, reprojected from EPSG:25830.
  - name: situation_id
    type: VARCHAR
    description: Premise status code.
  - name: situation_desc
    type: VARCHAR
    description: Premise status label, for example "Abierto", "Cerrado", "Uso vivienda".
  - name: access_type_desc
    type: VARCHAR
    description: Access type label, distinguishing street-level premises from those inside a grouping.
  - name: street_name
    type: VARCHAR
    description: Street name of the building containing the premise.
  - name: street_number
    type: VARCHAR
    description: Street number of the building containing the premise.
  - name: grouping_name
    type: VARCHAR
    description: Name of the commercial grouping (market, mall, gallery) if the premise sits inside one.
  - name: grouping_type_desc
    type: VARCHAR
    description: Type of commercial grouping if applicable.
  - name: signage_name
    type: VARCHAR
    description: Trading name displayed on the premise ("rotulo"), often blank.
  - name: postal_code
    type: VARCHAR
    description: Postal code.
  - name: opening_time
    type: VARCHAR
    description: First opening time of day as published, HH:MM, often blank.
  - name: closing_time
    type: VARCHAR
    description: Last closing time of day as published, HH:MM, often blank.
  - name: source_load_date
    type: VARCHAR
    description: Publisher's own load date for the row ("fx_carga"), the register's freshness stamp.
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
from pyproj import Transformer

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SOURCE_URL = (
    "https://datos.madrid.es/dataset/200085-0-censo-locales/resource/"
    "200085-1-censo-locales/download/200085-1-censo-locales.csv"
)
REQUEST_TIMEOUT = 600

# Only the columns needed downstream. The published file carries 46.
USE_COLUMNS = [
    "id_local",
    "id_distrito_local",
    "desc_distrito_local",
    "id_barrio_local",
    "desc_barrio_local",
    "id_seccion_censal_local",
    "coordenada_x_local",
    "coordenada_y_local",
    "id_situacion_local",
    "desc_situacion_local",
    "desc_tipo_acceso_local",
    "desc_vial_edificio",
    "num_edificio",
    "nombre_agrupacion",
    "desc_tipo_agrup",
    "rotulo",
    "cod_postal",
    "hora_apertura1",
    "hora_cierre1",
    "fx_carga",
]

RENAMES = {
    "id_distrito_local": "district_id",
    "desc_distrito_local": "district_name",
    "id_barrio_local": "barrio_id",
    "desc_barrio_local": "barrio_name",
    "id_seccion_censal_local": "census_section_id",
    "coordenada_x_local": "coord_x_25830",
    "coordenada_y_local": "coord_y_25830",
    "id_situacion_local": "situation_id",
    "desc_situacion_local": "situation_desc",
    "desc_tipo_acceso_local": "access_type_desc",
    "desc_vial_edificio": "street_name",
    "num_edificio": "street_number",
    "nombre_agrupacion": "grouping_name",
    "desc_tipo_agrup": "grouping_type_desc",
    "rotulo": "signage_name",
    "cod_postal": "postal_code",
    "hora_apertura1": "opening_time",
    "hora_cierre1": "closing_time",
    "fx_carga": "source_load_date",
}

STRING_COLUMNS = [
    "id_local",
    "district_id",
    "district_name",
    "barrio_id",
    "barrio_name",
    "census_section_id",
    "situation_id",
    "situation_desc",
    "access_type_desc",
    "street_name",
    "street_number",
    "grouping_name",
    "grouping_type_desc",
    "signage_name",
    "postal_code",
    "opening_time",
    "closing_time",
    "source_load_date",
]

# Madrid municipality bounding box, used only to flag out-of-range coordinates.
MADRID_BBOX = (-3.95, 40.28, -3.45, 40.68)


def download_csv(url: str) -> bytes:
    logger.info("Downloading %s", url)
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    logger.info("Downloaded %.1f MB", len(response.content) / 1e6)
    return response.content


def reproject(df: pd.DataFrame) -> pd.DataFrame:
    """Add WGS84 lon/lat from EPSG:25830 easting/northing.

    The register uses (0, 0) as its missing-coordinate sentinel rather than an empty field,
    so those rows are nulled before reprojection. Left as-is they would reproject to a point
    in the Atlantic off the coast of Africa.
    """
    sentinel = (df["coord_x_25830"] == 0) | (df["coord_y_25830"] == 0)
    if sentinel.any():
        logger.info(
            "Nulling %d rows whose coordinates are the (0, 0) missing-value sentinel",
            int(sentinel.sum()),
        )
        df.loc[sentinel, ["coord_x_25830", "coord_y_25830"]] = pd.NA

    transformer = Transformer.from_crs("EPSG:25830", "EPSG:4326", always_xy=True)
    has_coords = df["coord_x_25830"].notna() & df["coord_y_25830"].notna()
    df["lon"] = pd.NA
    df["lat"] = pd.NA
    if has_coords.any():
        lon, lat = transformer.transform(
            df.loc[has_coords, "coord_x_25830"].to_numpy(),
            df.loc[has_coords, "coord_y_25830"].to_numpy(),
        )
        df.loc[has_coords, "lon"] = lon
        df.loc[has_coords, "lat"] = lat
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")

    min_lon, min_lat, max_lon, max_lat = MADRID_BBOX
    inside = (
        df["lon"].between(min_lon, max_lon) & df["lat"].between(min_lat, max_lat)
    )
    logger.info(
        "Geocoding: %d of %d rows have coordinates, %d inside the Madrid bounding box",
        int(has_coords.sum()),
        len(df),
        int(inside.sum()),
    )
    outside = int((df["lon"].notna() & ~inside).sum())
    if outside:
        logger.warning("%d rows carry coordinates outside the Madrid bounding box", outside)

    # Missing coordinates are strongly structured by access type: street-front premises are
    # geocoded, premises inside markets, malls and galleries frequently are not. Log the
    # breakdown because it decides which premises can appear on a map at all.
    coverage = (
        df.assign(geocoded=df["lon"].notna())
        .groupby("access_type_desc", dropna=False)["geocoded"]
        .agg(["size", "sum"])
    )
    for access_type, row in coverage.iterrows():
        logger.info(
            "Geocoded by access type %-14s: %6d of %6d (%.1f%%)",
            str(access_type),
            int(row["sum"]),
            int(row["size"]),
            100.0 * row["sum"] / max(row["size"], 1),
        )
    return df


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
    logger.info("Parsed %d premise rows, %d columns", len(df), len(df.columns))

    df = df.rename(columns=RENAMES)

    for column in ("coord_x_25830", "coord_y_25830"):
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = reproject(df)

    for column in STRING_COLUMNS:
        df[column] = df[column].str.strip().replace({"": None})

    open_count = int((df["situation_desc"].str.upper() == "ABIERTO").sum())
    logger.info(
        "Status breakdown: %s",
        df["situation_desc"].value_counts(dropna=False).head(8).to_dict(),
    )
    logger.info("Open premises: %d", open_count)
    logger.info("Publisher load date (fx_carga) max: %s", df["source_load_date"].max())

    df["extracted_at"] = datetime.now(timezone.utc)

    ordered = [
        "id_local",
        "district_id",
        "district_name",
        "barrio_id",
        "barrio_name",
        "census_section_id",
        "coord_x_25830",
        "coord_y_25830",
        "lon",
        "lat",
        "situation_id",
        "situation_desc",
        "access_type_desc",
        "street_name",
        "street_number",
        "grouping_name",
        "grouping_type_desc",
        "signage_name",
        "postal_code",
        "opening_time",
        "closing_time",
        "source_load_date",
        "extracted_at",
    ]
    df = df[ordered]

    logger.info("Returning %d rows", len(df))
    return df
