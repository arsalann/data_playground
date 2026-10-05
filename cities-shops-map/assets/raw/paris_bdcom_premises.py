"""@bruin
name: raw.paris_bdcom_premises
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests the premise layer of APUR's BDCOM 2023, the ground-floor commercial census of
  Paris. Unlike every other source in this project, BDCOM is a door-to-door field survey
  rather than an administrative register, which is why it carries three things no licence
  register can: vacant premises as records, exact floor area, and the activity code of the
  same premise in each earlier survey wave.

  The ArcGIS REST layer is paginated with `resultOffset` because `maxRecordCount` is 1000.
  `outSR=4326` is passed so geometry arrives as WGS84 lon/lat rather than Lambert-93.

  The wave columns run c00 through c20 (2000, 2003, 2005, 2007, 2011, 2014, 2017, 2020),
  one more wave than the PLAN.md appraisal recorded, so churn history spans 20 years.

  Source: https://opendata.apur.org/datasets/bdcom-2023
  Service: https://carto2.apur.org/apur/rest/services/BDCOM/bdcom2023/MapServer/1
  Licence: ODbL (APUR)

materialization:
  type: table
  strategy: append

columns:
  - name: objectid
    type: INTEGER
    description: ArcGIS object identifier for the feature.
    primary_key: true
  - name: c_ord
    type: INTEGER
    description: APUR premise identifier within the survey.
  - name: arrondissement
    type: INTEGER
    description: Paris arrondissement number, 1-20.
  - name: quartier
    type: INTEGER
    description: Paris quartier administratif number, 1-80.
  - name: idcar_200m
    type: VARCHAR
    description: INSEE 200 m statistical grid cell identifier the premise falls in, supplied by the publisher.
  - name: lon
    type: DOUBLE
    description: Longitude in decimal degrees, WGS84, requested via outSR=4326.
  - name: lat
    type: DOUBLE
    description: Latitude in decimal degrees, WGS84, requested via outSR=4326.
  - name: street_number
    type: VARCHAR
    description: Street number, with any repetition letter appended.
  - name: street_name
    type: VARCHAR
    description: Street type and name concatenated, for example "RUE DE RIVOLI".
  - name: situation
    type: VARCHAR
    description: Premise situation as surveyed, for example whether it fronts the street or sits in a gallery.
  - name: premise_type
    type: VARCHAR
    description: Premise type as surveyed. The value "Local vacant" marks an empty unit available for occupation.
  - name: codact
    type: VARCHAR
    description: Activity code in the 224-post APUR nomenclature, joins to raw.paris_bdcom_nomenclature.
  - name: signage_name
    type: VARCHAR
    description: Trading name recorded by the surveyor ("enseigne").
  - name: surface_band
    type: VARCHAR
    description: Floor-area band as surveyed.
  - name: surface_exact_m2
    type: DOUBLE
    description: Exact floor area of the premise in square metres, where the surveyor recorded it.
  - name: niv47
    type: VARCHAR
    description: Activity code aggregated to 47 posts.
  - name: niv18
    type: VARCHAR
    description: Activity code aggregated to 18 posts.
  - name: niv8
    type: VARCHAR
    description: Activity code aggregated to 8 posts.
  - name: niv2
    type: VARCHAR
    description: Activity code aggregated to 2 posts.
  - name: is_chain
    type: INTEGER
    description: 1 if the premise belongs to a retail network or chain ("reseau"), else 0.
  - name: is_organic
    type: INTEGER
    description: 1 if the premise belongs to the organic sector ("bio"), else 0.
  - name: is_local_commerce
    type: VARCHAR
    description: Publisher flag marking the premise as neighbourhood retail ("commerce de proximite").
  - name: is_local_service
    type: VARCHAR
    description: Publisher flag marking the premise as a neighbourhood service ("service de proximite").
  - name: act_2000
    type: VARCHAR
    description: Activity label recorded at the same address in the 2000 survey wave, blank if none.
  - name: act_2003
    type: VARCHAR
    description: Activity label recorded at the same address in the 2003 survey wave, blank if none.
  - name: act_2005
    type: VARCHAR
    description: Activity label recorded at the same address in the 2005 survey wave, blank if none.
  - name: act_2007
    type: VARCHAR
    description: Activity label recorded at the same address in the 2007 survey wave, blank if none.
  - name: act_2011
    type: VARCHAR
    description: Activity label recorded at the same address in the 2011 survey wave, blank if none.
  - name: act_2014
    type: VARCHAR
    description: Activity label recorded at the same address in the 2014 survey wave, blank if none.
  - name: act_2017
    type: VARCHAR
    description: Activity label recorded at the same address in the 2017 survey wave, blank if none.
  - name: act_2020
    type: VARCHAR
    description: Activity label recorded at the same address in the 2020 survey wave, blank if none.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted.

@bruin"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

QUERY_URL = "https://carto2.apur.org/apur/rest/services/BDCOM/bdcom2023/MapServer/1/query"
PAGE_SIZE = 1000  # the layer's maxRecordCount
REQUEST_TIMEOUT = 180
MAX_RETRIES = 5
SLEEP_BETWEEN_PAGES = 0.3

# The `type` field is a single-letter premise-type code from the publisher's
# Code_Type_Local table, not a label. V is "Vide": an empty unit, which is what makes
# Paris the only city here with real candidate addresses instead of synthetic grid points.
VACANT_TYPE_CODE = "V"
# The 220-post nomenclature also carries two vacancy activity codes.
VACANT_ACTIVITY_CODES = ("AA101", "AA102")

WAVE_COLUMNS = {
    "c00_libact": "act_2000",
    "c03_libact": "act_2003",
    "c05_libact": "act_2005",
    "c07_libact": "act_2007",
    "c11_libact": "act_2011",
    "c14_libact": "act_2014",
    "c17_libact": "act_2017",
    "c20_libact": "act_2020",
}

RENAMES = {
    "OBJECTID": "objectid",
    "arro": "arrondissement",
    "qua": "quartier",
    "sit": "situation",
    "type": "premise_type",
    "ens": "signage_name",
    "surf": "surface_band",
    "surfexacte": "surface_exact_m2",
    "reseau": "is_chain",
    "bio": "is_organic",
    "b_commprox": "is_local_commerce",
    "b_servprox": "is_local_service",
    **WAVE_COLUMNS,
}


def fetch_count() -> int:
    response = requests.post(
        QUERY_URL,
        data={"where": "1=1", "returnCountOnly": "true", "f": "json"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return int(response.json()["count"])


def fetch_page(offset: int) -> list[dict[str, Any]]:
    payload = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": "4326",
        "orderByFields": "OBJECTID",
        "resultOffset": str(offset),
        "resultRecordCount": str(PAGE_SIZE),
        "f": "json",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(QUERY_URL, data=payload, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            body = response.json()
            if "error" in body:
                raise RuntimeError(f"ArcGIS error at offset {offset}: {body['error']}")
            return body.get("features", [])
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            if attempt == MAX_RETRIES:
                raise
            backoff = 2**attempt
            logger.warning(
                "Offset %d failed on attempt %d/%d (%s), retrying in %ds",
                offset,
                attempt,
                MAX_RETRIES,
                exc,
                backoff,
            )
            time.sleep(backoff)
    return []


def flatten(feature: dict[str, Any]) -> dict[str, Any]:
    row = dict(feature.get("attributes") or {})
    geometry = feature.get("geometry") or {}
    row["lon"] = geometry.get("x")
    row["lat"] = geometry.get("y")
    return row


def materialize() -> pd.DataFrame:
    expected = fetch_count()
    logger.info("Layer reports %d features", expected)

    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        features = fetch_page(offset)
        if not features:
            break
        rows.extend(flatten(f) for f in features)
        logger.info("Fetched %d / %d features", len(rows), expected)
        if len(features) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(SLEEP_BETWEEN_PAGES)

    df = pd.DataFrame(rows)
    logger.info("Assembled %d rows, %d columns", len(df), len(df.columns))
    if len(df) != expected:
        logger.warning(
            "Row count %d does not match the layer count %d; the service may have changed",
            len(df),
            expected,
        )

    df = df.rename(columns=RENAMES)

    # Street number and repetition letter arrive separately.
    df["street_number"] = (
        df["num"].astype("string").fillna("") + df["let"].astype("string").fillna("")
    ).str.strip()
    df["street_name"] = (
        df["typ_voie"].astype("string").fillna("").str.strip()
        + " "
        + df["lib_voie"].astype("string").fillna("").str.strip()
    ).str.strip()

    for column in ("objectid", "c_ord", "arrondissement", "quartier", "is_chain", "is_organic"):
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    for column in ("surface_exact_m2", "lon", "lat"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    for column in ("niv47", "niv18", "niv8", "niv2"):
        df[column] = df[column].astype("string")

    text_columns = [
        "idcar_200m",
        "situation",
        "premise_type",
        "codact",
        "signage_name",
        "surface_band",
        "is_local_commerce",
        "is_local_service",
        "street_number",
        "street_name",
        *WAVE_COLUMNS.values(),
    ]
    for column in text_columns:
        df[column] = df[column].astype("string").str.strip().replace({"": None})

    logger.info(
        "Premise types: %s", df["premise_type"].value_counts(dropna=False).head(12).to_dict()
    )
    # `type` is a single-letter code, not a label. V is "Vide" per the publisher's
    # Code_Type_Local table, and those are the vacant units usable as candidate sites.
    vacant = int((df["premise_type"] == VACANT_TYPE_CODE).sum())
    logger.info("Vacant premises (type = %s, 'Vide'): %d", VACANT_TYPE_CODE, vacant)
    logger.info(
        "Vacancy-flagged activity codes: %s",
        df.loc[df["codact"].isin(VACANT_ACTIVITY_CODES), "codact"]
        .value_counts()
        .to_dict(),
    )
    logger.info("Distinct codact values: %d", df["codact"].nunique())
    logger.info(
        "Geocoding: %d of %d rows carry lon/lat", int(df["lon"].notna().sum()), len(df)
    )
    if df["lon"].notna().any():
        logger.info(
            "Bounding box: lon %.4f..%.4f, lat %.4f..%.4f",
            df["lon"].min(),
            df["lon"].max(),
            df["lat"].min(),
            df["lat"].max(),
        )
    # `surfexacte` is recorded only for a small minority of premises. The banded `surf`
    # column is the one that is broadly populated, so record the availability of both:
    # PLAN.md credited Paris with per-premise floor area, and that holds only in bands.
    exact = df["surface_exact_m2"].dropna()
    logger.info(
        "Exact floor area (surfexacte): %d of %d rows (%.1f%%), median %.0f m2",
        len(exact),
        len(df),
        100.0 * len(exact) / max(len(df), 1),
        exact.median() if not exact.empty else 0.0,
    )
    logger.info(
        "Banded floor area (surf): %d of %d rows (%.1f%%), bands %s",
        int(df["surface_band"].notna().sum()),
        len(df),
        100.0 * df["surface_band"].notna().sum() / max(len(df), 1),
        df["surface_band"].value_counts(dropna=False).head(10).to_dict(),
    )

    df["extracted_at"] = datetime.now(timezone.utc)

    ordered = [
        "objectid",
        "c_ord",
        "arrondissement",
        "quartier",
        "idcar_200m",
        "lon",
        "lat",
        "street_number",
        "street_name",
        "situation",
        "premise_type",
        "codact",
        "signage_name",
        "surface_band",
        "surface_exact_m2",
        "niv47",
        "niv18",
        "niv8",
        "niv2",
        "is_chain",
        "is_organic",
        "is_local_commerce",
        "is_local_service",
        "act_2000",
        "act_2003",
        "act_2005",
        "act_2007",
        "act_2011",
        "act_2014",
        "act_2017",
        "act_2020",
        "extracted_at",
    ]
    df = df[ordered]

    logger.info("Returning %d rows", len(df))
    return df
