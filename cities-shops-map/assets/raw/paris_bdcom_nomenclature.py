"""@bruin
name: raw.paris_bdcom_nomenclature
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests APUR's BDCOM activity nomenclature: the 220-post `codact` lookup plus its
  47-post and 18-post aggregations, and the premise-type letter codes.

  Resolving this lookup is what corrected the Paris appraisal. The 18-post label for the
  whole food-and-drink block is "Cafes et Restaurants", which reads as though cafes were
  separable. At 220 posts the only bar code is CH403 "Bar ou Cafe sans tabac", so bars and
  cafes share one class and there is no pub or nightclub class at all.

  Two implementation notes:
  - The `t_codact` sheet stores its hierarchy labels as unevaluated VLOOKUP formulas, so
    only columns A and B are readable there. The `Regroupements_BDCom_2017` sheet carries
    the same hierarchy as literal values and is used instead.
  - The published lookup is the 2017 edition. APUR has not republished it for the 2023
    survey wave; BDCOM_2023_CODACT_OD.xlsx does not exist. The codes are unchanged.

  Source: https://www.apur.org/open_data/BDCOM_2017_CODACT_OD.xlsx
  Licence: ODbL (APUR)

materialization:
  type: table
  strategy: append

columns:
  - name: codact
    type: VARCHAR
    description: Activity code in the 220-post nomenclature, joins to raw.paris_bdcom_premises.codact.
    primary_key: true
  - name: libact
    type: VARCHAR
    description: Activity label at 220 posts, the finest level published.
  - name: niv47_code
    type: VARCHAR
    description: Activity code aggregated to 47 posts.
  - name: niv47_label
    type: VARCHAR
    description: Activity label at 47 posts, for example "Bar - Cafe - Debit de boissons".
  - name: niv18_code
    type: VARCHAR
    description: Activity code aggregated to 18 posts.
  - name: niv18_label
    type: VARCHAR
    description: Activity label at 18 posts, for example "Cafes et Restaurants".
  - name: premise_type_code
    type: VARCHAR
    description: Premise-type letter for the activity, for example C for Commerce or E for Equipement.
  - name: premise_type_label
    type: VARCHAR
    description: Premise-type label matching premise_type_code.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted.

@bruin"""

import io
import logging
import os
from datetime import datetime, timezone

import openpyxl
import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SOURCE_URL = "https://www.apur.org/open_data/BDCOM_2017_CODACT_OD.xlsx"
REQUEST_TIMEOUT = 180

HIERARCHY_SHEET = "Regroupements_BDCom_2017"
HIERARCHY_HEADER_ROWS = 2
CODACT_SHEET = "t_codact"
PREMISE_TYPE_SHEET = "Code_Type_Local"


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def load_workbook() -> openpyxl.Workbook:
    logger.info("Downloading %s", SOURCE_URL)
    response = requests.get(SOURCE_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    logger.info("Downloaded %.1f KB", len(response.content) / 1e3)
    return openpyxl.load_workbook(io.BytesIO(response.content), data_only=False)


def read_hierarchy(workbook: openpyxl.Workbook) -> pd.DataFrame:
    sheet = workbook[HIERARCHY_SHEET]
    records = []
    for row in sheet.iter_rows(
        min_row=HIERARCHY_HEADER_ROWS + 1, max_col=6, values_only=True
    ):
        codact = _text(row[0])
        if not codact:
            continue
        records.append(
            {
                "codact": codact,
                "libact": _text(row[1]),
                "niv47_code": _text(row[2]),
                "niv47_label": _text(row[3]),
                "niv18_code": _text(row[4]),
                "niv18_label": _text(row[5]),
            }
        )
    logger.info("Read %d codact rows from %s", len(records), HIERARCHY_SHEET)
    return pd.DataFrame(records)


def read_premise_types(workbook: openpyxl.Workbook) -> dict[str, str]:
    sheet = workbook[PREMISE_TYPE_SHEET]
    mapping: dict[str, str] = {}
    for row in sheet.iter_rows(min_row=2, max_col=2, values_only=True):
        code, label = _text(row[0]), _text(row[1])
        if code and label:
            mapping[code] = label
    logger.info("Read %d premise-type codes", len(mapping))
    return mapping


def read_codact_types(workbook: openpyxl.Workbook) -> dict[str, str]:
    """Column C of t_codact holds the premise-type letter per activity code.

    Columns D onward on that sheet are unevaluated VLOOKUP formulas and unusable.
    """
    sheet = workbook[CODACT_SHEET]
    mapping: dict[str, str] = {}
    for row in sheet.iter_rows(min_row=2, max_col=3, values_only=True):
        codact, premise_type = _text(row[0]), _text(row[2])
        if codact and premise_type:
            mapping[codact] = premise_type
    logger.info("Read premise-type letters for %d activity codes", len(mapping))
    return mapping


def materialize() -> pd.DataFrame:
    workbook = load_workbook()

    df = read_hierarchy(workbook)
    codact_types = read_codact_types(workbook)
    premise_types = read_premise_types(workbook)

    df["premise_type_code"] = df["codact"].map(codact_types)
    df["premise_type_label"] = df["premise_type_code"].map(premise_types)

    missing_type = int(df["premise_type_code"].isna().sum())
    if missing_type:
        logger.warning(
            "%d of %d activity codes have no premise-type letter in t_codact",
            missing_type,
            len(df),
        )

    food_drink = df[df["niv18_label"].str.contains("Restaurant", case=False, na=False)]
    logger.info(
        "Food and drink block: %d codes across %d 47-post classes",
        len(food_drink),
        food_drink["niv47_label"].nunique(),
    )
    logger.info(
        "47-post classes in that block: %s", sorted(food_drink["niv47_label"].dropna().unique())
    )

    bar_codes = df.loc[df["libact"].str.contains("Bar", case=False, na=False), "codact"].tolist()
    logger.info("Codes whose label mentions 'Bar': %s", bar_codes)

    df["extracted_at"] = datetime.now(timezone.utc)

    ordered = [
        "codact",
        "libact",
        "niv47_code",
        "niv47_label",
        "niv18_code",
        "niv18_label",
        "premise_type_code",
        "premise_type_label",
        "extracted_at",
    ]
    df = df[ordered]

    logger.info("Returning %d rows", len(df))
    return df
