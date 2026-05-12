"""@bruin

name: raw.aei_onet_tasks
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  O*NET-SOC Task Statements (v29.2) joined to Occupation Data. One row per O*NET task.

  Joining AEI `cluster_name` (lowercased task description text) to this table gives each
  AEI task an O*NET-SOC occupation code, which in turn allows a join to BLS SOC codes
  (truncated to 6 digits) for wage and employment enrichment.

  Source:
    - https://www.onetcenter.org/dl_files/database/db_29_2_text/Task%20Statements.txt
    - https://www.onetcenter.org/dl_files/database/db_29_2_text/Occupation%20Data.txt
  License: CC BY 4.0

materialization:
  type: table
  strategy: create+replace

columns:
  - name: task_id
    type: INTEGER
    description: O*NET task identifier (unique within Task Statements).
    primary_key: true
  - name: onet_soc_code
    type: VARCHAR
    description: O*NET-SOC 8-digit code (e.g. `15-1252.00`). Truncate to 6 digits (`15-1252`) to join to BLS SOC.
  - name: task_description
    type: VARCHAR
    description: Full task description text (original O*NET casing).
  - name: task_description_lower
    type: VARCHAR
    description: Lowercased task description, used as the natural join key to AEI `cluster_name`.
  - name: task_type
    type: VARCHAR
    description: Either `Core` or `Supplemental` — O*NET's classification for the task's importance.
  - name: occupation_title
    type: VARCHAR
    description: O*NET occupation title associated with this `onet_soc_code`.
  - name: occupation_description
    type: VARCHAR
    description: O*NET occupation long description.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this asset was materialized.

@bruin"""

import logging
import os
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

ONET_BASE = "https://www.onetcenter.org/dl_files/database/db_29_2_text"
TASKS_URL = f"{ONET_BASE}/Task%20Statements.txt"
OCC_URL = f"{ONET_BASE}/Occupation%20Data.txt"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
}


def fetch_tsv(url: str) -> pd.DataFrame:
    logger.info("Fetching %s", url)
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text), sep="\t", dtype="string")
    logger.info("  %d rows, cols=%s", len(df), list(df.columns))
    return df


def materialize():
    tasks_df = fetch_tsv(TASKS_URL)
    occ_df = fetch_tsv(OCC_URL)

    tasks_df = tasks_df.rename(columns={
        "O*NET-SOC Code": "onet_soc_code",
        "Task ID": "task_id",
        "Task": "task_description",
        "Task Type": "task_type",
    })[["task_id", "onet_soc_code", "task_description", "task_type"]]

    occ_df = occ_df.rename(columns={
        "O*NET-SOC Code": "onet_soc_code",
        "Title": "occupation_title",
        "Description": "occupation_description",
    })[["onet_soc_code", "occupation_title", "occupation_description"]]

    df = tasks_df.merge(occ_df, on="onet_soc_code", how="left")

    df["task_id"] = pd.to_numeric(df["task_id"], errors="coerce").astype("Int64")
    df["task_description_lower"] = df["task_description"].str.lower().str.strip()
    df["extracted_at"] = datetime.now(timezone.utc)

    df = df[[
        "task_id",
        "onet_soc_code",
        "task_description",
        "task_description_lower",
        "task_type",
        "occupation_title",
        "occupation_description",
        "extracted_at",
    ]]

    logger.info(
        "Final: %d tasks, %d unique SOC codes, %d unique occupations",
        len(df),
        df["onet_soc_code"].nunique(),
        df["occupation_title"].nunique(),
    )
    return df
