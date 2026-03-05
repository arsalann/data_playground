"""@bruin
name: stock_market_raw.tickers
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches current S&P 500 constituent tickers and metadata from Wikipedia.
  Includes ticker symbol, company name, GICS sector, GICS sub-industry,
  date added to the index, CIK, and founding year.

  Data source: https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
  License: CC BY-SA 4.0

materialization:
  type: table
  strategy: append

columns:
  - name: ticker
    type: VARCHAR
    description: Stock ticker symbol in Yahoo Finance format (e.g. BRK-B)
    primary_key: true
  - name: company_name
    type: VARCHAR
    description: Company name
  - name: sector
    type: VARCHAR
    description: GICS sector classification
  - name: sub_industry
    type: VARCHAR
    description: GICS sub-industry classification
  - name: headquarters
    type: VARCHAR
    description: Company headquarters location
  - name: date_added
    type: VARCHAR
    description: Date the company was added to the S&P 500 index
  - name: cik
    type: VARCHAR
    description: SEC Central Index Key
  - name: founded
    type: VARCHAR
    description: Year or date the company was founded
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched

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

SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
HEADERS = {"User-Agent": "bruin-data-pipeline/1.0 (stock-market)"}


def materialize():
    logger.info("Fetching S&P 500 constituents from Wikipedia")
    resp = requests.get(SP500_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    df = tables[0]

    result = pd.DataFrame({
        "ticker": df["Symbol"].str.replace(".", "-", regex=False),
        "company_name": df["Security"],
        "sector": df["GICS Sector"],
        "sub_industry": df["GICS Sub-Industry"],
        "headquarters": df["Headquarters Location"],
        "date_added": df["Date added"].astype(str),
        "cik": df["CIK"].astype(str),
        "founded": df["Founded"].astype(str),
    })

    result["extracted_at"] = datetime.now(timezone.utc)
    logger.info("Fetched %d S&P 500 tickers", len(result))
    return result
