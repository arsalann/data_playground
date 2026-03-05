"""@bruin
name: stock_market_raw.cash_flows
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches quarterly cash flow statements for S&P 500 constituents via yfinance.
  Uses BRUIN_START_DATE and BRUIN_END_DATE to filter by fiscal period end date.
  Captures all available cash flow fields from Yahoo Finance.

  Data source: Yahoo Finance (via yfinance library)
  Limitation: yfinance typically returns the last 4-5 quarters of data.

materialization:
  type: table
  strategy: append

columns:
  - name: ticker
    type: VARCHAR
    description: Stock ticker symbol
    primary_key: true
  - name: period_ending
    type: DATE
    description: Fiscal quarter end date
    primary_key: true
  - name: fiscal_year
    type: INTEGER
    description: Fiscal year derived from period ending date
  - name: fiscal_quarter
    type: INTEGER
    description: Fiscal quarter (1-4) derived from period ending date
  - name: operating_cash_flow
    type: DOUBLE
    description: Net cash from operating activities in USD
  - name: capital_expenditure
    type: DOUBLE
    description: Capital expenditure in USD (typically negative)
  - name: free_cash_flow
    type: DOUBLE
    description: Free cash flow (operating cash flow + capex) in USD
  - name: investing_cash_flow
    type: DOUBLE
    description: Net cash from investing activities in USD
  - name: financing_cash_flow
    type: DOUBLE
    description: Net cash from financing activities in USD
  - name: end_cash_position
    type: DOUBLE
    description: Cash position at end of period in USD
  - name: depreciation_and_amortization
    type: DOUBLE
    description: Depreciation and amortization expense in USD
  - name: stock_based_compensation
    type: DOUBLE
    description: Stock-based compensation expense in USD
  - name: change_in_working_capital
    type: DOUBLE
    description: Net change in working capital in USD
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched

@bruin"""

import io
import logging
import os
import re
import time
from datetime import datetime, timezone

import pandas as pd
import requests
import yfinance as yf

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
HEADERS = {"User-Agent": "bruin-data-pipeline/1.0 (stock-market)"}


def get_sp500_tickers() -> list[str]:
    resp = requests.get(SP500_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    return tables[0]["Symbol"].str.replace(".", "-", regex=False).tolist()


def to_snake_case(name: str) -> str:
    s = str(name)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.lower().strip("_")


def materialize():
    tickers = get_sp500_tickers()
    limit = os.environ.get("STOCK_TICKER_LIMIT")
    if limit:
        tickers = tickers[: int(limit)]

    logger.info(
        "Fetching quarterly cash flows for %d tickers (all available quarters)",
        len(tickers),
    )

    all_frames: list[pd.DataFrame] = []
    success = 0
    failed = 0

    for i, symbol in enumerate(tickers):
        try:
            t = yf.Ticker(symbol)
            df = t.quarterly_cashflow

            if df is None or df.empty:
                logger.debug("No cash flow data for %s", symbol)
                continue

            df_t = df.T
            df_t.index.name = "period_ending"
            df_t = df_t.reset_index()
            df_t["ticker"] = symbol
            df_t.columns = [to_snake_case(c) for c in df_t.columns]
            df_t["period_ending"] = pd.to_datetime(df_t["period_ending"]).dt.date

            if df_t.empty:
                continue

            all_frames.append(df_t)
            success += 1
        except Exception as e:
            logger.warning("Failed for %s: %s", symbol, e)
            failed += 1

        if (i + 1) % 50 == 0:
            logger.info("Progress: %d/%d tickers processed", i + 1, len(tickers))
            time.sleep(1)

    if not all_frames:
        raise RuntimeError(
            f"No cash flow data fetched. {failed} failures out of {len(tickers)} tickers."
        )

    result = pd.concat(all_frames, ignore_index=True)
    result["fiscal_year"] = pd.to_datetime(result["period_ending"]).dt.year
    result["fiscal_quarter"] = pd.to_datetime(result["period_ending"]).dt.quarter
    result["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Fetched %d rows for %d tickers (%d failed)",
        len(result), success, failed,
    )
    return result
