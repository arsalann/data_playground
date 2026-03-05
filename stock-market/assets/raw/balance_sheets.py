"""@bruin
name: stock_market_raw.balance_sheets
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches quarterly balance sheets for S&P 500 constituents via yfinance.
  Uses BRUIN_START_DATE and BRUIN_END_DATE to filter by fiscal period end date.
  Captures all available balance sheet fields from Yahoo Finance.

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
  - name: total_assets
    type: DOUBLE
    description: Total assets in USD
  - name: total_liabilities_net_minority_interest
    type: DOUBLE
    description: Total liabilities excluding minority interest in USD
  - name: stockholders_equity
    type: DOUBLE
    description: Total stockholders equity in USD
  - name: retained_earnings
    type: DOUBLE
    description: Retained earnings in USD
  - name: total_current_assets
    type: DOUBLE
    description: Total current assets in USD
  - name: total_current_liabilities
    type: DOUBLE
    description: Total current liabilities in USD
  - name: cash_and_cash_equivalents
    type: DOUBLE
    description: Cash and cash equivalents in USD
  - name: current_debt
    type: DOUBLE
    description: Short-term / current portion of debt in USD
  - name: long_term_debt
    type: DOUBLE
    description: Long-term debt in USD
  - name: total_debt
    type: DOUBLE
    description: Total debt (short + long term) in USD
  - name: net_debt
    type: DOUBLE
    description: Net debt (total debt minus cash) in USD
  - name: ordinary_shares_number
    type: DOUBLE
    description: Number of ordinary shares outstanding
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
        "Fetching quarterly balance sheets for %d tickers (all available quarters)",
        len(tickers),
    )

    all_frames: list[pd.DataFrame] = []
    success = 0
    failed = 0

    for i, symbol in enumerate(tickers):
        try:
            t = yf.Ticker(symbol)
            df = t.quarterly_balance_sheet

            if df is None or df.empty:
                logger.debug("No balance sheet data for %s", symbol)
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
            f"No balance sheet data fetched. {failed} failures out of {len(tickers)} tickers."
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
