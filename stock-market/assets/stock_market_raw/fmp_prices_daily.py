"""@bruin
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches daily OHLCV stock prices for S&P 500 constituents via FMP API.
  Uses BRUIN_START_DATE and BRUIN_END_DATE to control the date range.
  FMP provides split-adjusted prices back to year 2000+.

  Data source: Financial Modeling Prep (financialmodelingprep.com)

secrets:
  - key: fmp_api_key
    inject_as: FMP_API_KEY

materialization:
  type: table
  strategy: append

columns:
  - name: ticker
    type: VARCHAR
    description: Stock ticker symbol
    primary_key: true
  - name: date
    type: DATE
    description: Trading date
    primary_key: true
  - name: open
    type: DOUBLE
    description: Opening price in USD
  - name: high
    type: DOUBLE
    description: Intraday high price in USD
  - name: low
    type: DOUBLE
    description: Intraday low price in USD
  - name: close
    type: DOUBLE
    description: Closing price in USD
  - name: adj_close
    type: DOUBLE
    description: Adjusted closing price in USD (FMP prices are split-adjusted)
  - name: volume
    type: INTEGER
    description: Number of shares traded
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched

@bruin"""

import io
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

FMP_BASE = "https://financialmodelingprep.com/stable"
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
HEADERS = {"User-Agent": "bruin-data-pipeline/1.0 (stock-market-fmp)"}


def get_sp500_tickers() -> list[str]:
    resp = requests.get(SP500_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    return tables[0]["Symbol"].str.replace(".", "-", regex=False).tolist()


def fetch_prices(symbol: str, api_key: str, start: str, end: str) -> list[dict]:
    url = f"{FMP_BASE}/historical-price-eod/full"
    params = {"symbol": symbol, "from": start, "to": end, "apikey": api_key}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    return []


def materialize():
    api_key = os.environ.get("FMP_API_KEY")
    if not api_key:
        raise RuntimeError("FMP_API_KEY secret not configured")

    start_date = os.environ.get("BRUIN_START_DATE", "2000-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2020-01-03")

    tickers = get_sp500_tickers()
    limit = os.environ.get("STOCK_TICKER_LIMIT")
    if limit:
        tickers = tickers[: int(limit)]

    logger.info(
        "Downloading FMP prices for %d tickers, %s to %s",
        len(tickers), start_date, end_date,
    )

    all_rows: list[dict] = []
    success = 0
    failed = 0

    for i, symbol in enumerate(tickers):
        try:
            rows = fetch_prices(symbol, api_key, start_date, end_date)
            for r in rows:
                all_rows.append({
                    "ticker": symbol,
                    "date": r["date"],
                    "open": r.get("open"),
                    "high": r.get("high"),
                    "low": r.get("low"),
                    "close": r.get("close"),
                    "adj_close": r.get("close"),
                    "volume": r.get("volume"),
                })
            success += 1
        except Exception as e:
            logger.warning("Failed for %s: %s", symbol, e)
            failed += 1

        if (i + 1) % 50 == 0:
            logger.info("Progress: %d/%d tickers processed (%d rows so far)", i + 1, len(tickers), len(all_rows))
            time.sleep(1)

    if not all_rows:
        raise RuntimeError(f"No FMP price data fetched. {failed} failures out of {len(tickers)} tickers.")

    df = pd.DataFrame(all_rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Fetched %d price rows for %d tickers (%d failed)",
        len(df), success, failed,
    )
    return df
