"""@bruin
name: stock_market_raw.prices_daily
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches daily OHLCV stock prices for S&P 500 constituents via yfinance.
  Uses BRUIN_START_DATE and BRUIN_END_DATE to control the date range.
  Downloads all tickers in batches for efficiency.

  Data source: Yahoo Finance (via yfinance library)
  Limitation: unofficial API, no SLA. Will be replaced with FMP paid API.

materialization:
  type: table
  strategy: append

columns:
  - name: ticker
    type: VARCHAR
    description: Stock ticker symbol (Yahoo Finance format)
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
    description: Adjusted closing price (accounts for splits and dividends) in USD
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
from datetime import datetime, timedelta, timezone

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
BATCH_SIZE = 50


def get_sp500_tickers() -> list[str]:
    resp = requests.get(SP500_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    return tables[0]["Symbol"].str.replace(".", "-", regex=False).tolist()


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2020-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2020-01-03")

    tickers = get_sp500_tickers()
    limit = os.environ.get("STOCK_TICKER_LIMIT")
    if limit:
        tickers = tickers[: int(limit)]

    # yfinance end date is exclusive — add 1 day to include the end date
    end_exclusive = (
        datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    logger.info(
        "Downloading prices for %d tickers, %s to %s",
        len(tickers), start_date, end_date,
    )

    all_frames: list[pd.DataFrame] = []

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info("Batch %d/%d: %d tickers", batch_num, total_batches, len(batch))

        data = yf.download(
            " ".join(batch),
            start=start_date,
            end=end_exclusive,
            auto_adjust=False,
            progress=False,
            threads=True,
            group_by="ticker",
        )

        if data.empty:
            logger.warning("Batch %d returned no data", batch_num)
            continue

        if isinstance(data.columns, pd.MultiIndex):
            available = data.columns.get_level_values(0).unique()
            for tk in available:
                try:
                    tk_df = data[tk].dropna(subset=["Close"])
                    if tk_df.empty:
                        continue
                    tk_df = tk_df.reset_index()
                    tk_df["ticker"] = tk
                    all_frames.append(tk_df)
                except (KeyError, TypeError):
                    continue
        else:
            # Single ticker in batch
            data = data.dropna(subset=["Close"])
            if not data.empty:
                data = data.reset_index()
                data["ticker"] = batch[0]
                all_frames.append(data)

    if not all_frames:
        raise RuntimeError("No price data fetched for any ticker")

    df = pd.concat(all_frames, ignore_index=True)

    rename_map = {
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }
    df = df.rename(columns=rename_map)
    keep_cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
    df = df[[c for c in keep_cols if c in df.columns]]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info(
        "Fetched %d price rows for %d tickers",
        len(df), df["ticker"].nunique(),
    )
    return df
