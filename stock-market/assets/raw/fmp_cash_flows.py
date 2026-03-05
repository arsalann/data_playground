"""@bruin
name: stock_market_raw.fmp_cash_flows
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches quarterly cash flow statements for S&P 500 constituents via FMP API.
  Returns up to 120 quarters (~30 years) of historical data per ticker.
  Column names normalized to match yfinance cash flow format.

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
  - name: period_ending
    type: DATE
    description: Fiscal quarter end date
    primary_key: true
  - name: fiscal_year
    type: INTEGER
    description: Fiscal year from FMP
  - name: fiscal_quarter
    type: INTEGER
    description: Fiscal quarter (1-4)
  - name: operating_cash_flow
    type: DOUBLE
    description: Net cash from operating activities in USD
  - name: capital_expenditure
    type: DOUBLE
    description: Capital expenditure in USD (typically negative)
  - name: free_cash_flow
    type: DOUBLE
    description: Free cash flow in USD
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
    description: Depreciation and amortization in USD
  - name: stock_based_compensation
    type: DOUBLE
    description: Stock-based compensation in USD
  - name: change_in_working_capital
    type: DOUBLE
    description: Net change in working capital in USD
  - name: common_stock_dividend_paid
    type: DOUBLE
    description: Common dividends paid in USD
  - name: repurchase_of_capital_stock
    type: DOUBLE
    description: Share repurchases in USD
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

QUARTER_MAP = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}


def get_sp500_tickers() -> list[str]:
    resp = requests.get(SP500_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    return tables[0]["Symbol"].str.replace(".", "-", regex=False).tolist()


def fetch_cashflow(symbol: str, api_key: str) -> list[dict]:
    url = f"{FMP_BASE}/cash-flow-statement"
    params = {"symbol": symbol, "period": "quarter", "limit": 120, "apikey": api_key}
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

    tickers = get_sp500_tickers()
    limit = os.environ.get("STOCK_TICKER_LIMIT")
    if limit:
        tickers = tickers[: int(limit)]

    logger.info("Fetching FMP quarterly cash flows for %d tickers", len(tickers))

    all_rows: list[dict] = []
    success = 0
    failed = 0

    for i, symbol in enumerate(tickers):
        try:
            records = fetch_cashflow(symbol, api_key)
            for r in records:
                all_rows.append({
                    "ticker": symbol,
                    "period_ending": r["date"],
                    "fiscal_year": int(r.get("fiscalYear", 0)),
                    "fiscal_quarter": QUARTER_MAP.get(r.get("period", ""), 0),
                    "operating_cash_flow": r.get("operatingCashFlow"),
                    "capital_expenditure": r.get("capitalExpenditure"),
                    "free_cash_flow": r.get("freeCashFlow"),
                    "investing_cash_flow": r.get("netCashProvidedByInvestingActivities"),
                    "financing_cash_flow": r.get("netCashProvidedByFinancingActivities"),
                    "end_cash_position": r.get("cashAtEndOfPeriod"),
                    "depreciation_and_amortization": r.get("depreciationAndAmortization"),
                    "stock_based_compensation": r.get("stockBasedCompensation"),
                    "change_in_working_capital": r.get("changeInWorkingCapital"),
                    "common_stock_dividend_paid": r.get("commonDividendsPaid"),
                    "repurchase_of_capital_stock": r.get("commonStockRepurchased"),
                })
            success += 1
        except Exception as e:
            logger.warning("Failed for %s: %s", symbol, e)
            failed += 1

        if (i + 1) % 50 == 0:
            logger.info("Progress: %d/%d tickers processed", i + 1, len(tickers))
            time.sleep(1)

    if not all_rows:
        raise RuntimeError(f"No FMP cash flow data fetched. {failed} failures out of {len(tickers)} tickers.")

    df = pd.DataFrame(all_rows)
    df["period_ending"] = pd.to_datetime(df["period_ending"]).dt.date
    df["extracted_at"] = datetime.now(timezone.utc)

    float_cols = [c for c in df.columns if c not in ("ticker", "period_ending", "fiscal_year", "fiscal_quarter", "extracted_at")]
    for c in float_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    logger.info("Fetched %d rows for %d tickers (%d failed)", len(df), success, failed)
    return df
