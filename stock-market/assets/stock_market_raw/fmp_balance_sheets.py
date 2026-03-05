"""@bruin
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches quarterly balance sheets for S&P 500 constituents via FMP API.
  Returns up to 120 quarters (~30 years) of historical data per ticker.
  Column names normalized to match yfinance balance sheet format.

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
  - name: total_assets
    type: DOUBLE
    description: Total assets in USD
  - name: total_liabilities_net_minority_interest
    type: DOUBLE
    description: Total liabilities in USD
  - name: stockholders_equity
    type: DOUBLE
    description: Total stockholders equity in USD
  - name: retained_earnings
    type: DOUBLE
    description: Retained earnings in USD
  - name: cash_and_cash_equivalents
    type: DOUBLE
    description: Cash and cash equivalents in USD
  - name: current_assets
    type: DOUBLE
    description: Total current assets in USD
  - name: current_liabilities
    type: DOUBLE
    description: Total current liabilities in USD
  - name: current_debt
    type: DOUBLE
    description: Short-term debt in USD
  - name: long_term_debt
    type: DOUBLE
    description: Long-term debt in USD
  - name: total_debt
    type: DOUBLE
    description: Total debt (short + long term) in USD
  - name: net_debt
    type: DOUBLE
    description: Net debt (total debt minus cash) in USD
  - name: goodwill
    type: DOUBLE
    description: Goodwill in USD
  - name: net_tangible_assets
    type: DOUBLE
    description: Net tangible assets in USD
  - name: inventory
    type: DOUBLE
    description: Inventory in USD
  - name: accounts_receivable
    type: DOUBLE
    description: Accounts receivable in USD
  - name: accounts_payable
    type: DOUBLE
    description: Accounts payable in USD
  - name: working_capital
    type: DOUBLE
    description: Working capital (current assets minus current liabilities) in USD
  - name: ordinary_shares_number
    type: DOUBLE
    description: Shares outstanding (from weighted average shares in income statement)
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


def fetch_balance(symbol: str, api_key: str) -> list[dict]:
    url = f"{FMP_BASE}/balance-sheet-statement"
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

    logger.info("Fetching FMP quarterly balance sheets for %d tickers", len(tickers))

    all_rows: list[dict] = []
    success = 0
    failed = 0

    for i, symbol in enumerate(tickers):
        try:
            records = fetch_balance(symbol, api_key)
            for r in records:
                short_debt = r.get("shortTermDebt") or 0
                long_debt = r.get("longTermDebt") or 0
                total_debt = short_debt + long_debt
                cash = r.get("cashAndCashEquivalents") or 0
                cur_assets = r.get("totalCurrentAssets") or 0
                cur_liab = r.get("totalCurrentLiabilities") or 0
                goodwill_val = r.get("goodwill") or 0
                intangibles = r.get("intangibleAssets") or 0
                total_assets_val = r.get("totalAssets") or 0

                all_rows.append({
                    "ticker": symbol,
                    "period_ending": r["date"],
                    "fiscal_year": int(r.get("fiscalYear", 0)),
                    "fiscal_quarter": QUARTER_MAP.get(r.get("period", ""), 0),
                    "total_assets": r.get("totalAssets"),
                    "total_liabilities_net_minority_interest": r.get("totalLiabilities"),
                    "stockholders_equity": r.get("totalStockholdersEquity"),
                    "retained_earnings": r.get("retainedEarnings"),
                    "cash_and_cash_equivalents": r.get("cashAndCashEquivalents"),
                    "current_assets": r.get("totalCurrentAssets"),
                    "current_liabilities": r.get("totalCurrentLiabilities"),
                    "current_debt": r.get("shortTermDebt"),
                    "long_term_debt": r.get("longTermDebt"),
                    "total_debt": total_debt if total_debt else None,
                    "net_debt": (total_debt - cash) if total_debt else None,
                    "goodwill": r.get("goodwill"),
                    "net_tangible_assets": (total_assets_val - goodwill_val - intangibles) if total_assets_val else None,
                    "inventory": r.get("inventory"),
                    "accounts_receivable": r.get("accountsReceivables"),
                    "accounts_payable": r.get("accountPayables"),
                    "working_capital": (cur_assets - cur_liab) if cur_assets else None,
                    "ordinary_shares_number": None,
                })
            success += 1
        except Exception as e:
            logger.warning("Failed for %s: %s", symbol, e)
            failed += 1

        if (i + 1) % 50 == 0:
            logger.info("Progress: %d/%d tickers processed", i + 1, len(tickers))
            time.sleep(1)

    if not all_rows:
        raise RuntimeError(f"No FMP balance sheet data fetched. {failed} failures out of {len(tickers)} tickers.")

    df = pd.DataFrame(all_rows)
    df["period_ending"] = pd.to_datetime(df["period_ending"]).dt.date
    df["extracted_at"] = datetime.now(timezone.utc)

    float_cols = [c for c in df.columns if c not in ("ticker", "period_ending", "fiscal_year", "fiscal_quarter", "extracted_at")]
    for c in float_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    logger.info("Fetched %d rows for %d tickers (%d failed)", len(df), success, failed)
    return df
