"""@bruin
name: stock_market_raw.fmp_income_statements
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches quarterly income statements for S&P 500 constituents via FMP API.
  Returns up to 120 quarters (~30 years) of historical data per ticker.
  Column names normalized to match yfinance income statement format.

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
  - name: total_revenue
    type: DOUBLE
    description: Total revenue in USD
  - name: cost_of_revenue
    type: DOUBLE
    description: Cost of revenue in USD
  - name: gross_profit
    type: DOUBLE
    description: Gross profit in USD
  - name: operating_expense
    type: DOUBLE
    description: Total operating expenses in USD
  - name: operating_income
    type: DOUBLE
    description: Operating income in USD
  - name: net_income
    type: DOUBLE
    description: Net income in USD
  - name: basic_eps
    type: DOUBLE
    description: Basic earnings per share in USD
  - name: diluted_eps
    type: DOUBLE
    description: Diluted earnings per share in USD
  - name: ebitda
    type: DOUBLE
    description: EBITDA in USD
  - name: interest_expense
    type: DOUBLE
    description: Interest expense in USD
  - name: tax_provision
    type: DOUBLE
    description: Income tax expense in USD
  - name: research_and_development
    type: DOUBLE
    description: R&D expenses in USD
  - name: selling_general_and_administration
    type: DOUBLE
    description: SG&A expenses in USD
  - name: diluted_average_shares
    type: DOUBLE
    description: Diluted weighted average shares outstanding
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


def fetch_income(symbol: str, api_key: str) -> list[dict]:
    url = f"{FMP_BASE}/income-statement"
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

    logger.info("Fetching FMP quarterly income statements for %d tickers", len(tickers))

    all_rows: list[dict] = []
    success = 0
    failed = 0

    for i, symbol in enumerate(tickers):
        try:
            records = fetch_income(symbol, api_key)
            for r in records:
                all_rows.append({
                    "ticker": symbol,
                    "period_ending": r["date"],
                    "fiscal_year": int(r.get("fiscalYear", 0)),
                    "fiscal_quarter": QUARTER_MAP.get(r.get("period", ""), 0),
                    "total_revenue": r.get("revenue"),
                    "cost_of_revenue": r.get("costOfRevenue"),
                    "gross_profit": r.get("grossProfit"),
                    "operating_expense": r.get("operatingExpenses"),
                    "operating_income": r.get("operatingIncome"),
                    "net_income": r.get("netIncome"),
                    "basic_eps": r.get("eps"),
                    "diluted_eps": r.get("epsDiluted"),
                    "ebitda": r.get("ebitda"),
                    "interest_expense": r.get("interestExpense"),
                    "tax_provision": r.get("incomeTaxExpense"),
                    "research_and_development": r.get("researchAndDevelopmentExpenses"),
                    "selling_general_and_administration": r.get("sellingGeneralAndAdministrativeExpenses"),
                    "diluted_average_shares": r.get("weightedAverageShsOutDil"),
                })
            success += 1
        except Exception as e:
            logger.warning("Failed for %s: %s", symbol, e)
            failed += 1

        if (i + 1) % 50 == 0:
            logger.info("Progress: %d/%d tickers processed", i + 1, len(tickers))
            time.sleep(1)

    if not all_rows:
        raise RuntimeError(f"No FMP income data fetched. {failed} failures out of {len(tickers)} tickers.")

    df = pd.DataFrame(all_rows)
    df["period_ending"] = pd.to_datetime(df["period_ending"]).dt.date
    df["extracted_at"] = datetime.now(timezone.utc)

    float_cols = [c for c in df.columns if c not in ("ticker", "period_ending", "fiscal_year", "fiscal_quarter", "extracted_at")]
    for c in float_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    logger.info("Fetched %d rows for %d tickers (%d failed)", len(df), success, failed)
    return df
