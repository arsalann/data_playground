"""@bruin
name: raw.epias_mcp
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Fetches hourly Market Clearing Price (MCP) data from the EPIAS Day-Ahead
  Market. MCP is determined by the intersection of supply and demand curves
  in the day-ahead electricity market, in TRY, EUR, and USD.

  Data source: https://seffaflik.epias.com.tr/electricity-service
  Endpoint: POST /v1/markets/dam/data/mcp

secrets:
  - key: epias_username
  - key: epias_password

materialization:
  type: table
  strategy: create+replace

columns:
  - name: date
    type: TIMESTAMP
    description: Date and hour of the market clearing price (Turkish time, +03:00)
    primary_key: true
  - name: price_try
    type: DOUBLE
    description: Market Clearing Price in Turkish Lira (TRY/MWh)
  - name: price_eur
    type: DOUBLE
    description: Market Clearing Price in Euros (EUR/MWh)
  - name: price_usd
    type: DOUBLE
    description: Market Clearing Price in US Dollars (USD/MWh)
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this data was fetched from the API

@bruin"""

import logging
import os
import time
from datetime import datetime, timedelta
from urllib.parse import quote

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://seffaflik.epias.com.tr/electricity-service"
AUTH_URL = "https://giris.epias.com.tr/cas/v1/tickets"


def get_tgt():
    username = os.environ["epias_username"]
    password = os.environ["epias_password"]
    encoded_body = f"username={quote(username, safe='')}&password={quote(password, safe='')}"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/plain",
    }
    resp = requests.post(AUTH_URL, data=encoded_body, headers=headers, timeout=30)
    if resp.status_code != 200:
        logger.error("Auth failed (%d): %s", resp.status_code, resp.text[:300])
    resp.raise_for_status()
    tgt = resp.text.strip()
    if tgt.startswith("<"):
        raise RuntimeError(f"Auth returned HTML instead of TGT: {tgt[:200]}")
    logger.info("TGT obtained (length=%d)", len(tgt))
    return tgt


def epias_post(endpoint, body, tgt):
    url = f"{BASE_URL}{endpoint}"
    headers = {"TGT": tgt, "Content-Type": "application/json", "Accept": "application/json"}
    for attempt in range(5):
        try:
            resp = requests.post(url, json=body, headers=headers, timeout=60)
            if resp.status_code in (429, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("HTTP %d, retrying in %ds...", resp.status_code, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            wait = 10 * (attempt + 1)
            logger.warning("Timeout on attempt %d, retrying in %ds...", attempt + 1, wait)
            time.sleep(wait)
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            logger.warning("Connection error on attempt %d, retrying in %ds...", attempt + 1, wait)
            time.sleep(wait)
    raise RuntimeError(f"Failed after 5 retries: {endpoint}")


def fmt_date(dt):
    return dt.strftime("%Y-%m-%dT00:00:00+03:00")


def parse_items(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "content", "mcpList", "dayAheadMCPList"):
            if key in data and isinstance(data[key], list):
                return data[key]
        body = data.get("body", {})
        if isinstance(body, dict):
            for key in ("content", "items", "mcpList", "dayAheadMCPList"):
                if key in body and isinstance(body[key], list):
                    return body[key]
        if isinstance(body, list):
            return body
    return []


def fetch_mcp(tgt, start_date, end_date):
    all_rows = []
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=30), end_date)
        body = {
            "startDate": fmt_date(current),
            "endDate": fmt_date(chunk_end),
        }
        logger.info("Fetching MCP %s to %s", current.date(), chunk_end.date())
        data = epias_post("/v1/markets/dam/data/mcp", body, tgt)
        items = parse_items(data)

        for item in items:
            row = {
                "date": item.get("date", item.get("tarih")),
                "price_try": item.get("price", item.get("marketTradePrice", 0.0)),
                "price_eur": item.get("priceEur", item.get("priceEUR", 0.0)),
                "price_usd": item.get("priceUsd", item.get("priceUSD", 0.0)),
            }
            all_rows.append(row)

        logger.info("  Got %d records", len(items))
        current = chunk_end
        time.sleep(0.5)

    return all_rows


def materialize():
    start_str = os.environ.get("BRUIN_START_DATE", "2025-03-01")
    end_str = os.environ.get("BRUIN_END_DATE", "2026-03-03")
    logger.info("Interval: %s to %s", start_str, end_str)

    start_date = datetime.strptime(start_str[:10], "%Y-%m-%d")
    end_date = datetime.strptime(end_str[:10], "%Y-%m-%d") + timedelta(days=1)

    tgt = get_tgt()
    rows = fetch_mcp(tgt, start_date, end_date)

    if not rows:
        raise RuntimeError("No data fetched from MCP endpoint")

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["extracted_at"] = datetime.now()

    logger.info("Total records: %d", len(df))
    logger.info("Date range: %s to %s", df["date"].min(), df["date"].max())
    return df
