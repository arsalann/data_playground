"""@bruin

name: raw.arena_leaderboard
description: |
  ELO rankings from the LMArena (formerly LMSYS Chatbot Arena) leaderboard,
  the gold-standard benchmark for LLM quality based on human preference votes.
  Covers multiple categories: text, code, vision, document understanding, search,
  and generative AI (text-to-image, text-to-video, image editing).

  Each record is a model's ranking in a specific category with ELO score,
  confidence interval, and vote count.

  Source: https://api.wulong.dev/arena-ai-leaderboards/v1/ (third-party mirror, daily updates)
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: category
    type: VARCHAR
    description: Leaderboard category (text, code, vision, document, search, text-to-image, etc.)
    primary_key: true
  - name: rank
    type: INTEGER
    description: Model rank within this category (1 = best)
  - name: model_name
    type: VARCHAR
    description: Model identifier as it appears on the Arena leaderboard
    primary_key: true
  - name: vendor
    type: VARCHAR
    description: Model provider (Anthropic, OpenAI, Google, Meta, etc.)
  - name: license_type
    type: VARCHAR
    description: Model license (proprietary, open, etc.)
  - name: elo_score
    type: INTEGER
    description: ELO rating based on human preference votes (higher = better)
  - name: confidence_interval
    type: INTEGER
    description: 95% confidence interval width for the ELO score
  - name: vote_count
    type: INTEGER
    description: Number of human preference votes this model has received
  - name: leaderboard_date
    type: DATE
    description: Date of the leaderboard snapshot
  - name: extracted_at
    type: TIMESTAMP
    description: When this data was fetched from the API

@bruin"""

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

ARENA_BASE_URL = "https://api.wulong.dev/arena-ai-leaderboards/v1"
CATEGORIES = ["text", "code", "vision", "document", "search", "text-to-image", "text-to-video"]
MAX_RETRIES = 5


def fetch_with_retry(url, params=None, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 404:
                logger.warning("404 for %s — no data available", url)
                return None
            if resp.status_code in (429, 502, 503):
                wait = 15 * (attempt + 1)
                logger.warning("HTTP %d, retrying in %ds", resp.status_code, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            wait = 10 * (attempt + 1)
            logger.warning("Request error (attempt %d/%d): %s", attempt + 1, retries, e)
            time.sleep(wait)
    logger.error("All %d retries exhausted for %s", retries, url)
    return None


def materialize():
    logger.info("Fetching Arena leaderboard data for %d categories", len(CATEGORIES))

    all_rows = []

    for category in CATEGORIES:
        logger.info("Fetching category: %s", category)
        data = fetch_with_retry(
            f"{ARENA_BASE_URL}/leaderboard",
            params={"name": category}
        )

        if not data:
            logger.warning("No data for category %s", category)
            continue

        leaderboard_date = None
        if "date" in data:
            leaderboard_date = data["date"]

        models = data.get("models", [])
        logger.info("  %s: %d models", category, len(models))

        for m in models:
            all_rows.append({
                "category": category,
                "rank": m.get("rank"),
                "model_name": m.get("model"),
                "vendor": m.get("vendor"),
                "license_type": m.get("license"),
                "elo_score": m.get("score"),
                "confidence_interval": m.get("ci"),
                "vote_count": m.get("votes"),
                "leaderboard_date": leaderboard_date,
            })

        time.sleep(0.5)

    if not all_rows:
        logger.warning("No leaderboard data fetched")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["leaderboard_date"] = pd.to_datetime(df["leaderboard_date"], errors="coerce").dt.date
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Total records: %d across %d categories", len(df), df["category"].nunique())
    logger.info("Unique models: %d", df["model_name"].nunique())

    return df
