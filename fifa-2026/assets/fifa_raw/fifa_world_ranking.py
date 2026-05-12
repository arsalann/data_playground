"""@bruin

name: fifa_raw.fifa_world_ranking
description: |
  FIFA Men's World Ranking snapshot for April 2026 (or the most recent month
  before the tournament). One row per qualified team with rank, points, and
  snapshot date. Used by H3 to derive a ranking-implied probability for each
  team and compare it against Polymarket's market-implied probability.

  Best-effort live fetch from inside.fifa.com is attempted first; if the
  endpoint is unreachable or returns unexpected schema, the asset falls back
  to the manifest seed in `tournament_manifest.yml.fifa_world_ranking_seed`.
  The chosen path is recorded in the `source` column.

  Source: https://inside.fifa.com/fifa-world-ranking/men (live)
          tournament_manifest.yml (fallback seed, illustrative April 2025 values)
connection: bruin-playground-arsalan
tags:
  - fifa_2026
  - reference
  - raw_data

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: fifa_code
    type: VARCHAR
    description: 3-letter FIFA country code.
    primary_key: true
    checks:
      - name: not_null
  - name: snapshot_date
    type: DATE
    description: Ranking publication date.
    primary_key: true
    checks:
      - name: not_null
  - name: rank
    type: INT64
    description: World ranking position (1 = best).
    checks:
      - name: not_null
  - name: points
    type: DOUBLE
    description: FIFA ranking points (Elo-style).
    checks:
      - name: not_null
  - name: source
    type: VARCHAR
    description: |
      Either `inside.fifa.com:<dateId>` (live fetch succeeded, e.g.
      `inside.fifa.com:FRS_Male_Football_20260119`) or `manifest_seed`
      (April 2025 numbers in `tournament_manifest.yml`).
    checks:
      - name: not_null
  - name: extracted_at
    type: TIMESTAMP
    checks:
      - name: not_null

@bruin"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

MANIFEST_PATH = Path(__file__).parent / "tournament_manifest.yml"
# FIFA's inside.fifa.com endpoint accepts a dateId. The format changed in 2025
# from numeric ids (`id14870` = 2025-09-18) to the human-readable
# `FRS_Male_Football_YYYYMMDD` form. We try the most-recent April 2026 release
# (FRS_Male_Football_20260119) first, then fall back to id14870 (Sep 2025), and
# finally to the manifest seed (April 2025 numbers, illustrative). The
# `source` column on the output records which path succeeded.
LIVE_URLS = [
    "https://inside.fifa.com/api/ranking-overview?locale=en&dateId=FRS_Male_Football_20260119",
    "https://inside.fifa.com/api/ranking-overview?locale=en&dateId=id14870",
]


def fetch_live() -> tuple[list, str] | tuple[None, None]:
    """Try each LIVE_URL in order. Return (rankings, dateId) on first
    non-empty hit, else (None, None)."""
    for url in LIVE_URLS:
        try:
            r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0 (compatible; bruin-pipeline/1.0)"})
            r.raise_for_status()
            payload = r.json()
            rankings = payload.get("rankings") or []
            if rankings:
                date_id = url.split("dateId=")[-1]
                logger.info("Live FIFA ranking hit at %s: %d rows", date_id, len(rankings))
                return rankings, date_id
        except (requests.RequestException, ValueError) as e:
            logger.warning("Live FIFA ranking fetch failed for %s: %s", url, e)
    return None, None


def materialize():
    with open(MANIFEST_PATH, "r") as f:
        manifest = yaml.safe_load(f)
    snap_ts = datetime.now(timezone.utc)
    qualified_codes = {t["fifa_code"] for t in manifest["teams"]}

    live, date_id = fetch_live()
    rows = []
    if live:
        for r in live:
            code = (r.get("countryCode") or r.get("threeLetterAbbrev") or "").upper()
            if code not in qualified_codes:
                continue
            rows.append({
                "fifa_code": code,
                "snapshot_date": pd.to_datetime(r.get("rankingDate")).date() if r.get("rankingDate") else datetime.now(timezone.utc).date(),
                "rank": int(r.get("rank") or 0),
                "points": float(r.get("totalPoints") or 0),
                "source": f"inside.fifa.com:{date_id}",
                "extracted_at": snap_ts,
            })
        logger.info("Live FIFA ranking parsed: %d qualified teams matched", len(rows))

    if not rows:
        seed = manifest["fifa_world_ranking_seed"]
        snap_date = pd.to_datetime(seed["snapshot_date"]).date()
        for r in seed["rows"]:
            rows.append({
                "fifa_code": r["fifa_code"],
                "snapshot_date": snap_date,
                "rank": int(r["rank"]),
                "points": float(r["points"]),
                "source": "manifest_seed",
                "extracted_at": snap_ts,
            })
        logger.info("Using manifest seed: %d rows", len(rows))

    df = pd.DataFrame(rows)
    logger.info("fifa_world_ranking rows: %d", len(df))
    return df
