"""@bruin

name: raw.wat_article_snapshots
description: |
  Point-in-time wikilink snapshots for every Vital-Articles-Level-4 article on
  every analysis snapshot date. One row = one article on one snapshot date,
  containing the full list of `[[wikilinks]]` from that article's wikitext
  as of (or just before) that date.

  Snapshot dates: May 1 and December 1 of each year from 2019 through May 2026
  (14 snapshots). ~10K articles × 14 snapshots = up to ~140K rows.

  Method: two-pass against the MediaWiki Action API.

    Pass 1 (per article, parallel across articles): fetch revision metadata
    (`prop=revisions&rvprop=ids|timestamp&rvstart=2026-05-15&rvend=2019-11-30
    &rvdir=older&rvlimit=500`), paginating via `rvcontinue` until the window
    is exhausted. Locally select the rev_id closest at-or-before each snapshot
    date. Articles created after a snapshot date get a null rev_id (correctly
    represents "did not exist yet").

    Pass 2 (batched, 50 rev_ids per call): fetch wikitext content for all
    selected rev_ids (`prop=revisions&revids=A|B|...|Z&rvprop=ids|timestamp
    |content&rvslots=main`). Note: this batch-by-revids form is the only
    batch shape the API supports with rvprop=content — multi-title queries
    with rvstart are rejected.

    Wikilinks: regex-extract `[[Target]]` and `[[Target|Display]]` from
    wikitext, drop namespace prefixes (Category:, File:, etc.), drop
    self-links and anchor-only links. This captures EDITOR-CHOSEN links in
    the body (vs `parse&prop=links`, which also includes template-generated
    navbox links — a noisy signal for "is this article about X?").

  Total API call estimate: ~10–15K (pass 1, one per article + pagination) +
  ~2.8K (pass 2, 140K rev_ids / 50) ≈ 13–18K. At 4 concurrent workers, wall
  time ~30–60 min on a cold table.

  Idempotent: queries the destination table at start for already-loaded
  (article, snapshot_date) pairs and skips them. Uses materialization
  strategy=merge with the composite primary key.

  Rate-limit hygiene: maxlag=5 (Wikipedia self-throttles when replicas lag),
  polite User-Agent with contact email, exponential backoff on 429/503.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: append
image: python:3.11

depends:
  - raw.wat_vital_articles

columns:
  - name: article_title
    type: STRING
    description: Canonical Wikipedia article title (matches wat_vital_articles).
    primary_key: true
  - name: snapshot_date
    type: DATE
    description: Target snapshot date (always May 1 or Dec 1).
    primary_key: true
  - name: revision_id
    type: INT64
    description: ID of the revision that was at-or-before snapshot_date (null if article did not yet exist).
  - name: revision_timestamp
    type: TIMESTAMP
    description: Actual timestamp of that revision (UTC).
  - name: wikilinks
    type: STRING
    description: JSON-encoded array of wikilink target titles found in that revision.
  - name: wikilinks_count
    type: INT64
    description: Number of unique wikilinks captured (after dedup + filtering).
  - name: fetched_at
    type: TIMESTAMP
    description: UTC timestamp when this row was fetched from the API.

@bruin"""

import json
import logging
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import requests
from google.cloud import bigquery

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

API_URL = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "wikipedia-ai-trends/0.1 (arsalan.noorafkan@getbruin.com)"
PROJECT = "bruin-playground-arsalan"
UNIVERSE_TABLE = f"{PROJECT}.raw.wat_vital_articles"
DEST_TABLE = f"{PROJECT}.raw.wat_article_snapshots"

SNAPSHOT_DATES = [
    "2019-12-01",
    "2020-05-01", "2020-12-01",
    "2021-05-01", "2021-12-01",
    "2022-05-01", "2022-12-01",
    "2023-05-01", "2023-12-01",
    "2024-05-01", "2024-12-01",
    "2025-05-01", "2025-12-01",
    "2026-05-01",
]
SNAPSHOT_END_TS = {d: f"{d}T23:59:59Z" for d in SNAPSHOT_DATES}
WINDOW_START = "2001-01-01T00:00:00Z"
WINDOW_END = "2026-05-15T00:00:00Z"

MAX_WORKERS = int(os.environ.get("WAT_MAX_WORKERS", "4"))
ARTICLE_LIMIT = int(os.environ.get("WAT_ARTICLE_LIMIT", "0"))
REQUEST_DELAY = float(os.environ.get("WAT_REQUEST_DELAY", "0"))  # seconds between successful calls
REV_BATCH = 50  # MediaWiki cap

WIKILINK_RE = re.compile(r"\[\[([^\[\]\|\n]+?)(?:\|[^\[\]\n]*)?\]\]")
NON_ARTICLE_PREFIXES = (
    "File:", "Image:", "Category:", "Wikipedia:", "WP:", "Special:", "Help:",
    "Portal:", "Template:", "User:", "Talk:", "User talk:", "Wikipedia talk:",
    "Module:", "Draft:", "MediaWiki:", "Book:",
    "File talk:", "Category talk:", "Template talk:", "Portal talk:",
    "wikt:", "w:", "s:", "b:", "q:", "v:", "m:", "n:",
)

_thread_local = __import__("threading").local()


def get_session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"})
        _thread_local.session = s
    return s


def api_get(params: dict) -> dict:
    params = dict(params)
    params.setdefault("maxlag", 5)
    delay = 1.0
    last_exc: Exception | None = None
    for attempt in range(10):
        try:
            r = get_session().get(API_URL, params=params, timeout=120)
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError as e:
                    snippet = (r.text or "")[:120].replace("\n", " ")
                    logger.warning("Non-JSON 200 (attempt %d): %s", attempt + 1, snippet)
                    last_exc = e
                    time.sleep(delay + random.random())
                    delay = min(delay * 2, 60)
                    continue
                if isinstance(data, dict) and data.get("error", {}).get("code") == "maxlag":
                    retry_after = float(r.headers.get("Retry-After", 5))
                    time.sleep(retry_after + random.random())
                    continue
                if "error" in data:
                    raise RuntimeError(f"API error: {data['error']}")
                if REQUEST_DELAY > 0:
                    time.sleep(REQUEST_DELAY)
                return data
            if r.status_code in (429, 503):
                retry_after = float(r.headers.get("Retry-After", delay))
                logger.warning("HTTP %d (attempt %d), sleep %.1fs", r.status_code, attempt + 1, retry_after)
                time.sleep(retry_after + random.random())
                delay = min(delay * 2, 60)
                continue
            logger.warning("HTTP %d (attempt %d)", r.status_code, attempt + 1)
        except requests.RequestException as e:
            last_exc = e
            logger.warning("Network error attempt %d: %s", attempt + 1, e)
        time.sleep(delay + random.random())
        delay = min(delay * 2, 60)
    raise RuntimeError(f"API failed after retries: {last_exc}")


def extract_wikilinks(wikitext: str, self_title: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in WIKILINK_RE.findall(wikitext):
        target = raw.strip().split("#", 1)[0].strip()
        if not target:
            continue
        if any(target.startswith(p) for p in NON_ARTICLE_PREFIXES):
            continue
        target = target.replace("_", " ")
        if target and target[0].islower():
            target = target[0].upper() + target[1:]
        if target == self_title:
            continue
        if target in seen:
            continue
        seen.add(target)
        out.append(target)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Pass 1: revision history per article
# ─────────────────────────────────────────────────────────────────────────────

def fetch_history(title: str) -> tuple[list[dict], bool]:
    """Return (revisions in our window, descending by timestamp; missing flag)."""
    revs: list[dict] = []
    rvcontinue: str | None = None
    safety = 0
    while safety < 80:  # absurd cap on pagination
        params = {
            "action": "query",
            "titles": title,
            "prop": "revisions",
            "rvprop": "ids|timestamp",
            "rvstart": WINDOW_END,
            "rvend": WINDOW_START,
            "rvdir": "older",
            "rvlimit": 500,
            "format": "json",
            "formatversion": 2,
            "redirects": 1,
        }
        if rvcontinue:
            params["rvcontinue"] = rvcontinue
        data = api_get(params)
        pages = data.get("query", {}).get("pages", [])
        if not pages:
            return [], True
        page = pages[0]
        if page.get("missing"):
            return [], True
        revs.extend(page.get("revisions", []))
        cont = data.get("continue", {})
        rvcontinue = cont.get("rvcontinue")
        safety += 1
        if not rvcontinue:
            break
    return revs, False


def pick_revs_for_snapshots(revs_desc: list[dict]) -> dict[str, dict | None]:
    """For each snapshot date, find first rev with timestamp <= EOD of date."""
    out: dict[str, dict | None] = {}
    for date in SNAPSHOT_DATES:
        cutoff = SNAPSHOT_END_TS[date]
        chosen: dict | None = None
        for r in revs_desc:  # already descending
            if r["timestamp"] <= cutoff:
                chosen = r
                break
        out[date] = chosen
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Pass 2: content fetch by rev_id, batched
# ─────────────────────────────────────────────────────────────────────────────

def fetch_content_batch(rev_ids: list[int]) -> dict[int, tuple[str, str, str]]:
    """rev_id -> (title, timestamp, wikitext)."""
    data = api_get({
        "action": "query",
        "revids": "|".join(str(x) for x in rev_ids),
        "prop": "revisions",
        "rvprop": "ids|timestamp|content",
        "rvslots": "main",
        "format": "json",
        "formatversion": 2,
    })
    out: dict[int, tuple[str, str, str]] = {}
    for p in data.get("query", {}).get("pages", []) or []:
        title = p.get("title", "")
        for r in p.get("revisions", []) or []:
            content = (r.get("slots", {}).get("main", {}).get("content", "") or "")
            out[r.get("revid")] = (title, r.get("timestamp", ""), content)
    return out


# ─────────────────────────────────────────────────────────────────────────────

def load_universe(bq: bigquery.Client) -> list[str]:
    q = f"SELECT article_title FROM `{UNIVERSE_TABLE}` ORDER BY article_title"
    return [r.article_title for r in bq.query(q).result()]


def load_existing_pairs(bq: bigquery.Client) -> set[tuple[str, str]]:
    try:
        q = f"SELECT article_title, CAST(snapshot_date AS STRING) AS snapshot_date FROM `{DEST_TABLE}`"
        return {(r.article_title, r.snapshot_date) for r in bq.query(q).result()}
    except Exception as e:
        logger.info("Destination table not yet present (%s); starting fresh.", type(e).__name__)
        return set()


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def materialize():
    bq = bigquery.Client(project=PROJECT)
    universe = load_universe(bq)
    if ARTICLE_LIMIT > 0:
        universe = universe[:ARTICLE_LIMIT]
    logger.info("Universe: %d articles", len(universe))

    existing = load_existing_pairs(bq)
    logger.info("Already loaded: %d (article, date) pairs", len(existing))

    # Articles for which we still need any snapshot
    articles_needed = [
        a for a in universe
        if any((a, d) not in existing for d in SNAPSHOT_DATES)
    ]
    logger.info("Articles needing fetch: %d", len(articles_needed))

    if not articles_needed:
        logger.info("Nothing to do.")
        return pd.DataFrame(columns=[
            "article_title", "snapshot_date", "revision_id",
            "revision_timestamp", "wikilinks", "wikilinks_count", "fetched_at",
        ])

    fetched_at = datetime.now(timezone.utc)
    rows: list[dict] = []
    rev_id_to_request: list[tuple[int, str, str]] = []  # (rev_id, article_title, snapshot_date)

    # ─── Pass 1: revision history ─────────────────────────────────────────
    logger.info("Pass 1: fetching revision histories (parallel × %d)", MAX_WORKERS)
    pass1_started = time.time()
    pass1_done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_history, a): a for a in articles_needed}
        for fut in as_completed(futures):
            article = futures[fut]
            try:
                revs, missing = fut.result()
            except Exception as e:
                logger.error("History failed for %s: %s", article, e)
                continue
            if missing:
                # Article doesn't exist now — mark all snapshots null
                for d in SNAPSHOT_DATES:
                    if (article, d) in existing:
                        continue
                    rows.append({
                        "article_title": article,
                        "snapshot_date": d,
                        "revision_id": None,
                        "revision_timestamp": None,
                        "wikilinks": "[]",
                        "wikilinks_count": 0,
                    })
                pass1_done += 1
                continue
            picks = pick_revs_for_snapshots(revs)
            for d, rev in picks.items():
                if (article, d) in existing:
                    continue
                if rev is None:
                    rows.append({
                        "article_title": article,
                        "snapshot_date": d,
                        "revision_id": None,
                        "revision_timestamp": None,
                        "wikilinks": "[]",
                        "wikilinks_count": 0,
                    })
                else:
                    rev_id_to_request.append((rev["revid"], article, d))
            pass1_done += 1
            if pass1_done % 50 == 0:
                elapsed = time.time() - pass1_started
                rate = pass1_done / elapsed if elapsed else 0
                eta = (len(articles_needed) - pass1_done) / rate if rate else 0
                logger.info(
                    "Pass 1: %d/%d articles (%.1f art/s, ETA %.1f min)",
                    pass1_done, len(articles_needed), rate, eta / 60,
                )
    logger.info(
        "Pass 1 done: %d articles processed in %.1f min, %d rev_ids to fetch",
        pass1_done, (time.time() - pass1_started) / 60, len(rev_id_to_request),
    )

    # ─── Pass 2: batched content fetch ────────────────────────────────────
    # Group requests by rev_id (a rev_id may be referenced by multiple
    # snapshot dates of the same article, e.g., the article hasn't been
    # edited between Dec and May).
    revid_to_targets: dict[int, list[tuple[str, str]]] = {}
    for rid, article, date in rev_id_to_request:
        revid_to_targets.setdefault(rid, []).append((article, date))
    unique_revids = sorted(revid_to_targets.keys())
    logger.info("Pass 2: %d unique rev_ids → %d batches of %d",
                len(unique_revids), (len(unique_revids) + REV_BATCH - 1) // REV_BATCH, REV_BATCH)

    pass2_started = time.time()
    pass2_done_batches = 0
    pass2_batches = list(chunked(unique_revids, REV_BATCH))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_content_batch, b): b for b in pass2_batches}
        for fut in as_completed(futures):
            batch = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                logger.error("Content batch failed (%d revs): %s", len(batch), e)
                # Emit null rows for these so we don't get stuck re-fetching
                for rid in batch:
                    for article, date in revid_to_targets.get(rid, []):
                        rows.append({
                            "article_title": article,
                            "snapshot_date": date,
                            "revision_id": rid,
                            "revision_timestamp": None,
                            "wikilinks": "[]",
                            "wikilinks_count": 0,
                        })
                pass2_done_batches += 1
                continue
            for rid in batch:
                got = result.get(rid)
                targets = revid_to_targets.get(rid, [])
                if got is None:
                    # Revision missing/deleted: write null rows
                    for article, date in targets:
                        rows.append({
                            "article_title": article,
                            "snapshot_date": date,
                            "revision_id": rid,
                            "revision_timestamp": None,
                            "wikilinks": "[]",
                            "wikilinks_count": 0,
                        })
                    continue
                title, ts, content = got
                for article, date in targets:
                    links = extract_wikilinks(content, article)
                    rows.append({
                        "article_title": article,
                        "snapshot_date": date,
                        "revision_id": rid,
                        "revision_timestamp": ts,
                        "wikilinks": json.dumps(links, ensure_ascii=False),
                        "wikilinks_count": len(links),
                    })
            pass2_done_batches += 1
            if pass2_done_batches % 5 == 0:
                elapsed = time.time() - pass2_started
                rate = pass2_done_batches / elapsed if elapsed else 0
                eta = (len(pass2_batches) - pass2_done_batches) / rate if rate else 0
                logger.info(
                    "Pass 2: %d/%d batches (%.1f batch/s, ETA %.1f min)",
                    pass2_done_batches, len(pass2_batches), rate, eta / 60,
                )

    logger.info(
        "Pass 2 done in %.1f min. Total new rows: %d",
        (time.time() - pass2_started) / 60, len(rows),
    )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for r in rows:
        r["fetched_at"] = fetched_at
    df["fetched_at"] = fetched_at
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    df["revision_timestamp"] = pd.to_datetime(df["revision_timestamp"], errors="coerce", utc=True)
    # nullable-Int64 keeps schema as INT64 even when some rows have null rev_id
    df["revision_id"] = df["revision_id"].astype("Int64")
    df["wikilinks_count"] = df["wikilinks_count"].astype("Int64")
    return df
