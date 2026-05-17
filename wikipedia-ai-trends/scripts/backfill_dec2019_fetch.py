"""Phase A: fetch Dec-2019 Wikipedia revisions into a local DuckDB file.

Runs without BigQuery. Talks only to en.wikipedia.org. Resumable: rows already
present in the DuckDB are skipped on restart.

Output: ./wat_dec2019_backfill.duckdb
  Table: dec2019_snapshots
    article_title TEXT, revision_id BIGINT, revision_timestamp TIMESTAMP,
    wikilinks TEXT (JSON), wikilinks_count BIGINT, fetched_at TIMESTAMP
"""
from __future__ import annotations

import json
import logging
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

API_URL = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "wikipedia-ai-trends/0.1 (arsalan.noorafkan@getbruin.com)"
SNAPSHOT_DATE = "2019-12-01"
CUTOFF = "2019-12-01T23:59:59Z"

DUCKDB_PATH = Path(__file__).parent / "wat_dec2019_backfill.duckdb"

MAX_WORKERS = int(os.environ.get("WAT_MAX_WORKERS", "4"))
REQUEST_DELAY = float(os.environ.get("WAT_REQUEST_DELAY", "0.3"))
REV_BATCH = 50

SUBJECTS = [
    "People", "History", "Geography", "Arts", "Philosophy and religion",
    "Everyday life", "Society and social sciences", "Biology and health sciences",
    "Physical sciences", "Technology", "Mathematics",
]

LIST_ITEM_LINK = re.compile(
    r"^\s*#+:?\s*"
    r"(?:\{\{[^}]+\}\}\s*)*"
    r"'{0,3}\s*\[\["
    r"([^\]|#]+?)"
    r"(?:\#[^\]|]*)?"
    r"(?:\|[^\]]*)?"
    r"\]\]"
)
WIKILINK_RE = re.compile(r"\[\[([^\[\]\|\n]+?)(?:\|[^\[\]\n]*)?\]\]")
NON_ARTICLE_PREFIXES = (
    "File:", "Image:", "Category:", "Wikipedia:", "WP:", "Special:", "Help:",
    "Portal:", "Template:", "User:", "Talk:", "User talk:", "Wikipedia talk:",
    "Module:", "Draft:", "MediaWiki:", "Book:",
    "File talk:", "Category talk:", "Template talk:", "Portal talk:",
    "wikt:", "w:", "s:", "b:", "q:", "v:", "m:", "n:",
)

_thread_local = __import__("threading").local()


def session() -> requests.Session:
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
            r = session().get(API_URL, params=params, timeout=120)
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
                    time.sleep(float(r.headers.get("Retry-After", 5)) + random.random())
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


def parse_vital_articles_page(subject: str, wikitext: str) -> list[str]:
    titles: list[str] = []
    seen: set[str] = set()
    for raw in wikitext.splitlines():
        m = LIST_ITEM_LINK.match(raw.rstrip())
        if not m:
            continue
        title = m.group(1).strip().replace("_", " ")
        if ":" in title.split(" ")[0]:
            prefix = title.split(":")[0]
            if prefix in {"Wikipedia", "File", "Category", "Special", "Help", "Portal",
                          "Template", "User", "Talk", "Image", "WP", "m"}:
                continue
        if title in seen:
            continue
        seen.add(title)
        titles.append(title)
    return titles


def scrape_universe() -> list[str]:
    all_titles: list[str] = []
    seen: set[str] = set()
    for subject in SUBJECTS:
        page = f"Wikipedia:Vital articles/Level 4/{subject}"
        logger.info("Scraping %s", page)
        data = api_get({"action": "parse", "page": page, "prop": "wikitext", "format": "json", "redirects": 1})
        wt = data.get("parse", {}).get("wikitext", {}).get("*", "")
        titles = parse_vital_articles_page(subject, wt)
        logger.info("  -> %d titles in %s", len(titles), subject)
        for t in titles:
            if t not in seen:
                seen.add(t)
                all_titles.append(t)
    logger.info("Universe size: %d unique articles", len(all_titles))
    return all_titles


def fetch_dec2019_rev(title: str) -> tuple[int | None, str | None]:
    """Return (rev_id, timestamp) for the closest rev ≤ Dec-2019, or (None, None)."""
    data = api_get({
        "action": "query",
        "titles": title,
        "prop": "revisions",
        "rvprop": "ids|timestamp",
        "rvstart": CUTOFF,
        "rvdir": "older",
        "rvlimit": 1,
        "format": "json",
        "formatversion": 2,
        "redirects": 1,
    })
    pages = data.get("query", {}).get("pages", [])
    if not pages:
        return None, None
    page = pages[0]
    if page.get("missing"):
        return None, None
    revs = page.get("revisions", [])
    if not revs:
        return None, None
    return revs[0].get("revid"), revs[0].get("timestamp")


def fetch_content_batch(rev_ids: list[int]) -> dict[int, tuple[str, str, str]]:
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


def extract_wikilinks(wikitext: str, self_title: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in WIKILINK_RE.findall(wikitext):
        target = raw.strip().split("#", 1)[0].strip()
        if not target or any(target.startswith(p) for p in NON_ARTICLE_PREFIXES):
            continue
        target = target.replace("_", " ")
        if target and target[0].islower():
            target = target[0].upper() + target[1:]
        if target == self_title or target in seen:
            continue
        seen.add(target)
        out.append(target)
    return out


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def ensure_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS dec2019_snapshots (
            article_title TEXT PRIMARY KEY,
            revision_id BIGINT,
            revision_timestamp TIMESTAMP,
            wikilinks TEXT,
            wikilinks_count BIGINT,
            fetched_at TIMESTAMP
        )
    """)


def main():
    con = duckdb.connect(str(DUCKDB_PATH))
    ensure_table(con)

    done = {r[0] for r in con.execute("SELECT article_title FROM dec2019_snapshots").fetchall()}
    logger.info("Already in DuckDB: %d articles", len(done))

    universe = scrape_universe()
    todo = [t for t in universe if t not in done]
    logger.info("To fetch: %d articles (skipping %d already done)", len(todo), len(universe) - len(todo))
    if not todo:
        logger.info("Nothing to do.")
        return

    # ── Pass 1: fetch rev_id for each todo article ──
    logger.info("Pass 1: fetching Dec-2019 rev_id (parallel × %d, delay %.2fs)", MAX_WORKERS, REQUEST_DELAY)
    rev_for_article: dict[str, tuple[int | None, str | None]] = {}
    pass1_started = time.time()
    done_p1 = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_dec2019_rev, t): t for t in todo}
        for fut in as_completed(futures):
            title = futures[fut]
            try:
                rev_id, ts = fut.result()
            except Exception as e:
                logger.error("Pass 1 failed for %s: %s", title, e)
                rev_id, ts = None, None
            rev_for_article[title] = (rev_id, ts)
            done_p1 += 1
            if done_p1 % 50 == 0:
                elapsed = time.time() - pass1_started
                rate = done_p1 / elapsed if elapsed else 0
                eta = (len(todo) - done_p1) / rate if rate else 0
                logger.info("Pass 1: %d/%d (%.1f art/s, ETA %.1f min)",
                            done_p1, len(todo), rate, eta / 60)

    logger.info("Pass 1 done: %.1f min, %d non-null revs",
                (time.time() - pass1_started) / 60,
                sum(1 for v in rev_for_article.values() if v[0] is not None))

    # ── Persist null-rev rows immediately (no content needed) ──
    fetched_at = datetime.now(timezone.utc)
    null_rows = [(t, None, None, "[]", 0, fetched_at)
                 for t, (rid, _) in rev_for_article.items() if rid is None]
    if null_rows:
        con.executemany(
            "INSERT OR REPLACE INTO dec2019_snapshots VALUES (?, ?, ?, ?, ?, ?)",
            null_rows,
        )
        con.commit()
        logger.info("Persisted %d null-rev rows", len(null_rows))

    # ── Pass 2: batch-fetch content ──
    revid_to_titles: dict[int, list[str]] = {}
    for t, (rid, _) in rev_for_article.items():
        if rid is not None:
            revid_to_titles.setdefault(rid, []).append(t)
    unique_revids = sorted(revid_to_titles)
    batches = list(chunked(unique_revids, REV_BATCH))
    logger.info("Pass 2: %d unique revs → %d batches of %d", len(unique_revids), len(batches), REV_BATCH)

    pass2_started = time.time()
    done_p2 = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_content_batch, b): b for b in batches}
        for fut in as_completed(futures):
            batch = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                logger.error("Pass 2 batch (%d revs) failed: %s", len(batch), e)
                result = {}
            rows = []
            for rid in batch:
                got = result.get(rid)
                titles = revid_to_titles.get(rid, [])
                for t in titles:
                    if got is None:
                        rows.append((t, rid, None, "[]", 0, fetched_at))
                    else:
                        _, ts, content = got
                        links = extract_wikilinks(content, t)
                        rows.append((t, rid, ts, json.dumps(links, ensure_ascii=False),
                                     len(links), fetched_at))
            if rows:
                con.executemany(
                    "INSERT OR REPLACE INTO dec2019_snapshots VALUES (?, ?, ?, ?, ?, ?)",
                    rows,
                )
                con.commit()
            done_p2 += 1
            if done_p2 % 5 == 0:
                elapsed = time.time() - pass2_started
                rate = done_p2 / elapsed if elapsed else 0
                eta = (len(batches) - done_p2) / rate if rate else 0
                logger.info("Pass 2: %d/%d batches (%.1f batch/s, ETA %.1f min)",
                            done_p2, len(batches), rate, eta / 60)

    total = con.execute("SELECT COUNT(*) FROM dec2019_snapshots").fetchone()[0]
    valid = con.execute("SELECT COUNT(*) FROM dec2019_snapshots WHERE revision_id IS NOT NULL").fetchone()[0]
    logger.info("Done. DuckDB rows: %d (valid revs: %d)", total, valid)


if __name__ == "__main__":
    sys.exit(main() or 0)
