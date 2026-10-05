"""@bruin
name: raw.gdacs_flood_events
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Flood events from GDACS, the Global Disaster Alert and Coordination System
  (UN OCHA + European Commission JRC, https://www.gdacs.org). One row per event,
  fetched from the public GDACS API (no key):
    - event list: /gdacsapi/api/events/geteventlist/SEARCH (all alert levels)
    - affected-area polygon: /gdacsapi/api/polygons/getgeometry (latest episode)
    - Sendai-framework impact reports: /gdacsapi/api/events/geteventdata
  Most GDACS flood events are generated from GloFAS river-discharge forecasts
  and the Dartmouth Flood Observatory. GDACS content is free to reuse with
  attribution (https://www.gdacs.org/About/termofuse.aspx).

  The event search runs once per summer season in the run interval
  (SEASON_START / SEASON_END as MM-DD, default 06-01 to 09-20). Events that
  overlap two searches are fetched once.

materialization:
  type: table
  strategy: append

columns:
  - name: event_id
    type: INTEGER
    description: GDACS event identifier.
    primary_key: true
  - name: episode_id
    type: INTEGER
    description: Latest GDACS episode number for the event at fetch time.
  - name: glide
    type: VARCHAR
    description: GLIDE disaster identifier (e.g. FL-2026-000148-CHN), when assigned.
  - name: event_name
    type: VARCHAR
    description: GDACS display name (e.g. "Flood in Nepal").
  - name: country
    type: VARCHAR
    description: Primary country name reported by GDACS.
  - name: iso3
    type: VARCHAR
    description: Primary country ISO 3166-1 alpha-3 code.
  - name: affected_iso3
    type: VARCHAR
    description: Comma-separated ISO3 codes of every affected country.
  - name: alert_level
    type: VARCHAR
    description: Overall GDACS alert level - Green, Orange or Red (Red = highest expected humanitarian impact).
  - name: alert_score
    type: DOUBLE
    description: Numeric GDACS alert score (Green 1, Orange 2, Red 3; fractional values possible).
  - name: from_date
    type: TIMESTAMP
    description: Event start (UTC).
  - name: to_date
    type: TIMESTAMP
    description: Event end or latest update (UTC).
  - name: date_modified
    type: TIMESTAMP
    description: When GDACS last modified the event record (UTC).
  - name: source
    type: VARCHAR
    description: Upstream detection source (e.g. GLOFAS, DFO).
  - name: centroid_lon
    type: DOUBLE
    description: Event centroid longitude (WGS84 degrees).
  - name: centroid_lat
    type: DOUBLE
    description: Event centroid latitude (WGS84 degrees).
  - name: affected_geojson
    type: VARCHAR
    description: GeoJSON geometry of the GDACS "Affected area" polygon for the latest episode; NULL if none was published.
  - name: sendai_json
    type: VARCHAR
    description: JSON array of Sendai-framework impact reports (deaths, displaced, affected, ...) as published by GDACS.
  - name: report_url
    type: VARCHAR
    description: GDACS human-readable event report URL.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was fetched.

@bruin"""

import json
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

API = "https://www.gdacs.org/gdacsapi/api"
SEASON_START = os.environ.get("SEASON_START", "06-01")
SEASON_END = os.environ.get("SEASON_END", "09-20")
PAGE_SIZE = 100
MAX_RETRIES = 5


def get_json(session: requests.Session, url: str, params: dict | None = None):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=90)
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            if not resp.text.strip():
                return None
            return resp.json()
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout, ValueError) as exc:
            if attempt == MAX_RETRIES:
                logger.warning("Giving up on %s: %s", url, exc)
                return None
            wait = 2 ** attempt
            logger.warning("Request failed (%s), retry %d in %ds", exc, attempt, wait)
            time.sleep(wait)
    return None


def list_events(session: requests.Session, start: str, end: str) -> list[dict]:
    events, page = [], 1
    while True:
        data = get_json(
            session,
            f"{API}/events/geteventlist/SEARCH",
            {
                "eventlist": "FL",
                "fromDate": start,
                "toDate": end,
                "alertlevel": "Green;Orange;Red",
                "pagesize": PAGE_SIZE,
                "pagenumber": page,
            },
        )
        feats = (data or {}).get("features", [])
        logger.info("Event list page %d: %d events", page, len(feats))
        events.extend(feats)
        if len(feats) < PAGE_SIZE:
            return events
        page += 1
        time.sleep(0.5)


def affected_polygon(session: requests.Session, geometry_url: str) -> str | None:
    data = get_json(session, geometry_url)
    for feat in (data or {}).get("features", []):
        if feat.get("properties", {}).get("Class") == "Poly_Affected":
            return json.dumps(feat["geometry"])
    return None


def materialize():
    start = os.environ.get("BRUIN_START_DATE", "2026-06-01")[:10]
    end = os.environ.get("BRUIN_END_DATE", "2026-09-20")[:10]
    logger.info("Interval: %s to %s", start, end)

    session = requests.Session()
    features, seen = [], set()
    for year in range(int(start[:4]), int(end[:4]) + 1):
        ws = max(start, f"{year}-{SEASON_START}")
        we = min(end, f"{year}-{SEASON_END}")
        if ws > we:
            continue
        for feat in list_events(session, ws, we):
            eid = feat["properties"]["eventid"]
            if eid not in seen:
                seen.add(eid)
                features.append(feat)
        logger.info("Season %d: %d unique events so far", year, len(features))

    rows = []
    for i, feat in enumerate(features, 1):
        p = feat["properties"]
        lon, lat = feat["geometry"]["coordinates"][:2]
        urls = p.get("url") or {}
        polygon = affected_polygon(session, urls["geometry"]) if urls.get("geometry") else None
        details = get_json(session, f"{API}/events/geteventdata", {"eventtype": "FL", "eventid": p["eventid"]})
        sendai = ((details or {}).get("properties") or {}).get("sendai") or []
        rows.append(
            {
                "event_id": int(p["eventid"]),
                "episode_id": int(p["episodeid"]),
                "glide": p.get("glide") or None,
                "event_name": p.get("name"),
                "country": p.get("country"),
                "iso3": p.get("iso3"),
                "affected_iso3": ",".join(c["iso3"] for c in p.get("affectedcountries") or [] if c.get("iso3")),
                "alert_level": p.get("alertlevel"),
                "alert_score": float(p.get("alertscore") or 0),
                "from_date": pd.to_datetime(p.get("fromdate"), utc=True),
                "to_date": pd.to_datetime(p.get("todate"), utc=True),
                "date_modified": pd.to_datetime(p.get("datemodified"), utc=True),
                "source": p.get("source"),
                "centroid_lon": float(lon),
                "centroid_lat": float(lat),
                "affected_geojson": polygon,
                "sendai_json": json.dumps(sendai) if sendai else None,
                "report_url": urls.get("report"),
            }
        )
        if i % 25 == 0:
            logger.info("Enriched %d / %d events", i, len(features))
        time.sleep(0.5)

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now(timezone.utc)
    logger.info(
        "Fetched %d flood events (%d with affected-area polygons, %d with impact reports)",
        len(df),
        int(df["affected_geojson"].notna().sum()) if len(df) else 0,
        int(df["sendai_json"].notna().sum()) if len(df) else 0,
    )
    return df
