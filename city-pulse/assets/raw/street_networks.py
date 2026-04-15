"""@bruin

name: raw.street_networks
description: |
  Downloads and analyzes street network graphs for ~20 major world cities using
  OSMnx (OpenStreetMap). Computes urban form metrics that reveal each city's
  physical DNA: street orientation entropy (grid-ness), intersection density,
  dead-end proportion, circuity, and bearing distributions.

  The bearing distributions (36 bins at 10 degree intervals) are stored as JSON
  arrays for reconstructing polar "fingerprint" plots in the dashboard.

  Cities are selected to represent diverse urban planning traditions:
  grid (NYC, Chicago), organic (London, Tokyo), radial (Paris, Moscow),
  developing megacities (Lagos, Mumbai), and compact (Amsterdam, Singapore).

  The analysis produces street network "fingerprints" that can distinguish between
  planned grid cities (high orientation order) and organically grown cities (low
  orientation order). Results are joined with GHSL urban center data in staging
  to create comprehensive city profiles.

  Source: OpenStreetMap via OSMnx/Overpass API
  License: Open Data Commons Open Database License (ODbL)

  Environment variables:
    CITY_LIMIT - Number of cities to process (default: all). Set to 3 for quick tests.
connection: bruin-playground-arsalan
tags:
  - urban-planning
  - geospatial
  - network-analysis
  - openstreetmap
  - external-api
  - raw
  - public
  - batch

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: city_id
    type: VARCHAR
    description: URL-safe slug of the city name (e.g. "new_york")
    primary_key: true
    checks:
      - name: not_null
  - name: city_name
    type: VARCHAR
    description: Human-readable city name
    checks:
      - name: not_null
  - name: country_code
    type: VARCHAR
    description: ISO 3166-1 alpha-3 country code
  - name: osm_query
    type: VARCHAR
    description: The query string passed to OSMnx graph_from_place
  - name: latitude
    type: DOUBLE
    description: City center latitude (centroid of the network bounding box)
  - name: longitude
    type: DOUBLE
    description: City center longitude (centroid of the network bounding box)
  - name: orientation_entropy
    type: DOUBLE
    description: |
      Shannon entropy of street bearing distribution (36 bins, 10 degrees each).
      Lower values indicate more grid-like layout. Range approximately 2.5 (perfect grid)
      to 3.58 (uniform/random). Computed via scipy.stats.entropy.
  - name: orientation_order
    type: DOUBLE
    description: |
      1 - (entropy / max_entropy). Ranges 0 (random) to 1 (perfect grid).
      Max entropy = ln(36) = 3.584.
  - name: avg_street_length_m
    type: DOUBLE
    description: Mean edge (street segment) length in meters
  - name: intersection_count
    type: INTEGER
    description: Number of true intersections (nodes with 3+ edges)
  - name: dead_end_count
    type: INTEGER
    description: Number of dead-end nodes (degree 1)
  - name: dead_end_proportion
    type: DOUBLE
    description: Fraction of nodes that are dead ends (0 to 1)
  - name: node_count
    type: INTEGER
    description: Total number of nodes in the street network graph
  - name: edge_count
    type: INTEGER
    description: Total number of edges in the street network graph
  - name: total_length_km
    type: DOUBLE
    description: Total street network length in kilometers
  - name: avg_circuity
    type: DOUBLE
    description: |
      Average ratio of network distance to straight-line distance between
      random node pairs. Values close to 1.0 indicate direct routes;
      higher values indicate winding streets.
  - name: bearing_counts
    type: VARCHAR
    description: |
      JSON array of 36 integers representing the count of street segments
      in each 10-degree bearing bin (0-10, 10-20, ..., 350-360).
      Used to reconstruct polar orientation plots in the dashboard.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this analysis was performed
    checks:
      - name: not_null

@bruin"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Cities to analyze: name, (lat, lon) center point, ISO alpha-3 country code
# All cities use the SAME method: 10 km radius from city center.
# This ensures consistent spatial scale across all comparisons.
ANALYSIS_RADIUS_M = 10_000  # 10 km radius for all cities

CITIES = [
    ("New York",      (40.7128, -74.0060), "USA"),
    ("Chicago",       (41.8781, -87.6298), "USA"),
    ("Barcelona",     (41.3874,   2.1686), "ESP"),
    ("Buenos Aires",  (-34.6037, -58.3816), "ARG"),
    ("London",        (51.5074,  -0.1278), "GBR"),
    ("Tokyo",         (35.6762, 139.6503), "JPN"),
    ("Istanbul",      (41.0082,  28.9784), "TUR"),
    ("Rome",          (41.9028,  12.4964), "ITA"),
    ("Paris",         (48.8566,   2.3522), "FRA"),
    ("Moscow",        (55.7558,  37.6173), "RUS"),
    ("Washington DC", (38.9072, -77.0369), "USA"),
    ("Brasilia",      (-15.7975, -47.8919), "BRA"),
    ("Lagos",         (6.5244,    3.3792), "NGA"),
    ("Mumbai",        (19.0760,  72.8777), "IND"),
    ("Jakarta",       (-6.2088, 106.8456), "IDN"),
    ("Cairo",         (30.0444,  31.2357), "EGY"),
    ("Amsterdam",     (52.3676,   4.9041), "NLD"),
    ("Singapore",     (1.3521,  103.8198), "SGP"),
    ("Hong Kong",     (22.3193, 114.1694), "HKG"),
    ("Seoul",         (37.5665, 126.9780), "KOR"),
]

def slugify(name: str) -> str:
    """Convert city name to URL-safe slug."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")

def compute_bearing_histogram(G, num_bins: int = 36) -> list[int]:
    """Compute street bearing histogram using OSMnx."""
    import osmnx as ox

    bearings = ox.bearing.add_edge_bearings(G)
    edge_bearings = []
    for u, v, data in bearings.edges(data=True):
        if "bearing" in data:
            b = data["bearing"] % 360
            # Fold to 0-180 (undirected streets)
            if b >= 180:
                b -= 180
            edge_bearings.append(b)
            # Mirror to full circle
            edge_bearings.append(b + 180)

    if not edge_bearings:
        return [0] * num_bins

    bin_edges = np.linspace(0, 360, num_bins + 1)
    counts, _ = np.histogram(edge_bearings, bins=bin_edges)
    return counts.tolist()

def compute_entropy(counts: list[int]) -> float:
    """Compute Shannon entropy of bearing distribution."""
    from scipy.stats import entropy as shannon_entropy

    total = sum(counts)
    if total == 0:
        return 0.0
    probs = [c / total for c in counts]
    return float(shannon_entropy(probs))

def analyze_city(city_name: str, center: tuple, country_code: str) -> dict | None:
    """Download and analyze street network for one city using fixed radius from center."""
    import osmnx as ox

    ox.settings.timeout = 300

    logger.info("Analyzing %s (%.4f, %.4f, r=%dm)...", city_name, center[0], center[1], ANALYSIS_RADIUS_M)

    for attempt in range(3):
        try:
            G = ox.graph_from_point(center, dist=ANALYSIS_RADIUS_M, network_type="drive")
            break
        except Exception as e:
            wait = 60 * (attempt + 1)
            logger.warning(
                "  Error downloading %s (attempt %d/3): %s, retrying in %ds",
                city_name, attempt + 1, e, wait,
            )
            time.sleep(wait)
    else:
        logger.error("  Failed to download %s after 3 attempts, skipping", city_name)
        return None

    # Basic graph stats
    G_undirected = ox.convert.to_undirected(G)
    node_count = G_undirected.number_of_nodes()
    edge_count = G_undirected.number_of_edges()

    # Degree-based metrics
    degrees = dict(G_undirected.degree())
    dead_ends = sum(1 for d in degrees.values() if d == 1)
    intersections = sum(1 for d in degrees.values() if d >= 3)

    # Edge lengths
    edge_lengths = []
    for u, v, data in G_undirected.edges(data=True):
        if "length" in data:
            edge_lengths.append(data["length"])

    avg_length = float(np.mean(edge_lengths)) if edge_lengths else 0.0
    total_length_km = sum(edge_lengths) / 1000.0 if edge_lengths else 0.0

    # Bearing histogram and entropy
    bearing_counts = compute_bearing_histogram(G)
    entropy = compute_entropy(bearing_counts)
    max_entropy = np.log(len(bearing_counts))  # ln(36) ≈ 3.584
    orientation_order = 1.0 - (entropy / max_entropy) if max_entropy > 0 else 0.0

    # Circuity (sample-based for performance)
    try:
        stats = ox.stats.basic_stats(G_undirected)
        avg_circuity = stats.get("circuity_avg", None)
        if avg_circuity is None:
            avg_circuity = float("nan")
    except Exception:
        avg_circuity = float("nan")

    # Use the provided center point
    lat, lon = center

    result = {
        "city_id": slugify(city_name),
        "city_name": city_name,
        "country_code": country_code,
        "osm_query": f"point({center[0]:.4f},{center[1]:.4f}),r={ANALYSIS_RADIUS_M}m",
        "latitude": lat,
        "longitude": lon,
        "orientation_entropy": round(entropy, 4),
        "orientation_order": round(orientation_order, 4),
        "avg_street_length_m": round(avg_length, 1),
        "intersection_count": intersections,
        "dead_end_count": dead_ends,
        "dead_end_proportion": round(dead_ends / node_count, 4) if node_count > 0 else 0.0,
        "node_count": node_count,
        "edge_count": edge_count,
        "total_length_km": round(total_length_km, 2),
        "avg_circuity": round(avg_circuity, 4) if not np.isnan(avg_circuity) else None,
        "bearing_counts": json.dumps(bearing_counts),
    }

    logger.info(
        "  %s: %d nodes, %d edges, entropy=%.3f, order=%.3f",
        city_name, node_count, edge_count, entropy, orientation_order,
    )
    return result

def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2000-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2024-12-31")
    logger.info("Interval: %s to %s (unused for network analysis)", start_date, end_date)

    city_limit = int(os.environ.get("CITY_LIMIT", str(len(CITIES))))
    cities_to_process = CITIES[:city_limit]
    logger.info("Processing %d of %d cities", len(cities_to_process), len(CITIES))

    results = []
    for i, (name, center, code) in enumerate(cities_to_process):
        logger.info("City %d/%d", i + 1, len(cities_to_process))
        result = analyze_city(name, center, code)
        if result:
            results.append(result)
        if i < len(cities_to_process) - 1:
            time.sleep(2)  # Respect Overpass API rate limits

    if not results:
        raise RuntimeError("No cities were successfully analyzed — check Overpass API connectivity")

    df = pd.DataFrame(results)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Total: %d cities analyzed successfully", len(df))
    return df
