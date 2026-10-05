#!/usr/bin/env python3
"""Export the report tables to GeoJSON and JSON files the MapLibre page reads.

Run from the repository root:

    python3 cities-shops-map/map/export_geojson.py                    # full export, ~8 min
    python3 cities-shops-map/map/export_geojson.py --metadata-only    # small JSON only, ~15 s

Writes into cities-shops-map/map/data/, which is gitignored via map/.gitignore because the
full export is roughly 78 MB across 60-odd files. Re-run this script after
`bruin run cities-shops-map/` to refresh it.

Every query goes through `bruin query`, so the script needs no BigQuery credentials of its
own beyond the connection already configured in the repo-root .bruin.yml.
"""

from __future__ import annotations

import json
import logging
import math
import os
import shutil
import subprocess
import sys
from typing import Any

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

CONNECTION = "bruin-playground-arsalan"
QUERY_LIMIT = 200000
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Metres per degree used by the grid, matching staging.analysis_grid exactly.
METRES_PER_DEGREE_LAT = 110540.0
METRES_PER_DEGREE_LON_EQUATOR = 111320.0
CELL_SIZE_M = 250.0


def run_query(sql: str, description: str) -> list[dict[str, Any]]:
    """Execute SQL through the bruin CLI and return rows as dicts."""
    logger.info("Query: %s", description)
    result = subprocess.run(
        [
            "bruin", "query",
            "--connection", CONNECTION,
            "--output", "json",
            "--limit", str(QUERY_LIMIT),
            "--timeout", "900",
            "--description", description,
            "--query", sql,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error("Query failed: %s", result.stderr.strip() or result.stdout.strip())
        raise SystemExit(1)

    payload = json.loads(result.stdout)
    names = [c["name"] for c in payload["columns"]]
    rows = [dict(zip(names, row)) for row in payload["rows"]]
    logger.info("  -> %d rows", len(rows))
    return rows


def write_json(relative_path: str, payload: Any) -> int:
    path = os.path.join(OUTPUT_DIR, relative_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"), ensure_ascii=False)
    size = os.path.getsize(path)
    logger.info("  wrote %s (%.1f KB)", relative_path, size / 1e3)
    return size


def cell_polygon(
    center_lon: float, center_lat: float, half_lon: float, half_lat: float
) -> list[list[list[float]]]:
    west, east = round(center_lon - half_lon, 6), round(center_lon + half_lon, 6)
    south, north = round(center_lat - half_lat, 6), round(center_lat + half_lat, 6)
    return [[
        [west, south], [east, south], [east, north], [west, north], [west, south],
    ]]


def export_coverage_matrix() -> list[dict[str, Any]]:
    rows = run_query(
        """
        SELECT city, city_label, shop_type, shop_type_label, score,
               authority, coverage, granularity, freshness, geocoding, access,
               establishments_open, establishments_mappable, pct_geocoded,
               native_code_count, separability, is_usable, is_mappable,
               source_name, source_vintage, native_codes, caveats, caveats_json
        FROM report.coverage_matrix
        ORDER BY city, score DESC
        """,
        "export coverage matrix for the map caveat panel",
    )
    write_json("coverage_matrix.json", rows)
    return rows


def export_osm_gap() -> None:
    rows = run_query(
        """
        SELECT shop_type, register_count, osm_count, coverage_ratio,
               osm_matched, osm_matched_pct, osm_unmatched,
               register_missing_from_osm, register_missing_pct, interpretation
        FROM report.osm_register_gap
        ORDER BY register_count DESC
        """,
        "export OpenStreetMap versus register gap for the map methodology panel",
    )
    write_json("osm_gap.json", rows)


def export_city_metadata() -> dict[str, dict[str, Any]]:
    rows = run_query(
        """
        SELECT
            city,
            ANY_VALUE(lat_ref) AS lat_ref,
            COUNT(*) AS cells,
            MIN(center_lon) AS min_lon, MAX(center_lon) AS max_lon,
            MIN(center_lat) AS min_lat, MAX(center_lat) AS max_lat
        FROM staging.analysis_grid
        GROUP BY city
        """,
        "export per-city grid extent and latitude reference for map fitting",
    )
    cities: dict[str, dict[str, Any]] = {}
    for row in rows:
        lat_ref = float(row["lat_ref"])
        half_lon = (CELL_SIZE_M / 2) / (
            METRES_PER_DEGREE_LON_EQUATOR * math.cos(math.radians(lat_ref))
        )
        half_lat = (CELL_SIZE_M / 2) / METRES_PER_DEGREE_LAT
        cities[row["city"]] = {
            "lat_ref": lat_ref,
            "cells": int(row["cells"]),
            "half_lon": half_lon,
            "half_lat": half_lat,
            "bounds": [
                float(row["min_lon"]) - half_lon,
                float(row["min_lat"]) - half_lat,
                float(row["max_lon"]) + half_lon,
                float(row["max_lat"]) + half_lat,
            ],
        }
    write_json("cities.json", cities)
    return cities


def export_density(city: str, shop_type: str, meta: dict[str, Any]) -> int:
    rows = run_query(
        f"""
        SELECT cell_id, center_lon, center_lat, shops_per_1k_residents, density_class,
               density_rank, has_enough_residents, population_400m, shops_400m, all_shops_400m
        FROM report.shop_density
        WHERE city = '{city}' AND shop_type = '{shop_type}'
        """,
        f"export shop-density cells for {city}/{shop_type}",
    )
    features = []
    for r in rows:
        ratio = r["shops_per_1k_residents"]
        # -1 is the map class for "too few residents to publish a ratio". Keeping it as a
        # class rather than dropping the cell means the map shows a workplace district as a
        # workplace district instead of as a hole.
        density_class = r["density_class"]
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": cell_polygon(
                        float(r["center_lon"]),
                        float(r["center_lat"]),
                        meta["half_lon"],
                        meta["half_lat"],
                    ),
                },
                "properties": {
                    "id": r["cell_id"],
                    "per1k": None if ratio is None else round(float(ratio), 3),
                    "c": -1 if density_class is None else int(density_class),
                    "rank": None if r["density_rank"] is None else int(r["density_rank"]),
                    "pop": int(round(float(r["population_400m"]))),
                    "shops": int(r["shops_400m"]),
                    "allshops": int(r["all_shops_400m"]),
                },
            }
        )
    return write_json(
        f"density/{city}__{shop_type}.geojson",
        {"type": "FeatureCollection", "features": features},
    )


def export_density_bins() -> None:
    """Legend bin edges, so the legend states real values instead of a class number."""
    rows = run_query(
        """
        SELECT city, shop_type, density_class, cells, min_value, max_value,
               median_shops_400m, median_population_400m
        FROM report.density_bins
        ORDER BY city, shop_type, density_class
        """,
        "export density legend bin edges",
    )
    bins: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        bins.setdefault(f"{r['city']}|{r['shop_type']}", []).append(
            {
                "c": int(r["density_class"]),
                "cells": int(r["cells"]),
                "min": None if r["min_value"] is None else float(r["min_value"]),
                "max": None if r["max_value"] is None else float(r["max_value"]),
                "med_shops": None if r["median_shops_400m"] is None else float(r["median_shops_400m"]),
                "med_pop": None if r["median_population_400m"] is None else float(r["median_population_400m"]),
            }
        )
    write_json("density_bins.json", bins)


def export_points(city: str, shop_type: str) -> int:
    rows = run_query(
        f"""
        SELECT establishment_id, name, native_label, separability, lon, lat
        FROM report.shop_points
        WHERE city = '{city}' AND shop_type = '{shop_type}'
        """,
        f"export establishment points for {city}/{shop_type}",
    )
    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [round(float(r["lon"]), 5), round(float(r["lat"]), 5)],
            },
            "properties": {
                "name": r["name"] or "",
                "label": r["native_label"] or "",
                "sep": r["separability"],
            },
        }
        for r in rows
    ]
    return write_json(
        f"points/{city}__{shop_type}.geojson",
        {"type": "FeatureCollection", "features": features},
    )


def export_paris_vacant(shop_types: list[str]) -> int:
    """One file with every vacant premise, carrying the site score of each shop type.

    Emitting one file per shop type would repeat 9,060 identical geometries seven times.
    Densities are pivoted into explicit columns rather than an ARRAY_AGG(STRUCT(...)), because
    `bruin query --output json` serialises a struct array as a list of positional values, so
    the field names are lost on the way out.
    """
    pivots = ",\n".join(
        f"            MAX(IF(shop_type = '{t}', ROUND(shops_per_1k_residents, 3), NULL)) AS per1k_{t},\n"
        f"            MAX(IF(shop_type = '{t}', shops_400m, NULL)) AS shops_{t}"
        for t in shop_types
    )
    rows = run_query(
        f"""
        SELECT
            objectid, address, arrondissement, lon, lat, vacancy_reason,
            surface_exact_m2, surface_band,
            waves_occupied, distinct_activities, activity_changes, churn_risk,
{pivots}
        FROM report.paris_vacant_candidates
        GROUP BY objectid, address, arrondissement, lon, lat, vacancy_reason,
                 surface_exact_m2, surface_band,
                 waves_occupied, distinct_activities, activity_changes, churn_risk
        """,
        "export Paris vacant candidate addresses with per-shop-type scores",
    )
    features = []
    for r in rows:
        properties = {
            "addr": r["address"] or "",
            "arr": int(r["arrondissement"]) if r["arrondissement"] is not None else None,
            "why": r["vacancy_reason"],
            "m2": float(r["surface_exact_m2"]) if r["surface_exact_m2"] is not None else None,
            "band": r["surface_band"] or "",
            "waves": int(r["waves_occupied"]),
            "acts": int(r["distinct_activities"]),
            "churn": int(r["activity_changes"]),
            "risk": r["churn_risk"],
        }
        for shop_type in shop_types:
            per1k = r.get(f"per1k_{shop_type}")
            shops = r.get(f"shops_{shop_type}")
            if per1k is not None:
                properties[f"per1k_{shop_type}"] = round(float(per1k), 3)
            if shops is not None:
                properties[f"shops_{shop_type}"] = int(shops)
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [round(float(r["lon"]), 5), round(float(r["lat"]), 5)],
                },
                "properties": properties,
            }
        )
    return write_json(
        "vacant/paris.geojson", {"type": "FeatureCollection", "features": features}
    )


def main() -> None:
    # `--metadata-only` refreshes just the small JSON files (coverage matrix, OSM gap, city
    # extents, manifest) without re-exporting 70 MB of geometry. The geometry export takes
    # several minutes, and caveat or score text changes far more often than cell boundaries.
    metadata_only = "--metadata-only" in sys.argv[1:]

    if metadata_only:
        if not os.path.isdir(OUTPUT_DIR):
            logger.error("%s does not exist; run a full export first", OUTPUT_DIR)
            raise SystemExit(1)
        logger.info("Metadata-only refresh, leaving existing geometry in place")
        export_coverage_matrix()
        export_osm_gap()
        export_density_bins()
        export_city_metadata()
        logger.info("Done.")
        return

    if os.path.isdir(OUTPUT_DIR):
        logger.info("Clearing %s", OUTPUT_DIR)
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    matrix = export_coverage_matrix()
    export_osm_gap()
    export_density_bins()
    cities = export_city_metadata()

    scoreable = [
        (r["city"], r["shop_type"])
        for r in matrix
        if r["is_mappable"] in (True, "true", 1)
    ]
    logger.info("Exporting %d scoreable city and shop-type combinations", len(scoreable))

    total_bytes = 0
    manifest: dict[str, list[str]] = {}
    for city, shop_type in scoreable:
        meta = cities.get(city)
        if meta is None:
            logger.warning("No grid metadata for %s, skipping", city)
            continue
        total_bytes += export_density(city, shop_type, meta)
        total_bytes += export_points(city, shop_type)
        manifest.setdefault(city, []).append(shop_type)

    if manifest.get("paris"):
        total_bytes += export_paris_vacant(manifest["paris"])
    else:
        logger.warning("No scoreable Paris combinations, skipping the vacant-unit export")

    write_json("manifest.json", manifest)
    logger.info("Done. %.1f MB written to %s", total_bytes / 1e6, OUTPUT_DIR)


if __name__ == "__main__":
    sys.exit(main())
