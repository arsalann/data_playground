"""@bruin
name: raw.ghsl_population_100m
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Ingests resident population at 100 m resolution for the five study cities from the JRC
  Global Human Settlement Layer, GHS-POP R2023A, epoch 2025.

  Deliberate deviation from PLAN.md, stated here because it changes a documented limitation.
  The plan called for five national small-area sources: Madrid padron by census section,
  INSEE Filosofi 200 m, INEGI Censo 2020 AGEB, ONS LSOA mid-year estimates and ACS 5-year
  tract estimates. It also flagged, as a known limitation, that those five carry vintages up
  to six years apart, making cross-city per-capita figures unreliable. Using one global
  product removes that limitation instead of documenting it: every city gets the same
  estimator, the same 2025 epoch, the same 100 m resolution and the same projection, which
  is what the AGENTS.md consistent-spatial-methodology rule requires. The cost is that GHSL
  is a modelled disaggregation of census counts onto built-up area, not a direct census
  count, so it is more accurate about where people are than about exactly how many.

  Tiles are 1000 km squares in Mollweide (ESRI:54009). The tile grid origin was confirmed
  against the published raster rather than assumed: tile R4_C19 has bounds
  (-41000, 5000000, 959000, 6000000), which matches
  x_left = -18041000 + (col - 1) * 1e6 and y_top = 9000000 - (row - 1) * 1e6.

  Tiles are processed one at a time and deleted after use, because each unpacks to an
  800 MB float64 raster and a naive download-everything-first approach needs several GB.

  Source: https://human-settlement.emergency.copernicus.eu/download.php?ds=pop
  Licence: CC BY 4.0 (European Commission, Joint Research Centre)

materialization:
  type: table
  strategy: append

columns:
  - name: city
    type: VARCHAR
    description: City slug, one of madrid, paris, mexico_city, london, chicago.
    primary_key: true
  - name: moll_x
    type: INTEGER
    description: Mollweide easting in metres of the 100 m cell centre, ESRI:54009. Integral because the grid is aligned to 100 m.
    primary_key: true
  - name: moll_y
    type: INTEGER
    description: Mollweide northing in metres of the 100 m cell centre, ESRI:54009.
    primary_key: true
  - name: lon
    type: DOUBLE
    description: Longitude in decimal degrees, WGS84, of the 100 m cell centre.
  - name: lat
    type: DOUBLE
    description: Latitude in decimal degrees, WGS84, of the 100 m cell centre.
  - name: population
    type: DOUBLE
    description: Estimated residents in the 100 m cell in epoch 2025. Fractional because GHS-POP disaggregates census counts onto built-up area.
  - name: source_tile
    type: VARCHAR
    description: GHSL tile identifier the cell was read from, for example R4_C19.
  - name: extracted_at
    type: TIMESTAMP
    description: UTC timestamp when this row was extracted.

@bruin"""

import io
import logging
import os
import tempfile
import zipfile
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import rasterio
import requests
from pyproj import Transformer
from rasterio.windows import Window, from_bounds

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

RELEASE = "R2023A"
EPOCH = "E2025"
TILE_URL_TEMPLATE = (
    "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_{release}/"
    "GHS_POP_{epoch}_GLOBE_{release}_54009_100/V1-0/tiles/"
    "GHS_POP_{epoch}_GLOBE_{release}_54009_100_V1_0_{tile}.zip"
)
REQUEST_TIMEOUT = 1800

# Mollweide tile grid, confirmed against the published R4_C19 raster.
TILE_ORIGIN_X = -18041000.0
TILE_ORIGIN_Y = 9000000.0
TILE_SIZE_M = 1000000.0

MOLLWEIDE_CRS = "+proj=moll +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"

# Study-city bounding boxes as (min_lon, min_lat, max_lon, max_lat). These are generous
# municipal envelopes: population outside the administrative area is harmless because the
# analysis grid is built from establishment locations, which are inside the city.
CITIES: dict[str, tuple[float, float, float, float]] = {
    "madrid": (-3.889, 40.312, -3.518, 40.644),
    "paris": (2.224, 48.815, 2.470, 48.902),
    "mexico_city": (-99.365, 19.048, -98.940, 19.593),
    "london": (-0.5104, 51.2868, 0.3340, 51.6919),
    "chicago": (-87.9401, 41.6445, -87.5241, 42.0230),
}

# Densify the bbox edges before projecting: Mollweide meridians curve, so projecting only
# the four corners understates the true projected envelope.
EDGE_SAMPLES = 25


def project_bbox(
    bbox: tuple[float, float, float, float], transformer: Transformer
) -> tuple[float, float, float, float]:
    min_lon, min_lat, max_lon, max_lat = bbox
    lons = np.linspace(min_lon, max_lon, EDGE_SAMPLES)
    lats = np.linspace(min_lat, max_lat, EDGE_SAMPLES)
    grid_lon, grid_lat = np.meshgrid(lons, lats)
    x, y = transformer.transform(grid_lon.ravel(), grid_lat.ravel())
    return float(np.min(x)), float(np.min(y)), float(np.max(x)), float(np.max(y))


def tiles_for_bounds(bounds: tuple[float, float, float, float]) -> list[str]:
    min_x, min_y, max_x, max_y = bounds
    col_start = int((min_x - TILE_ORIGIN_X) // TILE_SIZE_M) + 1
    col_end = int((max_x - TILE_ORIGIN_X) // TILE_SIZE_M) + 1
    row_start = int((TILE_ORIGIN_Y - max_y) // TILE_SIZE_M) + 1
    row_end = int((TILE_ORIGIN_Y - min_y) // TILE_SIZE_M) + 1
    return [
        f"R{row}_C{col}"
        for row in range(row_start, row_end + 1)
        for col in range(col_start, col_end + 1)
    ]


def download_tile(tile: str, destination: str) -> str:
    url = TILE_URL_TEMPLATE.format(release=RELEASE, epoch=EPOCH, tile=tile)
    logger.info("Downloading tile %s", tile)
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    logger.info("Tile %s: downloaded %.1f MB", tile, len(response.content) / 1e6)

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        members = [n for n in archive.namelist() if n.lower().endswith(".tif")]
        if not members:
            raise ValueError(f"Tile {tile} contains no GeoTIFF; members {archive.namelist()}")
        archive.extract(members[0], destination)
    path = os.path.join(destination, members[0])
    logger.info("Tile %s: unpacked to %.0f MB", tile, os.path.getsize(path) / 1e6)
    return path


def read_city_from_tile(
    path: str,
    city: str,
    bbox: tuple[float, float, float, float],
    projected: tuple[float, float, float, float],
    tile: str,
) -> pd.DataFrame:
    with rasterio.open(path) as src:
        raster_bounds = src.bounds
        min_x = max(projected[0], raster_bounds.left)
        min_y = max(projected[1], raster_bounds.bottom)
        max_x = min(projected[2], raster_bounds.right)
        max_y = min(projected[3], raster_bounds.top)
        if min_x >= max_x or min_y >= max_y:
            return pd.DataFrame()

        window = from_bounds(min_x, min_y, max_x, max_y, transform=src.transform)
        window = Window(
            col_off=int(np.floor(window.col_off)),
            row_off=int(np.floor(window.row_off)),
            width=int(np.ceil(window.width)),
            height=int(np.ceil(window.height)),
        ).intersection(Window(0, 0, src.width, src.height))

        values = src.read(1, window=window)
        nodata = src.nodata
        rows, cols = np.nonzero(
            (values > 0) & (values != nodata if nodata is not None else True)
        )
        if rows.size == 0:
            return pd.DataFrame()

        population = values[rows, cols].astype(float)
        window_transform = src.window_transform(window)
        # Cell centres in Mollweide.
        moll_x = window_transform.c + (cols + 0.5) * window_transform.a
        moll_y = window_transform.f + (rows + 0.5) * window_transform.e

        to_wgs84 = Transformer.from_crs(src.crs, "EPSG:4326", always_xy=True)

    lon, lat = to_wgs84.transform(moll_x, moll_y)

    min_lon, min_lat, max_lon, max_lat = bbox
    keep = (
        (lon >= min_lon) & (lon <= max_lon) & (lat >= min_lat) & (lat <= max_lat)
    )
    if not keep.any():
        return pd.DataFrame()

    return pd.DataFrame(
        {
            "city": city,
            "moll_x": np.rint(moll_x[keep]).astype("int64"),
            "moll_y": np.rint(moll_y[keep]).astype("int64"),
            "lon": lon[keep],
            "lat": lat[keep],
            "population": population[keep],
            "source_tile": tile,
        }
    )


def materialize() -> pd.DataFrame:
    to_mollweide = Transformer.from_crs("EPSG:4326", MOLLWEIDE_CRS, always_xy=True)

    projected: dict[str, tuple[float, float, float, float]] = {}
    tile_to_cities: dict[str, list[str]] = {}
    for city, bbox in CITIES.items():
        bounds = project_bbox(bbox, to_mollweide)
        projected[city] = bounds
        tiles = tiles_for_bounds(bounds)
        logger.info("%s: projected bounds %s needs tiles %s", city, tuple(round(b) for b in bounds), tiles)
        for tile in tiles:
            tile_to_cities.setdefault(tile, []).append(city)

    logger.info("Distinct tiles required: %d (%s)", len(tile_to_cities), sorted(tile_to_cities))

    frames: list[pd.DataFrame] = []
    with tempfile.TemporaryDirectory() as workdir:
        for tile, cities in sorted(tile_to_cities.items()):
            path = download_tile(tile, workdir)
            try:
                for city in cities:
                    frame = read_city_from_tile(
                        path, city, CITIES[city], projected[city], tile
                    )
                    if frame.empty:
                        logger.info("%s: no populated cells in tile %s", city, tile)
                        continue
                    logger.info(
                        "%s: %d populated cells in tile %s, %.0f residents",
                        city,
                        len(frame),
                        tile,
                        frame["population"].sum(),
                    )
                    frames.append(frame)
            finally:
                os.remove(path)
                logger.info("Tile %s removed from disk", tile)

    df = pd.concat(frames, ignore_index=True)

    # A city spanning two tiles must not double-count a cell on the seam.
    before = len(df)
    df = df.drop_duplicates(subset=["city", "moll_x", "moll_y"], keep="first")
    if len(df) != before:
        logger.info("Dropped %d duplicate cells on tile seams", before - len(df))

    summary = df.groupby("city")["population"].agg(["size", "sum"])
    for city, row in summary.iterrows():
        logger.info(
            "%s: %d populated 100 m cells, %.0f residents in the bounding box",
            city,
            int(row["size"]),
            row["sum"],
        )

    df["extracted_at"] = datetime.now(timezone.utc)

    ordered = [
        "city",
        "moll_x",
        "moll_y",
        "lon",
        "lat",
        "population",
        "source_tile",
        "extracted_at",
    ]
    df = df[ordered]

    logger.info("Returning %d rows", len(df))
    return df
