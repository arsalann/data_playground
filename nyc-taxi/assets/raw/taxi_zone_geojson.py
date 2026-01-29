""" @bruin
name: raw.taxi_zone_geojson
type: python
image: python:3.11
connection: motherduck-prod

materialization:
  type: table
  strategy: create+replace
@bruin """

import io
import json
import os
import tempfile
from typing import Any, Dict, List

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import shape


def fetch_geojson(url: str) -> Dict[str, Any]:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.json()


def compute_centroids(features: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry")
        if not geom:
            continue
        g = shape(geom)
        centroid = g.centroid
        rows.append(
            {
                "location_id": int(props.get("LocationID") or props.get("OBJECTID") or 0),
                "borough": props.get("borough"),
                "zone": props.get("zone"),
                "service_zone": props.get("service_zone"),
                "centroid_lat": centroid.y,
                "centroid_lon": centroid.x,
            }
        )
    return pd.DataFrame(rows)


def materialize():
    url = os.environ.get(
        "TAXI_ZONE_GEOJSON_URL",
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip",
    )

    if url.endswith(".zip"):
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        import zipfile

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            candidate = [n for n in zf.namelist() if n.lower().endswith(".geojson")]
            if candidate:
                geo_bytes = zf.read(candidate[0])
                geo = json.loads(geo_bytes.decode("utf-8"))
            else:
                # Fallback: extract shapefile and convert via geopandas
                with tempfile.TemporaryDirectory() as tmpdir:
                    zf.extractall(tmpdir)
                    shp_files = [
                        os.path.join(tmpdir, n)
                        for n in os.listdir(tmpdir)
                        if n.lower().endswith(".shp")
                    ]
                    if not shp_files:
                        raise ValueError("No shapefile (.shp) found in zip")
                    gdf = gpd.read_file(shp_files[0])
                    # Reproject to WGS84 (EPSG:4326) for proper lat/lon
                    gdf = gdf.to_crs(epsg=4326)
                    geo = json.loads(gdf.to_json())
    else:
        geo = fetch_geojson(url)

    features = geo.get("features", [])
    df = compute_centroids(features)
    return df
