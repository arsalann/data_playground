"""Build the static DAC choropleth image for the regional affordability dashboard."""

from __future__ import annotations

import json
import logging
import math
import time
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
ASSET_DIR = ROOT / "dashboards" / "assets"
MAP_SVG_PATH = ASSET_DIR / "regional_affordability_map.svg"
MAP_PNG_PATH = ASSET_DIR / "regional_affordability_map.png"
CSV_PATH = ASSET_DIR / "regional_affordability_snapshot_2022.csv"

EUROPE_PREFIXES = {
    "AT", "BE", "BG", "CH", "CZ", "DE", "DK", "EE", "EL", "ES", "FI", "FR",
    "HR", "HU", "IE", "IS", "IT", "LT", "LV", "NL", "NO", "PL", "PT", "SI",
    "SK", "TR",
}
NORTH_AMERICA_PREFIXES = {"CA", "CR", "ME"}
COUNTRY_NAMES = {
    "BE": "Belgium",
    "CA": "Canada",
    "DE": "Germany",
    "LV": "Latvia",
    "ME": "Mexico",
    "NO": "Norway",
    "PT": "Portugal",
    "SI": "Slovenia",
    "SK": "Slovakia",
}

INCOME_DIMS = {
    "FREQ": ["A"],
    "TERRITORIAL_LEVEL": ["TL2"],
    "MEASURE": ["B5N", "B6N"],
    "ACTIVITY": ["_T"],
    "PRICES": ["V"],
    "UNIT_MEASURE": ["USD_PPP_PS"],
}
HOUSING_DIMS = {
    "FREQ": ["A"],
    "TERRITORIAL_LEVEL": ["TL2"],
    "MEASURE": ["HOUSE_COST"],
    "AGE": ["_Z"],
    "SEX": ["_Z"],
    "UNIT_MEASURE": ["PT_B6N_S14"],
}

CANADA_CODES = {
    "CA10": "CA-NL",
    "CA11": "CA-PE",
    "CA12": "CA-NS",
    "CA13": "CA-NB",
    "CA24": "CA-QC",
    "CA35": "CA-ON",
    "CA46": "CA-MB",
    "CA47": "CA-SK",
    "CA48": "CA-AB",
    "CA59": "CA-BC",
    "CA60": "CA-YT",
    "CA61": "CA-NT",
    "CA62": "CA-NU",
}
MEXICO_CODES = {
    "ME01": "MX-AGU",
    "ME02": "MX-BCN",
    "ME03": "MX-BCS",
    "ME04": "MX-CAM",
    "ME05": "MX-COA",
    "ME06": "MX-COL",
    "ME07": "MX-CHP",
    "ME08": "MX-CHH",
    "ME09": "MX-DIF",
    "ME10": "MX-DUR",
    "ME11": "MX-GUA",
    "ME12": "MX-GRO",
    "ME13": "MX-HID",
    "ME14": "MX-JAL",
    "ME15": "MX-MEX",
    "ME16": "MX-MIC",
    "ME17": "MX-MOR",
    "ME18": "MX-NAY",
    "ME19": "MX-NLE",
    "ME20": "MX-OAX",
    "ME21": "MX-PUE",
    "ME22": "MX-QUE",
    "ME23": "MX-ROO",
    "ME24": "MX-SLP",
    "ME25": "MX-SIN",
    "ME26": "MX-SON",
    "ME27": "MX-TAB",
    "ME28": "MX-TAM",
    "ME29": "MX-TLA",
    "ME30": "MX-VER",
    "ME31": "MX-YUC",
    "ME32": "MX-ZAC",
}


def country_prefix(region_code: str) -> str:
    if region_code.startswith("ME"):
        return "ME"
    if region_code.startswith("JP"):
        return "JP"
    return region_code[:2]


def fetch_series(dataset: str, dimensions: dict[str, list[str]]) -> tuple[list[dict], dict[str, str]]:
    docs: list[dict] = []
    labels: dict[str, str] = {}
    offset = 0
    total = None
    url = f"https://api.db.nomics.world/v22/series/OECD/{dataset}"

    while total is None or offset < total:
        params = {
            "dimensions": json.dumps(dimensions),
            "limit": 1000,
            "offset": offset,
            "observations": 1,
        }
        for attempt in range(5):
            try:
                response = requests.get(url, params=params, timeout=90)
                if response.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"retryable HTTP {response.status_code}")
                response.raise_for_status()
                payload = response.json()
                break
            except requests.RequestException as exc:
                wait = 2 ** attempt
                logger.warning("Retrying %s offset=%d in %ss: %s", dataset, offset, wait, exc)
                time.sleep(wait)
        else:
            raise RuntimeError(f"Failed to fetch {dataset} offset={offset}")

        series = payload["series"]
        total = series["num_found"]
        page = series["docs"]
        docs.extend(page)
        labels.update(payload["dataset"]["dimensions_values_labels"].get("REF_AREA", {}))
        offset += len(page)
        logger.info("Fetched %s series %d/%d", dataset, offset, total)

    return docs, labels


def build_snapshot() -> pd.DataFrame:
    income_docs, income_labels = fetch_series("DSD_REG_ECO%40DF_INC", INCOME_DIMS)
    housing_docs, housing_labels = fetch_series("DSD_REG_SOC%40DF_HOUSING", HOUSING_DIMS)
    labels = income_labels | housing_labels

    income_rows = []
    for item in income_docs:
        dims = item["dimensions"]
        region_code = dims["REF_AREA"]
        prefix = country_prefix(region_code)
        if prefix not in EUROPE_PREFIXES | NORTH_AMERICA_PREFIXES:
            continue
        for period, value in zip(item.get("period", []), item.get("value", [])):
            if value is None:
                continue
            income_rows.append(
                {
                    "region_code": region_code,
                    "year": int(period),
                    "region_name": labels.get(region_code, region_code),
                    "country_prefix": prefix,
                    "measure_code": dims["MEASURE"],
                    "value": float(value),
                }
            )

    housing_rows = []
    for item in housing_docs:
        dims = item["dimensions"]
        region_code = dims["REF_AREA"]
        prefix = country_prefix(region_code)
        if prefix not in EUROPE_PREFIXES | NORTH_AMERICA_PREFIXES:
            continue
        for period, value in zip(item.get("period", []), item.get("value", [])):
            if value is None:
                continue
            housing_rows.append(
                {
                    "region_code": region_code,
                    "year": int(period),
                    "housing_cost_pct_disposable_income": float(value),
                }
            )

    income = pd.DataFrame(income_rows)
    housing = pd.DataFrame(housing_rows)
    wide = (
        income.pivot_table(
            index=["region_code", "year", "region_name", "country_prefix"],
            columns="measure_code",
            values="value",
            aggfunc="last",
        )
        .reset_index()
        .rename(columns={"B5N": "primary_income_ppp_pc", "B6N": "disposable_income_ppp_pc"})
    )
    joined = wide.merge(housing, on=["region_code", "year"], how="inner")
    joined = joined.dropna(
        subset=[
            "primary_income_ppp_pc",
            "disposable_income_ppp_pc",
            "housing_cost_pct_disposable_income",
        ]
    )
    snapshot = joined[joined["year"].eq(2022)].copy()
    snapshot["disposable_after_housing_ppp_pc"] = snapshot["disposable_income_ppp_pc"] * (
        1 - snapshot["housing_cost_pct_disposable_income"] / 100
    )
    median_value = snapshot["disposable_after_housing_ppp_pc"].median()
    snapshot["affordability_score"] = snapshot["disposable_after_housing_ppp_pc"] / median_value * 100
    snapshot["country_name"] = snapshot["country_prefix"].map(COUNTRY_NAMES).fillna(snapshot["country_prefix"])
    snapshot["macro_region"] = snapshot["country_prefix"].map(
        lambda prefix: "North America" if prefix in NORTH_AMERICA_PREFIXES else "Europe"
    )

    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    snapshot.to_csv(CSV_PATH, index=False)
    logger.info("Wrote %d-region 2022 snapshot to %s", len(snapshot), CSV_PATH)
    return snapshot


def load_boundaries(snapshot: pd.DataFrame) -> gpd.GeoDataFrame:
    nuts_urls = [
        "https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/NUTS_RG_20M_2021_4326_LEVL_1.geojson",
        "https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/NUTS_RG_20M_2021_4326_LEVL_2.geojson",
    ]
    nuts_frames = [gpd.read_file(url)[["NUTS_ID", "geometry"]] for url in nuts_urls]
    nuts = pd.concat(nuts_frames, ignore_index=True).drop_duplicates("NUTS_ID")
    europe = nuts[nuts["NUTS_ID"].isin(snapshot["region_code"])].rename(columns={"NUTS_ID": "region_code"})

    ne = gpd.read_file("https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_1_states_provinces.zip")
    code_map = CANADA_CODES | MEXICO_CODES
    reverse_map = {v: k for k, v in code_map.items()}
    north_america = ne[ne["iso_3166_2"].isin(reverse_map)].copy()
    north_america["region_code"] = north_america["iso_3166_2"].map(reverse_map)
    north_america = north_america[["region_code", "geometry"]]

    boundaries = pd.concat([europe[["region_code", "geometry"]], north_america], ignore_index=True)
    mapped = boundaries.merge(snapshot, on="region_code", how="inner")
    missing = sorted(set(snapshot["region_code"]) - set(mapped["region_code"]))
    if missing:
        logger.warning("No boundary match for %d regions: %s", len(missing), ", ".join(missing))
    return gpd.GeoDataFrame(mapped, geometry="geometry", crs="EPSG:4326")


def load_context_boundaries() -> gpd.GeoDataFrame:
    """Load country outlines used to show excluded/no-data geography."""
    world = gpd.read_file("https://naturalearth.s3.amazonaws.com/50m_cultural/ne_50m_admin_0_countries.zip")
    return world.to_crs("EPSG:4326")[["geometry"]]


def draw_map(gdf: gpd.GeoDataFrame, context: gpd.GeoDataFrame) -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "axes.facecolor": "#111827",
            "figure.facecolor": "#111827",
            "savefig.facecolor": "#111827",
            "text.color": "#f9fafb",
            "axes.labelcolor": "#f9fafb",
            "xtick.color": "#d1d5db",
            "ytick.color": "#d1d5db",
        }
    )
    cmap = "viridis"
    vmin = math.floor(gdf["affordability_score"].min() / 10) * 10
    vmax = math.ceil(gdf["affordability_score"].max() / 10) * 10
    norm = Normalize(vmin=vmin, vmax=vmax)
    countries = (
        gdf[["country_name", "macro_region"]]
        .drop_duplicates()
        .sort_values(["macro_region", "country_name"], ascending=[False, True])
    )
    country_names = countries["country_name"].tolist()
    ncols = 3
    nrows = math.ceil(len(country_names) / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(15, 11.5), dpi=180)
    axes_flat = axes.flatten()

    for ax, country_name in zip(axes_flat, country_names):
        panel = gdf[gdf["country_name"].eq(country_name)].copy()
        minx, miny, maxx, maxy = panel.total_bounds
        width = maxx - minx
        height = maxy - miny
        pad_x = max(width * 0.10, 0.6)
        pad_y = max(height * 0.10, 0.6)
        xlim = (minx - pad_x, maxx + pad_x)
        ylim = (miny - pad_y, maxy + pad_y)
        context_panel = context.cx[xlim[0] : xlim[1], ylim[0] : ylim[1]]
        if not context_panel.empty:
            context_panel.plot(
                ax=ax,
                color="#253044",
                edgecolor="#4b5563",
                linewidth=0.35,
                alpha=0.95,
            )
        panel.plot(
            ax=ax,
            column="affordability_score",
            cmap=cmap,
            norm=norm,
            linewidth=0.35,
            edgecolor="#f3f4f6",
            missing_kwds={"color": "#374151"},
        )
        panel.boundary.plot(ax=ax, linewidth=0.25, color="#111827", alpha=0.8)
        median_score = panel["affordability_score"].median()
        ax.set_title(
            f"{country_name} ({len(panel)} regions, median {median_score:.0f})",
            fontsize=10.5,
            fontweight="bold",
            pad=8,
        )
        ax.set_xlim(*xlim)
        ax.set_ylim(*ylim)
        ax.set_aspect("equal", adjustable="box")
        ax.set_axis_off()

    for ax in axes_flat[len(country_names) :]:
        ax.set_axis_off()

    sm = ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=axes_flat.tolist(), orientation="horizontal", fraction=0.04, pad=0.025)
    cbar.set_label("Affordability score, median comparable TL2 region = 100", fontsize=11)
    cbar.ax.tick_params(labelsize=9)
    cbar.outline.set_edgecolor("#9ca3af")

    fig.suptitle(
        "OECD TL2 after-housing disposable income score by country, 2022",
        fontsize=16,
        fontweight="bold",
        y=0.975,
    )
    fig.text(
        0.5,
        0.03,
        "Panels are separate country maps. Color scale is shared across all countries; gray geography is neighboring context, not comparable data.",
        ha="center",
        fontsize=10,
        color="#d1d5db",
    )
    fig.savefig(MAP_SVG_PATH, bbox_inches="tight", pad_inches=0.2)
    fig.savefig(MAP_PNG_PATH, bbox_inches="tight", pad_inches=0.2)
    logger.info("Wrote map to %s and %s", MAP_SVG_PATH, MAP_PNG_PATH)


def main() -> None:
    snapshot = build_snapshot()
    gdf = load_boundaries(snapshot)
    context = load_context_boundaries()
    draw_map(gdf, context)


if __name__ == "__main__":
    main()
