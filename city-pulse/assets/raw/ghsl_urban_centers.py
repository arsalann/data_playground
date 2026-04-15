"""@bruin

name: raw.ghsl_urban_centers
description: |
  Foundation dataset for city-pulse urbanization analysis containing comprehensive urban center
  attributes for ~11,400 cities globally from the GHSL Urban Centre Database (GHS-UCDB R2024A).

  This European Commission JRC dataset provides the spatial and temporal foundation for analyzing
  global urbanization patterns, combining demographic, climatic, geographic, and socioeconomic
  dimensions across multiple epochs. The data spans cities from mega-centers like Tokyo
  (39M population) to emerging urban areas (14K minimum population threshold).

  **Data Integration**: Merges 5 thematic GeoPackage layers via GHSL urban center ID:
    - GENERAL_CHARACTERISTICS: city names, country codes, urban area boundaries
    - GHSL: multi-epoch population estimates (1975-2015) and building morphology
    - CLIMATE: BioClim annual temperature and precipitation (2010 baseline)
    - GEOGRAPHY: mean elevation and terrain characteristics
    - SOCIOECONOMIC: GDP PPP estimates and Human Development Index (2020)

  **Global Coverage**: 190 countries with urban centers meeting GHSL's population and
  density thresholds. Country distribution ranges from single-city nations to major
  urbanized regions (India: 663 centers, China: 653 centers, USA: 438 centers).

  **Data Lineage**: Primary data source for staging.city_profiles, which enriches this
  dataset with population growth rates, climate zones, and street network analysis.

  **Known Limitations**:
  - ~14 cities lack city names (rural/unnamed centers)
  - 155 cities missing HDI data (typically small or data-sparse regions)
  - Population estimates are modeled, not census-based
  - Climate data represents 2010 baseline, not current conditions

  Source: European Commission JRC Global Human Settlement Layer
  URL: https://ghsl.jrc.ec.europa.eu/
  License: Creative Commons Attribution 4.0 (CC BY 4.0)
connection: bruin-playground-arsalan
tags:
  - domain:urban_analytics
  - domain:geography
  - data_type:reference_data
  - data_type:external_source
  - sensitivity:public
  - pipeline_role:raw_foundation
  - update_pattern:snapshot
  - source:european_commission
  - coverage:global
  - temporal_span:multi_epoch

materialization:
  type: table
  strategy: create+replace
image: python:3.11

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: ghsl_id
    type: INTEGER
    description: |
      GHSL unique identifier for the urban center. Sequential integer assigned by JRC,
      not geographically ordered. Used as primary key for joining with street networks
      and other urban analysis layers.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: city_name
    type: VARCHAR
    description: |
      Primary city name as identified by GHSL spatial analysis. Names vary in language
      and transliteration (mix of local/English). 11,115 unique names across 11,422
      centers indicates some name reuse across countries. ~14 centers lack names
      (typically rural/unnamed settlements above population threshold).
  - name: country_code
    type: VARCHAR
    description: |
      ISO 3166-1 alpha-3 country code derived from GADM country names using pycountry.
      Covers 190 countries with fallback mapping for ambiguous names (e.g., "Turkey" → TUR,
      "Congo" disambiguation). Essential for country-level aggregation and World Bank joins.
    checks:
      - name: not_null
  - name: country_name
    type: VARCHAR
    description: |
      Country name from GHSL (sourced from GADM administrative boundaries).
      191 unique values include sovereign nations and some autonomous regions.
      Primary source for ISO code derivation with manual overrides for difficult cases.
    checks:
      - name: not_null
  - name: latitude
    type: DOUBLE
    description: |
      Urban center centroid latitude in WGS84 decimal degrees. Global coverage spans
      Arctic (69.7°N) to sub-Antarctic (54.8°S). Derived from GHSL urban area geometry
      after reprojection from Mollweide to geographic coordinates.
    checks:
      - name: not_null
  - name: longitude
    type: DOUBLE
    description: |
      Urban center centroid longitude in WGS84 decimal degrees. Global coverage includes
      Pacific islands (178.5°E to -175.2°W). Used for spatial analysis and climate zone
      classification in downstream processing.
    checks:
      - name: not_null
  - name: population_2015
    type: DOUBLE
    description: |
      Estimated population in 2015 from GHS-POP grid data. Modeled estimates, not census.
      Range: 13.9K to 39.1M (Tokyo metro area). Median: ~117K. Shows global urban
      hierarchy with power-law distribution typical of city systems.
  - name: population_2000
    type: DOUBLE
    description: |
      Estimated population in 2000 from GHS-POP grid data. Historical baseline for
      growth rate calculations in staging layer. Range: 137 to 30.6M. Smaller minimum
      reflects urban centers that grew above threshold by 2015.
  - name: population_1990
    type: DOUBLE
    description: |
      Estimated population in 1990 from GHS-POP grid data. Used for long-term growth
      analysis. Range: 4.2 to 29.2M. Lower bounds reflect nascent urban development
      in currently large centers.
  - name: population_1975
    type: DOUBLE
    description: |
      Estimated population in 1975 from GHS-POP grid data. Earliest epoch for urban
      growth trend analysis. Range: 0.32 to 24.7M. Near-zero values indicate rapid
      urbanization in previously rural areas.
  - name: area_km2
    type: INTEGER
    description: |
      Urban center built-up area in square kilometers from GHSL spatial boundaries.
      Mean: 57 km², high variation (σ=185) due to polycentric vs compact urban forms.
      Critical for density calculations and urban morphology analysis.
  - name: gdp_ppp
    type: DOUBLE
    description: |
      Gross Domestic Product in purchasing power parity terms (2020, USD) estimated
      for the urban center. Mean: $44.2M, max: $1.13B (major metropolitan areas).
      Used for economic development correlation analysis with urbanization patterns.
  - name: avg_building_height_m
    type: DOUBLE
    description: |
      Average building height in meters from GHS-BUILT-H 2020 analysis. Range: 2.5-18.5m,
      mean: 6.4m. Proxy for urban density and development intensity. Values >15m indicate
      significant high-rise development (major urban centers).
  - name: hdi
    type: DOUBLE
    description: |
      Human Development Index (2020) ranging from 0-1 scale. Mean: 0.69, indicating
      medium development. 155 missing values (1.4%) for small or data-sparse regions.
      Key variable for development-urbanization correlation analysis.
  - name: avg_temp_c
    type: DOUBLE
    description: |
      Annual mean temperature in degrees Celsius from WorldClim BioClim data (2010 baseline).
      Range: -7.7°C to 30.9°C covering Arctic to tropical urban centers. Used for
      climate zone classification and climate-urbanization pattern analysis.
  - name: precipitation_mm
    type: DOUBLE
    description: |
      Annual precipitation in millimeters from WorldClim BioClim data (2010 baseline).
      Range: 0.7mm (desert cities) to 23,005mm (tropical monsoon regions). High variance
      (σ=908mm) reflects global climate diversity in urban settlement patterns.
  - name: elevation_m
    type: DOUBLE
    description: |
      Mean elevation above sea level in meters. Range: -241m (below sea level) to 4,518m
      (high-altitude cities like La Paz). Mean: 356m. Used for topographic analysis
      of urban development constraints and climate interaction effects.
  - name: extracted_at
    type: TIMESTAMP
    description: |
      UTC timestamp when this data was fetched from GHSL servers. All rows share the
      same extraction timestamp as this is reference data downloaded in single batch.
      Used for data lineage tracking and refresh management.
    checks:
      - name: not_null

@bruin"""

import glob
import logging
import os
import tempfile
import time
import zipfile
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
import pycountry
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Known download URLs for GHS-UCDB R2024A (try in order)
GHSL_URLS = [
    "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_UCDB_GLOBE_R2024A/GHS_UCDB_GLOBE_R2024A/V1-0/GHS_UCDB_GLOBE_R2024A_V1_0.zip",
    "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_UCDB_GLOBE_R2024A/GHS_UCDB_GLOBE_R2024A/V1-0/GHS_UCDB_GLOBE_R2024A_V1_0.zip",
]

# R2024A layer name prefix/suffix
_LP = "GHS_UCDB_THEME_"
_LS = "_GLOBE_R2024A"

# Columns to read from each R2024A thematic layer (beyond the shared prefix columns)
LAYER_COLUMNS = {
    f"{_LP}GENERAL_CHARACTERISTICS{_LS}": {
        "read": ["ID_UC_G0", "GC_UCN_MAI_2025", "GC_CNT_GAD_2025", "GC_UCA_KM2_2025"],
        "geometry": True,
    },
    f"{_LP}GHSL{_LS}": {
        "read": ["ID_UC_G0", "GH_POP_TOT_1975", "GH_POP_TOT_1990", "GH_POP_TOT_2000",
                 "GH_POP_TOT_2015", "GH_BUH_AVG_2020"],
        "geometry": False,
    },
    f"{_LP}CLIMATE{_LS}": {
        "read": ["ID_UC_G0", "CL_B01_CUR_2010", "CL_B12_CUR_2010"],
        "geometry": False,
    },
    f"{_LP}GEOGRAPHY{_LS}": {
        "read": ["ID_UC_G0", "GE_ELV_AVG_2025"],
        "geometry": False,
    },
    f"{_LP}SOCIOECONOMIC{_LS}": {
        "read": ["ID_UC_G0", "SC_SEC_GDP_2020", "SC_SEC_HDI_2020"],
        "geometry": False,
    },
}

# R2024A column → output column name mapping
COLUMN_MAP = {
    "ID_UC_G0": "ghsl_id",
    "GC_UCN_MAI_2025": "city_name",
    "GC_CNT_GAD_2025": "country_name",
    "GC_UCA_KM2_2025": "area_km2",
    "GH_POP_TOT_1975": "population_1975",
    "GH_POP_TOT_1990": "population_1990",
    "GH_POP_TOT_2000": "population_2000",
    "GH_POP_TOT_2015": "population_2015",
    "GH_BUH_AVG_2020": "avg_building_height_m",
    "SC_SEC_GDP_2020": "gdp_ppp",
    "SC_SEC_HDI_2020": "hdi",
    "CL_B01_CUR_2010": "avg_temp_c",
    "CL_B12_CUR_2010": "precipitation_mm",
    "GE_ELV_AVG_2025": "elevation_m",
}

# Fallback mapping for country names pycountry can't resolve
COUNTRY_NAME_OVERRIDES = {
    "Democratic Republic of the Congo": "COD",
    "Northern Cyprus": "CYP",
    "Swaziland": "SWZ",
    "Turkey": "TUR",
    "Ivory Coast": "CIV",
    "East Timor": "TLS",
    "Republic of the Congo": "COG",
    "Myanmar (Burma)": "MMR",
    "Cape Verde": "CPV",
    "Palestine": "PSE",
    "Kosovo": "XKX",
    "Micronesia": "FSM",
}

def country_name_to_iso3(name: str) -> str:
    """Convert a country name to ISO 3166-1 alpha-3 code."""
    if not name or pd.isna(name):
        return "UNK"

    name = str(name).strip()

    if name in COUNTRY_NAME_OVERRIDES:
        return COUNTRY_NAME_OVERRIDES[name]

    try:
        return pycountry.countries.lookup(name).alpha_3
    except LookupError:
        pass

    try:
        results = pycountry.countries.search_fuzzy(name)
        if results:
            return results[0].alpha_3
    except LookupError:
        pass

    logger.warning("Could not map country name to ISO code: '%s'", name)
    return "UNK"

def download_ghsl(dest_dir: str) -> str:
    """Download GHSL ZIP and extract. Returns path to extracted GeoPackage."""
    for url in GHSL_URLS:
        logger.info("Trying GHSL download from: %s", url[:100])
        for attempt in range(3):
            try:
                resp = requests.get(url, timeout=600, stream=True)
                if resp.status_code == 200:
                    zip_path = os.path.join(dest_dir, "ghsl.zip")
                    size = 0
                    with open(zip_path, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=65536):
                            f.write(chunk)
                            size += len(chunk)
                    logger.info("Downloaded %.1f MB", size / 1_000_000)

                    with zipfile.ZipFile(zip_path) as zf:
                        zf.extractall(dest_dir)
                    os.unlink(zip_path)

                    gpkg_files = glob.glob(os.path.join(dest_dir, "**/*.gpkg"), recursive=True)
                    if gpkg_files:
                        logger.info("Found GeoPackage: %s", os.path.basename(gpkg_files[0]))
                        return gpkg_files[0]

                    raise RuntimeError("No .gpkg file found in GHSL ZIP archive")

                logger.warning("HTTP %d from %s", resp.status_code, url[:80])
            except requests.RequestException as e:
                wait = 15 * (attempt + 1)
                logger.warning("Download error (attempt %d): %s, retrying in %ds", attempt + 1, e, wait)
                time.sleep(wait)

    raise RuntimeError(
        "Failed to download GHSL Urban Centre Database from any known URL. "
        "Check https://human-settlement.emergency.copernicus.eu/download.php for updated links."
    )

def read_layers(gpkg_path: str) -> pd.DataFrame:
    """Read and merge multiple thematic layers from the R2024A GeoPackage."""
    merged = None

    for layer_name, config in LAYER_COLUMNS.items():
        cols_to_read = config["read"]
        logger.info("Reading layer %s (%d columns)...", layer_name.split("_THEME_")[1].split("_GLOBE")[0], len(cols_to_read))

        if config["geometry"]:
            gdf = gpd.read_file(gpkg_path, layer=layer_name, columns=cols_to_read)

            # Convert from Mollweide to WGS84 and extract centroids
            if gdf.crs and gdf.crs.to_epsg() != 4326:
                gdf = gdf.to_crs(epsg=4326)
            centroids = gdf.geometry.centroid
            gdf["latitude"] = centroids.y
            gdf["longitude"] = centroids.x
            gdf = pd.DataFrame(gdf.drop(columns=["geometry"]))
        else:
            gdf = gpd.read_file(gpkg_path, layer=layer_name, columns=cols_to_read, ignore_geometry=True)
            gdf = pd.DataFrame(gdf)

        if merged is None:
            merged = gdf
        else:
            new_cols = [c for c in gdf.columns if c not in merged.columns]
            merged = merged.merge(gdf[["ID_UC_G0"] + new_cols], on="ID_UC_G0", how="left")

    logger.info("Merged %d urban centers from %d layers", len(merged), len(LAYER_COLUMNS))
    return merged

def process_ghsl(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns, map country codes, clean data."""
    # Rename R2024A columns to our schema
    rename_cols = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename_cols)

    # Map country names to ISO alpha-3 codes
    if "country_name" in df.columns:
        logger.info("Mapping %d unique country names to ISO codes...", df["country_name"].nunique())
        name_to_iso = {}
        for name in df["country_name"].dropna().unique():
            name_to_iso[name] = country_name_to_iso3(name)
        df["country_code"] = df["country_name"].map(name_to_iso).fillna("UNK")
        unmapped = (df["country_code"] == "UNK").sum()
        if unmapped:
            logger.warning("%d urban centers with unmapped country codes", unmapped)

    # Convert ghsl_id to int
    if "ghsl_id" in df.columns:
        df["ghsl_id"] = pd.to_numeric(df["ghsl_id"], errors="coerce").astype("Int64")

    # BioClim temperature is stored as degrees C * 10 in some GHSL versions
    if "avg_temp_c" in df.columns and df["avg_temp_c"].median() > 100:
        df["avg_temp_c"] = df["avg_temp_c"] / 10.0
        logger.info("Converted temperature from deci-degrees to degrees C")

    # Select output columns in order
    output_cols = [
        "ghsl_id", "city_name", "country_code", "country_name",
        "latitude", "longitude",
        "population_2015", "population_2000", "population_1990", "population_1975",
        "area_km2", "gdp_ppp", "avg_building_height_m", "hdi",
        "avg_temp_c", "precipitation_mm", "elevation_m",
    ]
    available = [c for c in output_cols if c in df.columns]
    df = df[available]

    logger.info("Processed %d urban centers with %d columns", len(df), len(df.columns))
    return df

def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2000-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2024-12-31")
    logger.info("Interval: %s to %s (unused for reference data)", start_date, end_date)

    with tempfile.TemporaryDirectory() as tmpdir:
        gpkg_path = download_ghsl(tmpdir)
        raw_df = read_layers(gpkg_path)

    df = process_ghsl(raw_df)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Total: %d urban centers", len(df))
    return df
