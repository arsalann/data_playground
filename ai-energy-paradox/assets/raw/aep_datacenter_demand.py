"""@bruin
name: raw.aep_datacenter_demand
type: python
image: python:3.11
connection: bruin-playground-arsalan
description: |
  Seed table of published IEA, Goldman Sachs, EPRI, and hyperscaler
  data center and AI energy demand projections. Structured from report
  figures — not from a live API.

  Sources:
  - IEA "Energy and AI" report, Jan 2025
  - Goldman Sachs "AI, data centers, and the coming US power demand surge", Apr 2024
  - EPRI "Powering Intelligence", May 2024
  - Google, Microsoft, Meta sustainability reports (2024)

materialization:
  type: table
  strategy: create+replace

columns:
  - name: year
    type: INTEGER
    description: Year of the estimate or projection
    primary_key: true
  - name: category
    type: VARCHAR
    description: "Category: global_dc, ai_servers, national_share, hyperscaler"
    primary_key: true
  - name: metric_name
    type: VARCHAR
    description: "Metric: electricity_demand_twh, share_of_electricity_pct, power_demand_growth_pct"
    primary_key: true
  - name: region
    type: VARCHAR
    description: "Geographic scope: World, US, Ireland, Singapore, or company name"
  - name: value
    type: DOUBLE
    description: Numeric value of the metric
  - name: unit
    type: VARCHAR
    description: "Unit of measurement: TWh, %, GW"
  - name: is_projection
    type: BOOLEAN
    description: True if this is a forward projection, false if historical/actual
  - name: source
    type: VARCHAR
    description: Publication name and date for traceability
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when this seed data was generated

@bruin"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# fmt: off
SEED_DATA = [
    # ── Global data center total electricity demand (IEA) ──
    # Even years are published IEA figures; odd years are linear interpolations
    {"year": 2022, "category": "global_dc", "metric_name": "electricity_demand_twh", "region": "World", "value": 340, "unit": "TWh", "is_projection": False, "source": "IEA Energy and AI, Jan 2025"},
    {"year": 2023, "category": "global_dc", "metric_name": "electricity_demand_twh", "region": "World", "value": 378, "unit": "TWh", "is_projection": False, "source": "IEA Energy and AI, Jan 2025 (interpolated)"},
    {"year": 2024, "category": "global_dc", "metric_name": "electricity_demand_twh", "region": "World", "value": 415, "unit": "TWh", "is_projection": False, "source": "IEA Energy and AI, Jan 2025"},
    {"year": 2025, "category": "global_dc", "metric_name": "electricity_demand_twh", "region": "World", "value": 498, "unit": "TWh", "is_projection": True, "source": "IEA Energy and AI, Jan 2025 (interpolated)"},
    {"year": 2026, "category": "global_dc", "metric_name": "electricity_demand_twh", "region": "World", "value": 580, "unit": "TWh", "is_projection": True, "source": "IEA Energy and AI, Jan 2025"},
    {"year": 2027, "category": "global_dc", "metric_name": "electricity_demand_twh", "region": "World", "value": 670, "unit": "TWh", "is_projection": True, "source": "IEA Energy and AI, Jan 2025 (interpolated)"},
    {"year": 2028, "category": "global_dc", "metric_name": "electricity_demand_twh", "region": "World", "value": 760, "unit": "TWh", "is_projection": True, "source": "IEA Energy and AI, Jan 2025"},
    {"year": 2029, "category": "global_dc", "metric_name": "electricity_demand_twh", "region": "World", "value": 853, "unit": "TWh", "is_projection": True, "source": "IEA Energy and AI, Jan 2025 (interpolated)"},
    {"year": 2030, "category": "global_dc", "metric_name": "electricity_demand_twh", "region": "World", "value": 945, "unit": "TWh", "is_projection": True, "source": "IEA Energy and AI, Jan 2025"},

    # ── AI-optimized servers specifically (IEA) ──
    # Even years are published IEA figures; odd years are linear interpolations
    {"year": 2022, "category": "ai_servers", "metric_name": "electricity_demand_twh", "region": "World", "value": 50, "unit": "TWh", "is_projection": False, "source": "IEA Energy and AI, Jan 2025"},
    {"year": 2023, "category": "ai_servers", "metric_name": "electricity_demand_twh", "region": "World", "value": 72, "unit": "TWh", "is_projection": False, "source": "IEA Energy and AI, Jan 2025 (interpolated)"},
    {"year": 2024, "category": "ai_servers", "metric_name": "electricity_demand_twh", "region": "World", "value": 93, "unit": "TWh", "is_projection": False, "source": "IEA Energy and AI, Jan 2025"},
    {"year": 2025, "category": "ai_servers", "metric_name": "electricity_demand_twh", "region": "World", "value": 139, "unit": "TWh", "is_projection": True, "source": "IEA Energy and AI, Jan 2025 (interpolated)"},
    {"year": 2026, "category": "ai_servers", "metric_name": "electricity_demand_twh", "region": "World", "value": 185, "unit": "TWh", "is_projection": True, "source": "IEA Energy and AI, Jan 2025"},
    {"year": 2027, "category": "ai_servers", "metric_name": "electricity_demand_twh", "region": "World", "value": 248, "unit": "TWh", "is_projection": True, "source": "IEA Energy and AI, Jan 2025 (interpolated)"},
    {"year": 2028, "category": "ai_servers", "metric_name": "electricity_demand_twh", "region": "World", "value": 310, "unit": "TWh", "is_projection": True, "source": "IEA Energy and AI, Jan 2025"},
    {"year": 2029, "category": "ai_servers", "metric_name": "electricity_demand_twh", "region": "World", "value": 371, "unit": "TWh", "is_projection": True, "source": "IEA Energy and AI, Jan 2025 (interpolated)"},
    {"year": 2030, "category": "ai_servers", "metric_name": "electricity_demand_twh", "region": "World", "value": 432, "unit": "TWh", "is_projection": True, "source": "IEA Energy and AI, Jan 2025"},

    # ── Data centers as share of global electricity (IEA) ──
    {"year": 2024, "category": "global_dc", "metric_name": "share_of_electricity_pct", "region": "World", "value": 1.5, "unit": "%", "is_projection": False, "source": "IEA Energy and AI, Jan 2025"},
    {"year": 2030, "category": "global_dc", "metric_name": "share_of_electricity_pct", "region": "World", "value": 3.4, "unit": "%", "is_projection": True, "source": "IEA Energy and AI, Jan 2025"},

    # ── National-level data center electricity shares ──
    {"year": 2024, "category": "national_share", "metric_name": "dc_share_of_electricity_pct", "region": "Ireland", "value": 21.0, "unit": "%", "is_projection": False, "source": "IEA Energy and AI, Jan 2025"},
    {"year": 2024, "category": "national_share", "metric_name": "dc_share_of_electricity_pct", "region": "Singapore", "value": 7.0, "unit": "%", "is_projection": False, "source": "IEA Energy and AI, Jan 2025"},
    {"year": 2024, "category": "national_share", "metric_name": "dc_share_of_electricity_pct", "region": "Netherlands", "value": 4.6, "unit": "%", "is_projection": False, "source": "CBS Statistics Netherlands, Dec 2025"},
    {"year": 2024, "category": "national_share", "metric_name": "dc_share_of_electricity_pct", "region": "US", "value": 4.0, "unit": "%", "is_projection": False, "source": "IEA Energy and AI, Jan 2025"},
    {"year": 2024, "category": "national_share", "metric_name": "dc_share_of_electricity_pct", "region": "Germany", "value": 4.0, "unit": "%", "is_projection": False, "source": "Borderstep Institute, 2025"},
    {"year": 2024, "category": "national_share", "metric_name": "dc_share_of_electricity_pct", "region": "UK", "value": 2.6, "unit": "%", "is_projection": False, "source": "Oxford Economics, 2025"},
    {"year": 2024, "category": "national_share", "metric_name": "dc_share_of_electricity_pct", "region": "Japan", "value": 2.0, "unit": "%", "is_projection": False, "source": "Wood Mackenzie, 2025"},
    {"year": 2024, "category": "national_share", "metric_name": "dc_share_of_electricity_pct", "region": "France", "value": 2.0, "unit": "%", "is_projection": False, "source": "Ember Grids for Data Centres, Jun 2025"},
    {"year": 2024, "category": "national_share", "metric_name": "dc_share_of_electricity_pct", "region": "China", "value": 1.1, "unit": "%", "is_projection": False, "source": "IEA Energy and AI, Jan 2025 (derived: ~100 TWh / 9,800 TWh)"},
    {"year": 2030, "category": "national_share", "metric_name": "dc_share_of_electricity_pct", "region": "US", "value": 9.0, "unit": "%", "is_projection": True, "source": "IEA Energy and AI, Jan 2025 (midpoint of 6-12% range)"},

    # ── US data center power demand growth (Goldman Sachs) ──
    {"year": 2030, "category": "national_share", "metric_name": "power_demand_growth_pct", "region": "US", "value": 160.0, "unit": "%", "is_projection": True, "source": "Goldman Sachs AI/DC Power Demand, Apr 2024"},

    # ── EPRI US data center share ──
    {"year": 2023, "category": "national_share", "metric_name": "dc_share_of_electricity_pct", "region": "US", "value": 1.5, "unit": "%", "is_projection": False, "source": "EPRI Powering Intelligence, May 2024"},
    {"year": 2030, "category": "national_share", "metric_name": "dc_share_of_electricity_pct_epri", "region": "US", "value": 4.4, "unit": "%", "is_projection": True, "source": "EPRI Powering Intelligence, May 2024"},

    # ── Hyperscaler energy usage (2024 sustainability reports) ──
    {"year": 2024, "category": "hyperscaler", "metric_name": "electricity_demand_twh", "region": "Google", "value": 25.3, "unit": "TWh", "is_projection": False, "source": "Google Environmental Report, 2024"},
    {"year": 2024, "category": "hyperscaler", "metric_name": "electricity_demand_twh", "region": "Microsoft", "value": 24.0, "unit": "TWh", "is_projection": False, "source": "Microsoft Sustainability Report, 2024"},
    {"year": 2024, "category": "hyperscaler", "metric_name": "electricity_demand_twh", "region": "Meta", "value": 14.0, "unit": "TWh", "is_projection": False, "source": "Meta Sustainability Report, 2024"},
]
# fmt: on


def materialize():
    logger.info("Building data center demand seed table (%d rows)", len(SEED_DATA))

    df = pd.DataFrame(SEED_DATA)
    df["extracted_at"] = datetime.now(timezone.utc)

    logger.info("Seed table ready: %d rows, %d columns", len(df), len(df.columns))
    return df
