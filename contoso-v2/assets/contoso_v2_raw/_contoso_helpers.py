"""Shared utilities for the Contoso v2 fully-synthetic pipeline.

Replaces the v1 parquet-loader helpers with end-to-end synthetic generation
spanning 2010-01-01 to 2026-05-01. Provides:
  - Regime windows (early_growth, expansion, mature_ops, covid_shock, recovery,
    inflation, ai_boom, stabilization, recent) and per-metric impact multipliers
  - Long-term growth curve and seasonal multipliers (Q4 lift, holiday spikes,
    weekday patterns)
  - Holiday calendar, sparse-key generator, noisy-amount generator
  - Deterministic dimensional reference pools (customers, products, stores,
    employees) built from a seeded RNG so all raw assets see identical keys

NOT a Bruin asset (filename prefix _).
"""

from __future__ import annotations

import logging
import math
import os
import random
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Disk cache for expensive cross-asset reference data
# ---------------------------------------------------------------------------

CACHE_DIR = os.path.join(tempfile.gettempdir(), "contoso_v2_cache")
os.makedirs(CACHE_DIR, exist_ok=True)


def _cached_parquet(name: str, builder):
    """Read a cached parquet if present, else build + write + return."""
    path = os.path.join(CACHE_DIR, f"{name}.parquet")
    if os.path.exists(path):
        logger.info("Loading cached %s from %s", name, path)
        return pd.read_parquet(path)
    logger.info("Building %s (will cache to %s)", name, path)
    df = builder()
    df.to_parquet(path, index=False)
    return df


def clear_cache():
    """Delete the on-disk reference cache."""
    import shutil
    shutil.rmtree(CACHE_DIR, ignore_errors=True)
    os.makedirs(CACHE_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Pipeline window
# ---------------------------------------------------------------------------

PIPELINE_START = date(2010, 1, 1)
PIPELINE_END = date(2026, 5, 1)
TOTAL_DAYS = (PIPELINE_END - PIPELINE_START).days + 1

# ---------------------------------------------------------------------------
# Regimes
# ---------------------------------------------------------------------------

# Ordered list of (start, end, name); checked top-to-bottom (later wins on overlap)
REGIME_WINDOWS = [
    (date(2010, 1, 1), date(2014, 12, 31), "early_growth"),
    (date(2015, 1, 1), date(2017, 12, 31), "expansion"),
    (date(2018, 1, 1), date(2019, 12, 31), "mature_ops"),
    (date(2020, 1, 1), date(2020, 2, 29), "mature_ops"),
    (date(2020, 3, 1), date(2020, 6, 30), "covid_shock"),
    (date(2020, 7, 1), date(2021, 12, 31), "recovery"),
    (date(2022, 1, 1), date(2022, 12, 31), "inflation"),
    (date(2023, 1, 1), date(2024, 12, 31), "ai_boom"),
    (date(2025, 1, 1), date(2025, 12, 31), "stabilization"),
    (date(2026, 1, 1), date(2026, 5, 1), "recent"),
]


def regime_for(d: date | datetime | pd.Timestamp) -> str:
    """Return the regime name covering a given date."""
    d = _to_date(d)
    for start, end, name in REGIME_WINDOWS:
        if start <= d <= end:
            return name
    return "unknown"


def _to_date(d) -> date:
    if isinstance(d, pd.Timestamp):
        return d.date()
    if isinstance(d, datetime):
        return d.date()
    return d


# Per-regime, per-metric multipliers vs. baseline 1.0
# Metrics: sales, hiring, support, transit (shipping days), cost, ad_spend
EVENT_IMPACT = {
    "early_growth":  {"sales": 1.00, "hiring": 1.00, "support": 1.00, "transit": 1.00, "cost": 1.00, "ad_spend": 1.00},
    "expansion":     {"sales": 1.00, "hiring": 1.20, "support": 1.00, "transit": 1.00, "cost": 1.00, "ad_spend": 1.10},
    "mature_ops":    {"sales": 1.00, "hiring": 1.00, "support": 1.00, "transit": 1.00, "cost": 1.00, "ad_spend": 1.00},
    "covid_shock":   {"sales": 0.55, "hiring": 0.10, "support": 2.00, "transit": 1.85, "cost": 1.05, "ad_spend": 0.50},
    "recovery":      {"sales": 1.10, "hiring": 0.95, "support": 1.30, "transit": 1.20, "cost": 1.03, "ad_spend": 1.05},
    "inflation":     {"sales": 0.95, "hiring": 0.90, "support": 1.10, "transit": 1.10, "cost": 1.12, "ad_spend": 0.85},
    "ai_boom":       {"sales": 1.20, "hiring": 1.55, "support": 1.10, "transit": 1.00, "cost": 1.04, "ad_spend": 2.00},
    "stabilization": {"sales": 1.05, "hiring": 0.95, "support": 1.00, "transit": 1.00, "cost": 1.02, "ad_spend": 1.10},
    "recent":        {"sales": 1.00, "hiring": 1.00, "support": 1.00, "transit": 1.00, "cost": 1.03, "ad_spend": 1.05},
    "unknown":       {"sales": 1.00, "hiring": 1.00, "support": 1.00, "transit": 1.00, "cost": 1.00, "ad_spend": 1.00},
}


def event_impact(d, metric: str) -> float:
    return EVENT_IMPACT[regime_for(d)][metric]


# ---------------------------------------------------------------------------
# Growth + seasonality
# ---------------------------------------------------------------------------

def growth_multiplier(d) -> float:
    """Long-term growth curve normalized so 2020-01-01 ≈ 1.0.

    Calibrated landmarks:
      2010-01-01 ≈ 0.18, 2015-01-01 ≈ 0.55, 2020-01-01 ≈ 1.00,
      2024-01-01 ≈ 1.55, 2026-05-01 ≈ 1.45.
    Implemented as a piecewise linear interp.
    """
    d = _to_date(d)
    landmarks = [
        (date(2010, 1, 1), 0.18),
        (date(2014, 1, 1), 0.45),
        (date(2017, 6, 1), 0.78),
        (date(2020, 1, 1), 1.00),
        (date(2021, 1, 1), 1.05),
        (date(2022, 1, 1), 1.10),
        (date(2023, 1, 1), 1.25),
        (date(2024, 6, 1), 1.55),
        (date(2025, 6, 1), 1.50),
        (date(2026, 5, 1), 1.45),
    ]
    if d <= landmarks[0][0]:
        return landmarks[0][1]
    if d >= landmarks[-1][0]:
        return landmarks[-1][1]
    for (d0, v0), (d1, v1) in zip(landmarks, landmarks[1:]):
        if d0 <= d <= d1:
            frac = (d - d0).days / max((d1 - d0).days, 1)
            return v0 + frac * (v1 - v0)
    return 1.0


# Holidays — month-day pairs (US/EU retail-relevant)
FIXED_HOLIDAYS = {
    (1, 1): "New Year's Day",
    (2, 14): "Valentine's Day",
    (7, 4): "Independence Day",
    (10, 31): "Halloween",
    (12, 24): "Christmas Eve",
    (12, 25): "Christmas Day",
    (12, 26): "Boxing Day",
    (12, 31): "New Year's Eve",
}


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Return the nth (1-based) occurrence of weekday in given month/year."""
    d = date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    return d + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(year, month + 1, 1) - timedelta(days=1)
    while d.weekday() != weekday:
        d -= timedelta(days=1)
    return d


@lru_cache(maxsize=1)
def _holiday_map() -> dict:
    """Return dict mapping date -> holiday name across the pipeline window."""
    out: dict = {}
    for y in range(PIPELINE_START.year, PIPELINE_END.year + 1):
        for (m, d), name in FIXED_HOLIDAYS.items():
            try:
                out[date(y, m, d)] = name
            except ValueError:
                continue
        # Memorial Day = last Monday of May
        out[_last_weekday(y, 5, 0)] = "Memorial Day"
        # Labor Day = first Monday of September
        out[_nth_weekday(y, 9, 0, 1)] = "Labor Day"
        # Thanksgiving = 4th Thursday of November
        thanksgiving = _nth_weekday(y, 11, 3, 4)
        out[thanksgiving] = "Thanksgiving"
        # Black Friday = day after Thanksgiving
        out[thanksgiving + timedelta(days=1)] = "Black Friday"
        # Cyber Monday = Monday after Thanksgiving
        out[thanksgiving + timedelta(days=4)] = "Cyber Monday"
        # MLK Day = 3rd Monday of January
        out[_nth_weekday(y, 1, 0, 3)] = "MLK Day"
        # Mother's Day = 2nd Sunday of May
        out[_nth_weekday(y, 5, 6, 2)] = "Mother's Day"
        # Father's Day = 3rd Sunday of June
        out[_nth_weekday(y, 6, 6, 3)] = "Father's Day"
    return out


def holiday_name(d) -> str | None:
    return _holiday_map().get(_to_date(d))


def is_holiday(d) -> bool:
    return _to_date(d) in _holiday_map()


def is_holiday_window(d) -> bool:
    """Nov 20 - Dec 31 retail holiday window."""
    d = _to_date(d)
    return (d.month == 11 and d.day >= 20) or d.month == 12


# Holiday-day spike multipliers (applied on top of seasonal base)
HOLIDAY_SPIKES = {
    "Black Friday": 3.5,
    "Cyber Monday": 3.0,
    "Boxing Day": 2.0,
    "Christmas Eve": 1.6,
    "Independence Day": 1.4,
    "Memorial Day": 1.3,
    "Labor Day": 1.3,
    "Mother's Day": 1.4,
    "Father's Day": 1.25,
    "Valentine's Day": 1.5,
    "Halloween": 1.15,
    "Thanksgiving": 0.55,    # stores closed
    "Christmas Day": 0.20,   # stores closed
    "New Year's Day": 0.55,
    "MLK Day": 1.0,
    "New Year's Eve": 1.1,
}

# Weekday lift (Mon=0..Sun=6)
WEEKDAY_LIFT = {0: 0.85, 1: 0.95, 2: 1.05, 3: 1.05, 4: 1.10, 5: 1.15, 6: 1.10}

# Monthly base lift (1=Jan..12=Dec)
MONTH_LIFT = {1: 0.70, 2: 0.80, 3: 0.95, 4: 1.00, 5: 1.05, 6: 1.05,
              7: 1.00, 8: 1.10, 9: 1.05, 10: 1.10, 11: 1.40, 12: 1.60}


def seasonal_multiplier(d) -> float:
    """Combined growth × month × weekday × holiday multiplier."""
    d = _to_date(d)
    m = MONTH_LIFT[d.month] * WEEKDAY_LIFT[d.weekday()]
    h = HOLIDAY_SPIKES.get(holiday_name(d) or "", 1.0)
    return growth_multiplier(d) * m * h


# ---------------------------------------------------------------------------
# Sparse keys
# ---------------------------------------------------------------------------

def sparse_keys(n: int, gap_rate: float = 0.15, start: int = 1, seed: int = 42) -> list[int]:
    """Return n monotonically-increasing integers with random gaps.

    gap_rate ≈ fraction of "deleted" IDs interspersed.
    """
    rng = np.random.default_rng(seed)
    out: list[int] = []
    cur = start
    while len(out) < n:
        out.append(cur)
        # advance by 1 + bonus gaps drawn from geometric
        cur += 1 + int(rng.geometric(1 - gap_rate) - 1)
    return out


# ---------------------------------------------------------------------------
# Noisy amounts
# ---------------------------------------------------------------------------

def noisy_amount(base: float, sigma: float = 0.15, outlier_rate: float = 0.02,
                 rng: np.random.Generator | None = None) -> float:
    """Return base × lognormal noise, with rare tail-multiplier outliers."""
    rng = rng or np.random.default_rng()
    noise = float(rng.lognormal(mean=0.0, sigma=sigma))
    val = base * noise
    if rng.random() < outlier_rate:
        val *= rng.uniform(2.5, 6.0)
    return val


# ---------------------------------------------------------------------------
# Seeded helpers
# ---------------------------------------------------------------------------

def get_seeded_rng(seed: int = 42) -> np.random.Generator:
    return np.random.default_rng(seed)


def get_seeded_faker(seed: int = 42):
    from faker import Faker
    fake = Faker()
    Faker.seed(seed)
    return fake


def seed_all(seed: int = 42):
    random.seed(seed)
    np.random.seed(seed)


# ---------------------------------------------------------------------------
# Reference data: stores
# ---------------------------------------------------------------------------

# (city, country_code, country_name, currency_code, base_open_year)
STORE_TEMPLATES = [
    ("New York", "US", "United States", "USD", 2010),
    ("Los Angeles", "US", "United States", "USD", 2010),
    ("Chicago", "US", "United States", "USD", 2010),
    ("Houston", "US", "United States", "USD", 2011),
    ("Philadelphia", "US", "United States", "USD", 2010),
    ("Phoenix", "US", "United States", "USD", 2012),
    ("San Antonio", "US", "United States", "USD", 2013),
    ("San Diego", "US", "United States", "USD", 2011),
    ("Dallas", "US", "United States", "USD", 2010),
    ("San Jose", "US", "United States", "USD", 2013),
    ("Austin", "US", "United States", "USD", 2014),
    ("Jacksonville", "US", "United States", "USD", 2015),
    ("Fort Worth", "US", "United States", "USD", 2015),
    ("Columbus", "US", "United States", "USD", 2016),
    ("Indianapolis", "US", "United States", "USD", 2016),
    ("Charlotte", "US", "United States", "USD", 2016),
    ("Seattle", "US", "United States", "USD", 2014),
    ("Denver", "US", "United States", "USD", 2015),
    ("Washington", "US", "United States", "USD", 2014),
    ("Boston", "US", "United States", "USD", 2014),
    ("Nashville", "US", "United States", "USD", 2017),
    ("Detroit", "US", "United States", "USD", 2017),
    ("Portland", "US", "United States", "USD", 2017),
    ("Las Vegas", "US", "United States", "USD", 2018),
    ("Memphis", "US", "United States", "USD", 2018),
    ("Atlanta", "US", "United States", "USD", 2014),
    ("Miami", "US", "United States", "USD", 2014),
    ("Online US", "US", "United States", "USD", 2010),
    ("London", "GB", "United Kingdom", "GBP", 2010),
    ("Manchester", "GB", "United Kingdom", "GBP", 2012),
    ("Birmingham", "GB", "United Kingdom", "GBP", 2014),
    ("Leeds", "GB", "United Kingdom", "GBP", 2015),
    ("Glasgow", "GB", "United Kingdom", "GBP", 2016),
    ("Liverpool", "GB", "United Kingdom", "GBP", 2017),
    ("Edinburgh", "GB", "United Kingdom", "GBP", 2018),
    ("Bristol", "GB", "United Kingdom", "GBP", 2019),
    ("Online UK", "GB", "United Kingdom", "GBP", 2010),
    ("Berlin", "DE", "Germany", "EUR", 2010),
    ("Munich", "DE", "Germany", "EUR", 2011),
    ("Hamburg", "DE", "Germany", "EUR", 2013),
    ("Cologne", "DE", "Germany", "EUR", 2015),
    ("Frankfurt", "DE", "Germany", "EUR", 2014),
    ("Stuttgart", "DE", "Germany", "EUR", 2017),
    ("Düsseldorf", "DE", "Germany", "EUR", 2018),
    ("Online DE", "DE", "Germany", "EUR", 2011),
    ("Paris", "FR", "France", "EUR", 2010),
    ("Marseille", "FR", "France", "EUR", 2013),
    ("Lyon", "FR", "France", "EUR", 2014),
    ("Toulouse", "FR", "France", "EUR", 2016),
    ("Nice", "FR", "France", "EUR", 2017),
    ("Nantes", "FR", "France", "EUR", 2019),
    ("Online FR", "FR", "France", "EUR", 2011),
    ("Toronto", "CA", "Canada", "CAD", 2011),
    ("Vancouver", "CA", "Canada", "CAD", 2013),
    ("Montreal", "CA", "Canada", "CAD", 2014),
    ("Calgary", "CA", "Canada", "CAD", 2016),
    ("Ottawa", "CA", "Canada", "CAD", 2018),
    ("Online CA", "CA", "Canada", "CAD", 2012),
    ("Rome", "IT", "Italy", "EUR", 2012),
    ("Milan", "IT", "Italy", "EUR", 2013),
    ("Naples", "IT", "Italy", "EUR", 2016),
    ("Turin", "IT", "Italy", "EUR", 2018),
    ("Online IT", "IT", "Italy", "EUR", 2013),
    ("Amsterdam", "NL", "Netherlands", "EUR", 2013),
    ("Rotterdam", "NL", "Netherlands", "EUR", 2016),
    ("The Hague", "NL", "Netherlands", "EUR", 2018),
    ("Online NL", "NL", "Netherlands", "EUR", 2014),
    ("Sydney", "AU", "Australia", "AUD", 2013),
    ("Melbourne", "AU", "Australia", "AUD", 2014),
    ("Brisbane", "AU", "Australia", "AUD", 2017),
    ("Perth", "AU", "Australia", "AUD", 2019),
    ("Online AU", "AU", "Australia", "AUD", 2014),
    ("Madrid", "ES", "Spain", "EUR", 2018),
    ("Barcelona", "ES", "Spain", "EUR", 2019),
    ("Online ES", "ES", "Spain", "EUR", 2018),
    ("Stockholm", "SE", "Sweden", "SEK", 2020),
    ("Online SE", "SE", "Sweden", "SEK", 2021),
]


def build_stores() -> pd.DataFrame:
    """Build the store dimension. Deterministic + disk-cached."""
    return _cached_parquet("stores", _build_stores)


@lru_cache(maxsize=1)
def _build_stores() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    keys = sparse_keys(len(STORE_TEMPLATES), gap_rate=0.10, start=1, seed=7)

    rows = []
    # Indices that will get COVID temp closure or permanent close
    temp_close_idx = set(rng.choice(len(STORE_TEMPLATES), size=25, replace=False))
    perm_close_idx = set(rng.choice(
        [i for i in temp_close_idx], size=4, replace=False
    ))
    early_close_idx = set(rng.choice(
        [i for i in range(len(STORE_TEMPLATES)) if i not in temp_close_idx],
        size=4, replace=False,
    ))
    later_close_idx = set(rng.choice(
        [i for i in range(len(STORE_TEMPLATES))
         if i not in temp_close_idx and i not in early_close_idx],
        size=2, replace=False,
    ))

    for i, (city, country_code, country_name, ccy, base_year) in enumerate(STORE_TEMPLATES):
        store_key = keys[i]
        is_online = city.startswith("Online")

        # Add small jitter to open year/month
        open_month = int(rng.integers(1, 13))
        open_day = int(rng.integers(1, 28))
        try:
            open_date = date(base_year, open_month, open_day)
        except ValueError:
            open_date = date(base_year, open_month, 15)

        close_date = None
        temp_closed_start = None
        temp_closed_end = None

        if i in early_close_idx:
            close_date = date(2013, int(rng.integers(1, 13)), int(rng.integers(1, 28)))
        elif i in perm_close_idx:
            close_date = date(2020, int(rng.integers(4, 8)), int(rng.integers(1, 28)))
        elif i in later_close_idx:
            close_date = date(2023, int(rng.integers(1, 13)), int(rng.integers(1, 28)))

        if i in temp_close_idx and close_date != date(2020, int(rng.integers(4, 8)), 15):
            temp_closed_start = date(2020, 3, int(rng.integers(15, 25)))
            temp_closed_end = date(2020, int(rng.integers(6, 9)), int(rng.integers(1, 28)))

        rows.append({
            "store_key": int(store_key),
            "store_name": f"Contoso {city}",
            "city": city,
            "country_code": country_code,
            "country_name": country_name,
            "currency_code": ccy,
            "channel": "Online" if is_online else "Retail",
            "open_date": open_date,
            "close_date": close_date,
            "temp_closed_start": temp_closed_start,
            "temp_closed_end": temp_closed_end,
            "square_meters": int(rng.integers(80, 500)) if not is_online else None,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Reference data: products
# ---------------------------------------------------------------------------

CATEGORIES = [
    ("Computers", ["Laptops", "Desktops", "Monitors", "Printers", "Storage"]),
    ("Audio", ["Headphones", "Speakers", "Microphones", "Audio Cables"]),
    ("Cameras & Photo", ["DSLR", "Mirrorless", "Action Cameras", "Tripods", "Lenses"]),
    ("Cell Phones", ["Smartphones", "Phone Accessories", "Cases", "Chargers"]),
    ("Home Appliances", ["Refrigerators", "Washers", "Dishwashers", "Microwaves", "Vacuum Cleaners"]),
    ("TV & Video", ["Televisions", "Projectors", "Streaming Devices"]),
    ("Music, Movies & Games", ["Gaming Consoles", "Video Games", "Music"]),
    ("Smart Home & AI", ["Smart Speakers", "Smart Displays", "AI Assistants", "Robotics"]),
]

BRANDS = [
    ("Contoso", 0.20, 2010),
    ("Fabrikam", 0.15, 2010),
    ("Adventure Works", 0.10, 2010),
    ("Litware", 0.08, 2010),
    ("Northwind", 0.07, 2011),
    ("Proseware", 0.06, 2012),
    ("Tailspin", 0.05, 2013),
    ("WideWorld", 0.04, 2014),
    ("ContosoPro", 0.05, 2015),
    ("MoonRover", 0.04, 2018),
    ("LumenVR", 0.03, 2020),
    ("Synapse AI", 0.05, 2023),
    ("Halcyon AI", 0.03, 2023),
    ("Forge Robotics", 0.03, 2024),
    ("Nimbus Audio", 0.02, 2017),
]


def build_products(n_products: int = 2500) -> pd.DataFrame:
    """Build the product dimension. Deterministic + disk-cached."""
    return _cached_parquet("products", lambda: _build_products(n_products))


@lru_cache(maxsize=1)
def _build_products(n_products: int = 2500) -> pd.DataFrame:
    rng = np.random.default_rng(11)
    fake = get_seeded_faker(11)
    keys = sparse_keys(n_products, gap_rate=0.08, start=1001, seed=11)

    flat_subcats = [(c, s) for c, subs in CATEGORIES for s in subs]
    brand_names = [b[0] for b in BRANDS]
    brand_weights = np.array([b[1] for b in BRANDS])
    brand_weights = brand_weights / brand_weights.sum()
    brand_first_year = {b[0]: b[2] for b in BRANDS}

    rows = []
    for i in range(n_products):
        cat, subcat = flat_subcats[int(rng.integers(0, len(flat_subcats)))]
        brand = str(rng.choice(brand_names, p=brand_weights))
        first_listed_year = max(2010, brand_first_year[brand] + int(rng.integers(0, 4)))
        first_listed_month = int(rng.integers(1, 13))
        first_listed_day = int(rng.integers(1, 28))
        first_listed_date = date(first_listed_year, first_listed_month, first_listed_day)

        # Cost lognormal, price = cost × markup
        cost = float(rng.lognormal(mean=4.2, sigma=0.95))
        cost = max(2.5, min(cost, 4500.0))
        markup = float(rng.uniform(1.4, 2.6))
        price = round(cost * markup, 2)
        cost = round(cost, 2)

        discontinued = None
        # 3% discontinued, clustered mid-2020 (COVID rationalization) or 2022 (inflation cull)
        if rng.random() < 0.03:
            if rng.random() < 0.5:
                discontinued = date(2020, int(rng.integers(5, 10)), int(rng.integers(1, 28)))
            else:
                discontinued = date(2022, int(rng.integers(3, 11)), int(rng.integers(1, 28)))

        # AI-branded products only listed 2023+
        if "AI" in brand or brand == "Forge Robotics":
            first_listed_date = date(max(2023, first_listed_year),
                                     first_listed_month, first_listed_day)

        rows.append({
            "product_key": int(keys[i]),
            "product_name": f"{brand} {subcat[:-1] if subcat.endswith('s') else subcat} Model {i+100}",
            "brand": brand,
            "category_name": cat,
            "sub_category_name": subcat,
            "color": str(fake.color_name()),
            "cost": cost,
            "price": price,
            "first_listed_date": first_listed_date,
            "discontinued_date": discontinued,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Reference data: customers
# ---------------------------------------------------------------------------

# Country, segment-friendly weight (so we get realistic regional spread)
CUSTOMER_COUNTRIES = [
    ("US", "United States", "USD", 0.45),
    ("GB", "United Kingdom", "GBP", 0.13),
    ("DE", "Germany", "EUR", 0.11),
    ("FR", "France", "EUR", 0.09),
    ("CA", "Canada", "CAD", 0.07),
    ("IT", "Italy", "EUR", 0.05),
    ("NL", "Netherlands", "EUR", 0.04),
    ("AU", "Australia", "AUD", 0.04),
    ("ES", "Spain", "EUR", 0.02),
]

OCCUPATIONS = [
    "Engineer", "Teacher", "Nurse", "Manager", "Sales Representative",
    "Accountant", "Marketing Specialist", "Software Developer", "Designer",
    "Doctor", "Lawyer", "Consultant", "Analyst", "Researcher", "Student",
    "Retired", "Self-Employed", "Operations Manager", "Project Manager",
    "Architect", "Pharmacist", "Therapist", "Writer", "Editor", "Producer",
    "Customer Service", "Administrator", "Technician", "Electrician", "Mechanic",
]


def build_customers(n_customers: int = 100_000) -> pd.DataFrame:
    """Build the customer dimension. Deterministic + disk-cached."""
    return _cached_parquet("customers", lambda: _build_customers(n_customers))


@lru_cache(maxsize=1)
def _build_customers(n_customers: int = 100_000) -> pd.DataFrame:
    """Build the customer dimension. Deterministic.

    customer signup spread across pipeline window with growth-curve weighting.
    """
    rng = np.random.default_rng(23)
    fake = get_seeded_faker(23)
    keys = sparse_keys(n_customers, gap_rate=0.18, start=1, seed=23)

    # Sample signup dates weighted by growth curve
    days = np.arange(TOTAL_DAYS)
    day_dates = [PIPELINE_START + timedelta(days=int(d)) for d in days]
    weights = np.array([growth_multiplier(d) * MONTH_LIFT[d.month] for d in day_dates])
    weights = weights / weights.sum()
    signup_idx = rng.choice(len(days), size=n_customers, p=weights)
    signup_dates = [day_dates[i] for i in signup_idx]

    # Country codes
    country_codes = [c[0] for c in CUSTOMER_COUNTRIES]
    country_weights = np.array([c[3] for c in CUSTOMER_COUNTRIES])
    country_weights = country_weights / country_weights.sum()
    countries = rng.choice(country_codes, size=n_customers, p=country_weights)
    country_lookup = {c[0]: (c[1], c[2]) for c in CUSTOMER_COUNTRIES}

    # Genders
    genders = rng.choice(["male", "female"], size=n_customers, p=[0.49, 0.51])

    # Birth year distribution (skewed toward 25-55)
    birth_years = rng.integers(1945, 2008, size=n_customers)

    # Occupation weights
    occ_weights = rng.dirichlet(np.ones(len(OCCUPATIONS)) * 0.5)
    occupations = rng.choice(OCCUPATIONS, size=n_customers, p=occ_weights)

    # Names — sample from a small pool to stay fast (Faker is slow at 140K)
    NAME_POOL = 5000
    fake_first = [fake.first_name() for _ in range(NAME_POOL)]
    fake_last = [fake.last_name() for _ in range(NAME_POOL)]
    first_idx = rng.integers(0, NAME_POOL, size=n_customers)
    last_idx = rng.integers(0, NAME_POOL, size=n_customers)

    df = pd.DataFrame({
        "customer_key": keys,
        "given_name": [fake_first[i] for i in first_idx],
        "surname": [fake_last[i] for i in last_idx],
        "gender": genders,
        "birth_year": birth_years,
        "country_code": countries,
        "country_full": [country_lookup[c][0] for c in countries],
        "currency_code": [country_lookup[c][1] for c in countries],
        "occupation": occupations,
        "signup_date": signup_dates,
    })

    # Email
    df["email"] = (df["given_name"].str.lower() + "." + df["surname"].str.lower() +
                   "@" + df["country_code"].str.lower() + "mail.com")

    # 1.5% data quality issues: NULL birth_year or malformed email
    bad_idx = rng.choice(n_customers, size=int(n_customers * 0.015), replace=False)
    df.loc[bad_idx[:len(bad_idx)//2], "birth_year"] = None
    df.loc[bad_idx[len(bad_idx)//2:], "email"] = ""

    # Segment: New (signup <90d ago vs PIPELINE_END), Returning, VIP, Lapsed
    days_since = (pd.Timestamp(PIPELINE_END) - pd.to_datetime(df["signup_date"])).dt.days
    seg = pd.Series(["Returning"] * len(df))
    seg[days_since < 90] = "New"
    seg[days_since > 365 * 3] = "Lapsed"
    vip_idx = rng.choice(n_customers, size=int(n_customers * 0.05), replace=False)
    seg.iloc[vip_idx] = "VIP"
    df["segment"] = seg.values

    return df


# ---------------------------------------------------------------------------
# Daily orders volume
# ---------------------------------------------------------------------------

# Baseline orders/day calibrated so total is ~1.5M
BASE_ORDERS_PER_DAY = 250


def daily_order_count(d, rng: np.random.Generator | None = None) -> int:
    """Return realistic order count for a given date."""
    rng = rng or np.random.default_rng()
    base = BASE_ORDERS_PER_DAY
    mult = seasonal_multiplier(d) * event_impact(d, "sales")
    expected = base * mult
    return max(0, int(rng.gamma(shape=20, scale=expected / 20)))


# ---------------------------------------------------------------------------
# Reference data: orders + sales lines (the heaviest generator)
# ---------------------------------------------------------------------------

def build_orders() -> pd.DataFrame:
    """Build the orders header table. Disk-cached."""
    return _cached_parquet("orders", _build_orders)


def _build_orders() -> pd.DataFrame:
    rng = get_seeded_rng(101)

    customers = build_customers()
    stores = build_stores()

    # Filter store currency lookup
    store_data = stores[["store_key", "currency_code", "open_date",
                         "close_date", "channel", "country_code"]].copy()
    store_data["open_date"] = pd.to_datetime(store_data["open_date"])
    store_data["close_date"] = pd.to_datetime(store_data["close_date"])

    cust_signup = pd.to_datetime(customers["signup_date"]).values
    cust_keys = customers["customer_key"].values
    n_customers = len(cust_keys)

    # Channel weights evolve over time: in-store → online
    def channel_weights(d: date) -> tuple[float, float]:
        # Fraction in-store
        year_frac = (d.year - 2010) / 16.0
        in_store_frac = max(0.30, 0.72 - 0.40 * year_frac)
        if regime_for(d) == "covid_shock":
            in_store_frac *= 0.30
        return in_store_frac, 1.0 - in_store_frac

    rows = []
    order_keys_iter = iter(sparse_keys(2_000_000, gap_rate=0.10, start=1, seed=101))
    cur = PIPELINE_START
    day_count = 0

    # Pre-build per-day eligible store indices
    # (to avoid re-filtering 5966 times)
    store_open_dates = pd.to_datetime(store_data["open_date"]).values
    store_close_dates = pd.to_datetime(store_data["close_date"]).values
    store_channels = store_data["channel"].values
    store_currencies = store_data["currency_code"].values
    n_stores_total = len(store_data)

    while cur <= PIPELINE_END:
        day_count += 1
        n_orders = daily_order_count(cur, rng)
        if n_orders == 0:
            cur += timedelta(days=1)
            continue

        cur_ts = pd.Timestamp(cur)

        # Eligible customers: signed up by this date
        # (sample without strict filter — fast approximation)
        eligible_mask_cust = cust_signup <= cur_ts.to_datetime64()
        n_eligible_cust = int(eligible_mask_cust.sum())
        if n_eligible_cust < 10:
            cur += timedelta(days=1)
            continue

        # Sample customer indices from eligible (with replacement OK — repeat customers normal)
        eligible_cust_idx = np.flatnonzero(eligible_mask_cust)
        cust_pick = rng.choice(eligible_cust_idx, size=n_orders, replace=True)

        # Eligible stores: open and not closed
        store_eligible = (
            (store_open_dates <= cur_ts.to_datetime64())
            & (
                pd.isna(store_close_dates)
                | (store_close_dates >= cur_ts.to_datetime64())
            )
        )
        eligible_store_idx = np.flatnonzero(store_eligible)
        if len(eligible_store_idx) == 0:
            cur += timedelta(days=1)
            continue

        # Channel split
        in_store_frac, online_frac = channel_weights(cur)
        # eligible store channels
        elig_channels = store_channels[eligible_store_idx]
        retail_idx = eligible_store_idx[elig_channels == "Retail"]
        online_idx = eligible_store_idx[elig_channels == "Online"]
        if len(retail_idx) == 0:
            store_pick = rng.choice(online_idx, size=n_orders, replace=True)
        elif len(online_idx) == 0:
            store_pick = rng.choice(retail_idx, size=n_orders, replace=True)
        else:
            n_retail = int(n_orders * in_store_frac)
            n_online = n_orders - n_retail
            store_pick = np.concatenate([
                rng.choice(retail_idx, size=n_retail, replace=True),
                rng.choice(online_idx, size=n_online, replace=True),
            ])
            rng.shuffle(store_pick)

        # Build day rows
        order_keys_today = [next(order_keys_iter) for _ in range(n_orders)]
        for i in range(n_orders):
            ok = order_keys_today[i]
            cust_idx_i = int(cust_pick[i])
            store_idx_i = int(store_pick[i])

            # Order time within the day (favor evenings/weekends)
            hour = int(rng.integers(8, 23))
            minute = int(rng.integers(0, 60))
            order_dt = datetime(cur.year, cur.month, cur.day, hour, minute)

            # Status mix (4% cancelled, 2% pending, 2% returned, 92% completed)
            r = rng.random()
            if r < 0.92:
                status = "Completed"
            elif r < 0.96:
                status = "Cancelled"
            elif r < 0.98:
                status = "Returned"
            else:
                status = "Pending"

            # Delivery date: 0-5 days normal, +14d during COVID supply shock
            base_delay = max(0, int(rng.gamma(shape=2, scale=1.5)))
            transit_mult = event_impact(cur, "transit")
            delivery_delay = int(base_delay * transit_mult) + int(rng.integers(1, 3))
            delivery_dt = order_dt + timedelta(days=delivery_delay)

            rows.append((
                ok,
                int(cust_keys[cust_idx_i]),
                int(stores.iloc[store_idx_i]["store_key"]),
                order_dt,
                delivery_dt,
                store_currencies[store_idx_i],
                status,
                store_channels[store_idx_i],
            ))
        cur += timedelta(days=1)

    df = pd.DataFrame(rows, columns=[
        "order_key", "customer_key", "store_key", "order_date",
        "delivery_date", "currency_code", "status", "channel",
    ])
    logger.info("Built %d orders across %d days", len(df), day_count)
    return df


def build_sales_lines() -> pd.DataFrame:
    """Build sales line items. Disk-cached."""
    return _cached_parquet("sales_lines", _build_sales_lines)


def _build_sales_lines() -> pd.DataFrame:
    rng = get_seeded_rng(103)

    orders = build_orders()
    products = build_products()
    cur_xrate_lookup = _exchange_rate_lookup()

    prod_keys = products["product_key"].values
    prod_costs = products["cost"].values
    prod_prices = products["price"].values
    prod_first = pd.to_datetime(products["first_listed_date"]).values
    prod_disc = pd.to_datetime(products["discontinued_date"]).values

    # Lines per order distribution: 80% 1, 12% 2, 5% 3, 3% 4-6
    n_orders = len(orders)
    line_counts = rng.choice([1, 2, 3, 4, 5, 6], size=n_orders,
                             p=[0.78, 0.13, 0.05, 0.02, 0.015, 0.005])

    total_lines = int(line_counts.sum())
    logger.info("Building %d sales lines for %d orders", total_lines, n_orders)

    # Pre-allocate arrays
    out_order_key = np.empty(total_lines, dtype=np.int64)
    out_line_num = np.empty(total_lines, dtype=np.int32)
    out_order_date = np.empty(total_lines, dtype="datetime64[ns]")
    out_delivery_date = np.empty(total_lines, dtype="datetime64[ns]")
    out_customer_key = np.empty(total_lines, dtype=np.int64)
    out_store_key = np.empty(total_lines, dtype=np.int64)
    out_product_key = np.empty(total_lines, dtype=np.int64)
    out_quantity = np.empty(total_lines, dtype=np.int32)
    out_unit_price = np.empty(total_lines, dtype=np.float64)
    out_net_price = np.empty(total_lines, dtype=np.float64)
    out_unit_cost = np.empty(total_lines, dtype=np.float64)
    out_currency = np.empty(total_lines, dtype=object)
    out_xrate = np.empty(total_lines, dtype=np.float64)

    order_keys = orders["order_key"].values
    order_dates = pd.to_datetime(orders["order_date"]).values
    delivery_dates = pd.to_datetime(orders["delivery_date"]).values
    customer_keys = orders["customer_key"].values
    store_keys = orders["store_key"].values
    currencies = orders["currency_code"].values
    statuses = orders["status"].values

    cursor = 0
    for i in range(n_orders):
        n_lines = int(line_counts[i])
        # Filter eligible products at this date
        order_dt64 = order_dates[i]
        elig = (prod_first <= order_dt64) & (
            np.isnat(prod_disc) | (prod_disc >= order_dt64)
        )
        elig_idx = np.flatnonzero(elig)
        if len(elig_idx) == 0:
            elig_idx = np.arange(len(prod_keys))

        # Pick products (no replacement within order)
        if n_lines <= len(elig_idx):
            picks = rng.choice(elig_idx, size=n_lines, replace=False)
        else:
            picks = rng.choice(elig_idx, size=n_lines, replace=True)

        regime_d = regime_for(pd.Timestamp(order_dt64).date())
        cost_inflate = EVENT_IMPACT[regime_d]["cost"]

        for j, p in enumerate(picks):
            base_cost = float(prod_costs[p]) * cost_inflate
            base_price = float(prod_prices[p]) * cost_inflate

            # Quantity distribution
            qty_r = rng.random()
            if qty_r < 0.78:
                qty = 1
            elif qty_r < 0.90:
                qty = 2
            elif qty_r < 0.96:
                qty = 3
            else:
                qty = int(rng.integers(4, 11))

            # 1.5% returns: negative quantity
            if statuses[i] == "Returned" or rng.random() < 0.015:
                qty = -qty

            # Discount: 2% chance of >40% clearance, otherwise small variance
            if rng.random() < 0.02:
                discount = float(rng.uniform(0.40, 0.70))
            else:
                discount = float(rng.uniform(0.0, 0.12))
            net = base_price * (1 - discount)

            # Per-line price noise
            unit_price = round(base_price * float(rng.lognormal(0, 0.02)), 4)
            net_price = round(net * float(rng.lognormal(0, 0.02)), 4)
            unit_cost = round(base_cost * float(rng.lognormal(0, 0.02)), 4)

            ccy = currencies[i]
            xrate = cur_xrate_lookup.get(
                (pd.Timestamp(order_dt64).date(), ccy), 1.0
            )

            out_order_key[cursor] = order_keys[i]
            out_line_num[cursor] = j + 1
            out_order_date[cursor] = order_dt64
            out_delivery_date[cursor] = delivery_dates[i]
            out_customer_key[cursor] = customer_keys[i]
            out_store_key[cursor] = store_keys[i]
            out_product_key[cursor] = prod_keys[p]
            out_quantity[cursor] = qty
            out_unit_price[cursor] = unit_price
            out_net_price[cursor] = net_price
            out_unit_cost[cursor] = unit_cost
            out_currency[cursor] = ccy
            out_xrate[cursor] = xrate
            cursor += 1

    df = pd.DataFrame({
        "order_key": out_order_key[:cursor],
        "line_number": out_line_num[:cursor],
        "order_date": out_order_date[:cursor],
        "delivery_date": out_delivery_date[:cursor],
        "customer_key": out_customer_key[:cursor],
        "store_key": out_store_key[:cursor],
        "product_key": out_product_key[:cursor],
        "quantity": out_quantity[:cursor],
        "unit_price": out_unit_price[:cursor],
        "net_price": out_net_price[:cursor],
        "unit_cost": out_unit_cost[:cursor],
        "currency_code": out_currency[:cursor],
        "exchange_rate": out_xrate[:cursor],
    })
    return df


def build_exchange_rates() -> pd.DataFrame:
    """Build daily exchange rates for non-USD currencies. Disk-cached.

    Convention: `exchange_rate` is USD per unit of local currency, so
    `local_amount * exchange_rate = USD amount`.
    """
    return _cached_parquet("exchange_rates", _build_exchange_rates)


# (currency, base_rate_to_usd, daily_vol_pct)
EXCHANGE_CURRENCIES = [
    ("USD", 1.00, 0.000),
    ("EUR", 1.30, 0.005),
    ("GBP", 1.55, 0.005),
    ("CAD", 0.95, 0.004),
    ("AUD", 0.92, 0.005),
    ("SEK", 0.13, 0.005),
    ("JPY", 0.011, 0.004),
]

EXCHANGE_VOL_EVENTS = [
    # (start, end, currency, drift_pct_per_day)
    (date(2016, 6, 23), date(2016, 7, 7), "GBP", -0.013),
    (date(2020, 3, 1), date(2020, 4, 15), "EUR", 0.002),
    (date(2020, 3, 1), date(2020, 4, 15), "GBP", -0.002),
    (date(2020, 3, 1), date(2020, 4, 15), "CAD", -0.002),
    (date(2020, 3, 1), date(2020, 4, 15), "AUD", -0.003),
    (date(2022, 4, 1), date(2022, 11, 1), "EUR", -0.0008),
    (date(2022, 4, 1), date(2022, 11, 1), "GBP", -0.0008),
    (date(2022, 4, 1), date(2022, 11, 1), "CAD", -0.0006),
    (date(2022, 4, 1), date(2022, 11, 1), "AUD", -0.0009),
]


def _build_exchange_rates() -> pd.DataFrame:
    rng = get_seeded_rng(53)
    cur_rates = {ccy: rate for ccy, rate, _ in EXCHANGE_CURRENCIES}
    base_rates = {ccy: rate for ccy, rate, _ in EXCHANGE_CURRENCIES}
    vol = {ccy: v for ccy, _, v in EXCHANGE_CURRENCIES}

    rows = []
    cur = PIPELINE_START
    while cur <= PIPELINE_END:
        for ccy in cur_rates:
            if ccy == "USD":
                continue
            drift = float(rng.normal(0, vol[ccy]))
            mean_revert_pull = (base_rates[ccy] - cur_rates[ccy]) * 0.005
            for ev_start, ev_end, ev_ccy, ev_drift in EXCHANGE_VOL_EVENTS:
                if ccy == ev_ccy and ev_start <= cur <= ev_end:
                    drift += ev_drift
            cur_rates[ccy] = max(
                0.001,
                cur_rates[ccy] * (1 + drift)
                + mean_revert_pull * cur_rates[ccy] / base_rates[ccy],
            )

        for ccy, rate in cur_rates.items():
            rows.append((cur, ccy, "USD", round(rate, 6)))
        cur += timedelta(days=1)

    df = pd.DataFrame(rows, columns=[
        "date", "from_currency", "to_currency", "exchange_rate",
    ])
    return df


@lru_cache(maxsize=1)
def _exchange_rate_lookup() -> dict:
    """(date, currency) -> rate dict. In-process cache only."""
    df = build_exchange_rates()
    df["date_only"] = pd.to_datetime(df["date"]).dt.date
    return {
        (r.date_only, r.from_currency): float(r.exchange_rate)
        for r in df.itertuples()
    }


# ---------------------------------------------------------------------------
# Reference data: marketing campaigns
# ---------------------------------------------------------------------------

# Channel mix shifts across the years: 2010 leans Email/Display; 2024 Paid Search/Social.
# Each entry: (channel, weight_2010, weight_2024). Linear interp by year.
CHANNEL_MIX_LANDMARKS = [
    ("Email",       0.32, 0.18),
    ("Display",     0.30, 0.10),
    ("Paid Search", 0.18, 0.30),
    ("Social",      0.10, 0.28),
    ("Referral",    0.10, 0.14),
]

CHANNEL_METRICS = {
    "Email":       {"ctr": 0.025, "conv_rate": 0.040, "cpm": 5.0},
    "Paid Search": {"ctr": 0.035, "conv_rate": 0.030, "cpm": 15.0},
    "Social":      {"ctr": 0.012, "conv_rate": 0.015, "cpm": 8.0},
    "Display":     {"ctr": 0.008, "conv_rate": 0.010, "cpm": 4.0},
    "Referral":    {"ctr": 0.045, "conv_rate": 0.050, "cpm": 3.0},
}

CAMPAIGN_THEMES = [
    # (theme_name, anchor_month, duration_min, duration_max, budget_tier_bias)
    # budget_tier_bias: 'seasonal' = bimodal toward big budgets; 'always_on' = small
    ("BlackFriday",      11, 14, 30, "seasonal"),
    ("CyberMonday",      11, 7,  14, "seasonal"),
    ("HolidaySeason",    11, 30, 60, "seasonal"),
    ("BoxingDay",        12, 7,  14, "seasonal"),
    ("NewYearSale",      1,  10, 21, "seasonal"),
    ("ValentinesDay",    2,  10, 21, "seasonal"),
    ("SpringClearance",  3,  21, 45, "always_on"),
    ("MothersDay",       5,  14, 21, "seasonal"),
    ("MemorialDay",      5,  7,  14, "seasonal"),
    ("FathersDay",       6,  14, 21, "seasonal"),
    ("FourthOfJuly",     7,  7,  14, "seasonal"),
    ("BackToSchool",     8,  21, 45, "seasonal"),
    ("LaborDay",         9,  7,  14, "seasonal"),
    ("Halloween",        10, 14, 21, "always_on"),
    ("BrandAwareness",   None, 60, 120, "always_on"),
    ("ProductLaunch",    None, 30, 60, "always_on"),
    ("LoyaltyReward",    None, 30, 60, "always_on"),
]

CAMPAIGN_SEGMENTS = ["All Customers", "New Customers", "Returning Customers", "VIP", "Lapsed"]


def _channel_weights_for_year(year: int) -> tuple[list[str], np.ndarray]:
    """Linear interp channel weights between 2010 and 2024 anchors."""
    frac = max(0.0, min(1.0, (year - 2010) / 14.0))
    names = [c[0] for c in CHANNEL_MIX_LANDMARKS]
    weights = np.array([c[1] + frac * (c[2] - c[1]) for c in CHANNEL_MIX_LANDMARKS])
    weights = weights / weights.sum()
    return names, weights


def _yearly_campaign_count(year: int) -> int:
    """Annual campaign count, climbing with growth and AI-boom 3x cadence."""
    base = {
        2010: 16, 2011: 18, 2012: 22, 2013: 26, 2014: 30, 2015: 36,
        2016: 42, 2017: 46, 2018: 48, 2019: 48, 2020: 28,
        2021: 44, 2022: 46, 2023: 95, 2024: 110, 2025: 60, 2026: 22,
    }
    return base.get(year, 30)


def build_campaigns() -> pd.DataFrame:
    """Build marketing campaigns master table. Disk-cached."""
    return _cached_parquet("campaigns", _build_campaigns)


def _build_campaigns() -> pd.DataFrame:
    rng = get_seeded_rng(131)
    products = build_products()
    prod_keys = products["product_key"].values
    prod_first = pd.to_datetime(products["first_listed_date"]).values
    prod_disc = pd.to_datetime(products["discontinued_date"]).values
    ai_brand_mask = products["brand"].isin(
        ["Synapse AI", "Halcyon AI", "Forge Robotics"]
    ).values

    sparse_iter = iter(sparse_keys(8_000, gap_rate=0.06, start=1, seed=131))

    rows = []
    for year in range(2010, 2027):
        n = _yearly_campaign_count(year)
        names, channel_weights = _channel_weights_for_year(year)
        themes_seasonal = [t for t in CAMPAIGN_THEMES if t[1] is not None]
        themes_always = [t for t in CAMPAIGN_THEMES if t[1] is None]

        for _ in range(n):
            # 75% seasonal anchored, 25% always-on
            if rng.random() < 0.75 and themes_seasonal:
                theme = themes_seasonal[int(rng.integers(0, len(themes_seasonal)))]
                anchor_month = theme[1]
                # Start ~10-30d before anchor month
                lead_days = int(rng.integers(5, 25))
                anchor_day = int(rng.integers(1, 21))
                try:
                    anchor_date = date(year, anchor_month, anchor_day)
                except ValueError:
                    anchor_date = date(year, anchor_month, 15)
                start_date = anchor_date - timedelta(days=lead_days)
            else:
                theme = themes_always[int(rng.integers(0, len(themes_always)))]
                start_month = int(rng.integers(1, 13))
                start_day = int(rng.integers(1, 28))
                try:
                    start_date = date(year, start_month, start_day)
                except ValueError:
                    start_date = date(year, start_month, 15)

            # Skip campaigns 2020-03 to 2020-05 (ad freeze)
            if date(2020, 3, 1) <= start_date <= date(2020, 5, 31):
                if rng.random() < 0.95:
                    continue

            # Clamp to pipeline
            if start_date < PIPELINE_START or start_date > PIPELINE_END:
                continue

            duration = int(rng.integers(theme[2], theme[3] + 1))
            end_date = start_date + timedelta(days=duration)

            # Channel
            channel = str(rng.choice(names, p=channel_weights))

            # Budget — bimodal: $5-15K always-on vs $50-500K seasonal
            tier = theme[4]
            if tier == "seasonal" or rng.random() < 0.25:
                budget = float(rng.uniform(50_000, 500_000))
            else:
                budget = float(rng.uniform(5_000, 15_000))
            # AI boom doubles ad spend
            budget *= event_impact(start_date, "ad_spend")
            budget = round(budget, 2)
            spend = round(budget * float(rng.uniform(0.70, 1.10)), 2)

            # 70% target a product (eligible at start_date)
            product_key = None
            if rng.random() < 0.70:
                elig = (prod_first <= np.datetime64(start_date)) & (
                    np.isnat(prod_disc) | (prod_disc >= np.datetime64(start_date))
                )
                # Tilt toward AI products in 2023+
                if start_date.year >= 2023 and rng.random() < 0.30:
                    elig = elig & ai_brand_mask
                eligible_idx = np.flatnonzero(elig)
                if len(eligible_idx) > 0:
                    product_key = int(prod_keys[int(rng.choice(eligible_idx))])

            segment = str(rng.choice(CAMPAIGN_SEGMENTS,
                                     p=[0.35, 0.20, 0.20, 0.10, 0.15]))

            name = (
                f"{theme[0]} {start_date.year} - {channel} "
                f"({start_date.strftime('%b')})"
            )

            rows.append({
                "campaign_key": int(next(sparse_iter)),
                "campaign_name": name,
                "theme": theme[0],
                "channel": channel,
                "start_date": start_date,
                "end_date": end_date,
                "budget_amount": budget,
                "spend_amount": spend,
                "currency": "USD",
                "target_segment": segment,
                "product_key": product_key,
            })
    df = pd.DataFrame(rows)
    return df


# ---------------------------------------------------------------------------
# Reference data: employees
# ---------------------------------------------------------------------------

DEPT_HEADCOUNT_SHARE = {
    1: 0.02, 2: 0.32, 3: 0.07, 4: 0.05, 5: 0.04, 6: 0.16, 7: 0.10,
    8: 0.09, 9: 0.03, 10: 0.05, 11: 0.02, 12: 0.05,
}


def annual_target_headcount(year: int) -> int:
    """Total active employees by year."""
    landmarks = {
        2010: 60, 2012: 90, 2014: 150, 2016: 400, 2018: 900,
        2019: 1200, 2020: 1300, 2021: 1450, 2022: 1700, 2023: 2400,
        2024: 3200, 2025: 3300, 2026: 3300,
    }
    return landmarks.get(year, 60)


def build_employees() -> pd.DataFrame:
    """Build employee dimension. Disk-cached."""
    return _cached_parquet("employees", _build_employees)


def _build_employees() -> pd.DataFrame:
    rng = get_seeded_rng(43)
    fake = get_seeded_faker(43)
    stores = build_stores()
    store_keys = stores["store_key"].tolist()

    # Plan hire/term events year-by-year so headcount lands on landmarks
    records = []
    active_set: list[dict] = []
    next_emp_idx = 0
    sparse_iter = iter(sparse_keys(20_000, gap_rate=0.20, start=1001, seed=43))

    salary_ranges = {
        "L1": (35_000, 55_000), "L2": (55_000, 85_000), "L3": (85_000, 130_000),
        "L4": (130_000, 200_000), "L5": (200_000, 350_000),
    }
    level_dist = {"L1": 0.42, "L2": 0.32, "L3": 0.16, "L4": 0.08, "L5": 0.02}

    dept_titles = {
        1: ["Chief Executive", "Chief Officer", "VP", "Director"],
        2: ["Sales Associate", "Sales Manager", "Account Executive", "Regional Lead", "VP Sales"],
        3: ["Marketing Specialist", "Brand Manager", "Campaign Manager", "Director Marketing", "VP Marketing"],
        4: ["Accountant", "Financial Analyst", "Controller", "Director Finance", "VP Finance"],
        5: ["HR Coordinator", "Recruiter", "HR Manager", "Director HR"],
        6: ["Software Engineer", "Senior Engineer", "Staff Engineer", "Engineering Manager", "VP Engineering"],
        7: ["Support Agent", "Senior Support Agent", "Support Manager", "Director Support"],
        8: ["Operations Coordinator", "Operations Manager", "Logistics Lead", "Director Operations"],
        9: ["Paralegal", "Counsel", "General Counsel"],
        10: ["Product Analyst", "Product Manager", "Director Product"],
        11: ["Facilities Tech", "Facilities Manager"],
        12: ["Data Analyst", "Data Engineer", "Analytics Manager", "Director Data"],
    }

    levels_for_dept = {
        1: ["L4", "L5"],
        9: ["L2", "L3", "L4"],
        11: ["L1", "L2", "L3"],
        7: ["L1", "L2", "L3", "L4"],
    }

    fake_first = [fake.first_name() for _ in range(2000)]
    fake_last = [fake.last_name() for _ in range(2000)]

    for year in range(2010, 2027):
        target = annual_target_headcount(year)
        # Attrition rate
        attrition = 0.08
        if year == 2020:
            attrition = 0.12   # COVID layoffs (mostly April)
        elif year == 2022:
            attrition = 0.15   # great resignation
        elif year >= 2025:
            attrition = 0.07

        # Apply terminations
        n_term = int(len(active_set) * attrition)
        if n_term > 0 and len(active_set) > 0:
            term_idx = rng.choice(len(active_set), size=min(n_term, len(active_set)),
                                  replace=False)
            for idx in sorted(term_idx, reverse=True):
                rec = active_set.pop(idx)
                # Term date weighted toward April for 2020 layoffs
                if year == 2020:
                    term_month = int(rng.choice([4, 4, 4, 5, 6, 7, 8, 9, 10, 11]))
                else:
                    term_month = int(rng.integers(1, 13))
                term_day = int(rng.integers(1, 28))
                rec["termination_date"] = date(year, term_month, term_day)
                rec["status"] = "Terminated"
                records.append(rec)

        # Hire to target
        gap = target - len(active_set)
        if gap < 0:
            gap = 0  # already at target after attrition

        # Distribute hires by month — concentrate in regime-favored months
        hiring_mult_year = annual_target_headcount(year) / max(annual_target_headcount(year - 1), 1) - 1
        hire_dates_year = []
        for _ in range(gap):
            # Bias hiring to Jul/Aug (back to school) and Oct/Nov (Q4 retail) — for sales/support
            # Tech hires (eng/data/product) bias to Jan/Mar/Sept
            month = int(rng.choice(
                list(range(1, 13)),
                p=[0.06, 0.06, 0.08, 0.07, 0.07, 0.06, 0.13, 0.13, 0.10, 0.10, 0.08, 0.06]
            ))
            day = int(rng.integers(1, 28))
            try:
                hire_dates_year.append(date(year, month, day))
            except ValueError:
                hire_dates_year.append(date(year, month, 15))
            # Skip hires during COVID freeze
            if year == 2020 and 3 <= month <= 8:
                if rng.random() < 0.85:
                    hire_dates_year.pop()

        # Hire each
        for hd in hire_dates_year:
            dept_keys = list(DEPT_HEADCOUNT_SHARE.keys())
            dept_weights = list(DEPT_HEADCOUNT_SHARE.values())
            # Pre-2015 no Engineering/Product/Data
            if year < 2015:
                allowed = [d for d in dept_keys if d not in (6, 10, 12)]
                allowed_w = [DEPT_HEADCOUNT_SHARE[d] for d in allowed]
                allowed_w = np.array(allowed_w) / sum(allowed_w)
                dept_key = int(rng.choice(allowed, p=allowed_w))
            elif year >= 2023:
                # AI boom — boost Engineering/Data
                tilt = {6: 0.32, 12: 0.10, 10: 0.07}
                w = [tilt.get(d, DEPT_HEADCOUNT_SHARE[d] * 0.7) for d in dept_keys]
                w = np.array(w) / sum(w)
                dept_key = int(rng.choice(dept_keys, p=w))
            else:
                w = np.array(dept_weights) / sum(dept_weights)
                dept_key = int(rng.choice(dept_keys, p=w))

            allowed_levels = levels_for_dept.get(
                dept_key, ["L1", "L2", "L3", "L4", "L5"]
            )
            level_weights = np.array([level_dist.get(l, 0) for l in allowed_levels])
            level_weights = level_weights / level_weights.sum()
            level = str(rng.choice(allowed_levels, p=level_weights))

            sal_min, sal_max = salary_ranges[level]
            base_salary = float(rng.lognormal(
                mean=math.log((sal_min + sal_max) / 2), sigma=0.18
            ))
            base_salary = max(sal_min * 0.85, min(base_salary, sal_max * 1.1))
            # Cohort effect: 2020+ hires get 8% bump
            if year >= 2020:
                base_salary *= 1.08

            store_key = None
            if dept_key in (2, 7, 8, 11):
                # Filter to stores open by hire date
                eligible = [
                    s for s, od in zip(stores["store_key"],
                                       pd.to_datetime(stores["open_date"]))
                    if od.date() <= hd
                ]
                if eligible:
                    store_key = int(rng.choice(eligible))

            first = fake_first[int(rng.integers(0, len(fake_first)))]
            last = fake_last[int(rng.integers(0, len(fake_last)))]

            # 1.5% data quality issues
            email = f"{first.lower()}.{last.lower()}@contoso.com"
            if rng.random() < 0.015:
                email = email.replace("@", "")  # malformed

            rec = {
                "employee_key": next(sparse_iter),
                "first_name": first,
                "last_name": last,
                "email": email,
                "hire_date": hd,
                "termination_date": None,
                "department_key": dept_key,
                "store_key": store_key,
                "job_title": str(rng.choice(dept_titles[dept_key])),
                "level": level,
                "manager_key": None,  # filled below
                "salary": round(base_salary, 2),
                "currency": "USD",
                "status": "Active",
            }
            active_set.append(rec)

    # Append all currently-active records
    records.extend(active_set)

    df = pd.DataFrame(records)

    # Manager assignment: L1-L3 report to L3+ in same dept
    sf = df.copy().reset_index(drop=True)
    by_dept = {}
    for idx, r in sf.iterrows():
        if r["level"] in ("L3", "L4", "L5"):
            by_dept.setdefault(r["department_key"], []).append(int(r["employee_key"]))
    mgr_col = []
    for _, r in sf.iterrows():
        if r["level"] in ("L1", "L2", "L3"):
            cands = [m for m in by_dept.get(r["department_key"], [])
                     if m != r["employee_key"]]
            mgr_col.append(int(rng.choice(cands)) if cands else None)
        else:
            mgr_col.append(None)
    sf["manager_key"] = mgr_col

    # 1.5% NULL manager_key for non-leadership
    null_mgr_idx = rng.choice(len(sf), size=int(len(sf) * 0.015), replace=False)
    sf.loc[null_mgr_idx, "manager_key"] = None

    return sf
