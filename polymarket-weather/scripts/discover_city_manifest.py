"""Bootstrap helper for city_manifest.yml.

For each city in CITY_TARGETS, queries Meteostat for stations within ~30 km of the
city centre, looks up ICAO via station metadata, ranks each candidate by hourly
temperature completeness across Jan-Apr 2026, marks the station whose ICAO matches
the Polymarket primary as `role: primary`, and emits YAML to stdout.

Run from polymarket-weather/:

    python scripts/discover_city_manifest.py > /tmp/manifest.yml

Then hand-review the YAML before committing it to assets/polymarket_weather_raw/city_manifest.yml.
"""

from __future__ import annotations

import sys
import warnings
from datetime import datetime
from typing import Any

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

from meteostat import Point, hourly, stations  # type: ignore

WINDOW_START = datetime(2026, 1, 1)
WINDOW_END = datetime(2026, 4, 30, 23)
EXPECTED_HOURS = 24 * (WINDOW_END - WINDOW_START).days + 24
RADIUS_METRES = 30_000
MAX_CANDIDATES = 12
KEEP_PER_CITY = 5

CITY_TARGETS = [
    {
        "name": "London",
        "timezone": "Europe/London",
        "series_slug": "london-daily-weather",
        "primary_icao": "EGLC",
        "city_centre": (51.5074, -0.1278),
        "primary_centre": (51.5, 0.1167),  # London City Airport
        "openmeteo_grid": {"lat": 51.507, "lon": -0.128},
    },
    {
        "name": "Seoul",
        "timezone": "Asia/Seoul",
        "series_slug": "seoul-daily-weather",
        "primary_icao": "RKSI",
        "city_centre": (37.5665, 126.9780),
        "primary_centre": (37.469, 126.450),  # Incheon International Airport
        "openmeteo_grid": {"lat": 37.566, "lon": 126.978},
    },
    {
        "name": "Toronto",
        "timezone": "America/Toronto",
        "series_slug": "toronto-daily-weather",
        "primary_icao": "CYYZ",
        "city_centre": (43.6532, -79.3832),
        "primary_centre": (43.6772, -79.6306),  # Toronto Pearson International Airport
        "openmeteo_grid": {"lat": 43.653, "lon": -79.383},
    },
]

PARIS_ENTRY = {
    "name": "Paris",
    "timezone": "Europe/Paris",
    "series_slug": "paris-daily-weather",
    "primary_icao": "LFPG",
    "primary_station_id": "07157",
    "openmeteo_grid": {"lat": 48.857, "lon": 2.353},
    "stations": [
        {"id": "07157", "icao": "LFPG", "role": "primary", "lat": 49.010, "lon": 2.548, "name": "Paris-Charles de Gaulle"},
        {"id": "07150", "icao": "LFPB", "role": "peer", "lat": 48.969, "lon": 2.441, "name": "Paris / Le Bourget"},
        {"id": "07156", "icao": None, "role": "peer", "lat": 48.822, "lon": 2.338, "name": "Paris-Montsouris"},
        {"id": "07149", "icao": "LFPO", "role": "peer", "lat": 48.723, "lon": 2.379, "name": "Paris-Orly"},
        {"id": "07147", "icao": "LFPV", "role": "peer", "lat": 48.774, "lon": 2.197, "name": "Villacoublay"},
        {"id": "07145", "icao": "LFPT", "role": "peer", "lat": 48.774, "lon": 2.009, "name": "Trappes"},
    ],
}


def candidate_meta(station_id: str) -> dict[str, Any] | None:
    meta = stations.meta(station_id)
    if meta is None:
        return None
    ids = meta.identifiers or {}
    return {
        "id": meta.id,
        "name": meta.name,
        "icao": ids.get("icao"),
        "wmo": ids.get("wmo"),
        "lat": float(meta.latitude),
        "lon": float(meta.longitude),
        "elevation_m": meta.elevation,
        "timezone": meta.timezone,
    }


def fetch_completeness(station_id: str) -> tuple[int, float]:
    try:
        df = hourly(station_id, WINDOW_START, WINDOW_END).fetch()
    except Exception as e:  # noqa: BLE001
        print(f"  ! {station_id}: hourly fetch failed: {e}", file=sys.stderr)
        return 0, 0.0
    if df is None or df.empty or "temp" not in df.columns:
        return 0, 0.0
    rows = int(df["temp"].notna().sum())
    return rows, rows / EXPECTED_HOURS


def discover_city(target: dict[str, Any]) -> dict[str, Any]:
    name = target["name"]
    centre_lat, centre_lon = target["city_centre"]
    primary_icao = target["primary_icao"]
    primary_centre = target.get("primary_centre")
    print(f"# {name}: city_centre=({centre_lat}, {centre_lon}) primary={primary_icao}", file=sys.stderr)

    nearby = stations.nearby(Point(centre_lat, centre_lon), radius=RADIUS_METRES, limit=MAX_CANDIDATES)

    if primary_centre:
        airport = stations.nearby(Point(*primary_centre), radius=10_000, limit=5)
        for aid in airport.index:
            if aid not in nearby.index:
                row = airport.loc[aid].to_dict()
                row["distance"] = float(row.get("distance", 0.0)) + 1_000_000  # synthetic large distance to keep ordering stable
                nearby.loc[aid] = row

    candidates: list[dict[str, Any]] = []
    for sid, row in nearby.iterrows():
        meta = candidate_meta(str(sid))
        if not meta:
            continue
        rows, frac = fetch_completeness(str(sid))
        rec = {
            **meta,
            "distance_km": round(float(row.get("distance", 0.0)) / 1000.0, 1),
            "rows_2026q1q2": rows,
            "completeness": round(frac, 3),
        }
        candidates.append(rec)
        print(
            f"  {rec['id']:>8s} icao={rec['icao'] or '----':4s} dist={rec['distance_km']:.1f}km "
            f"rows={rows:5d} ({frac:.0%}) name={rec['name']}",
            file=sys.stderr,
        )

    primary = [c for c in candidates if c["icao"] == primary_icao]
    others = [c for c in candidates if c["icao"] != primary_icao]
    others.sort(key=lambda c: c["completeness"], reverse=True)

    if not primary:
        print(f"  WARNING: no candidate matched primary ICAO {primary_icao}; YAML will need manual edit", file=sys.stderr)

    chosen = primary + [o for o in others if o["completeness"] > 0.5][: KEEP_PER_CITY - len(primary)]
    chosen = chosen[:KEEP_PER_CITY]
    for c in chosen:
        c["role"] = "primary" if c["icao"] == primary_icao else "peer"

    return {
        "name": name,
        "timezone": target["timezone"],
        "series_slug": target["series_slug"],
        "primary_icao": primary_icao,
        "primary_station_id": primary[0]["id"] if primary else None,
        "openmeteo_grid": target["openmeteo_grid"],
        "stations": [
            {
                "id": c["id"],
                "icao": c["icao"],
                "role": c["role"],
                "lat": round(c["lat"], 4),
                "lon": round(c["lon"], 4),
                "name": c["name"],
                "completeness": c["completeness"],
            }
            for c in chosen
        ],
    }


def to_yaml(cities: list[dict[str, Any]]) -> str:
    lines = ["cities:"]
    for c in cities:
        lines.append(f"  - name: {c['name']}")
        lines.append(f"    timezone: {c['timezone']}")
        lines.append(f"    series_slug: {c['series_slug']}")
        lines.append(f"    primary_icao: {c['primary_icao']}")
        if c.get("primary_station_id"):
            lines.append(f"    primary_station_id: \"{c['primary_station_id']}\"")
        else:
            lines.append("    primary_station_id: null  # FIXME: no primary match")
        og = c["openmeteo_grid"]
        lines.append(f"    openmeteo_grid: {{lat: {og['lat']}, lon: {og['lon']}}}")
        lines.append("    stations:")
        for s in c["stations"]:
            icao = f"\"{s['icao']}\"" if s.get("icao") else "null"
            name = (s.get("name") or "").replace("\"", "'")
            comp = s.get("completeness")
            comp_str = f", completeness: {comp}" if comp is not None else ""
            lines.append(
                f"      - {{id: \"{s['id']}\", icao: {icao}, role: {s['role']}, "
                f"lat: {s['lat']}, lon: {s['lon']}, name: \"{name}\"{comp_str}}}"
            )
    return "\n".join(lines) + "\n"


def main():
    cities = [PARIS_ENTRY] + [discover_city(t) for t in CITY_TARGETS]
    print(to_yaml(cities))


if __name__ == "__main__":
    main()
