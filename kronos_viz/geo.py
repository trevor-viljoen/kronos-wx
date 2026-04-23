"""
Geographic reference data for the Oklahoma domain.

Provides state and county boundary polylines as (lon, lat) arrays suitable
for drawing as Scatter3d traces at z=0 (the "floor" of the 3D scene).

Boundary data is fetched from the US Census Bureau TIGER GeoJSON service on
first use and cached in-process.  A minimal hardcoded fallback (OK + TX + KS
simplified outlines) is used if the network is unavailable.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from functools import lru_cache
from typing import Iterator

import numpy as np

logger = logging.getLogger(__name__)

# Oklahoma domain bounding box — same as ERA5Client.OK_BBOX
DOMAIN_LON_MIN, DOMAIN_LON_MAX = -103.0, -94.5
DOMAIN_LAT_MIN, DOMAIN_LAT_MAX = 33.5, 37.0

# States to render — FIPS codes for OK + its 6 neighbors
_STATE_FIPS = {"40", "48", "20", "08", "35", "29", "05"}  # OK TX KS CO NM MO AR

# Census Bureau low-resolution state boundaries (20m)
_STATES_URL = (
    "https://raw.githubusercontent.com/plotly/datasets/master/"
    "geojson-counties-fips.json"
)

# Simpler fallback — Plotly's bundled US-states GeoJSON
_STATES_FALLBACK_URL = (
    "https://raw.githubusercontent.com/python-visualization/folium/"
    "master/examples/data/us-states.json"
)


# ── Simplified fallback domain outline ───────────────────────────────────────
# Coarse polygon ring for the Oklahoma-domain extent plus state borders.
# Used only when both network fetches fail.

_OK_SIMPLIFIED = np.array([
    [-103.0, 37.0], [-94.5, 37.0], [-94.5, 36.5], [-94.6, 35.0],
    [-94.4, 33.6], [-96.0, 33.6], [-97.0, 33.7], [-99.0, 34.0],
    [-100.0, 34.0], [-100.0, 36.5], [-100.0, 37.0], [-103.0, 37.0],
])

_KS_SIMPLIFIED = np.array([
    [-102.05, 40.0], [-94.6, 39.1], [-94.6, 37.0], [-100.0, 37.0],
    [-102.05, 37.0], [-102.05, 40.0],
])

_TX_PANHANDLE = np.array([
    [-103.0, 36.5], [-100.0, 36.5], [-100.0, 34.0], [-103.0, 34.0],
    [-103.0, 36.5],
])


def _segments_from_polygon_ring(coords: list[list[float]]) -> tuple[list[float], list[float]]:
    """Convert a GeoJSON coordinate ring to (lons, lats) with NaN breaks."""
    if not coords:
        return [], []
    lons: list[float] = []
    lats: list[float] = []
    for lon, lat in coords:
        lons.append(lon)
        lats.append(lat)
    lons.append(float("nan"))
    lats.append(float("nan"))
    return lons, lats


def _extract_from_geojson(geojson: dict, fips_set: set[str]) -> tuple[list[float], list[float]]:
    """Walk a GeoJSON FeatureCollection and extract boundary polylines."""
    all_lons: list[float] = []
    all_lats: list[float] = []

    for feature in geojson.get("features", []):
        fips = feature.get("id") or feature.get("properties", {}).get("id", "")
        fips_str = str(fips).zfill(2)[:2]
        if fips_str not in fips_set:
            # Try the name-based approach for folium-style GeoJSON
            name = feature.get("properties", {}).get("name", "")
            _name_fips = {
                "Oklahoma": "40", "Texas": "48", "Kansas": "20",
                "Colorado": "08", "New Mexico": "35", "Missouri": "29",
                "Arkansas": "05",
            }
            if name not in _name_fips:
                continue

        geom = feature.get("geometry", {})
        gtype = geom.get("type", "")
        coords_list = geom.get("coordinates", [])

        if gtype == "Polygon":
            for ring in coords_list:
                lons, lats = _segments_from_polygon_ring(ring)
                all_lons.extend(lons)
                all_lats.extend(lats)
        elif gtype == "MultiPolygon":
            for polygon in coords_list:
                for ring in polygon:
                    lons, lats = _segments_from_polygon_ring(ring)
                    all_lons.extend(lons)
                    all_lats.extend(lats)

    return all_lons, all_lats


@lru_cache(maxsize=1)
def get_state_boundaries() -> tuple[list[float], list[float]]:
    """
    Return (lons, lats) arrays for state boundary polylines in the
    Oklahoma domain, suitable for Scatter3d at z=0.

    NaN values separate disjoint segments (standard Plotly convention).
    Falls back to a simplified hardcoded outline if network is unavailable.
    """
    for url in (_STATES_FALLBACK_URL,):
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                geojson = json.loads(resp.read())
            lons, lats = _extract_from_geojson(geojson, _STATE_FIPS)
            if len(lons) > 10:
                logger.debug("Loaded state boundaries from %s (%d segments)", url, len(lons))
                return lons, lats
        except Exception as exc:
            logger.warning("Could not fetch state boundaries from %s: %s", url, exc)

    # Hardcoded fallback
    logger.info("Using simplified hardcoded state outlines")
    all_lons: list[float] = []
    all_lats: list[float] = []
    for arr in (_OK_SIMPLIFIED, _KS_SIMPLIFIED, _TX_PANHANDLE):
        all_lons.extend(arr[:, 0].tolist() + [float("nan")])
        all_lats.extend(arr[:, 1].tolist() + [float("nan")])
    return all_lons, all_lats


@lru_cache(maxsize=1)
def get_ok_counties() -> tuple[list[float], list[float]]:
    """
    Return Oklahoma county boundary polylines.

    Fetches from Census TIGER at 500k resolution.  Falls back silently to
    empty arrays (county lines are supplementary context, not required).
    """
    url = (
        "https://raw.githubusercontent.com/plotly/datasets/master/"
        "geojson-counties-fips.json"
    )
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            geojson = json.loads(resp.read())

        all_lons: list[float] = []
        all_lats: list[float] = []
        for feature in geojson.get("features", []):
            fips = str(feature.get("id", "")).zfill(5)
            if not fips.startswith("40"):  # Oklahoma state FIPS = 40
                continue
            geom = feature.get("geometry", {})
            gtype = geom.get("type", "")
            for coords_item in (
                geom.get("coordinates", [])
                if gtype == "Polygon"
                else [p for poly in geom.get("coordinates", []) for p in poly]
            ):
                lons, lats = _segments_from_polygon_ring(coords_item)
                all_lons.extend(lons)
                all_lats.extend(lats)
        return all_lons, all_lats
    except Exception as exc:
        logger.debug("County boundaries unavailable: %s", exc)
        return [], []


# Named reference points for annotation
REFERENCE_CITIES = {
    "Oklahoma City": (-97.52, 35.47),
    "Tulsa":         (-95.99, 36.15),
    "Amarillo":      (-101.83, 35.22),
    "Dodge City":    (-99.97, 37.76),
    "Norman (OUN)":  (-97.44, 35.22),
    "Lamont (LMN)":  (-97.49, 36.71),
}
