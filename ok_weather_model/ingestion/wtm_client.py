"""
Texas Mesonet observations via the Texas Water Development Board public API.

No API key required. Endpoint returns current observations from 20+ networks
across Texas — includes West Texas Mesonet, NWS/FAA, LCRA, and others.

API base: https://www.texasmesonet.org/api
"""
from __future__ import annotations

import logging
import math

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.texasmesonet.org/api/CurrentData"

# Rough bounding box for stations relevant to Oklahoma severe weather:
# captures West Texas dryline source region + moisture return corridor.
# Excludes deep south Texas and far east Texas to keep the plot readable.
_LAT_MIN = 25.5
_LAT_MAX = 37.0
_LON_MIN = -106.5
_LON_MAX = -93.5


def _c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def _ms_to_mph(ms: float) -> float:
    return ms * 2.23694


def _dewpoint_f(temp_c: float, rh: float) -> float:
    """Magnus-Tetens dewpoint from temperature (°C) and relative humidity (%)."""
    a, b = 17.625, 243.04
    gamma = math.log(max(rh, 1.0) / 100.0) + a * temp_c / (b + temp_c)
    td_c = b * gamma / (a - gamma)
    return _c_to_f(td_c)


def _pick(*values) -> float | None:
    """Return the first non-None, finite value; coerces strings to float."""
    for v in values:
        if v is None:
            continue
        try:
            f = float(v)
            if math.isfinite(f):
                return f
        except (TypeError, ValueError):
            continue
    return None


def fetch_texas_mesonet_observations() -> list[dict]:
    """
    Fetch current Texas Mesonet observations.

    Returns a list of dicts with the same shape as OK Mesonet display_obs:
        station_id, lat, lon, temp_f, dewpoint_f,
        wind_dir, wind_speed, wind_gust
    """
    try:
        resp = httpx.get(_BASE_URL, timeout=20.0)
        resp.raise_for_status()
        records = resp.json()
    except httpx.HTTPStatusError as exc:
        logger.warning("Texas Mesonet API HTTP %s", exc.response.status_code)
        return []
    except Exception as exc:
        logger.warning("Texas Mesonet fetch error: %s", exc)
        return []

    # Response is {"units": {...}, "data": [...]}
    if isinstance(records, dict):
        records = records.get("data", [])
    if not isinstance(records, list):
        logger.warning("Texas Mesonet: unexpected response shape")
        return []

    results: list[dict] = []
    for r in records:
        try:
            lat = r.get("latitude")
            lon = r.get("longitude")
            if lat is None or lon is None:
                continue
            lat, lon = float(lat), float(lon)

            # Spatial filter — only stations relevant to OK severe wx coverage
            if not (_LAT_MIN <= lat <= _LAT_MAX and _LON_MIN <= lon <= _LON_MAX):
                continue

            stid = r.get("displayId") or r.get("name") or str(r.get("stationId", ""))
            if not stid:
                continue

            # Temperature: prefer 2m, fall back to primary
            temp_c = _pick(r.get("airTemp2m"), r.get("airTemp"))
            if temp_c is None:
                continue

            rh = r.get("humidity")
            if rh is None or not math.isfinite(float(rh)) or float(rh) <= 0:
                continue
            rh = float(rh)

            # Wind: prefer 2m (most consistently reported), fall back to primary
            wspd_ms  = _pick(r.get("windSpeed2m"),    r.get("windSpeed"))
            wdir     = _pick(r.get("windDirection2m"), r.get("windDirection"))
            wgust_ms = _pick(r.get("windGust2m"),     r.get("windGust"))

            if wspd_ms is None or wdir is None:
                continue

            results.append({
                "station_id": str(stid),
                "lat":        lat,
                "lon":        lon,
                "temp_f":     round(_c_to_f(float(temp_c)), 1),
                "dewpoint_f": round(_dewpoint_f(float(temp_c), rh), 1),
                "wind_dir":   round(float(wdir)),
                "wind_speed": round(_ms_to_mph(float(wspd_ms)), 1),
                "wind_gust":  round(_ms_to_mph(float(wgust_ms)), 1) if wgust_ms is not None else None,
            })
        except Exception as exc:
            logger.debug("Skipping TX station %s: %s", r.get("displayId", "?"), exc)
            continue

    logger.info("Texas Mesonet: %d stations in domain", len(results))
    return results
