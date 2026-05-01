"""
Surface observations from two sources:

1. Texas Mesonet TWDB (texasmesonet.org) — 200+ stations across TX/OK/NM/KS/LA/CO
   from NWS/FAA (ASOS), TWDB, RAWS, LCRA, CRN, HADS, MEXICO networks.

2. West Texas Mesonet TTU (api.mesonet.ttu.edu) — 180 authoritative WTM stations
   in TX/NM/CO with direct dewpoint, 10m wind speed, and wind barb direction.

Both are public, no API key required.
"""
from __future__ import annotations

import logging
import math

import httpx

logger = logging.getLogger(__name__)

_TWDB_URL = "https://www.texasmesonet.org/api/CurrentDataAllSites"
_TTU_URL  = "https://api.mesonet.ttu.edu/mesoweb/public/map/latest/"

# Exclude offshore platforms — no value for inland severe wx analysis.
_EXCLUDE_NETWORKS = {"NOS-NWLON"}


def _c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def _ms_to_mph(ms: float) -> float:
    return ms * 2.23694


def _kts_to_mph(kts: float) -> float:
    return kts * 1.15078


def _dewpoint_f(temp_c: float, rh: float) -> float:
    """Magnus-Tetens dewpoint from temperature (°C) and relative humidity (%)."""
    a, b = 17.625, 243.04
    gamma = math.log(max(rh, 1.0) / 100.0) + a * temp_c / (b + temp_c)
    return _c_to_f(b * gamma / (a - gamma))


def _fval(raw) -> float | None:
    """Coerce a raw JSON value (string or number) to float, or None if missing/invalid."""
    if raw is None:
        return None
    try:
        f = float(raw)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def fetch_texas_mesonet_observations() -> list[dict]:
    """
    Fetch current observations from all Texas Mesonet partner networks (TWDB).

    Returns a list of dicts: station_id, lat, lon, temp_f, dewpoint_f,
    wind_dir, wind_speed, wind_gust
    """
    try:
        resp = httpx.get(_TWDB_URL, timeout=20.0)
        resp.raise_for_status()
        body = resp.json()
    except httpx.HTTPStatusError as exc:
        logger.warning("Texas Mesonet API HTTP %s", exc.response.status_code)
        return []
    except Exception as exc:
        logger.warning("Texas Mesonet fetch error: %s", exc)
        return []

    records = body.get("data") if isinstance(body, dict) else body
    if not isinstance(records, list):
        logger.warning("Texas Mesonet: unexpected response shape")
        return []

    results: list[dict] = []
    for r in records:
        try:
            if r.get("mesonet") in _EXCLUDE_NETWORKS:
                continue

            lat = _fval(r.get("latitude"))
            lon = _fval(r.get("longitude"))
            if lat is None or lon is None:
                continue

            stid = r.get("station") or r.get("stationAbbreviation") or str(r.get("objectId", ""))
            if not stid:
                continue

            temp_c = _fval(r.get("airTemp"))
            rh     = _fval(r.get("humidity"))
            wspd   = _fval(r.get("windSpeed"))
            wdir   = _fval(r.get("windDirection"))
            wgust  = _fval(r.get("windGust"))

            if any(v is None for v in (temp_c, rh, wspd, wdir)):
                continue
            if rh <= 0:                          # type: ignore[operator]
                continue

            results.append({
                "station_id": str(stid),
                "lat":        lat,
                "lon":        lon,
                "temp_f":     round(_c_to_f(temp_c), 1),          # type: ignore[arg-type]
                "dewpoint_f": round(_dewpoint_f(temp_c, rh), 1),  # type: ignore[arg-type]
                "wind_dir":   round(wdir),                        # type: ignore[arg-type]
                "wind_speed": round(_ms_to_mph(wspd), 1),         # type: ignore[arg-type]
                "wind_gust":  round(_ms_to_mph(wgust), 1) if wgust is not None else None,
            })
        except Exception as exc:
            logger.debug("Skipping TWDB station %s: %s", r.get("station", "?"), exc)
            continue

    networks = {r.get("mesonet") for r in records if r.get("mesonet") not in _EXCLUDE_NETWORKS}
    logger.info("TWDB: %d stations from: %s", len(results), ", ".join(sorted(str(n) for n in networks if n)))
    return results


def fetch_wtm_ttu_observations() -> list[dict]:
    """
    Fetch current observations from the authoritative West Texas Mesonet TTU API.

    GeoJSON FeatureCollection — 180 stations in TX/NM/CO.
    Direct dewpoint (dp1p5m), 10m wind speed (wspd10m m/s),
    wind barb array (wbarb: [speed_knots, direction_deg]).

    Returns a list of dicts: station_id, lat, lon, temp_f, dewpoint_f,
    wind_dir, wind_speed, wind_gust
    """
    try:
        resp = httpx.get(_TTU_URL, timeout=20.0)
        resp.raise_for_status()
        body = resp.json()
    except httpx.HTTPStatusError as exc:
        logger.warning("WTM TTU API HTTP %s", exc.response.status_code)
        return []
    except Exception as exc:
        logger.warning("WTM TTU fetch error: %s", exc)
        return []

    features = body.get("features", []) if isinstance(body, dict) else []
    if not features:
        logger.warning("WTM TTU: no features in response")
        return []

    results: list[dict] = []
    for feat in features:
        try:
            props = feat.get("properties", {})
            geom  = feat.get("geometry", {})

            if not props.get("isCur", True):
                continue  # stale observation

            coords = geom.get("coordinates", [])
            if len(coords) < 2:
                # Fall back to lat/lon in properties
                lat = _fval(props.get("lat"))
                lon = _fval(props.get("lon"))
            else:
                lon, lat = _fval(coords[0]), _fval(coords[1])

            if lat is None or lon is None:
                continue

            stid   = props.get("mid", "")
            temp_c = _fval(props.get("temp1p5m"))
            dp_c   = _fval(props.get("dp1p5m"))    # direct dewpoint — no calculation needed
            wspd   = _fval(props.get("wspd10m"))   # m/s
            wgust  = _fval(props.get("wgust10m"))  # m/s

            # wbarb: [speed_knots, direction_deg]
            wbarb = props.get("wbarb")
            wdir  = _fval(wbarb[1]) if isinstance(wbarb, list) and len(wbarb) >= 2 else None

            if any(v is None for v in (temp_c, dp_c, wspd, wdir)):
                continue

            results.append({
                "station_id": str(stid),
                "lat":        lat,
                "lon":        lon,
                "temp_f":     round(_c_to_f(temp_c), 1),   # type: ignore[arg-type]
                "dewpoint_f": round(_c_to_f(dp_c), 1),     # type: ignore[arg-type]
                "wind_dir":   round(wdir),                  # type: ignore[arg-type]
                "wind_speed": round(_ms_to_mph(wspd), 1),  # type: ignore[arg-type]
                "wind_gust":  round(_ms_to_mph(wgust), 1) if wgust is not None else None,
            })
        except Exception as exc:
            logger.debug("Skipping WTM station %s: %s", feat.get("properties", {}).get("mid", "?"), exc)
            continue

    logger.info("WTM TTU: %d stations loaded", len(results))
    return results
