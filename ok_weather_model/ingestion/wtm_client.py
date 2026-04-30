"""
Multi-network surface observations via the Texas Mesonet public API.

Uses the CurrentDataAllSites endpoint which aggregates 200+ stations across
TX, OK, NM, KS, LA, CO, and northern Mexico from networks including:
  WTEXAS (West Texas Mesonet/TTU), TWDB, NWS/FAA (ASOS/AWOS),
  RAWS, LCRA, CRN, HADS, MEXICO

No API key required.
API base: https://www.texasmesonet.org/api
"""
from __future__ import annotations

import logging
import math

import httpx

logger = logging.getLogger(__name__)

_URL = "https://www.texasmesonet.org/api/CurrentDataAllSites"

# Exclude offshore platforms — no value for inland severe wx analysis.
_EXCLUDE_NETWORKS = {"NOS-NWLON"}


def _c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def _ms_to_mph(ms: float) -> float:
    return ms * 2.23694


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
    Fetch current observations from all Texas Mesonet partner networks.

    Returns a list of dicts with the same shape as OK Mesonet display_obs:
        station_id, lat, lon, temp_f, dewpoint_f,
        wind_dir, wind_speed, wind_gust
    """
    try:
        resp = httpx.get(_URL, timeout=20.0)
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
            logger.debug("Skipping station %s: %s", r.get("station", "?"), exc)
            continue

    networks = {r.get("mesonet") for r in records if r.get("mesonet") not in _EXCLUDE_NETWORKS}
    logger.info("Texas Mesonet: %d stations from networks: %s", len(results), ", ".join(sorted(str(n) for n in networks if n)))
    return results
