"""
West Texas Mesonet observations via Synoptic Data API.

Requires a free Synoptic API token: https://synopticdata.com/
Set SYNOPTIC_API_KEY in .env. If not set, WTM obs are silently skipped.
"""
from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)

_SYNOPTIC_BASE = "https://api.synopticdata.com/v2"


def fetch_wtm_observations(api_key: str) -> list[dict]:
    """
    Fetch current West Texas Mesonet observations from the Synoptic Data API.

    Returns a list of dicts with the same shape as the Oklahoma Mesonet
    display_obs entries produced by MesonetClient._parse_mdf_raw():
        station_id, lat, lon, temp_f, dewpoint_f,
        wind_dir, wind_speed, wind_gust
    """
    url = f"{_SYNOPTIC_BASE}/stations/latest"
    params = {
        "token":   api_key,
        "network": "wtm",
        "vars":    "air_temp,dew_point_temperature,wind_speed,wind_direction,wind_gust",
        "units":   "english",   # °F and mph
        "output":  "json",
    }
    try:
        resp = httpx.get(url, params=params, timeout=20.0)
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        logger.warning("Synoptic API HTTP %s fetching WTM obs", exc.response.status_code)
        return []
    except Exception as exc:
        logger.warning("Synoptic API fetch error: %s", exc)
        return []

    data = resp.json()
    summary = data.get("SUMMARY", {})
    if summary.get("RESPONSE_CODE") != 1:
        logger.warning("Synoptic API error: %s", summary.get("RESPONSE_MESSAGE"))
        return []

    results: list[dict] = []
    for stn in data.get("STATION", []):
        try:
            lat  = float(stn["LATITUDE"])
            lon  = float(stn["LONGITUDE"])
            stid = stn["STID"]
            obs  = stn.get("OBSERVATIONS", {})

            def _val(key: str) -> float | None:
                raw = obs.get(f"{key}_value_1")
                if raw is None:
                    return None
                try:
                    return float(raw)
                except (TypeError, ValueError):
                    return None

            temp  = _val("air_temp")
            dewpt = _val("dew_point_temperature")
            wdir  = _val("wind_direction")
            wspd  = _val("wind_speed")
            wgust = _val("wind_gust")

            if any(v is None for v in (temp, dewpt, wdir, wspd)):
                continue

            results.append({
                "station_id": stid,
                "lat":        lat,
                "lon":        lon,
                "temp_f":     round(temp, 1),      # type: ignore[arg-type]
                "dewpoint_f": round(dewpt, 1),     # type: ignore[arg-type]
                "wind_dir":   round(wdir),         # type: ignore[arg-type]
                "wind_speed": round(wspd, 1),      # type: ignore[arg-type]
                "wind_gust":  round(wgust, 1) if wgust is not None else None,
            })
        except Exception as exc:
            logger.debug("Skipping WTM station %s: %s", stn.get("STID", "?"), exc)
            continue

    logger.info("WTM: %d stations loaded", len(results))
    return results
