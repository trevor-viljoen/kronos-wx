"""
HRRR virtual sounding: extract a SoundingProfile from HRRR pressure-level fields.

Uses the HRRR ``prs`` product (pressure-level data) to construct a vertical
sounding profile at an arbitrary lat/lon grid point.  The resulting
SoundingProfile passes through the same pipeline as a real radiosonde:

    extract_virtual_sounding_from_hrrr(run_str, fxx, lat, lon)
        → SoundingProfile(raw_source="hrrr-virtual")
        → compute_thermodynamic_indices()
        → compute_kinematic_profile()
        → ML model features (full 28-feature vector, no NaN imputation)

This closes the gap described in issue #18: for next-day forecasting, the
HRRR F12 (valid 12Z tomorrow) virtual sounding provides the same pre-convective
environment the models were trained on, derived from an NWP forecast grid
instead of a real radiosonde.

Fields fetched from HRRR prs product
--------------------------------------
  t   — temperature           K       (:TMP:[0-9]+ mb:)
  gh  — geopotential height   m       (:HGT:[0-9]+ mb:)
  u   — u-wind component      m/s     (:UGRD:[0-9]+ mb:)
  v   — v-wind component      m/s     (:VGRD:[0-9]+ mb:)
  q   — specific humidity     kg/kg   (:SPFH:[0-9]+ mb:)

Levels retained: 1000–200 mb (all isobaric levels in that range).
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np

from ..models.enums import OklahomaSoundingStation
from ..models.sounding import SoundingLevel, SoundingProfile
from .era5_diagnostics import (
    _nearest_sounding_station,
    _specific_humidity_to_dewpoint_c,
)

logger = logging.getLogger(__name__)

# Pressure level bounds: keep levels between these values (inclusive)
_LEVEL_MIN_HPA: float = 200.0
_LEVEL_MAX_HPA: float = 1000.0

# GRIB2 search patterns for HRRR prs product.
# `[0-9]+` matches any integer mb level without creating regex capture groups
# (which trigger a spurious UserWarning in herbie's pandas str.contains call).
_PRS_PATTERNS: dict[str, str] = {
    "t":  ":TMP:[0-9]+ mb:",
    "gh": ":HGT:[0-9]+ mb:",
    "u":  ":UGRD:[0-9]+ mb:",
    "v":  ":VGRD:[0-9]+ mb:",
    "q":  ":SPFH:[0-9]+ mb:",
}


def extract_virtual_sounding_from_hrrr(
    valid_time: datetime,
    fxx: int,
    lat: float,
    lon: float,
) -> Optional[SoundingProfile]:
    """
    Fetch HRRR pressure-level fields and extract a vertical sounding at (lat, lon).

    Parameters
    ----------
    valid_time : datetime (UTC) of the desired valid time.
    fxx        : Forecast hour offset (0 = analysis, 1–48 = forecast).
                 For 12Z next-day predictions, use fxx = 9–12 from a recent run.
    lat        : Target latitude (°N)
    lon        : Target longitude (negative = °W)

    Returns
    -------
    SoundingProfile with raw_source="hrrr-virtual", or None on failure.
    """
    try:
        from herbie import Herbie
    except ImportError:
        logger.error("herbie-data is not installed")
        return None

    run_time = valid_time - timedelta(hours=fxx)
    run_str  = run_time.strftime("%Y-%m-%d %H:%M")

    logger.debug(
        "HRRR virtual sounding: run=%s F%02d valid=%s at (%.2f, %.2f)",
        run_str, fxx, valid_time.strftime("%Y-%m-%d %H:%MZ"), lat, lon,
    )

    try:
        H = Herbie(run_str, model="hrrr", product="prs", fxx=fxx, verbose=False)
    except Exception as exc:
        logger.warning("Herbie init failed for prs run=%s F%02d: %s", run_str, fxx, exc)
        return None

    # ── Fetch each field ──────────────────────────────────────────────────────
    fields: dict[str, np.ndarray] = {}  # var_name → (n_levels,) array
    pres_arr: Optional[np.ndarray] = None
    lat2d: Optional[np.ndarray] = None
    lon2d: Optional[np.ndarray] = None
    yi = xi = None  # nearest-grid-point indices

    suffix = "anl:" if fxx == 0 else f"{fxx} hour fcst:"

    for var, base_pattern in _PRS_PATTERNS.items():
        pattern = base_pattern[:-1] + suffix  # append correct time-type
        try:
            ds = H.xarray(pattern, remove_grib=True)
        except Exception as exc:
            logger.debug("HRRR prs field '%s' unavailable: %s", var, exc)
            continue

        if "isobaricInhPa" not in ds.dims:
            logger.debug("HRRR prs '%s' has no isobaricInhPa dimension — skipping", var)
            continue

        # Build the nearest-grid-point lookup once from the first field
        if lat2d is None:
            lat2d = ds["latitude"].values   # (y, x)
            lon2d = ds["longitude"].values  # (y, x), 0-360
            target_lon = 360.0 + lon if lon < 0 else lon
            dist2 = (lat2d - lat) ** 2 + (lon2d - target_lon) ** 2
            yi, xi = np.unravel_index(np.argmin(dist2), dist2.shape)
            logger.debug(
                "HRRR prs grid point: y=%d x=%d  lat=%.2f lon=%.2f",
                yi, xi,
                float(lat2d[yi, xi]),
                float(lon2d[yi, xi]) - 360.0,
            )

        # Level coordinate (hPa, sorted descending by herbie)
        levels_hpa = ds["isobaricInhPa"].values.astype(float)

        # Filter to sounding-relevant levels only
        level_mask = (levels_hpa >= _LEVEL_MIN_HPA) & (levels_hpa <= _LEVEL_MAX_HPA)
        if pres_arr is None:
            pres_arr = levels_hpa[level_mask]
        else:
            # Align to the same levels we already committed to
            level_mask = np.isin(levels_hpa, pres_arr)

        # Extract profile at nearest grid point
        data_var = list(ds.data_vars)[0]
        col = ds[data_var].values[:, yi, xi]   # (n_levels,)
        fields[var] = col[level_mask]

    if pres_arr is None or "t" not in fields:
        logger.warning(
            "HRRR prs virtual sounding failed: insufficient fields for %s F%02d",
            run_str, fxx,
        )
        return None

    # ── Ensure consistent level ordering (decreasing pressure = surface first) ─
    order = np.argsort(pres_arr)[::-1]
    pres_arr = pres_arr[order]
    for k in list(fields.keys()):
        fields[k] = fields[k][order]

    # ── Build SoundingLevel objects ───────────────────────────────────────────
    levels: list[SoundingLevel] = []
    t_col  = fields.get("t",  np.full_like(pres_arr, np.nan))
    gh_col = fields.get("gh", np.full_like(pres_arr, np.nan))
    u_col  = fields.get("u",  np.zeros_like(pres_arr))
    v_col  = fields.get("v",  np.zeros_like(pres_arr))
    q_col  = fields.get("q",  np.zeros_like(pres_arr))

    for i, pres in enumerate(pres_arr):
        T_k  = float(t_col[i])
        gh   = float(gh_col[i])
        u    = float(u_col[i])
        v    = float(v_col[i])
        q    = float(q_col[i])

        if not math.isfinite(T_k) or T_k < 150.0 or T_k > 360.0:
            continue
        if not math.isfinite(gh):
            continue

        T_c  = T_k - 273.15
        Td_c = _specific_humidity_to_dewpoint_c(max(q, 0.0), pres)
        Td_c = min(Td_c, T_c)

        wind_kt  = math.sqrt(u ** 2 + v ** 2) * 1.94384
        wind_dir = math.degrees(math.atan2(-u, -v)) % 360.0

        levels.append(SoundingLevel(
            pressure=float(pres),
            height=gh,
            temperature=T_c,
            dewpoint=Td_c,
            wind_direction=wind_dir,
            wind_speed=wind_kt,
        ))

    if len(levels) < 10:
        logger.warning(
            "HRRR prs virtual sounding: only %d valid levels at (%.2f, %.2f) — too few",
            len(levels), lat, lon,
        )
        return None

    station = _nearest_sounding_station(lat, lon)
    vt_aware = valid_time if valid_time.tzinfo else valid_time.replace(tzinfo=timezone.utc)

    logger.info(
        "HRRR prs virtual sounding: %d levels  run=%s F%02d  station=%s",
        len(levels), run_str, fxx, station.value,
    )
    return SoundingProfile(
        station=station,
        valid_time=vt_aware,
        levels=levels,
        raw_source="hrrr-virtual",
    )


def find_best_hrrr_prs_run(
    valid_time: datetime,
    now_utc: datetime,
    min_fxx: int = 1,
    max_fxx: int = 48,
    max_candidates: int = 8,
) -> Optional[tuple[datetime, int]]:
    """
    Find the most recently initialized HRRR run whose F+fxx reaches valid_time.

    Returns (run_time, fxx) of the best posted run, or None if none found.
    A run is "posted" when now - run_time >= 60 minutes.
    """
    try:
        from herbie import Herbie as _Herbie
    except ImportError:
        return None

    now_floor  = now_utc.replace(minute=0, second=0, microsecond=0)
    posted_cut = now_floor - timedelta(hours=1)

    candidates: list[tuple[datetime, int]] = []
    for h_back in range(max_fxx):
        run_try = posted_cut - timedelta(hours=h_back)
        fxx_try = int((valid_time - run_try).total_seconds() / 3600)
        if min_fxx <= fxx_try <= max_fxx:
            candidates.append((run_try, fxx_try))

    for run_try, fxx_try in candidates[:max_candidates]:
        run_str = run_try.strftime("%Y-%m-%d %H:%M")
        try:
            H = _Herbie(run_str, model="hrrr", product="prs", fxx=fxx_try, verbose=False)
            if H.grib is not None:
                return (run_try, fxx_try)
        except Exception:
            continue

    return None
