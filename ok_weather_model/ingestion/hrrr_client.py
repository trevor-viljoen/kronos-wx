"""
HRRR analysis client for Oklahoma severe weather parameters.

Fetches HRRR F00 (analysis) fields from the NOAA AWS archive using herbie,
extracts values at all 77 Oklahoma county centroids, and returns a
HRRRCountySnapshot.

Data availability
-----------------
The NOAA AWS archive (s3://noaa-hrrr-bdp-pds/) holds HRRR data from
approximately July 2016 onward with good reliability.  Data from
September 2014 – June 2016 is patchy.  Pre-2014 cases cannot be
enriched with HRRR data.

Fields extracted from the HRRR sfc product
-------------------------------------------
  MLCAPE   :CAPE:90-0 mb above ground        J/kg
  MLCIN    :CIN:90-0 mb above ground         J/kg  (stored as positive)
  SBCAPE   :CAPE:surface                     J/kg
  SBCIN    :CIN:surface                      J/kg  (stored as positive)
  SRH_0_3km :HLCY:3000-0 m above ground      m²/s²
  SRH_0_1km :HLCY:1000-0 m above ground      m²/s²
  BWD_0_6km  computed from VUCSH+VVCSH       kt
  lapse_rate_700_500  computed from TMP/HGT  °C/km
  dewpoint_2m_F  :DPT:2 m above ground       °F
  LCL_height_m   :HGT:cloud base             m AGL

Derived
-------
  EHI = (MLCAPE × SRH_0_3km) / 160,000
  STP = (MLCAPE/1500) × ((2000−LCL)/1000) × (SRH_0_1km/150) × (BWD_0_6km/40)
        (STP = 0 when LCL > 2000 m or MLCIN > 200 J/kg)

Caching
-------
herbie caches downloaded GRIB2 subsets locally (~/.local/share/herbie/ by
default, or HERBIE_CACHE_DIR in the environment).  Re-running the same
query is instant after the first download.
"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime, timezone
from typing import Optional

import numpy as np

from ..models.enums import OklahomaCounty
from ..models.hrrr import HRRRCountyPoint, HRRRCountySnapshot

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# HRRR archive is reliable from this date onward on AWS
_ARCHIVE_START = date(2016, 7, 15)

# Oklahoma bounding box — 0-360 longitude convention used by HRRR
_OK_LAT_MIN:     float = 33.5
_OK_LAT_MAX:     float = 37.5
_OK_LON_MIN_360: float = 256.0    # ≈ -104°W
_OK_LON_MAX_360: float = 266.5    # ≈ -93.5°W

# m/s → knots
_MS_TO_KT: float = 1.94384

# K → °C offset (not needed since we take differences for lapse rate)
_K_TO_C: float = 273.15


# ── Search strings (herbie regex against the GRIB2 index) ────────────────────
# Each group fetches one Dataset; fields at compatible levels can share a fetch.

_SRCH: dict[str, str] = {
    "mlcape":   ":CAPE:90-0 mb above ground:anl:",
    "mlcin":    ":CIN:90-0 mb above ground:anl:",
    "sbcape":   ":CAPE:surface:anl:",
    "sbcin":    ":CIN:surface:anl:",
    "hlcy_3km": ":HLCY:3000-0 m above ground:anl:",
    "hlcy_1km": ":HLCY:1000-0 m above ground:anl:",
    "vucsh6":   ":VUCSH:0-6000 m above ground:anl:",
    "vvcsh6":   ":VVCSH:0-6000 m above ground:anl:",
    "tmp500":   ":TMP:500 mb:anl:",
    "tmp700":   ":TMP:700 mb:anl:",
    "hgt500":   ":HGT:500 mb:anl:",
    "hgt700":   ":HGT:700 mb:anl:",
    "dpt2m":    ":DPT:2 m above ground:anl:",
    "cldbase":  ":HGT:cloud base:anl:",
}


# ── Internal helpers ──────────────────────────────────────────────────────────

def _first_var(ds) -> str:
    """Return the name of the first data variable in an xarray Dataset."""
    return list(ds.data_vars)[0]


def _build_county_lookup(
    lat2d: np.ndarray,
    lon2d: np.ndarray,
) -> dict[OklahomaCounty, tuple[int, int]]:
    """
    Build a nearest-neighbor lookup table mapping each OklahomaCounty to the
    (row, col) index of the closest HRRR grid point.

    Searches only within the Oklahoma bounding box (~47k points) rather than
    the full 1.9M-point CONUS grid for efficiency.
    """
    # Mask to Oklahoma bounding box
    ok_mask = (
        (lat2d >= _OK_LAT_MIN) & (lat2d <= _OK_LAT_MAX) &
        (lon2d >= _OK_LON_MIN_360) & (lon2d <= _OK_LON_MAX_360)
    )
    rows, cols = np.where(ok_mask)
    ok_lats = lat2d[rows, cols]
    ok_lons = lon2d[rows, cols]   # 0-360

    lookup: dict[OklahomaCounty, tuple[int, int]] = {}
    for county in OklahomaCounty:
        c_lat = county.lat
        c_lon = 360.0 + county.lon   # convert −W to 0-360
        dist = (ok_lats - c_lat) ** 2 + (ok_lons - c_lon) ** 2
        best = int(dist.argmin())
        lookup[county] = (int(rows[best]), int(cols[best]))

    return lookup


def _extract(
    ds,
    lookup: dict[OklahomaCounty, tuple[int, int]],
) -> dict[OklahomaCounty, float]:
    """Extract the first data variable's value at each county grid point."""
    arr = ds[_first_var(ds)].values
    return {county: float(arr[i, j]) for county, (i, j) in lookup.items()}


def _compute_stp(
    mlcape: float,
    mlcin: float,
    lcl_m: Optional[float],
    srh_1km: float,
    bwd_6km_kt: float,
) -> Optional[float]:
    """
    Compute Significant Tornado Parameter (STP).

    Formula: (MLCAPE/1500) × ((2000−LCL)/1000) × (SRH_0_1km/150) × (BWD_6km/40)

    Returns 0 when LCL > 2000 m or MLCIN > 200 J/kg (strongly capped or
    high cloud base environment where the formula is not meaningful).
    Returns None when LCL is not available.
    """
    if lcl_m is None:
        return None
    if mlcin > 200 or lcl_m > 2000:
        return 0.0
    lcl_term = max(0.0, (2000.0 - lcl_m) / 1000.0)
    stp = (mlcape / 1500.0) * lcl_term * (srh_1km / 150.0) * (bwd_6km_kt / 40.0)
    return max(0.0, stp)


# ── Public API ────────────────────────────────────────────────────────────────

class HRRRClient:
    """
    Fetches HRRR analysis fields and extracts them at Oklahoma county centroids.

    Usage
    -----
    >>> with HRRRClient() as hc:
    ...     snap = hc.get_county_snapshot(datetime(2026, 4, 23, 12, tzinfo=timezone.utc))
    ...     oun_pt = snap.get(OklahomaCounty.CLEVELAND)
    ...     print(oun_pt.MLCAPE, oun_pt.SRH_0_1km)

    The client is also usable without the context manager; the context manager
    form is provided for consistency with SoundingClient / MesonetClient.
    """

    def __enter__(self) -> "HRRRClient":
        return self

    def __exit__(self, *_) -> None:
        pass

    # ── Core fetch ────────────────────────────────────────────────────────────

    def get_county_snapshot(
        self,
        valid_time: datetime,
        fxx: int = 0,
    ) -> Optional[HRRRCountySnapshot]:
        """
        Fetch HRRR analysis and extract severe weather fields at all 77
        Oklahoma county centroids.

        Parameters
        ----------
        valid_time : UTC datetime of the desired analysis valid time.
                     Must be ≥ ARCHIVE_START (2016-07-15).
        fxx        : Forecast hour.  0 = analysis (recommended).

        Returns
        -------
        HRRRCountySnapshot, or None if the run is not in the AWS archive.
        """
        try:
            from herbie import Herbie
        except ImportError:
            logger.error("herbie-data is not installed. Run: pip install herbie-data")
            return None

        if valid_time.date() < _ARCHIVE_START:
            logger.debug(
                "HRRR archive starts %s; %s is before that — skipping",
                _ARCHIVE_START, valid_time.date(),
            )
            return None

        # herbie wants a naive or tz-aware datetime; it normalizes to UTC
        vt_str = valid_time.strftime("%Y-%m-%d %H:%M")
        logger.debug("Fetching HRRR %s F%02d", vt_str, fxx)

        try:
            H = Herbie(vt_str, model="hrrr", product="sfc", fxx=fxx, verbose=False)
        except Exception as exc:
            logger.warning("HRRR Herbie init failed for %s: %s", vt_str, exc)
            return None

        # ── Fetch each field; build county lookup on first success ────────────
        run_time_dt = H.date  # datetime of model run
        if not isinstance(run_time_dt, datetime):
            run_time_dt = datetime.fromisoformat(str(run_time_dt))
        if run_time_dt.tzinfo is None:
            run_time_dt = run_time_dt.replace(tzinfo=timezone.utc)

        raw: dict[str, dict[OklahomaCounty, float]] = {}
        lookup: Optional[dict] = None

        for key, srch in _SRCH.items():
            try:
                ds = H.xarray(srch, remove_grib=True)
            except Exception as exc:
                logger.debug("HRRR field '%s' not available for %s: %s", key, vt_str, exc)
                continue

            # Build the county lookup once from the first successful field
            if lookup is None:
                lat2d = ds["latitude"].values
                lon2d = ds["longitude"].values
                lookup = _build_county_lookup(lat2d, lon2d)
                logger.debug("County lookup built from '%s' field", key)

            raw[key] = _extract(ds, lookup)

        if lookup is None or not raw:
            logger.warning("No HRRR fields could be loaded for %s", vt_str)
            return None

        # ── Assemble per-county points ────────────────────────────────────────
        county_points: list[HRRRCountyPoint] = []

        for county in OklahomaCounty:
            def _v(key: str, default: float = 0.0) -> float:
                return raw.get(key, {}).get(county, default)

            mlcape  = max(0.0, _v("mlcape"))
            mlcin   = abs(_v("mlcin"))        # store as positive magnitude
            sbcape  = max(0.0, _v("sbcape"))
            sbcin   = abs(_v("sbcin"))
            srh_3km = _v("hlcy_3km")
            srh_1km = _v("hlcy_1km")

            # Bulk wind difference: sqrt(u² + v²) in m/s → kt
            u6 = _v("vucsh6")
            v6 = _v("vvcsh6")
            bwd_ms = math.sqrt(u6 ** 2 + v6 ** 2)
            bwd_kt = bwd_ms * _MS_TO_KT

            # Lapse rate 700–500 mb  (°C/km)
            lapse: Optional[float] = None
            if "tmp700" in raw and "tmp500" in raw and "hgt700" in raw and "hgt500" in raw:
                t700 = _v("tmp700") - _K_TO_C   # K → °C
                t500 = _v("tmp500") - _K_TO_C
                z700 = _v("hgt700")              # m
                z500 = _v("hgt500")
                dz = z500 - z700
                if dz > 0:
                    lapse = (t700 - t500) / (dz / 1000.0)  # °C/km

            # 2m dewpoint: HRRR native is K
            dpt_k = _v("dpt2m")
            dpt_f = (dpt_k - _K_TO_C) * 9 / 5 + 32 if dpt_k > 100 else dpt_k

            # LCL height from HRRR cloud base (m AGL)
            lcl_m: Optional[float] = None
            raw_lcl = raw.get("cldbase", {}).get(county)
            if raw_lcl is not None and 0 < raw_lcl < 15000:
                lcl_m = raw_lcl

            # EHI
            ehi: Optional[float] = None
            if mlcape > 0:
                ehi = (mlcape * srh_3km) / 160_000.0

            # STP
            stp = _compute_stp(mlcape, mlcin, lcl_m, srh_1km, bwd_kt)

            county_points.append(HRRRCountyPoint(
                county=county,
                MLCAPE=mlcape,
                MLCIN=mlcin,
                SBCAPE=sbcape,
                SBCIN=sbcin,
                SRH_0_1km=srh_1km,
                SRH_0_3km=srh_3km,
                BWD_0_6km=bwd_kt,
                lapse_rate_700_500=lapse,
                dewpoint_2m_F=dpt_f,
                LCL_height_m=lcl_m,
                EHI=ehi,
                STP=stp,
            ))

        logger.info(
            "HRRR snapshot built: %s F%02d — %d counties",
            vt_str, fxx, len(county_points),
        )
        return HRRRCountySnapshot(
            valid_time=valid_time,
            run_time=run_time_dt,
            fxx=fxx,
            counties=county_points,
        )

    def get_12z_analysis(self, case_date: date) -> Optional[HRRRCountySnapshot]:
        """
        Convenience wrapper: fetch the 12Z F00 analysis for a case date.
        Returns None for dates before the HRRR archive start.
        """
        vt = datetime(case_date.year, case_date.month, case_date.day,
                      12, 0, tzinfo=timezone.utc)
        return self.get_county_snapshot(vt, fxx=0)
