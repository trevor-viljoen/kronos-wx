"""
Mesonet-based dryline detection for Oklahoma.

The dryline is a moisture discontinuity marking the boundary between moist
Gulf air (east) and dry continental/elevated-mixed-layer air (west). It is the
primary initiation trigger on the majority of significant Oklahoma tornado days.

Primary detection signature: sharp east-west dewpoint gradient.
Secondary confirmation: wind shift from S/SSE (moist) to SW/W (dry).

Algorithm
---------
1. Collect all valid Mesonet observations within ±7 min of valid_time.
2. Bin stations into three latitude bands (south/central/north).
3. Within each band, walk stations W→E to find the eastern edge of the
   CONTIGUOUS dry sector (Td ≤ DRY_SECTOR_TD_MAX_F). The first moist station
   breaks the run — rain-cooled outflow or storm pockets east of the moist
   sector are naturally ignored because moist-sector air separates them from
   the genuine dry sector. Pair the dry edge with the nearest moist station
   to its east that clears the gradient thresholds.
4. The dryline longitude in that band = midpoint of the max-gradient pair.
5. Build a polyline through the detected lat/lon points (S→N).
6. Confidence = blend of gradient sharpness and band coverage.

A dryline is only reported when at least one band clears the gradient threshold.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Optional

from ..models.boundary import BoundaryObservation
from ..models.enums import BoundaryType, OklahomaCounty
from ..models.mesonet import MesonetTimeSeries

# ── Detection thresholds ───────────────────────────────────────────────────────

# Minimum Td drop (°F) per degree of longitude to flag as dryline
MIN_TD_GRADIENT_F_PER_DEG: float = 8.0

# Minimum *absolute* Td drop (°F) across the boundary pair (filters noise between
# two nearly-equal stations that happen to be far apart).
# Lowered from 10°F → 8°F to pair with the raised DRY_SECTOR_TD_MAX_F; the 2°F
# reduction is safe because DRY_SECTOR_TD_MAX_F=52 already blocks moist-sector pairs.
MIN_TD_ABSOLUTE_DROP_F: float = 8.0

# A gradient this strong (°F/deg lon) is assigned confidence = 1.0 from gradient alone
STRONG_GRADIENT_F_PER_DEG: float = 20.0

# The western station of a candidate pair must be in the dry sector (Td below this).
# Outflow boundaries occur in the moist sector — both stations have high dewpoints.
# Classic OK dry-sector dewpoints: 20–45°F, but early-season or weak drylines can
# sit at 48–52°F in the transition zone when Gulf moisture hasn't fully surged west.
# Raised from 45°F → 52°F to capture weak/early-season drylines; the absolute-drop
# filter below keeps outflow boundaries (which have similar Td on both sides) from
# registering as drylines.
DRY_SECTOR_TD_MAX_F: float = 52.0

# Max station-pair separation to consider (degrees longitude).
# Allows skipping one station that has missing/bad data.
MAX_PAIR_DLON: float = 2.5

# Observation matching window (seconds)
_OBS_WINDOW_S: float = 7 * 60

# Approximate miles per degree longitude at 35.5°N
_MILES_PER_DEG_LON: float = 53.0

# Oklahoma latitude bands for per-band detection
# (lat_min, lat_max, representative_lat, band_name)
_LAT_BANDS: list[tuple[float, float, float, str]] = [
    (36.0, 37.5, 36.5, "north"),
    (35.0, 36.0, 35.5, "central"),
    (33.6, 35.0, 34.3, "south"),
]


# ── Internal helpers ───────────────────────────────────────────────────────────

def _nearest_obs(ts: MesonetTimeSeries, valid_time: datetime):
    """Return the observation nearest to valid_time within the matching window."""
    best = None
    best_dt: float = _OBS_WINDOW_S + 1
    for ob in ts.observations:
        dt = abs((ob.valid_time - valid_time).total_seconds())
        if dt < best_dt:
            best_dt = dt
            best = ob
    return best if best_dt <= _OBS_WINDOW_S else None


def _max_gradient_pair(
    stations: list[tuple[float, float, float]],  # (lon, lat, td_F)
) -> Optional[tuple[float, float]]:
    """
    Locate the dryline as the eastern edge of the contiguous dry sector.

    Algorithm:
      1. Sort stations W→E and walk east until the first station whose Td
         exceeds DRY_SECTOR_TD_MAX_F, marking the dry sector's eastern edge.
         This is the key physical constraint: a rain-cooled storm pocket east
         of the moist sector is ignored because moist-sector air between it
         and the genuine dry sector breaks the contiguous run.
      2. Pair the dry-edge station with the nearest eastern station (within
         MAX_PAIR_DLON) that produces a gradient ≥ MIN_TD_GRADIENT_F_PER_DEG
         and an absolute Td jump ≥ MIN_TD_ABSOLUTE_DROP_F.

    Returns (midpoint_lon, gradient_F_per_deg) or None if no dry sector or
    no valid moist-sector station can be paired with it.
    """
    if len(stations) < 2:
        return None

    # Sort W→E; Oklahoma lons are negative so ascending sort = W→E
    srt = sorted(stations, key=lambda s: s[0])

    # ── Step 1: find the eastern edge of the contiguous dry sector ────────────
    dry_edge_idx: Optional[int] = None
    for i, (lon, _lat, td) in enumerate(srt):
        if td <= DRY_SECTOR_TD_MAX_F:
            dry_edge_idx = i
        else:
            break   # first moist station ends the contiguous run

    if dry_edge_idx is None:
        return None  # no dry sector in this band

    lon_w, _, td_w = srt[dry_edge_idx]

    # ── Step 2: find the best moist-sector pairing station ────────────────────
    best_gradient = 0.0
    best_lon: Optional[float] = None

    for j in range(dry_edge_idx + 1, len(srt)):
        lon_e, _, td_e = srt[j]
        dlon = abs(lon_e - lon_w)
        if dlon > MAX_PAIR_DLON:
            break
        if dlon < 0.1:
            continue
        dtd = td_e - td_w
        if dtd < MIN_TD_ABSOLUTE_DROP_F:
            continue
        gradient = dtd / dlon
        if gradient > best_gradient:
            best_gradient = gradient
            best_lon = (lon_w + lon_e) / 2.0

    if best_gradient >= MIN_TD_GRADIENT_F_PER_DEG and best_lon is not None:
        return (best_lon, best_gradient)
    return None


def _counties_near_polyline(
    lons: list[float],
    lats: list[float],
    lon_tolerance: float = 0.5,
) -> list[OklahomaCounty]:
    """
    Return OklahomaCounty members whose centroid lon is within `lon_tolerance`
    degrees of the polyline and whose lat falls within the polyline's lat range.
    """
    if not lons or not lats:
        return []

    lat_min = min(lats) - 0.2
    lat_max = max(lats) + 0.2

    # Interpolate the polyline to get the dryline lon at each county's latitude
    def _polyline_lon_at(lat: float) -> Optional[float]:
        """Linear interpolation along the S→N polyline."""
        for j in range(len(lats) - 1):
            if lats[j] <= lat <= lats[j + 1]:
                t = (lat - lats[j]) / (lats[j + 1] - lats[j])
                return lons[j] + t * (lons[j + 1] - lons[j])
        return None

    result: list[OklahomaCounty] = []
    for county in OklahomaCounty:
        if not (lat_min <= county.lat <= lat_max):
            continue
        poly_lon = _polyline_lon_at(county.lat)
        if poly_lon is None:
            continue
        if abs(county.lon - poly_lon) <= lon_tolerance:
            result.append(county)

    return result


# ── Public API ─────────────────────────────────────────────────────────────────

def detect_dryline(
    station_series: dict[str, MesonetTimeSeries],
    valid_time: datetime,
    station_coords: Optional[dict[str, tuple[float, float]]] = None,
) -> Optional[BoundaryObservation]:
    """
    Detect the dryline position from Mesonet observations at valid_time.

    Args:
        station_series: dict mapping station_id → MesonetTimeSeries, as
            returned by MesonetClient.get_historical_case_data().
        valid_time: UTC datetime to analyze.

    Returns:
        BoundaryObservation with boundary_type=DRYLINE, or None if no clear
        dryline gradient is found in the station network.
    """
    # ── 1. Collect observations and bin by latitude band ───────────────────────
    band_data: dict[str, list[tuple[float, float, float]]] = {
        b[3]: [] for b in _LAT_BANDS
    }

    for ts in station_series.values():
        ob = _nearest_obs(ts, valid_time)
        if ob is None:
            continue
        # Use actual station coordinates when available; fall back to county centroid.
        if station_coords and ts.station_id in station_coords:
            lat, lon = station_coords[ts.station_id]
        else:
            lat = ts.county.lat
            lon = ts.county.lon
        for lat_min, lat_max, _rep, band_name in _LAT_BANDS:
            if lat_min <= lat < lat_max:
                band_data[band_name].append((lon, lat, ob.dewpoint))
                break

    # ── 2. Find dryline longitude per band ────────────────────────────────────
    band_results: list[tuple[float, float, float]] = []  # (rep_lat, dryline_lon, gradient)

    for lat_min, lat_max, rep_lat, band_name in _LAT_BANDS:
        result = _max_gradient_pair(band_data[band_name])
        if result:
            dryline_lon, gradient = result
            band_results.append((rep_lat, dryline_lon, gradient))

    if not band_results:
        return None

    # ── 3. Build S→N polyline ─────────────────────────────────────────────────
    band_results.sort(key=lambda x: x[0])   # south → north
    lats = [r[0] for r in band_results]
    lons = [r[1] for r in band_results]

    # Extend a single-point detection into a short N-S segment so the polyline
    # validator (≥2 points) is satisfied.
    if len(lats) == 1:
        lats = [lats[0] - 0.8, lats[0] + 0.8]
        lons = [lons[0], lons[0]]

    # ── 4. Confidence ─────────────────────────────────────────────────────────
    gradients = [r[2] for r in band_results]
    avg_gradient = sum(gradients) / len(gradients)
    band_coverage = len(band_results) / len(_LAT_BANDS)

    gradient_score = min(
        1.0,
        (avg_gradient - MIN_TD_GRADIENT_F_PER_DEG)
        / (STRONG_GRADIENT_F_PER_DEG - MIN_TD_GRADIENT_F_PER_DEG),
    )
    # Clamp to [0, 1] in case avg_gradient < MIN (shouldn't happen, but safe)
    gradient_score = max(0.0, gradient_score)

    confidence = round(0.35 * band_coverage + 0.65 * gradient_score, 2)
    confidence = max(0.05, min(1.0, confidence))

    # ── 5. Counties intersected ───────────────────────────────────────────────
    counties = _counties_near_polyline(lons, lats)

    return BoundaryObservation(
        valid_time=valid_time,
        boundary_type=BoundaryType.DRYLINE,
        position_lat=lats,
        position_lon=lons,
        counties_intersected=counties,
        confidence=confidence,
        detected_by="mesonet_td_gradient",
    )


def compute_dryline_surge_rate(
    early: BoundaryObservation,
    late: BoundaryObservation,
) -> Optional[float]:
    """
    Compute eastward surge rate (mph) between two dryline observations.

    Positive = eastward surge, negative = retrograding dryline.

    Uses the mean longitude of each polyline to represent dryline position.
    Returns None if the time gap is less than 30 minutes.
    """
    dt_hours = (late.valid_time - early.valid_time).total_seconds() / 3600.0
    if dt_hours < 0.5:
        return None

    mean_lon_early = sum(early.position_lon) / len(early.position_lon)
    mean_lon_late  = sum(late.position_lon)  / len(late.position_lon)

    # Positive dlon = eastward movement (both lons are negative; late > early means moved east)
    dlon = mean_lon_late - mean_lon_early
    miles = dlon * _MILES_PER_DEG_LON
    return miles / dt_hours


def analyze_dryline_from_mesonet(
    station_series: dict[str, MesonetTimeSeries],
    case_date,
) -> dict:
    """
    Run dryline detection at 15Z, 18Z, and 21Z for a case day.

    Returns a dict with keys:
        boundaries      — list[BoundaryObservation] for each hour with detection
        dryline_lon_18Z — mean dryline longitude at 18Z (°W, negative), or None
        surge_rate_mph  — eastward surge rate between 15Z→18Z or 18Z→21Z, or None
    """
    from datetime import date as _date

    if isinstance(case_date, _date):
        year, month, day = case_date.year, case_date.month, case_date.day
    else:
        year, month, day = case_date.year, case_date.month, case_date.day

    from datetime import timezone
    _UTC = timezone.utc

    snapshots: dict[int, Optional[BoundaryObservation]] = {}
    for hour in (15, 18, 21):
        vt = datetime(year, month, day, hour, 0, tzinfo=_UTC)
        snapshots[hour] = detect_dryline(station_series, vt)

    boundaries = [b for b in snapshots.values() if b is not None]

    # 18Z dryline longitude
    dryline_lon_18Z: Optional[float] = None
    if snapshots[18] is not None:
        dryline_lon_18Z = sum(snapshots[18].position_lon) / len(snapshots[18].position_lon)

    # Surge rate: prefer 15Z→18Z; fall back to 18Z→21Z
    surge_rate_mph: Optional[float] = None
    if snapshots[15] is not None and snapshots[18] is not None:
        surge_rate_mph = compute_dryline_surge_rate(snapshots[15], snapshots[18])
    elif snapshots[18] is not None and snapshots[21] is not None:
        surge_rate_mph = compute_dryline_surge_rate(snapshots[18], snapshots[21])

    return {
        "boundaries":      boundaries,
        "dryline_lon_18Z": dryline_lon_18Z,
        "surge_rate_mph":  surge_rate_mph,
    }
