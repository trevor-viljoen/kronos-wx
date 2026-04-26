"""
Mesonet-based outflow boundary detection for Oklahoma.

Outflow boundaries are the leading edge of cold-pool air that spreads from
existing or decaying convection.  They are surface wind shift lines with
associated temperature drops and pressure rises.  When an outflow boundary
encounters a dryline or frontal zone the result is often explosive initiation.

Detection algorithm
-------------------
For each Mesonet station, compare the most-recent observation against
observations from 30 and 60 minutes earlier:

  Signature = wind shift > WS_MIN_DEG  AND
              temperature drop > T_DROP_MIN_F  AND
              pressure rise   > P_RISE_MIN_MB

Stations with a confirmed outflow signature are clustered by proximity
(< CLUSTER_RADIUS_DEG latitude or longitude) into candidate boundary segments.
Each cluster becomes one BoundaryObservation polyline, sorted W→E along the
representative latitude of the cluster.

Only signatures detected using *recent* observations (within OBS_WINDOW_S of
valid_time) are used.  If fewer than MIN_CLUSTER_STATIONS stations form a
cluster, it is discarded as noise.
"""

from __future__ import annotations

import math
import logging
from datetime import datetime, timedelta
from typing import Optional

from ..models.boundary import BoundaryObservation
from ..models.enums import BoundaryType, OklahomaCounty
from ..models.mesonet import MesonetTimeSeries, MesonetObservation

log = logging.getLogger(__name__)

# ── Detection thresholds ─────────────────────────────────────────────────────
WS_MIN_DEG:      float = 30.0   # minimum wind direction shift (°)
T_DROP_MIN_F:    float = 2.0    # minimum temperature drop (°F)
P_RISE_MIN_MB:   float = 0.5   # minimum pressure rise (mb)

# Look back window: compare current obs to obs within this window ago
LOOKBACK_SECS_SHORT: int = 30 * 60   # 30 min
LOOKBACK_SECS_LONG:  int = 60 * 60   # 60 min

# Obs must be within OBS_WINDOW_S of valid_time to be "current"
OBS_WINDOW_S: float = 7 * 60   # 7 min

# Cluster stations within this lat/lon degree radius (≈ 30–35 miles)
CLUSTER_RADIUS_DEG: float = 0.5

# Minimum stations in a cluster to report as a boundary
MIN_CLUSTER_STATIONS: int = 2

# Approximate unit conversions for distance
_MILES_PER_DEG_LAT: float = 69.0
_MILES_PER_DEG_LON: float = 53.0


# ── Internal helpers ─────────────────────────────────────────────────────────

def _angle_diff(a: float, b: float) -> float:
    """
    Signed angular difference a − b on [−180, 180], accounting for 0/360 wrap.
    Positive = clockwise shift.
    """
    diff = (a - b + 180) % 360 - 180
    return diff


def _nearest_obs_at(
    ts: MesonetTimeSeries,
    target_time: datetime,
    window_s: float = OBS_WINDOW_S,
) -> Optional[MesonetObservation]:
    """Return the observation in ts nearest to target_time within window_s."""
    best: Optional[MesonetObservation] = None
    best_dt: float = window_s + 1
    for ob in ts.observations:
        dt = abs((ob.valid_time - target_time).total_seconds())
        if dt < best_dt:
            best_dt = dt
            best = ob
    return best if best_dt <= window_s else None


def _has_outflow_signature(
    current: MesonetObservation,
    earlier: MesonetObservation,
) -> bool:
    """
    Return True if the change from earlier → current looks like an outflow
    passage at this station.
    """
    # Wind shift (|Δdir|)
    shift = abs(_angle_diff(current.wind_direction, earlier.wind_direction))
    if shift < WS_MIN_DEG:
        return False

    # Temperature drop (earlier - current, positive = cooler now)
    t_drop = earlier.temperature - current.temperature
    if t_drop < T_DROP_MIN_F:
        return False

    # Pressure rise (current - earlier, positive = higher pressure now)
    p_rise = current.pressure - earlier.pressure
    if p_rise < P_RISE_MIN_MB:
        return False

    return True


def _cluster_stations(
    flagged: list[tuple[float, float, str]],  # (lat, lon, station_id)
) -> list[list[tuple[float, float, str]]]:
    """
    Simple single-linkage clustering: two stations are in the same cluster
    if they are within CLUSTER_RADIUS_DEG of each other.
    Returns a list of clusters, each cluster a list of (lat, lon, station_id).
    """
    if not flagged:
        return []

    clusters: list[list[tuple[float, float, str]]] = []
    used = [False] * len(flagged)

    for i, pt in enumerate(flagged):
        if used[i]:
            continue
        cluster = [pt]
        used[i] = True
        for j in range(i + 1, len(flagged)):
            if used[j]:
                continue
            dist_lat = abs(flagged[j][0] - pt[0])
            dist_lon = abs(flagged[j][1] - pt[1])
            if dist_lat <= CLUSTER_RADIUS_DEG and dist_lon <= CLUSTER_RADIUS_DEG:
                cluster.append(flagged[j])
                used[j] = True
        clusters.append(cluster)

    return clusters


def _counties_along_path(
    lons: list[float],
    lats: list[float],
    tolerance_deg: float = 0.4,
) -> list[OklahomaCounty]:
    """Return OklahomaCounty members near the polyline."""
    if not lons or not lats:
        return []
    lat_min = min(lats) - tolerance_deg
    lat_max = max(lats) + tolerance_deg
    lon_min = min(lons) - tolerance_deg
    lon_max = max(lons) + tolerance_deg
    result = []
    for county in OklahomaCounty:
        if lat_min <= county.lat <= lat_max and lon_min <= county.lon <= lon_max:
            result.append(county)
    return result


# ── Public API ────────────────────────────────────────────────────────────────

def detect_outflow_boundaries(
    station_series: dict[str, MesonetTimeSeries],
    valid_time: datetime,
    station_coords: Optional[dict[str, tuple[float, float]]] = None,
) -> list[BoundaryObservation]:
    """
    Detect outflow boundaries in the Mesonet station network.

    Parameters
    ----------
    station_series : dict mapping station_id → MesonetTimeSeries
    valid_time     : UTC datetime of the analysis snapshot
    station_coords : optional station_id → (lat, lon) for precise positioning;
                     falls back to county centroid when not provided

    Returns
    -------
    list[BoundaryObservation]
        Zero or more detected outflow boundaries.  Each has:
          boundary_type = OUTFLOW
          detected_by   = "mesonet_wind_pressure"
          confidence    = f(cluster_size, signal strength)
    """
    short_ago = valid_time - timedelta(seconds=LOOKBACK_SECS_SHORT)
    long_ago  = valid_time - timedelta(seconds=LOOKBACK_SECS_LONG)

    # Step 1: flag stations with an outflow signature
    flagged: list[tuple[float, float, str]] = []   # (lat, lon, station_id)
    signal_strengths: dict[str, float] = {}

    for sid, ts in station_series.items():
        current = _nearest_obs_at(ts, valid_time)
        if current is None:
            continue

        # Use actual station coordinates when available
        if station_coords and sid in station_coords:
            lat, lon = station_coords[sid]
        else:
            lat, lon = ts.county.lat, ts.county.lon

        # Try 30-min lookback first, then 60-min
        earlier = _nearest_obs_at(ts, short_ago, window_s=7 * 60)
        if earlier is None:
            earlier = _nearest_obs_at(ts, long_ago, window_s=7 * 60)
        if earlier is None:
            continue

        if _has_outflow_signature(current, earlier):
            flagged.append((lat, lon, sid))
            # Signal strength: sum of normalized deviations above threshold
            wind_shift = abs(_angle_diff(current.wind_direction, earlier.wind_direction))
            t_drop     = max(0.0, earlier.temperature - current.temperature)
            p_rise     = max(0.0, current.pressure - earlier.pressure)
            strength   = (
                (wind_shift - WS_MIN_DEG) / 90.0
                + (t_drop - T_DROP_MIN_F) / 10.0
                + (p_rise - P_RISE_MIN_MB) / 2.0
            )
            signal_strengths[sid] = max(0.0, strength)

    if not flagged:
        return []

    # Step 2: cluster nearby stations
    clusters = _cluster_stations(flagged)

    # Step 3: build BoundaryObservation per cluster
    results: list[BoundaryObservation] = []

    for cluster in clusters:
        if len(cluster) < MIN_CLUSTER_STATIONS:
            continue

        # Sort cluster W→E (lons are negative in OK, so sort ascending = W→E)
        cluster_sorted = sorted(cluster, key=lambda pt: pt[1])

        # Build polyline from cluster centroids
        # Use the individual station positions; extend to a 2-point segment if needed
        lats = [pt[0] for pt in cluster_sorted]
        lons = [pt[1] for pt in cluster_sorted]

        # If all stations at nearly same lat, create a short N-S line
        if max(lats) - min(lats) < 0.2:
            mean_lat = sum(lats) / len(lats)
            mean_lon = sum(lons) / len(lons)
            lats = [mean_lat - 0.3, mean_lat + 0.3]
            lons = [mean_lon,       mean_lon]

        if len(lats) < 2:
            continue

        # Confidence: based on cluster size and average signal strength
        cluster_sids = [pt[2] for pt in cluster]
        avg_strength = (
            sum(signal_strengths.get(sid, 0.0) for sid in cluster_sids)
            / len(cluster_sids)
        )
        size_score   = min(1.0, len(cluster) / 5.0)      # saturates at 5 stations
        signal_score = min(1.0, avg_strength)
        confidence   = round(0.5 * size_score + 0.5 * signal_score, 2)
        confidence   = max(0.10, min(0.85, confidence))   # outflow is noisier than dryline

        counties = _counties_along_path(lons, lats)

        try:
            obs = BoundaryObservation(
                valid_time=valid_time,
                boundary_type=BoundaryType.OUTFLOW,
                position_lat=lats,
                position_lon=lons,
                counties_intersected=counties,
                confidence=confidence,
                detected_by="mesonet_wind_pressure",
            )
            results.append(obs)
        except Exception as exc:
            log.debug("Skipping outflow cluster: %s", exc)

    log.info(
        "Outflow detection: %d flagged stations → %d boundaries (%s)",
        len(flagged),
        len(results),
        valid_time.strftime("%H:%MZ"),
    )
    return results
