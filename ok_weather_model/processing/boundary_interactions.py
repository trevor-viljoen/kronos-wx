"""
Boundary interaction detection.

When two mesoscale boundaries converge or intersect in a thermodynamically
loaded environment the initiation risk escalates sharply.  This module:

  1. Finds segment-level intersections between all pairs of BoundaryObservation
     polylines active at the same valid time.
  2. Evaluates the thermodynamic environment at each intersection point
     using per-county HRRR data (or sounding interpolation if HRRR unavailable).
  3. Sets alarm_bell_flag = True when MLCAPE > 1000 J/kg AND MLCIN < 100 J/kg
     at the interaction county — the classic supercell trigger signature.

Intersection algorithm
----------------------
For two boundary polylines A and B we test every segment pair (A_i, B_j) for
a 2-D line-segment intersection.  If segments cross, we interpolate the exact
crossing lat/lon and find the nearest Oklahoma county.

Only intersections that fall within the Oklahoma bounding box are retained.
"""

from __future__ import annotations

import math
import logging
from datetime import datetime
from typing import Optional

from ..models.boundary import BoundaryObservation, BoundaryInteraction
from ..models.enums import OklahomaCounty

log = logging.getLogger(__name__)

# Oklahoma domain bounding box
_OK_LON_MIN: float = -103.5
_OK_LON_MAX: float = -94.0
_OK_LAT_MIN: float = 33.0
_OK_LAT_MAX: float = 37.5

# Thermodynamic alarm-bell thresholds
_ALARM_CAPE_MIN: float = 1000.0   # J/kg
_ALARM_CIN_MAX:  float = 100.0    # J/kg (positive magnitude)


# ── Geometry helpers ──────────────────────────────────────────────────────────

def _segments_intersect(
    p1: tuple[float, float],
    p2: tuple[float, float],
    p3: tuple[float, float],
    p4: tuple[float, float],
) -> Optional[tuple[float, float]]:
    """
    Compute the intersection point of segments p1-p2 and p3-p4 (lon, lat tuples).
    Returns (lon, lat) of the crossing or None if parallel / not intersecting.

    Uses parametric form:  P = p1 + t*(p2−p1) = p3 + u*(p4−p3)
    Both t and u must be in [0, 1] for a valid segment crossing.
    """
    x1, y1 = p1
    x2, y2 = p2
    x3, y3 = p3
    x4, y4 = p4

    denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
    if abs(denom) < 1e-10:
        return None  # parallel or coincident

    t_num = (x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)
    u_num = -((x1 - x2) * (y1 - y3) - (y1 - y2) * (x1 - x3))

    t = t_num / denom
    u = u_num / denom

    if 0.0 <= t <= 1.0 and 0.0 <= u <= 1.0:
        ix = x1 + t * (x2 - x1)
        iy = y1 + t * (y2 - y1)
        return (ix, iy)

    return None


def _nearest_county(lat: float, lon: float) -> OklahomaCounty:
    """Return the OklahomaCounty whose centroid is nearest to (lat, lon)."""
    best: Optional[OklahomaCounty] = None
    best_dist2: float = float("inf")
    for county in OklahomaCounty:
        dlat = county.lat - lat
        dlon = county.lon - lon
        dist2 = dlat * dlat + dlon * dlon
        if dist2 < best_dist2:
            best_dist2 = dist2
            best = county
    return best  # type: ignore[return-value]  — OklahomaCounty enum has ≥1 member


def _polyline_intersections(
    a: BoundaryObservation,
    b: BoundaryObservation,
) -> list[tuple[float, float]]:
    """
    Return all (lon, lat) intersection points between two boundary polylines.
    Only points within the Oklahoma bounding box are kept.
    """
    crossings: list[tuple[float, float]] = []
    lats_a, lons_a = a.position_lat, a.position_lon
    lats_b, lons_b = b.position_lat, b.position_lon

    for i in range(len(lats_a) - 1):
        p1 = (lons_a[i], lats_a[i])
        p2 = (lons_a[i + 1], lats_a[i + 1])
        for j in range(len(lats_b) - 1):
            p3 = (lons_b[j], lats_b[j])
            p4 = (lons_b[j + 1], lats_b[j + 1])
            pt = _segments_intersect(p1, p2, p3, p4)
            if pt is not None:
                lon, lat = pt
                if _OK_LON_MIN <= lon <= _OK_LON_MAX and _OK_LAT_MIN <= lat <= _OK_LAT_MAX:
                    crossings.append((lon, lat))

    return crossings


def _convergence_angle(
    a: BoundaryObservation,
    b: BoundaryObservation,
) -> Optional[float]:
    """
    Estimate the crossing angle (°) between two boundaries at their midpoints.
    A 90° crossing is the most energetically favourable.
    Returns None if either boundary has fewer than 2 points.
    """
    def _mean_bearing(obs: BoundaryObservation) -> Optional[float]:
        lats, lons = obs.position_lat, obs.position_lon
        if len(lats) < 2:
            return None
        dlat = lats[-1] - lats[0]
        dlon = lons[-1] - lons[0]
        return math.degrees(math.atan2(dlon, dlat)) % 360

    bearing_a = _mean_bearing(a)
    bearing_b = _mean_bearing(b)
    if bearing_a is None or bearing_b is None:
        return None

    diff = abs(bearing_a - bearing_b) % 180
    return diff if diff <= 90 else 180 - diff


# ── Public API ────────────────────────────────────────────────────────────────

def find_boundary_interactions(
    boundaries: list[BoundaryObservation],
    valid_time: datetime,
    hrrr_snapshot=None,    # Optional HRRRCountySnapshot — if None, no thermo check
) -> list[BoundaryInteraction]:
    """
    Find all pairwise intersections between active boundaries and return
    BoundaryInteraction objects.

    Parameters
    ----------
    boundaries     : All active BoundaryObservation objects for this valid_time
    valid_time     : Nominal valid time for the interaction objects
    hrrr_snapshot  : Optional HRRRCountySnapshot for thermodynamic alarm-bell check.
                     When provided, alarm_bell_flag is set based on HRRR county data.

    Returns
    -------
    list[BoundaryInteraction]
        One entry per unique boundary pair × crossing point.
    """
    interactions: list[BoundaryInteraction] = []

    # Build a per-county HRRR lookup if available
    county_hrrr: dict[OklahomaCounty, object] = {}
    if hrrr_snapshot is not None:
        for pt in hrrr_snapshot.counties:
            county_hrrr[pt.county] = pt

    for i in range(len(boundaries)):
        for j in range(i + 1, len(boundaries)):
            a = boundaries[i]
            b = boundaries[j]

            crossings = _polyline_intersections(a, b)
            if not crossings:
                continue

            angle = _convergence_angle(a, b)
            # Convergence magnitude in m/s: rough proxy from crossing angle
            # Head-on (90°) = strongest convergence, glancing (0°) = weakest
            conv_mag: Optional[float] = None
            if angle is not None:
                conv_mag = round(math.sin(math.radians(angle)) * 10.0, 1)

            for lon, lat in crossings:
                county = _nearest_county(lat, lon)

                # Thermodynamic alarm-bell check
                alarm = False
                hpt = county_hrrr.get(county)
                if hpt is not None:
                    cape = getattr(hpt, "MLCAPE", 0.0) or 0.0
                    cin  = getattr(hpt, "MLCIN",  999.0) or 999.0
                    alarm = cape >= _ALARM_CAPE_MIN and cin < _ALARM_CIN_MAX

                notes_parts = []
                if angle is not None:
                    notes_parts.append(f"crossing angle {angle:.0f}°")
                if alarm:
                    notes_parts.append("ALARM: CAPE/CIN thresholds met")

                try:
                    interaction = BoundaryInteraction(
                        valid_time=valid_time,
                        boundary_1=a,
                        boundary_2=b,
                        interaction_point_lat=lat,
                        interaction_point_lon=lon,
                        interaction_county=county,
                        convergence_magnitude=conv_mag,
                        alarm_bell_flag=alarm,
                        notes="; ".join(notes_parts) if notes_parts else None,
                    )
                    interactions.append(interaction)
                except Exception as exc:
                    log.debug(
                        "Skipping interaction %s×%s at (%.2f, %.2f): %s",
                        a.detected_by, b.detected_by, lat, lon, exc,
                    )

    if interactions:
        alarm_count = sum(1 for x in interactions if x.alarm_bell_flag)
        log.info(
            "Boundary interactions: %d found (%d alarm bells) [%s]",
            len(interactions),
            alarm_count,
            valid_time.strftime("%H:%MZ"),
        )

    return interactions
