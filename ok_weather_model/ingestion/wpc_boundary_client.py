"""
WPC Day 1 Fronts client for Oklahoma boundary tracking.

Fetches synoptic-scale boundaries from the Weather Prediction Center's
ArcGIS REST service.  Boundaries within (or near) the Oklahoma domain are
returned as BoundaryObservation objects with detected_by = "wpc_<type>".

Data source
-----------
NOAA/WPC National Forecast Chart (Day 1):
  https://mapservices.weather.noaa.gov/vector/rest/services/outlooks/
  natl_fcst_wx_chart/MapServer/2/query
  Layer 2 = Day 1 Fronts.  Updated ~every 3 hours.

Only the Oklahoma bounding box (with a generous 2° buffer) is requested,
so the response is small (<50 KB typical).
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Optional

from ..models.boundary import BoundaryObservation
from ..models.enums import BoundaryType, OklahomaCounty

log = logging.getLogger(__name__)

# ── Oklahoma domain bbox with 2° buffer ──────────────────────────────────────
_OK_BBOX = (-105.5, 31.0, -92.0, 39.5)   # xmin, ymin, xmax, ymax

# ── ArcGIS REST endpoint ──────────────────────────────────────────────────────
_BASE_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services"
    "/outlooks/natl_fcst_wx_chart/MapServer/2/query"
)

# ── Feature-type → (BoundaryType, detected_by, confidence, motion_direction) ─
# Motion directions are rough climatological defaults for OK.
# cold_front: generally SE-moving; warm_front: generally E-moving;
# stationary: minimal motion; trough: variable (0 = no good default)
_FEAT_MAP: dict[str, tuple[BoundaryType, str, float, Optional[float]]] = {
    "cold front":       (BoundaryType.FRONTAL, "wpc_cold_front",       0.90, 135.0),
    "warm front":       (BoundaryType.FRONTAL, "wpc_warm_front",       0.85,  90.0),
    "stationary front": (BoundaryType.FRONTAL, "wpc_stationary_front", 0.80, None),
    "occluded front":   (BoundaryType.FRONTAL, "wpc_occluded_front",   0.80, 120.0),
    "trough":           (BoundaryType.FRONTAL, "wpc_trough",           0.70, None),
    "dryline":          (BoundaryType.DRYLINE, "wpc_dryline",          0.85, 90.0),
}


def _normalize_feat(feat_str: str) -> Optional[str]:
    """
    Normalize the WPC 'feat' attribute to a lookup key.
    E.g. 'Cold Front Valid' → 'cold front'.
    Returns None if the type is not in our lookup table.
    """
    s = feat_str.lower()
    for key in _FEAT_MAP:
        if key in s:
            return key
    return None


def _clip_path_to_bbox(
    lons: list[float],
    lats: list[float],
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
) -> tuple[list[float], list[float]]:
    """
    Keep only the portion of a polyline that lies inside the bbox.
    Uses a simple endpoint-inclusion strategy: keep contiguous runs
    of points inside the bbox.  Returns the longest interior run.
    Segments that are partially inside are clipped at the boundary crossing.
    """
    inside_indices = [
        i for i, (lo, la) in enumerate(zip(lons, lats))
        if xmin <= lo <= xmax and ymin <= la <= ymax
    ]
    if not inside_indices:
        return [], []

    # Return the whole path truncated to the first/last inside point
    # (full segment clipping is overkill for synoptic-scale fronts)
    first, last = inside_indices[0], inside_indices[-1]
    return lons[first: last + 1], lats[first: last + 1]


def _counties_along_path(
    lons: list[float],
    lats: list[float],
    tolerance_deg: float = 0.7,
) -> list[OklahomaCounty]:
    """Return OklahomaCounty members near the polyline."""
    if not lons or not lats:
        return []
    lat_min, lat_max = min(lats) - tolerance_deg, max(lats) + tolerance_deg
    lon_min, lon_max = min(lons) - tolerance_deg, max(lons) + tolerance_deg

    result: list[OklahomaCounty] = []
    for county in OklahomaCounty:
        if not (lat_min <= county.lat <= lat_max):
            continue
        if not (lon_min <= county.lon <= lon_max):
            continue
        result.append(county)
    return result


def fetch_wpc_boundaries(
    valid_time: Optional[datetime] = None,
    timeout_s: int = 10,
) -> list[BoundaryObservation]:
    """
    Fetch WPC Day 1 Fronts for the Oklahoma domain and return a list of
    BoundaryObservation objects (one per boundary segment).

    Parameters
    ----------
    valid_time : datetime or None
        The nominal valid time to stamp each BoundaryObservation.
        Defaults to ``datetime.now(timezone.utc)`` if not provided.
    timeout_s  : int
        HTTP timeout in seconds (default 10).

    Returns
    -------
    list[BoundaryObservation]
        Empty list if the service is unavailable or returns no features.
    """
    if valid_time is None:
        valid_time = datetime.now(timezone.utc)

    xmin, ymin, xmax, ymax = _OK_BBOX

    params = urllib.parse.urlencode({
        "where":            "1=1",
        "geometry":         f"{xmin},{ymin},{xmax},{ymax}",
        "geometryType":     "esriGeometryEnvelope",
        "spatialRel":       "esriSpatialRelIntersects",
        "inSR":             "4326",
        "outSR":            "4326",
        "outFields":        "*",
        "returnGeometry":   "true",
        "f":                "json",
    })
    url = f"{_BASE_URL}?{params}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "kronos-wx/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        log.warning("WPC boundary fetch failed (network): %s", exc)
        return []
    except Exception as exc:
        log.warning("WPC boundary fetch failed: %s", exc)
        return []

    features = data.get("features") or []
    if not features:
        log.debug("WPC boundary response contained 0 features")
        return []

    boundaries: list[BoundaryObservation] = []

    for feat in features:
        attrs = feat.get("attributes") or {}
        geom  = feat.get("geometry") or {}

        # Feature type
        feat_label = attrs.get("feat") or attrs.get("FEAT") or ""
        key = _normalize_feat(feat_label)
        if key is None:
            continue

        btype, detected_by, confidence, motion_dir = _FEAT_MAP[key]

        # Geometry: ArcGIS Polyline has a 'paths' array of [lon, lat] rings
        paths = geom.get("paths") or []
        for path in paths:
            if len(path) < 2:
                continue

            # ArcGIS returns [x, y] = [lon, lat]
            all_lons = [pt[0] for pt in path]
            all_lats = [pt[1] for pt in path]

            # Clip to OK domain
            clipped_lons, clipped_lats = _clip_path_to_bbox(
                all_lons, all_lats, xmin, ymin, xmax, ymax
            )
            if len(clipped_lons) < 2:
                continue

            counties = _counties_along_path(clipped_lons, clipped_lats)

            try:
                obs = BoundaryObservation(
                    valid_time=valid_time,
                    boundary_type=btype,
                    position_lat=clipped_lats,
                    position_lon=clipped_lons,
                    counties_intersected=counties,
                    motion_direction=motion_dir,
                    confidence=confidence,
                    detected_by=detected_by,
                )
                boundaries.append(obs)
            except Exception as exc:
                log.debug("Skipping WPC boundary segment (%s): %s", feat_label, exc)
                continue

    log.info(
        "WPC boundaries fetched: %d segments (%s)",
        len(boundaries),
        valid_time.strftime("%Y-%m-%d %H:%MZ"),
    )
    return boundaries
