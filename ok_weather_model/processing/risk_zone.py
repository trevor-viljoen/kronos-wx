"""
County-level severe weather risk zone computation.

Approach
--------
We hold rawinsonde soundings at three stations spanning the region:
  FWD — Fort Worth, TX  (32.83°N) — southern anchor (TX panhandle border)
  OUN — Norman, OK      (35.22°N) — central Oklahoma reference
  LMN — Lamont, OK      (36.69°N) — northern Oklahoma reference

For every Oklahoma county we piecewise-linearly interpolate thermodynamic
and kinematic parameters across the three stations based on latitude:
  lat < FWD_LAT            → FWD values (southern extrapolation anchor)
  FWD_LAT ≤ lat < OUN_LAT → FWD↔OUN segment
  OUN_LAT ≤ lat < LMN_LAT → OUN↔LMN segment
  lat ≥ LMN_LAT            → LMN values (northern extrapolation anchor)

This gives a coarse but physically grounded meridional gradient across the
FWD→OUN→LMN corridor without requiring model data.

A dryline position (BoundaryObservation) optionally shifts county risk
upward for counties within the initiation corridor: the 50-mile band
centered on the dryline is the highest-probability initiation zone, and
the 50-mile band just east of the dryline is the most dangerous area for
any forced initiation.

Risk tiers
----------
EXTREME          — Near-certain significant tornadoes if anything initiates;
                   MLCIN < 100, MLCAPE > 1500, SRH 0-1km > 250, EHI > 3.5
HIGH             — Likely significant tornadoes with initiation;
                   moderate CIN with strong kinematic support
DANGEROUS_CAPPED — Strong cap but extreme kinematics; boundary-forced
                   initiation could produce violent tornadoes with minimal
                   warning (the April 23, 2026 northern-OK pattern)
MODERATE         — Some tornado potential if cap erodes; limited kinematics
MARGINAL         — Isolated storm potential; marginal environment
LOW              — No meaningful tornado threat

Output
------
compute_risk_zones() returns a list of RiskZone objects sorted by tier
severity (highest first).  Each zone lists the counties it covers and
the interpolated environment parameters driving the classification.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

from ..models.enums import OklahomaCounty
from ..models.boundary import BoundaryObservation

# ── Station anchor latitudes ──────────────────────────────────────────────────
_FWD_LAT: float = 32.83   # Fort Worth, TX  (WMO 72249)
_OUN_LAT: float = 35.22   # Norman, OK      (WMO 72357)
_LMN_LAT: float = 36.69   # Lamont, OK      (WMO 74646)

# ── Miles per degree (approximate, Oklahoma) ─────────────────────────────────
_MILES_PER_DEG_LAT: float = 69.0
_MILES_PER_DEG_LON: float = 53.0   # at ~35.5°N

# ── Risk tier ordering (highest = most dangerous) ────────────────────────────
_TIER_RANK = {
    "EXTREME":           5,
    "HIGH":              4,
    "DANGEROUS_CAPPED":  3,
    "MODERATE":          2,
    "MARGINAL":          1,
    "LOW":               0,
}

_TIER_COLOR = {
    "EXTREME":           "bright_red",
    "HIGH":              "red",
    "DANGEROUS_CAPPED":  "magenta",
    "MODERATE":          "yellow",
    "MARGINAL":          "green",
    "LOW":               "dim",
}


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class CountyEnvironment:
    """Interpolated severe-weather environment for one county."""
    county:       OklahomaCounty
    MLCAPE:       float         # J/kg
    MLCIN:        float         # J/kg (positive magnitude)
    cap_strength: float         # °C
    SRH_0_1km:    float         # m²/s²
    SRH_0_3km:    float         # m²/s²
    BWD_0_6km:    float         # kt
    EHI:          float         # dimensionless
    near_dryline: bool = False  # within 50 mi of dryline


@dataclass
class RiskZone:
    """
    A geographic cluster of counties sharing the same risk tier.

    ``span_miles`` is the approximate E-W × N-S extent of the zone.
    """
    tier:        str
    counties:    list[OklahomaCounty] = field(default_factory=list)
    center_lat:  float = 0.0
    center_lon:  float = 0.0
    lat_min:     float = 0.0
    lat_max:     float = 0.0
    lon_min:     float = 0.0
    lon_max:     float = 0.0
    span_ew_mi:  float = 0.0
    span_ns_mi:  float = 0.0
    # Representative parameter snapshot (worst-case county in zone)
    peak_MLCAPE:    float = 0.0
    peak_MLCIN:     float = 0.0
    peak_SRH_0_1km: float = 0.0
    peak_EHI:       float = 0.0

    @property
    def tier_rank(self) -> int:
        return _TIER_RANK.get(self.tier, 0)

    @property
    def color(self) -> str:
        return _TIER_COLOR.get(self.tier, "white")


# ── Internal helpers ──────────────────────────────────────────────────────────

def _lerp(a: float, b: float, t: float) -> float:
    """Linear interpolation: t=0 → a, t=1 → b, clamped to [0,1]."""
    t = max(0.0, min(1.0, t))
    return a + t * (b - a)


def _interpolate_at_lat(
    lat: float,
    fwd_val: Optional[float],
    oun_val: Optional[float],
    lmn_val: Optional[float],
) -> Optional[float]:
    """
    Piecewise-linear interpolation across the FWD → OUN → LMN corridor.

    Segment selection by latitude:
      lat < FWD_LAT            → FWD anchor (clamped south)
      FWD_LAT ≤ lat < OUN_LAT → FWD↔OUN segment
      OUN_LAT ≤ lat < LMN_LAT → OUN↔LMN segment
      lat ≥ LMN_LAT            → LMN anchor (clamped north)

    Falls back gracefully: if a segment endpoint is missing the next
    available anchor is used, degrading to the two-station case.
    Returns None only if all three values are absent/NaN.
    """
    import math as _math
    def _bad(v):
        return v is None or (isinstance(v, float) and _math.isnan(v))

    # Collect available stations as (lat, value) pairs
    anchors: list[tuple[float, float]] = []
    if not _bad(fwd_val):
        anchors.append((_FWD_LAT, fwd_val))   # type: ignore[arg-type]
    if not _bad(oun_val):
        anchors.append((_OUN_LAT, oun_val))   # type: ignore[arg-type]
    if not _bad(lmn_val):
        anchors.append((_LMN_LAT, lmn_val))   # type: ignore[arg-type]

    if not anchors:
        return None
    if len(anchors) == 1:
        return anchors[0][1]

    # Sort by latitude (should already be sorted, but be safe)
    anchors.sort(key=lambda x: x[0])

    # Clamp outside the covered range
    if lat <= anchors[0][0]:
        return anchors[0][1]
    if lat >= anchors[-1][0]:
        return anchors[-1][1]

    # Find the enclosing segment
    for i in range(len(anchors) - 1):
        lat_lo, val_lo = anchors[i]
        lat_hi, val_hi = anchors[i + 1]
        if lat_lo <= lat <= lat_hi:
            t = (lat - lat_lo) / (lat_hi - lat_lo)
            return _lerp(val_lo, val_hi, t)

    # Fallback (should not reach)
    return anchors[-1][1]


def _dryline_distance_miles(county: OklahomaCounty, dryline: BoundaryObservation) -> Optional[float]:
    """
    Approximate east-west distance (miles) from the county centroid to the
    nearest point on the dryline polyline.  Positive = county is east of line.
    """
    if not dryline.position_lat or not dryline.position_lon:
        return None

    # Interpolate the dryline longitude at this county's latitude
    lats = dryline.position_lat
    lons = dryline.position_lon

    # Simple linear interpolation along S→N polyline
    dl_lon: Optional[float] = None
    for j in range(len(lats) - 1):
        if lats[j] <= county.lat <= lats[j + 1]:
            t = (county.lat - lats[j]) / (lats[j + 1] - lats[j])
            dl_lon = lons[j] + t * (lons[j + 1] - lons[j])
            break

    # Outside polyline lat range: use nearest endpoint
    if dl_lon is None:
        if county.lat < min(lats):
            dl_lon = lons[lats.index(min(lats))]
        else:
            dl_lon = lons[lats.index(max(lats))]

    dlon = county.lon - dl_lon   # negative = county is west of dryline
    return dlon * _MILES_PER_DEG_LON


def _score_environment(env: CountyEnvironment) -> str:
    """Map a county environment to a risk tier string."""
    cape = env.MLCAPE
    cin  = env.MLCIN
    cap  = env.cap_strength
    srh1 = env.SRH_0_1km
    srh3 = env.SRH_0_3km
    ehi  = env.EHI

    # Minimum instability for any meaningful risk
    if cape < 200:
        return "LOW"

    # EXTREME: uncapped, high instability, extreme kinematics
    if cin < 50 and cape >= 1500 and srh1 >= 250 and ehi >= 3.5:
        return "EXTREME"

    # HIGH: cap is manageable, strong kinematics
    if cin < 100 and cape >= 1000 and srh1 >= 150 and ehi >= 2.0:
        return "HIGH"
    if cin < 80 and cape >= 800 and srh3 >= 300:
        return "HIGH"

    # DANGEROUS_CAPPED: strong cap but violent kinematics + boundary nearby
    # (or just extreme kinematics regardless of boundary)
    if cin >= 80 and cape >= 800 and srh1 >= 150 and ehi >= 2.0:
        return "DANGEROUS_CAPPED"
    if cin >= 80 and cape >= 1000 and srh1 >= 200 and env.near_dryline:
        return "DANGEROUS_CAPPED"

    # MODERATE: some potential, moderate kinematics
    if cape >= 500 and cin < 200 and (srh1 >= 100 or ehi >= 1.0):
        return "MODERATE"
    if cape >= 500 and cin < 150 and srh3 >= 200:
        return "MODERATE"

    # MARGINAL
    if cape >= 200 and cin < 250 and srh1 >= 50:
        return "MARGINAL"

    return "LOW"


# ── Public API ────────────────────────────────────────────────────────────────

def build_county_environments(
    oun_indices,
    oun_kinematics,
    lmn_indices=None,
    lmn_kinematics=None,
    dryline: Optional[BoundaryObservation] = None,
    fwd_indices=None,
    fwd_kinematics=None,
) -> list[CountyEnvironment]:
    """
    Build a CountyEnvironment for every Oklahoma county by piecewise-
    interpolating across the FWD → OUN → LMN sounding corridor.

    Parameters
    ----------
    oun_indices    : ThermodynamicIndices from OUN sounding (required)
    oun_kinematics : KinematicProfile from OUN sounding (may be None)
    lmn_indices    : ThermodynamicIndices from LMN sounding (may be None)
    lmn_kinematics : KinematicProfile from LMN sounding (may be None)
    dryline        : detected BoundaryObservation (may be None)
    fwd_indices    : ThermodynamicIndices from FWD sounding (may be None)
    fwd_kinematics : KinematicProfile from FWD sounding (may be None)
    """
    # Extract scalar values; None when a station is unavailable
    def _ti(obj, attr):
        return getattr(obj, attr, None) if obj is not None else None

    fwd_cape  = _ti(fwd_indices, "MLCAPE")
    fwd_cin   = _ti(fwd_indices, "MLCIN")
    fwd_cap   = _ti(fwd_indices, "cap_strength")
    fwd_srh1  = _ti(fwd_kinematics, "SRH_0_1km")
    fwd_srh3  = _ti(fwd_kinematics, "SRH_0_3km")
    fwd_bwd6  = _ti(fwd_kinematics, "BWD_0_6km")
    fwd_ehi   = _ti(fwd_kinematics, "EHI") or 0.0

    oun_cape  = _ti(oun_indices, "MLCAPE")
    oun_cin   = _ti(oun_indices, "MLCIN")
    oun_cap   = _ti(oun_indices, "cap_strength")
    oun_srh1  = _ti(oun_kinematics, "SRH_0_1km")
    oun_srh3  = _ti(oun_kinematics, "SRH_0_3km")
    oun_bwd6  = _ti(oun_kinematics, "BWD_0_6km")
    oun_ehi   = _ti(oun_kinematics, "EHI") or 0.0

    lmn_cape  = _ti(lmn_indices, "MLCAPE")
    lmn_cin   = _ti(lmn_indices, "MLCIN")
    lmn_cap   = _ti(lmn_indices, "cap_strength")
    lmn_srh1  = _ti(lmn_kinematics, "SRH_0_1km")
    lmn_srh3  = _ti(lmn_kinematics, "SRH_0_3km")
    lmn_bwd6  = _ti(lmn_kinematics, "BWD_0_6km")
    lmn_ehi   = _ti(lmn_kinematics, "EHI") or 0.0

    environments: list[CountyEnvironment] = []

    for county in OklahomaCounty:
        lat = county.lat

        cape  = _interpolate_at_lat(lat, fwd_cape,  oun_cape,  lmn_cape)  or 0.0
        cin   = _interpolate_at_lat(lat, fwd_cin,   oun_cin,   lmn_cin)   or 0.0
        cap   = _interpolate_at_lat(lat, fwd_cap,   oun_cap,   lmn_cap)   or 0.0
        srh1  = _interpolate_at_lat(lat, fwd_srh1,  oun_srh1,  lmn_srh1)  or 0.0
        srh3  = _interpolate_at_lat(lat, fwd_srh3,  oun_srh3,  lmn_srh3)  or 0.0
        bwd6  = _interpolate_at_lat(lat, fwd_bwd6,  oun_bwd6,  lmn_bwd6)  or 0.0
        ehi   = _interpolate_at_lat(lat, fwd_ehi,   oun_ehi,   lmn_ehi)   or 0.0

        # Dryline proximity: county is "near dryline" if within ±50 miles E-W
        near_dl = False
        if dryline is not None:
            dist = _dryline_distance_miles(county, dryline)
            if dist is not None and -50 <= dist <= 50:
                near_dl = True

        environments.append(CountyEnvironment(
            county=county,
            MLCAPE=cape,
            MLCIN=cin,
            cap_strength=cap,
            SRH_0_1km=srh1,
            SRH_0_3km=srh3,
            BWD_0_6km=bwd6,
            EHI=ehi,
            near_dryline=near_dl,
        ))

    return environments


def compute_risk_zones(
    oun_indices,
    oun_kinematics,
    lmn_indices=None,
    lmn_kinematics=None,
    dryline: Optional[BoundaryObservation] = None,
    min_tier: str = "MARGINAL",
    fwd_indices=None,
    fwd_kinematics=None,
) -> list[RiskZone]:
    """
    Compute county-level risk zones for Oklahoma.

    Returns a list of RiskZone objects (one per tier) sorted highest→lowest.
    Only tiers at or above `min_tier` are returned.

    Parameters
    ----------
    min_tier       : Minimum tier to include ("LOW", "MARGINAL", "MODERATE",
                     "DANGEROUS_CAPPED", "HIGH", "EXTREME"). Default "MARGINAL"
                     suppresses low-noise counties.
    fwd_indices    : ThermodynamicIndices from FWD sounding — extends the
                     interpolation corridor southward to 32.83°N (may be None)
    fwd_kinematics : KinematicProfile from FWD sounding (may be None)
    """
    environments = build_county_environments(
        oun_indices, oun_kinematics,
        lmn_indices, lmn_kinematics,
        dryline,
        fwd_indices=fwd_indices,
        fwd_kinematics=fwd_kinematics,
    )

    min_rank = _TIER_RANK.get(min_tier, 1)

    # Score every county
    scored: dict[str, list[CountyEnvironment]] = {}
    for env in environments:
        tier = _score_environment(env)
        if _TIER_RANK.get(tier, 0) >= min_rank:
            scored.setdefault(tier, []).append(env)

    # Build RiskZone per tier
    zones: list[RiskZone] = []
    for tier, envs in scored.items():
        counties = [e.county for e in envs]
        lats = [c.lat for c in counties]
        lons = [c.lon for c in counties]
        lat_min, lat_max = min(lats), max(lats)
        lon_min, lon_max = min(lons), max(lons)
        center_lat = sum(lats) / len(lats)
        center_lon = sum(lons) / len(lons)
        span_ns = (lat_max - lat_min) * _MILES_PER_DEG_LAT
        span_ew = (lon_max - lon_min) * _MILES_PER_DEG_LON

        peak = max(envs, key=lambda e: e.SRH_0_1km + e.EHI * 50)

        zones.append(RiskZone(
            tier=tier,
            counties=counties,
            center_lat=center_lat,
            center_lon=center_lon,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            span_ew_mi=span_ew,
            span_ns_mi=span_ns,
            peak_MLCAPE=peak.MLCAPE,
            peak_MLCIN=peak.MLCIN,
            peak_SRH_0_1km=peak.SRH_0_1km,
            peak_EHI=peak.EHI,
        ))

    zones.sort(key=lambda z: z.tier_rank, reverse=True)
    return zones


def compute_risk_zones_from_hrrr(
    hrrr_snapshot,
    dryline: Optional[BoundaryObservation] = None,
    min_tier: str = "MARGINAL",
) -> list[RiskZone]:
    """
    Compute county risk zones using HRRR per-county data directly.

    This is the preferred path when a HRRRCountySnapshot is available —
    it uses actual 3km model analysis at every county centroid rather than
    the OUN/LMN N-S interpolation.

    Parameters
    ----------
    hrrr_snapshot : HRRRCountySnapshot from HRRRClient.get_county_snapshot()
    dryline       : Optional detected BoundaryObservation
    min_tier      : Minimum tier to include (default "MARGINAL")
    """
    if hrrr_snapshot is None:
        return []

    min_rank = _TIER_RANK.get(min_tier, 1)

    scored: dict[str, list[CountyEnvironment]] = {}

    for pt in hrrr_snapshot.counties:
        county = pt.county

        # Dryline proximity flag
        near_dl = False
        if dryline is not None:
            dist = _dryline_distance_miles(county, dryline)
            if dist is not None and -50 <= dist <= 50:
                near_dl = True

        env = CountyEnvironment(
            county=county,
            MLCAPE=pt.MLCAPE,
            MLCIN=pt.MLCIN,
            cap_strength=0.0,      # HRRR doesn't output cap strength directly
            SRH_0_1km=pt.SRH_0_1km,
            SRH_0_3km=pt.SRH_0_3km,
            BWD_0_6km=pt.BWD_0_6km,
            EHI=pt.EHI or 0.0,
            near_dryline=near_dl,
        )

        tier = _score_environment(env)
        if _TIER_RANK.get(tier, 0) >= min_rank:
            scored.setdefault(tier, []).append(env)

    # Build RiskZone per tier (same assembly as compute_risk_zones)
    zones: list[RiskZone] = []
    for tier, envs in scored.items():
        counties = [e.county for e in envs]
        lats = [c.lat for c in counties]
        lons = [c.lon for c in counties]
        lat_min, lat_max = min(lats), max(lats)
        lon_min, lon_max = min(lons), max(lons)
        center_lat = sum(lats) / len(lats)
        center_lon = sum(lons) / len(lons)
        span_ns = (lat_max - lat_min) * _MILES_PER_DEG_LAT
        span_ew = (lon_max - lon_min) * _MILES_PER_DEG_LON
        peak = max(envs, key=lambda e: e.SRH_0_1km + e.EHI * 50)

        zones.append(RiskZone(
            tier=tier,
            counties=counties,
            center_lat=center_lat,
            center_lon=center_lon,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            span_ew_mi=span_ew,
            span_ns_mi=span_ns,
            peak_MLCAPE=peak.MLCAPE,
            peak_MLCIN=peak.MLCIN,
            peak_SRH_0_1km=peak.SRH_0_1km,
            peak_EHI=peak.EHI,
        ))

    zones.sort(key=lambda z: z.tier_rank, reverse=True)
    return zones
