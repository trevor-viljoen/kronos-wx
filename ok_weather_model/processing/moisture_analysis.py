"""
Surface moisture diagnostics from Mesonet network snapshots.

Computes the spatial structure of boundary layer moisture across Oklahoma —
primarily used to diagnose Gulf moisture return, which is the critical
ingredient separating productive severe weather setups from cap busts.

Key output: MoistureReturnProfile
    - State-mean surface dewpoint
    - Southern/northern tier gradients (how far north Gulf moisture has arrived)
    - Moisture return index (small = well-mixed statewide, large = moisture
      concentrated in southern counties, hasn't returned north)
    - Estimated dewpoint axis latitude
"""
from __future__ import annotations

import logging
from typing import Optional

from ..models.mesonet import MesonetObservation
from ..models.enums import OklahomaCounty

logger = logging.getLogger(__name__)

# Southern tier: first counties to receive Gulf moisture return
# Roughly south of 35°N — leading edge of southerly moisture feed
_SOUTH_COUNTIES = {
    OklahomaCounty.LOVE, OklahomaCounty.MARSHALL, OklahomaCounty.CARTER,
    OklahomaCounty.MURRAY, OklahomaCounty.JOHNSTON, OklahomaCounty.ATOKA,
    OklahomaCounty.COAL, OklahomaCounty.BRYAN, OklahomaCounty.CHOCTAW,
    OklahomaCounty.MCCURTAIN, OklahomaCounty.PUSHMATAHA, OklahomaCounty.LE_FLORE,
    OklahomaCounty.TILLMAN, OklahomaCounty.COTTON, OklahomaCounty.COMANCHE,
    OklahomaCounty.JEFFERSON,
}

# Northern tier: last to see moisture return; high dewpoints here mean
# Gulf moisture is well-established statewide
_NORTH_COUNTIES = {
    OklahomaCounty.KAY, OklahomaCounty.GRANT, OklahomaCounty.ALFALFA,
    OklahomaCounty.WOODS, OklahomaCounty.WOODWARD, OklahomaCounty.HARPER,
    OklahomaCounty.BEAVER, OklahomaCounty.TEXAS, OklahomaCounty.CIMARRON,
}

# Deep Gulf moisture threshold: 60°F surface dewpoint is the classic
# "moist, juicy air mass" marker in Oklahoma severe weather forecasting
_GULF_MOISTURE_TD_F = 60.0


class MoistureReturnProfile:
    """
    Snapshot of statewide moisture structure derived from Mesonet observations.

    Attributes
    ----------
    state_mean_dewpoint_f : float
        Statewide mean surface dewpoint (°F), all stations with valid data.
    south_ok_dewpoint_f : float
        Mean surface Td across southern Oklahoma counties (°F).
        Reflects the leading edge of Gulf moisture return.
    north_ok_dewpoint_f : float
        Mean surface Td across northern Oklahoma counties (°F).
    moisture_return_gradient_f : float
        south_ok_dewpoint − north_ok_dewpoint (°F).
        Near zero: Gulf moisture is well-mixed statewide.
        Large positive: moisture concentrated south, hasn't returned north.
    moisture_axis_lat : Optional[float]
        Estimated latitude of the 60°F dewpoint contour (°N), derived from
        a linear interpolation between southern and northern tier means.
        None if all stations are above or below threshold.
    gulf_moisture_fraction : float
        Fraction of all stations reporting Td ≥ 60°F (0.0–1.0).
    n_stations : int
        Number of stations with valid dewpoint observations.
    """

    def __init__(
        self,
        state_mean_dewpoint_f: float,
        south_ok_dewpoint_f: float,
        north_ok_dewpoint_f: float,
        moisture_return_gradient_f: float,
        moisture_axis_lat: Optional[float],
        gulf_moisture_fraction: float,
        n_stations: int,
    ):
        self.state_mean_dewpoint_f = state_mean_dewpoint_f
        self.south_ok_dewpoint_f = south_ok_dewpoint_f
        self.north_ok_dewpoint_f = north_ok_dewpoint_f
        self.moisture_return_gradient_f = moisture_return_gradient_f
        self.moisture_axis_lat = moisture_axis_lat
        self.gulf_moisture_fraction = gulf_moisture_fraction
        self.n_stations = n_stations

    def __repr__(self) -> str:
        axis = f"{self.moisture_axis_lat:.1f}°N" if self.moisture_axis_lat else "N/A"
        return (
            f"MoistureReturn(state={self.state_mean_dewpoint_f:.1f}°F "
            f"south={self.south_ok_dewpoint_f:.1f}°F "
            f"north={self.north_ok_dewpoint_f:.1f}°F "
            f"gradient={self.moisture_return_gradient_f:+.1f}°F "
            f"axis={axis} "
            f"gulf_frac={self.gulf_moisture_fraction:.0%})"
        )


def compute_moisture_return(
    observations: list[MesonetObservation],
) -> Optional[MoistureReturnProfile]:
    """
    Compute Gulf moisture return diagnostics from a Mesonet network snapshot.

    Accepts any list of MesonetObservation objects — typically a single
    5-minute network snapshot from MesonetClient.get_snapshot_observations().

    Returns None if fewer than 5 stations have valid dewpoint observations.
    """
    valid = [
        obs for obs in observations
        if obs.dewpoint is not None and not _is_suspect(obs.dewpoint)
    ]

    if len(valid) < 5:
        logger.debug("compute_moisture_return: only %d valid stations", len(valid))
        return None

    # ── Statewide mean ─────────────────────────────────────────────────────────
    all_td = [obs.dewpoint for obs in valid]
    state_mean = sum(all_td) / len(all_td)

    # ── Tier means ────────────────────────────────────────────────────────────
    south_td = [
        obs.dewpoint for obs in valid
        if obs.county in _SOUTH_COUNTIES
    ]
    north_td = [
        obs.dewpoint for obs in valid
        if obs.county in _NORTH_COUNTIES
    ]

    south_mean = sum(south_td) / len(south_td) if south_td else state_mean
    north_mean = sum(north_td) / len(north_td) if north_td else state_mean
    gradient = south_mean - north_mean

    # ── Gulf moisture fraction ─────────────────────────────────────────────────
    gulf_count = sum(1 for td in all_td if td >= _GULF_MOISTURE_TD_F)
    gulf_frac = gulf_count / len(all_td)

    # ── Moisture axis latitude ─────────────────────────────────────────────────
    # Linear interpolation: if south is warm enough and north is not, estimate
    # the latitude where 60°F crosses.
    axis_lat: Optional[float] = None
    if south_mean >= _GULF_MOISTURE_TD_F > north_mean:
        # Weighted interpolation between tier centroid latitudes
        south_lat = 34.3   # approximate centroid of southern tier
        north_lat = 36.7   # approximate centroid of northern tier
        # Fraction from south where dewpoint crosses threshold
        frac = (south_mean - _GULF_MOISTURE_TD_F) / max(south_mean - north_mean, 0.1)
        axis_lat = south_lat + frac * (north_lat - south_lat)
        axis_lat = round(max(33.5, min(37.0, axis_lat)), 2)

    return MoistureReturnProfile(
        state_mean_dewpoint_f=round(state_mean, 1),
        south_ok_dewpoint_f=round(south_mean, 1),
        north_ok_dewpoint_f=round(north_mean, 1),
        moisture_return_gradient_f=round(gradient, 1),
        moisture_axis_lat=axis_lat,
        gulf_moisture_fraction=round(gulf_frac, 3),
        n_stations=len(valid),
    )


def _is_suspect(dewpoint_f: float) -> bool:
    """Reject physically implausible Oklahoma surface dewpoints."""
    return dewpoint_f < -20.0 or dewpoint_f > 85.0
