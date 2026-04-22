"""
Cap erosion budget calculations.

The cap erosion budget framework treats the convective inhibition (CIN) as
a balance sheet: erosion forcings subtract from CIN while preservation
forcings add to it.  The net tendency determines whether the cap will erode
before the synoptic forcing window closes.

This is the central diagnostic tool of the KRONOS-WX system.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from ..models import (
    OklahomaCounty,
    ThermodynamicIndices,
    CountySurfaceState,
    CapErosionBudget,
    CapErosionTrajectory,
    ErosionMechanism,
)

logger = logging.getLogger(__name__)

# Typical solar heating efficiency for Oklahoma plains
# (J/kg per °F of surface temperature rise — empirical)
HEATING_EFFICIENCY = 8.0  # J/kg CIN eroded per °F surface warming per hour

# Threshold values for forcing classification
STRONG_FORCING_THRESHOLD = -20.0  # J/kg/hr — strong dynamic forcing
WEAK_FORCING_THRESHOLD = -5.0     # J/kg/hr — weak forcing

# CIN thresholds for cap state classification
STRONG_CAP = 100.0       # J/kg — very strong cap
MODERATE_CAP = 50.0      # J/kg — typical capping inversion
WEAK_CAP = 25.0          # J/kg — cap easily eroded by heating alone
MARGINAL_CAP = 10.0      # J/kg — effectively no cap


def compute_cap_erosion_budget(
    sounding: ThermodynamicIndices,
    mesonet_state: CountySurfaceState,
    era5_fields: Optional[dict] = None,
) -> CapErosionBudget:
    """
    Compute the instantaneous cap erosion budget.

    Estimates each forcing term and preservation term from available data,
    then sums to a net_tendency (J/kg per hour).

    Args:
        sounding: ThermodynamicIndices from nearest sounding (spatial or temporal)
        mesonet_state: CountySurfaceState at the analysis time
        era5_fields: Optional dict from ERA5Client.get_synoptic_analysis()

    Returns:
        CapErosionBudget with all forcing terms populated
    """
    current_CIN = sounding.MLCIN
    valid_time = mesonet_state.valid_time
    county = mesonet_state.county

    # ── Surface heating forcing ───────────────────────────────────────────────
    # If heating rate is available, estimate CIN reduction from surface parcel
    # warming.  Faster heating → faster erosion.
    heating_rate = mesonet_state.heating_rate_1hr or 0.0
    if heating_rate > 0:
        # Positive heating rate erodes CIN (negative forcing convention)
        heating_forcing = -1.0 * heating_rate * HEATING_EFFICIENCY
    else:
        heating_forcing = 0.0  # no forcing from cooling

    # ── Dynamic forcing ───────────────────────────────────────────────────────
    # Estimated from ERA5 vertical velocity or synoptic analysis
    dynamic_forcing = _estimate_dynamic_forcing(sounding, era5_fields, valid_time)

    # ── Boundary forcing ──────────────────────────────────────────────────────
    # Zero unless a boundary is known to be impinging — set by caller
    # after boundary analysis
    boundary_forcing = 0.0

    # ── Lapse rate forcing ────────────────────────────────────────────────────
    # Steepening lapse rates aloft (daytime destabilization) reduce cap
    # Proxy: 700–500mb lapse rate departure from moist adiabat (6.5°C/km)
    if sounding.lapse_rate_700_500 > 7.5:
        lapse_rate_forcing = -5.0  # steep lapse rates add erosion pressure
    elif sounding.lapse_rate_700_500 > 6.5:
        lapse_rate_forcing = -2.0
    else:
        lapse_rate_forcing = 0.0

    # ── Preservation: subsidence ──────────────────────────────────────────────
    subsidence_forcing = _estimate_subsidence_forcing(era5_fields, county, valid_time)

    # ── Preservation: EML advection ───────────────────────────────────────────
    # When fresh EML air is advecting in from the west, cap can be reinforced
    eml_advection_forcing = 0.0
    if sounding.EML_base is not None and sounding.EML_depth is not None:
        if sounding.EML_depth > 50.0:  # substantial EML present
            eml_advection_forcing = 3.0  # small positive reinforcement

    # ── Preservation: cold pool ───────────────────────────────────────────────
    cold_pool_forcing = 0.0  # requires radar/Mesonet wind shift detection

    # ── Net tendency ─────────────────────────────────────────────────────────
    net_tendency = (
        heating_forcing
        + dynamic_forcing
        + boundary_forcing
        + lapse_rate_forcing
        + subsidence_forcing
        + eml_advection_forcing
        + cold_pool_forcing
    )

    # ── Trajectory projection ─────────────────────────────────────────────────
    projected_erosion_time = None
    if net_tendency < 0 and current_CIN > 0:
        hours_to_zero = current_CIN / abs(net_tendency)
        if hours_to_zero < 24.0:  # only project if within a day
            projected_erosion_time = valid_time + timedelta(hours=hours_to_zero)

    # ── Confidence ────────────────────────────────────────────────────────────
    # Higher confidence when we have good data coverage
    confidence = mesonet_state.data_quality_score * 0.7
    if era5_fields:
        confidence = min(confidence + 0.2, 1.0)

    return CapErosionBudget(
        valid_time=valid_time,
        county=county,
        current_CIN=current_CIN,
        heating_forcing=heating_forcing,
        dynamic_forcing=dynamic_forcing,
        boundary_forcing=boundary_forcing,
        lapse_rate_forcing=lapse_rate_forcing,
        subsidence_forcing=subsidence_forcing,
        EML_advection_forcing=eml_advection_forcing,
        cold_pool_forcing=cold_pool_forcing,
        net_tendency=net_tendency,
        projected_erosion_time=projected_erosion_time,
        confidence=confidence,
    )


def compute_convective_temp_gap(
    surface_temp: float,
    convective_temp: float,
) -> float:
    """
    Compute the convective temperature gap: Tc minus current surface temp.

    Positive → cap still holding (surface hasn't reached convective temp).
    Negative → surface has exceeded convective temp (free convection possible).

    Args:
        surface_temp: Current surface temperature (°F)
        convective_temp: Convective temperature Tc (°F)

    Returns:
        Gap in °F
    """
    return convective_temp - surface_temp


def estimate_erosion_trajectory(
    budgets: list[CapErosionBudget],
    forcing_window_close: Optional[datetime] = None,
) -> CapErosionTrajectory:
    """
    Build a CapErosionTrajectory from a time series of budget snapshots.

    Determines whether erosion was achieved, when it occurred, and whether
    it fell within the synoptic forcing window.

    Args:
        budgets: List of CapErosionBudget objects in chronological order
        forcing_window_close: When the synoptic forcing exits the region

    Returns:
        CapErosionTrajectory summarizing the cap's lifecycle
    """
    if not budgets:
        raise ValueError("budgets list cannot be empty")

    # Sort by time
    budgets = sorted(budgets, key=lambda b: b.valid_time)
    county = budgets[0].county
    analysis_time = budgets[0].valid_time

    # ── Check for erosion achievement ─────────────────────────────────────────
    erosion_achieved = False
    erosion_time: Optional[datetime] = None

    for budget in budgets:
        if budget.current_CIN <= MARGINAL_CAP:
            erosion_achieved = True
            erosion_time = budget.valid_time
            break

    # Update forcing_window_close flags in budgets
    for budget in budgets:
        if forcing_window_close is not None:
            budget.forcing_window_close = forcing_window_close
            if erosion_achieved and erosion_time:
                budget.will_erode_in_window = erosion_time <= forcing_window_close
            else:
                budget.will_erode_in_window = False

    # ── Primary mechanism classification ─────────────────────────────────────
    primary_mechanism = _classify_primary_mechanism(budgets)

    # ── Bust risk score ───────────────────────────────────────────────────────
    bust_risk = _compute_bust_risk(
        budgets, erosion_achieved, forcing_window_close
    )

    return CapErosionTrajectory(
        county=county,
        analysis_time=analysis_time,
        budget_history=budgets,
        erosion_achieved=erosion_achieved,
        erosion_time=erosion_time,
        primary_mechanism=primary_mechanism,
        bust_risk_score=bust_risk,
    )


def compute_heating_rate_needed(
    current_CIN: float,
    hours_remaining: float,
) -> float:
    """
    Compute the surface heating rate (°F/hour) required to erode the cap
    within the available time, assuming heating is the primary mechanism.

    Args:
        current_CIN: Current CIN magnitude (J/kg)
        hours_remaining: Hours until forcing window closes

    Returns:
        Required heating rate in °F/hour.
        Returns float('inf') if hours_remaining <= 0.
    """
    if hours_remaining <= 0:
        return float("inf")
    if current_CIN <= MARGINAL_CAP:
        return 0.0

    # CIN needed per hour → surface warming per hour
    cin_rate_needed = current_CIN / hours_remaining  # J/kg per hour
    heating_rate_needed = cin_rate_needed / HEATING_EFFICIENCY  # °F/hour

    return heating_rate_needed


# ── Private estimation helpers ────────────────────────────────────────────────

def _estimate_dynamic_forcing(
    sounding: ThermodynamicIndices,
    era5_fields: Optional[dict],
    valid_time: datetime,
) -> float:
    """
    Estimate dynamic (synoptic-scale lift) forcing contribution to CIN erosion.

    Returns J/kg per hour (negative = eroding).
    """
    if era5_fields is None:
        return 0.0

    # Use 700mb temperature as proxy for synoptic forcing
    # Warm anomaly at 700mb (warm advection aloft) reduces CIN
    t700 = era5_fields.get("temp_700mb_ok", {})
    mean_t700 = t700.get("mean_c", None)

    if mean_t700 is None:
        return 0.0

    # Empirical: every 1°C warmer at 700mb relative to -5°C climatology
    # contributes ~2 J/kg/hr of dynamic erosion
    t700_anomaly = mean_t700 - (-5.0)  # positive = warmer than climatology
    dynamic_forcing = max(-2.0 * t700_anomaly, -30.0)  # cap at -30 J/kg/hr

    return dynamic_forcing


def _estimate_subsidence_forcing(
    era5_fields: Optional[dict],
    county: OklahomaCounty,
    valid_time: datetime,
) -> float:
    """
    Estimate subsidence preservation term.
    Returns J/kg per hour (positive = building cap).
    """
    if era5_fields is None:
        return 0.0

    # If jet is well to the north/east (right entrance / downstream), subsidence
    # is more likely.  Simple heuristic for now.
    jet = era5_fields.get("jet_position", {})
    jet_lat = jet.get("lat", None)

    if jet_lat is None:
        return 0.0

    # If jet is north of Oklahoma (~37°N), subsidence likely over most of state
    if jet_lat > 38.0:
        return 5.0   # moderate subsidence
    elif jet_lat > 36.0:
        return 2.0
    else:
        return 0.0


def _classify_primary_mechanism(budgets: list[CapErosionBudget]) -> ErosionMechanism:
    """
    Classify the dominant erosion mechanism from the budget history.
    Compares cumulative forcing contributions.
    """
    total_heating = sum(min(b.heating_forcing, 0) for b in budgets)
    total_dynamic = sum(min(b.dynamic_forcing, 0) for b in budgets)
    total_boundary = sum(min(b.boundary_forcing, 0) for b in budgets)

    forcings = {
        ErosionMechanism.HEATING: abs(total_heating),
        ErosionMechanism.DYNAMIC: abs(total_dynamic),
        ErosionMechanism.BOUNDARY: abs(total_boundary),
    }

    dominant = max(forcings, key=forcings.get)
    dominant_mag = forcings[dominant]
    total_mag = sum(forcings.values())

    if total_mag == 0:
        return ErosionMechanism.UNKNOWN

    dominant_fraction = dominant_mag / total_mag

    if dominant_fraction > 0.6:
        return dominant
    else:
        return ErosionMechanism.COMBINED


def _compute_bust_risk(
    budgets: list[CapErosionBudget],
    erosion_achieved: bool,
    forcing_window_close: Optional[datetime],
) -> float:
    """
    Compute bust risk score (0.0 = confident outbreak, 1.0 = confident bust).
    """
    if not budgets:
        return 0.5

    if erosion_achieved:
        return 0.1  # eroded → low bust risk

    last_budget = budgets[-1]

    # High CIN with no erosion tendency → high bust risk
    if last_budget.current_CIN > STRONG_CAP:
        return 0.9
    elif last_budget.current_CIN > MODERATE_CAP:
        return 0.7

    # Check if there's time remaining in the forcing window
    if forcing_window_close is not None:
        hours_left = (forcing_window_close - last_budget.valid_time).total_seconds() / 3600
        if hours_left < 1.0:
            return 0.85  # window closing, cap not eroded
        if hours_left < 3.0 and last_budget.current_CIN > WEAK_CAP:
            return 0.65

    # Moderate CIN with some erosion tendency → moderate risk
    if last_budget.net_tendency < STRONG_FORCING_THRESHOLD:
        return 0.3  # strong forcing, cap likely to erode
    elif last_budget.net_tendency < WEAK_FORCING_THRESHOLD:
        return 0.5
    else:
        return 0.7  # little forcing, cap likely to hold
