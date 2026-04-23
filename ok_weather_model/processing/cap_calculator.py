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
from ..models.boundary import BoundaryObservation
from ..models.kinematic import KinematicProfile

logger = logging.getLogger(__name__)

# Typical solar heating efficiency for Oklahoma plains
# (J/kg per °F of surface temperature rise — empirical)
HEATING_EFFICIENCY = 8.0  # J/kg CIN eroded per °F surface warming per hour

# Threshold values for forcing classification
STRONG_FORCING_THRESHOLD = -20.0  # J/kg/hr — strong dynamic forcing
WEAK_FORCING_THRESHOLD = -5.0     # J/kg/hr — weak forcing

# Boundary convergence scaling: J/kg/hr of CIN erosion per knot of boundary-normal inflow.
# Calibrated so that a strong dryline case (~30 kt perpendicular LLJ) contributes
# ~15 J/kg/hr — comparable in magnitude to peak surface heating forcing.
BOUNDARY_CONVERGENCE_SCALE = 0.5  # J/kg/hr per knot of normal inflow

# CIN thresholds for cap state classification
STRONG_CAP = 100.0       # J/kg — very strong cap
MODERATE_CAP = 50.0      # J/kg — typical capping inversion
WEAK_CAP = 25.0          # J/kg — cap easily eroded by heating alone
MARGINAL_CAP = 10.0      # J/kg — effectively no cap


def compute_cap_erosion_budget(
    sounding: ThermodynamicIndices,
    mesonet_state: CountySurfaceState,
    era5_fields: Optional[dict] = None,
    boundary: Optional[BoundaryObservation] = None,
    kinematics: Optional[KinematicProfile] = None,
) -> CapErosionBudget:
    """
    Compute the instantaneous cap erosion budget.

    Estimates each forcing term and preservation term from available data,
    then sums to a net_tendency (J/kg per hour).

    Args:
        sounding: ThermodynamicIndices from nearest sounding (spatial or temporal)
        mesonet_state: CountySurfaceState at the analysis time
        era5_fields: Optional dict from ERA5Client.get_synoptic_analysis()
        boundary: Optional mesoscale boundary intersecting or approaching the county
        kinematics: Optional KinematicProfile used to resolve inflow angle vs boundary

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
    # Scaled by the boundary-normal component of low-level inflow.
    # Zero when no boundary is provided or inflow is parallel to the boundary.
    if boundary is not None and kinematics is not None:
        angle_info = compute_boundary_inflow_angle(boundary, kinematics)
        boundary_forcing = _boundary_forcing_from_normal_inflow(
            angle_info["normal_inflow_component_kt"]
        )
    else:
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
    # Primary: CIN has dropped to/below MARGINAL_CAP in an observed sounding.
    # Secondary: a budget snapshot projects erosion within the analysis window
    # (current_CIN / |net_tendency| < remaining hours in forcing window).
    erosion_achieved = False
    erosion_time: Optional[datetime] = None

    for budget in budgets:
        if budget.current_CIN <= MARGINAL_CAP:
            erosion_achieved = True
            erosion_time = budget.valid_time
            break

    if not erosion_achieved:
        # Use the earliest projected erosion time from any budget snapshot,
        # but only projections that fall within the convective window:
        # same calendar day through 03Z of the following day.
        case_date = budgets[0].valid_time.date()
        window_end = datetime(
            case_date.year, case_date.month, case_date.day, 3, 0,
            tzinfo=budgets[0].valid_time.tzinfo
        ) + timedelta(days=1)   # 03Z next day
        projected_times: list[datetime] = [
            b.projected_erosion_time
            for b in budgets
            if b.projected_erosion_time is not None
            and b.projected_erosion_time <= window_end
        ]
        if projected_times:
            erosion_achieved = True
            erosion_time = min(projected_times)

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


def compute_ces_from_sounding(
    indices: "ThermodynamicIndices",
    surface_temp_12Z_c: float,
    case_date: "date",
) -> dict:
    """
    Compute Cap Erosion Score using sounding data and the Oklahoma
    climatological heating model.  Does not require Mesonet data.

    Physics:
        Oklahoma's convective cap is primarily an Elevated Mixed Layer (EML)
        warm nose at 600–700mb.  The cap erodes when the afternoon boundary
        layer mixes deep enough to overcome the warm nose.  The surface
        temperature needed to drive that mixing is estimated as:

            T_eff = T_12Z  +  cap_strength × 4.5  +  MLCIN / 8.0

        Where:
          - cap_strength × 4.5 converts the °C warm-nose excess to the
            equivalent surface °F increase needed (via the sub-cap lapse rate)
          - MLCIN / 8.0 converts J/kg inhibition to °F needed using the
            empirical Oklahoma heating efficiency (~8 J/kg per °F per hour)

        The heating model then estimates when T_surface(t) ≥ T_eff.

    The convective_temp_gap fields represent T_eff − T_surface at each
    analysis hour (positive = cap still holding, negative = cap has broken).

    Args:
        indices: ThermodynamicIndices from 12Z OUN sounding
        surface_temp_12Z_c: Surface temperature at 12Z (°C) from sounding level 0
        case_date: Calendar date of the case

    Returns:
        dict with keys:
            convective_temp_gap_12Z: float (°F, positive = cap holding)
            convective_temp_gap_15Z: float (°F)
            convective_temp_gap_18Z: float (°F)
            cap_erosion_time: Optional[time] (UTC, or None if cap never erodes)
            cap_behavior: CapBehavior
    """
    from datetime import time as time_type
    from ..models.enums import CapBehavior

    doy = case_date.timetuple().tm_yday
    t12z_f = surface_temp_12Z_c * 9.0 / 5.0 + 32.0

    # Effective convective temperature: surface must warm this much above 12Z
    # to break the EML cap and drive deep convection
    CAP_LAPSE_FACTOR = 4.5   # °F surface warming per °C of cap strength
    t_eff_f = t12z_f + (indices.cap_strength * CAP_LAPSE_FACTOR) + (indices.MLCIN / HEATING_EFFICIENCY)

    # Tc gaps at standard synoptic analysis hours
    gaps: dict = {}
    for label, hour in [("12Z", 12.0), ("15Z", 15.0), ("18Z", 18.0)]:
        t_sfc = _okla_surface_temp_f(t12z_f, hour, doy)
        gaps[f"convective_temp_gap_{label}"] = compute_convective_temp_gap(t_sfc, t_eff_f)

    # Scan hourly 12Z–02Z for erosion, then interpolate sub-hourly
    erosion_time = None
    for h in range(12, 27):
        t_sfc = _okla_surface_temp_f(t12z_f, float(h), doy)
        if t_sfc >= t_eff_f:
            if h > 12:
                t_prev = _okla_surface_temp_f(t12z_f, float(h - 1), doy)
                frac = (t_eff_f - t_prev) / (t_sfc - t_prev) if t_sfc > t_prev else 0.0
                erosion_utc = (h - 1) + frac
            else:
                erosion_utc = float(h)

            hour_of_day = int(erosion_utc) % 24
            minute_of_hour = int(round((erosion_utc % 1) * 60))
            if minute_of_hour == 60:
                hour_of_day = (hour_of_day + 1) % 24
                minute_of_hour = 0
            erosion_time = time_type(hour_of_day, minute_of_hour)
            break

    # CapBehavior classification
    if erosion_time is None:
        cap_behavior = CapBehavior.NO_EROSION
    elif erosion_time.hour <= 18:
        cap_behavior = CapBehavior.EARLY_EROSION
    elif erosion_time.hour <= 21:
        cap_behavior = CapBehavior.CLEAN_EROSION
    else:
        cap_behavior = CapBehavior.LATE_EROSION

    return {
        **gaps,
        "cap_erosion_time": erosion_time,
        "cap_behavior": cap_behavior,
    }


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


# ── Boundary-inflow angle ─────────────────────────────────────────────────────

def compute_boundary_inflow_angle(
    boundary: BoundaryObservation,
    kinematics: KinematicProfile,
) -> dict:
    """
    Compute the angle between low-level inflow and a mesoscale boundary.

    The boundary-normal inflow component drives convergence along the boundary:
    maximum when inflow is perpendicular (angle = 90°), zero when parallel.

    Inflow source priority:
        1. LLJ (850 mb) — best proxy for dryline inflow
        2. Mean 0–1 km wind from KinematicProfile levels
        3. Returns zeros if neither is available

    Physics:
        convergence_efficiency = sin(angle_to_boundary)
        normal_inflow_component = convergence_efficiency × inflow_speed

    Args:
        boundary: BoundaryObservation with a valid orientation_angle
        kinematics: KinematicProfile containing LLJ or wind level data

    Returns dict with keys:
        boundary_orientation_deg  — strike of boundary (0–180°)
        inflow_direction_deg      — direction inflow is moving toward (0–360°)
        inflow_speed_kt           — inflow speed (knots)
        angle_to_boundary_deg     — angle between inflow and boundary line (0–90°);
                                    90° = perpendicular = maximum convergence
        convergence_efficiency    — sin(angle_to_boundary), 0.0–1.0
        normal_inflow_component_kt — convergence_efficiency × inflow_speed_kt
        inflow_source             — "LLJ_850mb" | "mean_0_1km" | "none"
    """
    import math

    orientation = boundary.orientation_angle
    if orientation is None:
        orientation = boundary.compute_orientation_from_polyline()

    # ── Resolve inflow vector ─────────────────────────────────────────────────
    inflow_speed_kt: float = 0.0
    inflow_from_deg: float = 0.0
    inflow_source: str = "none"

    if kinematics.LLJ_speed is not None and kinematics.LLJ_direction is not None:
        inflow_speed_kt = kinematics.LLJ_speed
        inflow_from_deg = kinematics.LLJ_direction
        inflow_source = "LLJ_850mb"
    elif kinematics.levels:
        # Fall back to mean wind in the lowest 1 km
        low_levels = [lv for lv in kinematics.levels if lv.height <= 1000.0]
        if low_levels:
            u_mean = sum(lv.u_component for lv in low_levels) / len(low_levels)
            v_mean = sum(lv.v_component for lv in low_levels) / len(low_levels)
            speed_ms = math.sqrt(u_mean ** 2 + v_mean ** 2)
            inflow_speed_kt = speed_ms * 1.94384
            inflow_from_deg = math.degrees(math.atan2(-u_mean, -v_mean)) % 360
            inflow_source = "mean_0_1km"

    if inflow_speed_kt == 0.0:
        return {
            "boundary_orientation_deg": orientation,
            "inflow_direction_deg": 0.0,
            "inflow_speed_kt": 0.0,
            "angle_to_boundary_deg": 0.0,
            "convergence_efficiency": 0.0,
            "normal_inflow_component_kt": 0.0,
            "inflow_source": inflow_source,
        }

    # ── Compute angle between inflow and boundary ─────────────────────────────
    # Convert meteorological FROM direction to the direction the wind moves TOWARD
    inflow_toward_deg = (inflow_from_deg + 180.0) % 360.0

    # Unit vector for inflow direction (toward)
    inflow_rad = math.radians(inflow_toward_deg)
    inflow_u = math.sin(inflow_rad)   # east component
    inflow_v = math.cos(inflow_rad)   # north component

    # Unit vector along boundary strike direction
    boundary_rad = math.radians(orientation)
    boundary_u = math.sin(boundary_rad)
    boundary_v = math.cos(boundary_rad)

    # |cross product| of two unit 2-D vectors = |sin(angle between them)|
    # angle_to_boundary = 0° → inflow parallel to boundary (no convergence)
    # angle_to_boundary = 90° → inflow perpendicular to boundary (max convergence)
    cross = abs(inflow_u * boundary_v - inflow_v * boundary_u)
    cross = min(cross, 1.0)  # guard against floating-point overshoot
    angle_to_boundary_deg = math.degrees(math.asin(cross))
    convergence_efficiency = cross  # = sin(angle_to_boundary)

    return {
        "boundary_orientation_deg": orientation,
        "inflow_direction_deg": inflow_toward_deg,
        "inflow_speed_kt": inflow_speed_kt,
        "angle_to_boundary_deg": angle_to_boundary_deg,
        "convergence_efficiency": convergence_efficiency,
        "normal_inflow_component_kt": convergence_efficiency * inflow_speed_kt,
        "inflow_source": inflow_source,
    }


def _boundary_forcing_from_normal_inflow(normal_inflow_kt: float) -> float:
    """
    Convert the boundary-normal inflow component to a CIN erosion rate.

    Returns J/kg per hour (negative = eroding the cap).
    A 30-kt perpendicular inflow → roughly -15 J/kg/hr.
    """
    return -1.0 * normal_inflow_kt * BOUNDARY_CONVERGENCE_SCALE


# ── Private estimation helpers ────────────────────────────────────────────────

def _heating_amplitude(day_of_year: int) -> float:
    """
    Climatological max surface heating above 12Z temp (°F) for central Oklahoma.

    Uses a sinusoidal fit that peaks near the summer solstice:
        ~12°F in winter, ~20°F in spring, ~22°F at summer solstice.

    Calibrated against Oklahoma Mesonet 30-year climatology for 12Z→peak ΔT.
    """
    import math
    return 15.0 + 7.0 * math.sin(2 * math.pi * (day_of_year - 80) / 365)


def _okla_surface_temp_f(
    t12z_f: float,
    hour_utc: float,
    day_of_year: int,
) -> float:
    """
    Estimate Oklahoma surface temperature (°F) at a given UTC hour,
    anchored to the 12Z surface temperature from the sounding.

    Heating curve:
        12Z–21Z: sin^1 ramp from 0 to peak amplitude
        21Z–02Z: linear cooling back toward 12Z value

    Peak at 21Z = 4pm CDT, consistent with Oklahoma spring climatology.
    Hours >= 24 are handled as next-day UTC (25 = 01Z next day).
    """
    import math

    t_peak_utc = 21.0
    t_end_utc = 26.0   # 02Z next day — effectively back to near-12Z temp

    amplitude = _heating_amplitude(day_of_year)

    if hour_utc < 12:
        hour_utc += 24  # handle post-midnight hours in 12Z-relative frame

    if hour_utc <= t_peak_utc:
        frac = (hour_utc - 12.0) / (t_peak_utc - 12.0)
        delta_t = amplitude * math.sin(math.pi / 2 * frac)
    else:
        frac = min((hour_utc - t_peak_utc) / (t_end_utc - t_peak_utc), 1.0)
        delta_t = amplitude * (1.0 - frac)

    return t12z_f + delta_t


def _estimate_dynamic_forcing(
    sounding: ThermodynamicIndices,
    era5_fields: Optional[dict],
    valid_time: datetime,
) -> float:
    """
    Estimate dynamic (synoptic-scale) forcing contribution to CIN erosion.

    Prefers the advection-based result from compute_synoptic_cap_forcing()
    when available (era5_fields["cap_advection"]).  Falls back to a cruder
    700 mb temperature-anomaly proxy when only get_synoptic_analysis() data
    is present.

    Returns J/kg per hour (negative = eroding).
    """
    if era5_fields is None:
        return 0.0

    # ── Preferred: advection-based diagnostics ────────────────────────────────
    cap_adv = era5_fields.get("cap_advection")
    if cap_adv is not None:
        return cap_adv.get("dynamic_cap_forcing_jkg_hr", 0.0)

    # ── Legacy fallback: 700 mb temperature anomaly ───────────────────────────
    # Warm anomaly at 700 mb relative to -5°C climatology is a crude proxy for
    # whether synoptic forcing is present; replaced by proper advection above.
    t700 = era5_fields.get("temp_700mb_ok", {})
    mean_t700 = t700.get("mean_c", None)
    if mean_t700 is None:
        return 0.0
    t700_anomaly = mean_t700 - (-5.0)
    return max(-2.0 * t700_anomaly, -30.0)


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
