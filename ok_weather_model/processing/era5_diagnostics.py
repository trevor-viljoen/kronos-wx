"""
Diagnostic computations on ERA5 xarray Datasets.

This module bridges ERA5Client (raw data retrieval) and cap_calculator.py
(budget terms) by computing physically meaningful cap-erosion diagnostics
from the 3D ERA5 fields.

Primary entry point for the cap budget:

    ds = era5_client.get_upper_air_fields(case_date)
    cap_adv = compute_synoptic_cap_forcing(ds, valid_time)
    era5_fields = {"cap_advection": cap_adv, ...}
    budget = compute_cap_erosion_budget(sounding, surface, era5_fields=era5_fields)

Physical basis
--------------
The Oklahoma cap (EML warm nose, typically 600–700 mb) erodes from three
synoptic-scale mechanisms captured here:

  1. Cold thermal advection at 700 mb — directly cools the EML warm nose,
     reducing the temperature excess over a rising parcel.

  2. Differential thermal advection (500 mb warming faster than 700 mb) —
     steepens the 700–500 mb lapse rate, thinning the stable layer atop
     the EML from above.

  3. Upward vertical motion at 700 mb (negative omega) — synoptic-scale QG
     lift reduces CIN by forcing parcels upward toward the LFC.

All three yield negative J/kg/hr contributions to the cap erosion budget.
"""

import logging
import math
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import numpy as np

if TYPE_CHECKING:
    import xarray as xr

from ..models import OklahomaSoundingStation, SoundingLevel, SoundingProfile

logger = logging.getLogger(__name__)

# ── Conversion constants ──────────────────────────────────────────────────────

R_EARTH = 6_371_000.0  # meters

# J/kg/hr of CIN erosion per K/hr of cold advection at 700 mb (cap level).
# Physics: a -1 K/hr cold advection sustained for 3 hr removes ~3°C from the
# EML warm nose; at ~4.5 °F/°C × 8 J/kg/°F the implied CIN change is ~108 J/kg
# over that window, or ~36 J/kg/hr. We use 12 J/kg/hr as a conservative
# single-hour estimate because advection is rarely uniform across the layer.
CAP_ADVECTION_700_SCALE = 12.0  # J/kg/hr per K/hr

# J/kg/hr per K/hr of differential advection (500 mb - 700 mb cooling excess).
# Secondary term; destabilizes the layer above the cap but doesn't directly
# remove the warm nose.
DIFF_ADVECTION_SCALE = 8.0  # J/kg/hr per K/hr

# J/kg/hr per Pa/s of upward vertical motion at 700 mb (omega < 0 = up).
# A moderate outbreak day has ω ~ -0.5 Pa/s → -7.5 J/kg/hr, which is
# meaningful but secondary to heating and advection.
OMEGA_SCALE = 15.0  # J/kg/hr per Pa/s

# Per-term caps to prevent any single mechanism from dominating unrealistically
_MAX_COLD_ADV_FORCING = -30.0  # J/kg/hr
_MAX_DIFF_ADV_FORCING = -15.0  # J/kg/hr
_MAX_OMEGA_FORCING = -25.0     # J/kg/hr


# ── Public API ────────────────────────────────────────────────────────────────

def compute_thermal_advection(
    ds: "xr.Dataset",
    level_mb: float,
    valid_time: datetime,
) -> float:
    """
    Compute the Oklahoma-domain mean horizontal temperature advection at a
    given pressure level and time.

    Formula:
        ADV(T) = -(u * ∂T/∂x + v * ∂T/∂y)

    Positive → warm advection (warmer air moving into the domain).
    Negative → cold advection.

    Args:
        ds: ERA5 xarray Dataset from ERA5Client.get_upper_air_fields().
            Must contain 'temperature', 'u_component_of_wind',
            'v_component_of_wind' with dims (time, level, latitude, longitude).
        level_mb: Target pressure level in mb (nearest level is used).
        valid_time: UTC time to select (nearest available time is used).

    Returns:
        Domain-mean temperature advection in K/hr.
        Returns 0.0 on missing data or computation failure.
    """
    required = {"temperature", "u_component_of_wind", "v_component_of_wind"}
    missing = required - set(ds.data_vars)
    if missing:
        logger.warning("ERA5 dataset missing fields for thermal advection: %s", missing)
        return 0.0

    try:
        T = ds["temperature"].sel(level=level_mb, method="nearest")
        u = ds["u_component_of_wind"].sel(level=level_mb, method="nearest")
        v = ds["v_component_of_wind"].sel(level=level_mb, method="nearest")

        # ERA5 netCDF encodes times as timezone-naive UTC; strip tzinfo if present
        # so xarray's dtype comparison doesn't raise.
        t_naive = valid_time.replace(tzinfo=None) if valid_time.tzinfo is not None else valid_time
        T = T.sel(time=t_naive, method="nearest")
        u = u.sel(time=t_naive, method="nearest")
        v = v.sel(time=t_naive, method="nearest")

        deg_to_rad = np.pi / 180.0

        # ∂T/∂x: differentiate w.r.t. longitude (K/degree), convert to K/m
        dT_dlon = T.differentiate("longitude")          # K/degree
        lat_rad = np.deg2rad(T.latitude)
        m_per_deg_lon = R_EARTH * np.cos(lat_rad) * deg_to_rad
        dT_dx = dT_dlon / m_per_deg_lon                # K/m

        # ∂T/∂y: differentiate w.r.t. latitude (K/degree), convert to K/m
        dT_dlat = T.differentiate("latitude")           # K/degree
        m_per_deg_lat = R_EARTH * deg_to_rad
        dT_dy = dT_dlat / m_per_deg_lat                # K/m

        # ADV(T) in K/s, positive = warm advection
        adv_ks = -(u * dT_dx + v * dT_dy)

        # Spatial mean over Oklahoma domain, convert to K/hr
        return float(adv_ks.mean().values) * 3600.0

    except Exception as exc:
        logger.warning("Thermal advection failed at %.0f mb: %s", level_mb, exc)
        return 0.0


def compute_synoptic_cap_forcing(
    ds: "xr.Dataset",
    valid_time: datetime,
) -> dict:
    """
    Compute all synoptic-scale cap forcing diagnostics from an ERA5 Dataset.

    Aggregates thermal advection at 850, 700, and 500 mb plus 700 mb vertical
    motion into a single dict suitable for use as era5_fields["cap_advection"]
    in compute_cap_erosion_budget().

    Returns dict with keys:
        thermal_advection_850mb        K/hr, domain mean
        thermal_advection_700mb        K/hr
        thermal_advection_500mb        K/hr
        differential_advection_700_500 K/hr; positive = 700mb warming faster
                                       than 500mb (cap strengthening);
                                       negative = destabilizing aloft (cap
                                       thinning from above)
        vertical_motion_700mb          Pa/s; negative = upward motion
        dynamic_cap_forcing_jkg_hr     J/kg/hr, negative = net cap erosion
    """
    adv_850 = compute_thermal_advection(ds, 850.0, valid_time)
    adv_700 = compute_thermal_advection(ds, 700.0, valid_time)
    adv_500 = compute_thermal_advection(ds, 500.0, valid_time)
    omega_700 = _mean_vertical_motion(ds, 700.0, valid_time)

    # Positive diff_adv → 700 mb warming faster than 500 mb → cap strengthening.
    # Negative diff_adv → 500 mb warming faster (or cooling less) → lapse rate
    #                      steepens above the cap → thinning from above.
    diff_adv = adv_700 - adv_500

    # ── CIN forcing terms (all ≤ 0 for erosion) ──────────────────────────────
    # Cold advection at cap level cools the EML warm nose directly
    forcing_cold_adv = max(
        min(adv_700, 0.0) * CAP_ADVECTION_700_SCALE,
        _MAX_COLD_ADV_FORCING,
    )

    # Negative diff_adv → destabilizing lapse rate aloft → cap thinning
    forcing_diff_adv = max(
        min(diff_adv, 0.0) * DIFF_ADVECTION_SCALE,
        _MAX_DIFF_ADV_FORCING,
    )

    # Upward motion (omega < 0) provides QG lift over the cap
    forcing_omega = max(
        min(omega_700, 0.0) * OMEGA_SCALE,
        _MAX_OMEGA_FORCING,
    )

    dynamic_forcing = forcing_cold_adv + forcing_diff_adv + forcing_omega

    return {
        "thermal_advection_850mb": adv_850,
        "thermal_advection_700mb": adv_700,
        "thermal_advection_500mb": adv_500,
        "differential_advection_700_500": diff_adv,
        "vertical_motion_700mb": omega_700,
        "dynamic_cap_forcing_jkg_hr": dynamic_forcing,
    }


# ── Private helpers ───────────────────────────────────────────────────────────

# ── Virtual sounding extraction ───────────────────────────────────────────────

# Approximate lat/lon for the four Oklahoma-area sounding stations.
# Used to assign the nearest station label to a virtual sounding.
_STATION_COORDS: dict[OklahomaSoundingStation, tuple[float, float]] = {
    OklahomaSoundingStation.OUN: (35.22, -97.44),   # Norman, OK
    OklahomaSoundingStation.LMN: (36.71, -97.49),   # Lamont, OK
    OklahomaSoundingStation.AMA: (35.23, -101.70),  # Amarillo, TX
    OklahomaSoundingStation.DDC: (37.76, -99.97),   # Dodge City, KS
}

_GRAVITY = 9.80665  # m/s² — standard gravity for geopotential → height conversion


def extract_virtual_sounding(
    ds: "xr.Dataset",
    lat: float,
    lon: float,
    valid_time: datetime,
) -> Optional[SoundingProfile]:
    """
    Extract a virtual SoundingProfile at a given lat/lon from ERA5.

    Selects the nearest ERA5 grid point to (lat, lon), converts pressure-level
    fields to SoundingLevel objects, and returns a SoundingProfile suitable for
    passing to compute_thermodynamic_indices().

    For best results, request the dataset with SOUNDING_PRESSURE_LEVELS from
    era5_client.py (22 levels).  The minimum required by MetPy is 10 levels.

    Height in the returned SoundingLevel objects is metres AMSL (geopotential /
    standard gravity), not strictly AGL.  For Oklahoma (300–600 m ASL) the
    difference is small and does not affect relative parcel calculations.

    Required Dataset variables:
        temperature           K
        specific_humidity     kg/kg
        u_component_of_wind   m/s
        v_component_of_wind   m/s
        geopotential          m²/s²

    Args:
        ds: ERA5 xarray Dataset with dims (time, level, latitude, longitude).
        lat: Target latitude (°N)
        lon: Target longitude (negative = °W)
        valid_time: UTC time; timezone-aware or naive both accepted.

    Returns:
        SoundingProfile with raw_source="virtual", or None on missing data.
    """
    required = {
        "temperature", "specific_humidity",
        "u_component_of_wind", "v_component_of_wind", "geopotential",
    }
    missing = required - set(ds.data_vars)
    if missing:
        logger.warning("ERA5 dataset missing fields for virtual sounding: %s", missing)
        return None

    try:
        t_naive = valid_time.replace(tzinfo=None) if valid_time.tzinfo is not None else valid_time

        # Select nearest grid point and time across all levels at once
        point = ds.sel(latitude=lat, longitude=lon, method="nearest")
        point = point.sel(time=t_naive, method="nearest")

        T_col    = point["temperature"].values              # (n_levels,) K
        q_col    = point["specific_humidity"].values        # kg/kg
        u_col    = point["u_component_of_wind"].values      # m/s
        v_col    = point["v_component_of_wind"].values      # m/s
        phi_col  = point["geopotential"].values             # m²/s²
        pres_arr = point.level.values.astype(float)         # hPa

        # Sort by decreasing pressure (surface → top), as required by SoundingProfile
        order = np.argsort(pres_arr)[::-1]
        T_col   = T_col[order]
        q_col   = q_col[order]
        u_col   = u_col[order]
        v_col   = v_col[order]
        phi_col = phi_col[order]
        pres_arr = pres_arr[order]

        levels: list[SoundingLevel] = []
        for i, pres in enumerate(pres_arr):
            T_k   = float(T_col[i])
            q     = float(q_col[i])
            u     = float(u_col[i])
            v     = float(v_col[i])
            phi   = float(phi_col[i])

            # Basic sanity filter — skip clearly bad fill values
            if T_k < 150.0 or T_k > 350.0:
                continue
            if not math.isfinite(T_k) or not math.isfinite(q):
                continue

            T_c  = T_k - 273.15
            Td_c = _specific_humidity_to_dewpoint_c(max(q, 0.0), pres)
            # Dewpoint cannot exceed temperature
            Td_c = min(Td_c, T_c)

            height_m = phi / _GRAVITY

            wind_speed_kt = math.sqrt(u**2 + v**2) * 1.94384
            wind_dir = math.degrees(math.atan2(-u, -v)) % 360.0

            levels.append(SoundingLevel(
                pressure=pres,
                height=height_m,
                temperature=T_c,
                dewpoint=Td_c,
                wind_direction=wind_dir,
                wind_speed=wind_speed_kt,
            ))

        if len(levels) < 2:
            logger.warning(
                "Virtual sounding at (%.2f, %.2f) has only %d valid levels — skipping",
                lat, lon, len(levels)
            )
            return None

        station = _nearest_sounding_station(lat, lon)

        actual_time = valid_time if valid_time.tzinfo else valid_time.replace(
            tzinfo=__import__("datetime").timezone.utc
        )

        return SoundingProfile(
            station=station,
            valid_time=actual_time,
            levels=levels,
            raw_source="virtual",
        )

    except Exception as exc:
        logger.warning("Virtual sounding extraction failed at (%.2f, %.2f): %s", lat, lon, exc)
        return None


# ── Private helpers ───────────────────────────────────────────────────────────

def _nearest_sounding_station(lat: float, lon: float) -> OklahomaSoundingStation:
    """Return the OklahomaSoundingStation whose coordinates are closest to (lat, lon)."""
    best_station = OklahomaSoundingStation.OUN
    best_dist_sq = float("inf")
    for station, (s_lat, s_lon) in _STATION_COORDS.items():
        dist_sq = (lat - s_lat) ** 2 + (lon - s_lon) ** 2
        if dist_sq < best_dist_sq:
            best_dist_sq = dist_sq
            best_station = station
    return best_station


def _specific_humidity_to_dewpoint_c(q_kgkg: float, pressure_hpa: float) -> float:
    """
    Convert specific humidity (kg/kg) at a given pressure level to dewpoint (°C).

    Uses the WMO-standard Magnus formula.  Returns −40°C for negligible moisture.
    """
    if q_kgkg <= 0.0 or pressure_hpa <= 0.0:
        return -40.0

    # Mixing ratio (kg/kg)
    w = q_kgkg / (1.0 - q_kgkg)

    # Vapour pressure (hPa)
    e = w * pressure_hpa / (0.622 + w)

    if e <= 0.0:
        return -40.0

    # Magnus formula (°C)
    ln_e = math.log(e / 6.112)
    return 243.5 * ln_e / (17.67 - ln_e)


def _mean_vertical_motion(
    ds: "xr.Dataset",
    level_mb: float,
    valid_time: datetime,
) -> float:
    """
    Return the domain-mean ERA5 vertical velocity (omega, Pa/s) at a level.
    Negative = upward motion, positive = downward (standard meteorological
    convention for omega = dp/dt).
    Returns 0.0 on missing data.
    """
    if "vertical_velocity" not in ds.data_vars:
        return 0.0
    try:
        t_naive = valid_time.replace(tzinfo=None) if valid_time.tzinfo is not None else valid_time
        omega = ds["vertical_velocity"].sel(level=level_mb, method="nearest")
        omega = omega.sel(time=t_naive, method="nearest")
        return float(omega.mean().values)
    except Exception as exc:
        logger.warning("Vertical motion extraction failed at %.0f mb: %s", level_mb, exc)
        return 0.0
