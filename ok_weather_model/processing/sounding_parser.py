"""
MetPy-based sounding computations.

Converts raw SoundingProfile data into derived ThermodynamicIndices
and KinematicProfile objects.  All MetPy calculations use proper unit
handling via pint.

Ground truth validation: May 3, 1999 OUN 12Z sounding.
Published benchmark values for this case:
    SBCAPE  ~ 4500–5000 J/kg
    SBCIN   ~ 50–150 J/kg (moderate cap)
    SRH_0_3 ~ 400–500 m²/s²
    LCL     ~ 500–800 m AGL
    BWD_0_6 ~ 50–60 kts
"""

import logging
import math
from datetime import datetime, timezone
from typing import Optional

import numpy as np

from ..models import (
    OklahomaSoundingStation,
    SoundingProfile,
    SoundingLevel,
    ThermodynamicIndices,
    KinematicProfile,
    WindLevel,
    HodographShape,
    EMLCharacteristics,
)

logger = logging.getLogger(__name__)


def compute_thermodynamic_indices(
    profile: SoundingProfile,
) -> ThermodynamicIndices:
    """
    Compute full thermodynamic parameter set from a SoundingProfile using MetPy.

    Args:
        profile: Parsed rawinsonde data with at least surface through 500mb coverage

    Returns:
        ThermodynamicIndices with all computed parameters

    Raises:
        ImportError: if metpy or pint are not installed
        ValueError: if sounding has insufficient levels for computation
    """
    try:
        import metpy.calc as mpcalc
        from metpy.units import units
        import pint_xarray  # noqa — ensures unit registry is initialized
    except ImportError as exc:
        raise ImportError(
            "MetPy is required for thermodynamic calculations. "
            "Install with: pip install metpy"
        ) from exc

    if len(profile.levels) < 20:
        raise ValueError(
            f"Sounding has only {len(profile.levels)} levels — insufficient for MetPy calc"
        )
    min_pressure = min(lev.pressure for lev in profile.levels)
    if min_pressure > 500:
        raise ValueError(
            f"Sounding top is {min_pressure:.0f} hPa — must reach at least 500 hPa"
        )

    levels = profile.levels

    # Build unit-aware arrays
    pressure = np.array([lev.pressure for lev in levels]) * units("hPa")
    temperature = np.array([lev.temperature for lev in levels]) * units("degC")
    dewpoint = np.array([lev.dewpoint for lev in levels]) * units("degC")
    height = np.array([lev.height for lev in levels]) * units("meter")

    # ── Surface-based parcel ───────────────────────────────────────────────────
    sb_cape_c, sb_cin_c = mpcalc.surface_based_cape_cin(pressure, temperature, dewpoint)
    sb_cape = float(sb_cape_c.to("J/kg").magnitude)
    sb_cin = abs(float(sb_cin_c.to("J/kg").magnitude))

    # ── Mixed layer parcel (lowest 100 hPa) ───────────────────────────────────
    ml_cape_c, ml_cin_c = mpcalc.mixed_layer_cape_cin(pressure, temperature, dewpoint)
    ml_cape = float(ml_cape_c.to("J/kg").magnitude)
    ml_cin = abs(float(ml_cin_c.to("J/kg").magnitude))

    # ── Most unstable parcel ───────────────────────────────────────────────────
    mu_cape_c, mu_cin_c = mpcalc.most_unstable_cape_cin(pressure, temperature, dewpoint)
    mu_cape = float(mu_cape_c.to("J/kg").magnitude)

    # ── LCL ───────────────────────────────────────────────────────────────────
    lcl_pres, lcl_temp = mpcalc.lcl(pressure[0], temperature[0], dewpoint[0])
    lcl_height_m = float(
        mpcalc.pressure_to_height_std(lcl_pres).to("meter").magnitude
    )
    # More precise: interpolate height at LCL pressure
    lcl_height_m = _pressure_to_height(lcl_pres.magnitude, pressure.magnitude, height.magnitude)

    # ── LFC ───────────────────────────────────────────────────────────────────
    try:
        lfc_pres, lfc_temp = mpcalc.lfc(pressure, temperature, dewpoint)
        lfc_height_m = _pressure_to_height(
            lfc_pres.magnitude, pressure.magnitude, height.magnitude
        )
    except Exception:
        lfc_height_m = lcl_height_m  # LFC = LCL when no inhibition

    # ── Equilibrium Level ─────────────────────────────────────────────────────
    try:
        el_pres, el_temp = mpcalc.el(pressure, temperature, dewpoint)
        el_height_m = _pressure_to_height(
            el_pres.magnitude, pressure.magnitude, height.magnitude
        )
    except Exception:
        el_height_m = float(height[-1].magnitude)

    # ── Cap strength: max temperature excess of environment over surface parcel ─
    cap_strength_c = _compute_cap_strength(
        pressure, temperature, dewpoint, lcl_height_m, lfc_height_m, height
    )

    # ── Convective temperature ────────────────────────────────────────────────
    conv_temp_f = _compute_convective_temperature(pressure, temperature, dewpoint)

    # ── EML detection ─────────────────────────────────────────────────────────
    eml_base, eml_top = _detect_eml(pressure, temperature, height)

    # ── Lapse rates ───────────────────────────────────────────────────────────
    lr_700_500 = _lapse_rate_between(700.0, 500.0, pressure, temperature, height)
    lr_850_500 = _lapse_rate_between(850.0, 500.0, pressure, temperature, height)

    # ── Precipitable water ────────────────────────────────────────────────────
    pw = mpcalc.precipitable_water(pressure, dewpoint)
    pw_inches = float(pw.to("inches").magnitude)

    # ── 850mb mixing ratio ────────────────────────────────────────────────────
    mr_850 = _mixing_ratio_at_level(850.0, pressure, temperature, dewpoint)

    # ── Wet bulb zero ─────────────────────────────────────────────────────────
    wbz_height = _compute_wet_bulb_zero(pressure, temperature, dewpoint, height)

    return ThermodynamicIndices(
        valid_time=profile.valid_time,
        station=profile.station,
        MLCAPE=max(ml_cape, 0.0),
        MLCIN=max(ml_cin, 0.0),
        SBCAPE=max(sb_cape, 0.0),
        SBCIN=max(sb_cin, 0.0),
        MUCAPE=max(mu_cape, 0.0),
        LCL_height=max(lcl_height_m, 0.0),
        LFC_height=max(lfc_height_m, 0.0),
        EL_height=max(el_height_m, 0.0),
        convective_temperature=conv_temp_f,
        cap_strength=cap_strength_c,
        EML_base=eml_base,
        EML_top=eml_top,
        lapse_rate_700_500=lr_700_500,
        lapse_rate_850_500=lr_850_500,
        precipitable_water=pw_inches,
        mixing_ratio_850=mr_850,
        wet_bulb_zero=wbz_height,
    )


def compute_modified_indices(
    profile: SoundingProfile,
    surface_temp_c: float,
    surface_dewpoint_c: float,
) -> ThermodynamicIndices:
    """
    Compute thermodynamic indices with the surface level replaced by current
    Mesonet observations (the "daytime modified sounding" technique).

    The 12Z radiosonde surface level reflects pre-dawn conditions — cold, dry,
    before the mixed layer has formed.  Substituting the current Mesonet surface
    T and Td into the bottom level while keeping the 12Z thermodynamic profile
    aloft gives an accurate estimate of afternoon CAPE and CIN.

    This is the standard operational approach: use sounding aloft for lapse
    rates and cap structure; use Mesonet surface for mixed-layer parcel origin.

    Args:
        profile:             12Z SoundingProfile (thermodynamic structure aloft)
        surface_temp_c:      current Mesonet surface temperature in °C
        surface_dewpoint_c:  current Mesonet surface dewpoint in °C

    Returns:
        ThermodynamicIndices reflecting the current daytime environment
    """
    if not profile.levels:
        raise ValueError("Profile has no levels")

    # Replace only the surface level — everything above stays as-is from the sounding.
    surface = profile.levels[0]
    modified_surface = SoundingLevel(
        pressure=surface.pressure,
        height=surface.height,
        temperature=surface_temp_c,
        dewpoint=surface_dewpoint_c,
        wind_direction=surface.wind_direction,
        wind_speed=surface.wind_speed,
    )
    modified_levels = [modified_surface] + list(profile.levels[1:])

    modified_profile = SoundingProfile(
        station=profile.station,
        valid_time=profile.valid_time,
        levels=modified_levels,
        raw_source=profile.raw_source,
    )
    return compute_thermodynamic_indices(modified_profile)


def compute_kinematic_profile(
    profile: SoundingProfile,
    thermodynamics: Optional[ThermodynamicIndices] = None,
) -> KinematicProfile:
    """
    Compute wind shear and composite parameters from a SoundingProfile.

    Args:
        profile: Parsed sounding with wind data at multiple levels
        thermodynamics: Optional ThermodynamicIndices for composite parameter calc

    Returns:
        KinematicProfile with SRH, BWD, Bunkers motion, STP, SCP, EHI
    """
    try:
        import metpy.calc as mpcalc
        from metpy.units import units
    except ImportError as exc:
        raise ImportError("MetPy is required for kinematic calculations.") from exc

    levels = profile.levels
    pressure = np.array([lev.pressure for lev in levels]) * units("hPa")
    height = np.array([lev.height for lev in levels]) * units("meter")
    u = np.array([
        -lev.wind_speed * 0.514444 * math.sin(math.radians(lev.wind_direction))
        for lev in levels
    ]) * units("m/s")
    v = np.array([
        -lev.wind_speed * 0.514444 * math.cos(math.radians(lev.wind_direction))
        for lev in levels
    ]) * units("m/s")

    # ── Bunkers storm motion ───────────────────────────────────────────────────
    try:
        bunkers_right, bunkers_left, mean_wind = mpcalc.bunkers_storm_motion(
            pressure, u, v, height
        )
        rm_u = float(bunkers_right[0].to("m/s").magnitude)
        rm_v = float(bunkers_right[1].to("m/s").magnitude)
        lm_u = float(bunkers_left[0].to("m/s").magnitude)
        lm_v = float(bunkers_left[1].to("m/s").magnitude)
    except Exception as exc:
        # Common cause: sounding has too few levels or insufficient height
        # coverage for MetPy's 0–6 km integration window.  Not a data error —
        # log at DEBUG so sparse special-hour soundings don't spam the console.
        logger.debug("Bunkers storm motion not computed: %s", exc)
        rm_u, rm_v, lm_u, lm_v = 0.0, 0.0, 0.0, 0.0

    storm_motion_right = (rm_u, rm_v)
    storm_motion_left = (lm_u, lm_v)
    rm_u_units = rm_u * units("m/s")
    rm_v_units = rm_v * units("m/s")

    # ── Storm-relative helicity ────────────────────────────────────────────────
    srh_0_1, _, _ = mpcalc.storm_relative_helicity(
        height, u, v, depth=1000 * units("meter"),
        storm_u=rm_u_units, storm_v=rm_v_units
    )
    srh_0_3, _, _ = mpcalc.storm_relative_helicity(
        height, u, v, depth=3000 * units("meter"),
        storm_u=rm_u_units, storm_v=rm_v_units
    )

    # ── Bulk wind difference ───────────────────────────────────────────────────
    bwd_0_1 = _bulk_wind_diff(height, u, v, 0.0, 1000.0)
    bwd_0_6 = _bulk_wind_diff(height, u, v, 0.0, 6000.0)

    # ── Mean wind 0–6km ───────────────────────────────────────────────────────
    try:
        u_mean, v_mean = mpcalc.mean_pressure_weighted(
            pressure, u, v,
            height=height, depth=6000 * units("meter")
        )
        mean_wspd_kts = float(
            mpcalc.wind_speed(u_mean, v_mean).to("knots").magnitude
        )
    except Exception:
        mean_wspd_kts = bwd_0_6 / 2.0

    # ── LLJ (850mb) ───────────────────────────────────────────────────────────
    llj_spd, llj_dir = _llj_at_level(850.0, pressure, u, v)

    # ── Hodograph shape ───────────────────────────────────────────────────────
    shape = _classify_hodograph(height, u, v)

    # ── WindLevel list ────────────────────────────────────────────────────────
    wind_levels = [
        WindLevel(
            pressure=lev.pressure,
            height=lev.height,
            u_component=float(u[i].magnitude),
            v_component=float(v[i].magnitude),
        )
        for i, lev in enumerate(levels)
    ]

    # ── Composite parameters ─────────────────────────────────────────────────
    srh_val = float(srh_0_3.to("m**2/s**2").magnitude)
    srh_1km_val = float(srh_0_1.to("m**2/s**2").magnitude)

    ehi = stp = scp = None
    if thermodynamics is not None:
        mlcape = thermodynamics.MLCAPE
        sbcape = thermodynamics.SBCAPE
        mlcin = thermodynamics.MLCIN
        lcl_h = thermodynamics.LCL_height
        mucape = thermodynamics.MUCAPE

        # Energy Helicity Index
        if mlcape > 0:
            ehi = (mlcape * srh_val) / 160000.0

        # Significant Tornado Parameter (Thompson et al. 2003)
        if sbcape > 0 and mlcin < 250 and lcl_h < 2000 and bwd_0_6 > 10:
            stp = (
                (sbcape / 1500.0)
                * ((2000.0 - lcl_h) / 1000.0)
                * (srh_1km_val / 150.0)
                * (bwd_0_6 / 20.0)
                * ((200.0 - mlcin) / 150.0)
            )
            stp = max(stp, 0.0)

        # Supercell Composite Parameter (Thompson et al. 2004)
        if mucape > 0:
            scp = (
                (mucape / 1000.0)
                * (srh_val / 50.0)
                * (bwd_0_6 / 20.0)
            )
            scp = max(scp, 0.0)

    return KinematicProfile(
        valid_time=profile.valid_time,
        station=profile.station,
        levels=wind_levels,
        SRH_0_1km=srh_1km_val,
        SRH_0_3km=srh_val,
        BWD_0_1km=bwd_0_1,
        BWD_0_6km=bwd_0_6,
        LLJ_speed=llj_spd,
        LLJ_direction=llj_dir,
        mean_wind_0_6km=mean_wspd_kts,
        hodograph_shape=shape,
        storm_motion_bunkers_right=storm_motion_right,
        storm_motion_bunkers_left=storm_motion_left,
        EHI=ehi,
        STP=stp,
        SCP=scp,
    )


def compute_convective_temp_from_profile(profile: SoundingProfile) -> float:
    """
    Compute the convective temperature (°F) directly from a SoundingProfile.

    Use this when you need the correct Tc without going through the full
    compute_thermodynamic_indices() pipeline.  Useful for CES computation
    when the stored ThermodynamicIndices.convective_temperature may be stale.

    Returns convective temperature in °F.
    """
    try:
        from metpy.units import units
    except ImportError as exc:
        raise ImportError("MetPy is required.") from exc

    levels = profile.levels
    pressure = np.array([lev.pressure for lev in levels]) * units("hPa")
    temperature = np.array([lev.temperature for lev in levels]) * units("degC")
    dewpoint = np.array([lev.dewpoint for lev in levels]) * units("degC")

    return _compute_convective_temperature(pressure, temperature, dewpoint)


# ── Private computation helpers ────────────────────────────────────────────────

def _pressure_to_height(
    target_pres: float,
    pressure_array: np.ndarray,
    height_array: np.ndarray,
) -> float:
    """Log-linear interpolation of height at a target pressure level."""
    return float(np.interp(
        np.log(target_pres),
        np.log(pressure_array[::-1]),
        height_array[::-1],
    ))


def _compute_cap_strength(
    pressure, temperature, dewpoint, lcl_height, lfc_height, height
) -> float:
    """
    Compute cap strength as the maximum temperature excess of the environment
    over a surface-based parcel between the LCL and LFC.

    Returns 0.0 if LFC <= LCL (no cap).
    """
    try:
        import metpy.calc as mpcalc
        from metpy.units import units

        if lfc_height <= lcl_height:
            return 0.0

        # Parcel path from surface to LFC
        parcel_path = mpcalc.parcel_profile(pressure, temperature[0], dewpoint[0])

        # Find levels between LCL and LFC
        lcl_pres = float(np.interp(lcl_height, height.magnitude, pressure.magnitude))
        lfc_pres = float(np.interp(lfc_height, height.magnitude, pressure.magnitude))

        mask = (pressure.magnitude <= lcl_pres) & (pressure.magnitude >= lfc_pres)
        if not mask.any():
            return 0.0

        # Temperature excess = environment minus parcel (positive = cap exists)
        # parcel_path is in Kelvin from MetPy; convert to °C for comparison
        parcel_c = parcel_path[mask].to("degC").magnitude
        temp_excess = temperature.magnitude[mask] - parcel_c
        max_excess = float(temp_excess.max())
        return max(max_excess, 0.0)

    except Exception as exc:
        logger.warning("Cap strength computation failed: %s", exc)
        return 0.0


def _compute_convective_temperature(pressure, temperature, dewpoint) -> float:
    """
    Estimate the convective temperature: the surface temperature at which
    surface-based CIN drops to near zero (free convection imminent).

    Method: iteratively raise only the surface level temperature until
    surface_based_cape_cin returns |CIN| < 10 J/kg.
    """
    try:
        import metpy.calc as mpcalc
        from metpy.units import units

        sfc_temp_c = float(temperature[0].to("degC").magnitude)
        temp_mag = temperature.magnitude.copy()

        for delta in np.arange(0.0, 30.0, 0.5):
            temp_mag[0] = sfc_temp_c + delta
            test_temps = temp_mag * units("degC")
            try:
                _, cin = mpcalc.surface_based_cape_cin(pressure, test_temps, dewpoint)
                cin_val = abs(float(cin.to("J/kg").magnitude))
                if cin_val < 10.0:
                    return (sfc_temp_c + delta) * 9.0 / 5.0 + 32.0  # °C → °F
            except Exception:
                continue

        # Fallback: dewpoint + empirical offset (~25°F above morning dewpoint)
        sfc_dew_f = float(dewpoint[0].to("degC").magnitude) * 9.0 / 5.0 + 32.0
        return sfc_dew_f + 25.0

    except Exception as exc:
        logger.warning("Convective temperature calculation failed: %s", exc)
        sfc_temp_f = float(temperature[0].magnitude) * 9.0 / 5.0 + 32.0
        return sfc_temp_f + 10.0


def _detect_eml(pressure, temperature, height) -> tuple[Optional[float], Optional[float]]:
    """
    Detect the Elevated Mixed Layer by looking for an adiabatic or
    super-adiabatic layer above the boundary layer (typically 700–500mb).

    Returns (base_pressure, top_pressure) in mb, or (None, None) if no EML.
    """
    try:
        import metpy.calc as mpcalc
        from metpy.units import units

        # Compute potential temperature
        theta = mpcalc.potential_temperature(pressure, temperature)

        # Look for a well-mixed (constant theta) layer between 850 and 500mb
        search_mask = (pressure.magnitude >= 500) & (pressure.magnitude <= 850)
        if not search_mask.any():
            return None, None

        p_search = pressure[search_mask]
        theta_search = theta[search_mask]
        t_search = temperature[search_mask]

        # Find where temp lapse rate approaches dry adiabatic (~9.8 °C/km)
        # Simple heuristic: find layer where dTheta/dp < threshold
        eml_base = eml_top = None

        for i in range(1, len(p_search)):
            dp = float(p_search[i - 1].magnitude - p_search[i].magnitude)
            dtheta = float(theta_search[i].magnitude - theta_search[i - 1].magnitude)

            if dp > 0 and abs(dtheta / dp) < 0.05:  # nearly adiabatic
                if eml_base is None:
                    eml_base = float(p_search[i - 1].magnitude)
                eml_top = float(p_search[i].magnitude)

        return eml_base, eml_top

    except Exception:
        return None, None


def _lapse_rate_between(
    p_lower: float,
    p_upper: float,
    pressure,
    temperature,
    height,
) -> float:
    """Compute environmental lapse rate (°C/km) between two pressure levels."""
    try:
        t_lower = float(np.interp(p_lower, pressure.magnitude[::-1], temperature.magnitude[::-1]))
        t_upper = float(np.interp(p_upper, pressure.magnitude[::-1], temperature.magnitude[::-1]))
        h_lower = float(np.interp(np.log(p_lower), np.log(pressure.magnitude[::-1]), height.magnitude[::-1]))
        h_upper = float(np.interp(np.log(p_upper), np.log(pressure.magnitude[::-1]), height.magnitude[::-1]))

        dz_km = (h_upper - h_lower) / 1000.0
        if dz_km == 0:
            return 0.0
        return (t_lower - t_upper) / dz_km  # positive = unstable
    except Exception:
        return 0.0


def _mixing_ratio_at_level(
    level_pres: float,
    pressure,
    temperature,
    dewpoint,
) -> float:
    """Compute mixing ratio (g/kg) at a specified pressure level."""
    try:
        import metpy.calc as mpcalc
        from metpy.units import units

        mr = mpcalc.mixing_ratio_from_relative_humidity(
            pressure,
            temperature,
            mpcalc.relative_humidity_from_dewpoint(temperature, dewpoint),
        )
        mr_at_level = float(np.interp(level_pres, pressure.magnitude[::-1], mr.magnitude[::-1]))
        return mr_at_level * 1000.0  # kg/kg → g/kg
    except Exception:
        return 0.0


def _compute_wet_bulb_zero(pressure, temperature, dewpoint, height) -> float:
    """Find the height AGL where wet-bulb temperature crosses 0°C."""
    try:
        import metpy.calc as mpcalc
        from metpy.units import units

        wet_bulb = mpcalc.wet_bulb_temperature(pressure, temperature, dewpoint)
        wb_c = wet_bulb.to("degC").magnitude

        # Find zero crossing (from above freezing to below)
        for i in range(1, len(wb_c)):
            if wb_c[i - 1] >= 0 >= wb_c[i]:
                # Linear interpolation
                frac = wb_c[i - 1] / (wb_c[i - 1] - wb_c[i])
                wbz = float(height[i - 1].magnitude + frac * (height[i].magnitude - height[i - 1].magnitude))
                return max(wbz, 0.0)

        return 0.0
    except Exception:
        return 0.0


def _bulk_wind_diff(height, u, v, bottom_m: float, top_m: float) -> float:
    """Compute bulk wind difference magnitude (knots) between two heights."""
    try:
        u_bot = float(np.interp(bottom_m, height.magnitude, u.magnitude))
        v_bot = float(np.interp(bottom_m, height.magnitude, v.magnitude))
        u_top = float(np.interp(top_m, height.magnitude, u.magnitude))
        v_top = float(np.interp(top_m, height.magnitude, v.magnitude))

        du = u_top - u_bot
        dv = v_top - v_bot
        shear_ms = math.sqrt(du**2 + dv**2)
        return shear_ms * 1.94384  # m/s → knots
    except Exception:
        return 0.0


def _llj_at_level(
    level_pres: float,
    pressure,
    u,
    v,
) -> tuple[Optional[float], Optional[float]]:
    """Extract wind speed and direction at a given pressure level."""
    try:
        u_lev = float(np.interp(level_pres, pressure.magnitude[::-1], u.magnitude[::-1]))
        v_lev = float(np.interp(level_pres, pressure.magnitude[::-1], v.magnitude[::-1]))
        speed_ms = math.sqrt(u_lev**2 + v_lev**2)
        speed_kts = speed_ms * 1.94384
        direction = math.degrees(math.atan2(-u_lev, -v_lev)) % 360
        return speed_kts, direction
    except Exception:
        return None, None


def _classify_hodograph(height, u, v) -> HodographShape:
    """
    Classify hodograph shape by computing the veering angle in the 0–3km layer.

    CURVED:   veering > 120°
    STRAIGHT: veering < 60°
    HYBRID:   60°–120°
    """
    try:
        levels_of_interest = [0, 500, 1000, 1500, 2000, 2500, 3000]  # meters
        angles = []

        for h_m in levels_of_interest:
            u_h = float(np.interp(h_m, height.magnitude, u.magnitude))
            v_h = float(np.interp(h_m, height.magnitude, v.magnitude))
            angles.append(math.degrees(math.atan2(u_h, v_h)) % 360)

        total_veering = 0.0
        for i in range(1, len(angles)):
            diff = (angles[i] - angles[i - 1]) % 360
            if diff > 180:
                diff -= 360
            total_veering += diff

        if total_veering > 120:
            return HodographShape.CURVED
        elif total_veering < 60:
            return HodographShape.STRAIGHT
        else:
            return HodographShape.HYBRID

    except Exception:
        return HodographShape.HYBRID
