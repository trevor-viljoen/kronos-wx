"""
Tests for cap_calculator.py — heating model and Cap Erosion Score.
"""

import math
from datetime import date, time

import pytest

from ok_weather_model.processing.cap_calculator import (
    _heating_amplitude,
    _okla_surface_temp_f,
    compute_ces_from_sounding,
    compute_convective_temp_gap,
    HEATING_EFFICIENCY,
)
from ok_weather_model.models.enums import CapBehavior


# ── _heating_amplitude ─────────────────────────────────────────────────────────

class TestHeatingAmplitude:
    def test_summer_solstice_is_maximum(self):
        """Peak heating amplitude near summer solstice (~day 172)."""
        amp_summer = _heating_amplitude(172)
        amp_winter = _heating_amplitude(355)
        assert amp_summer > amp_winter

    def test_spring_amplitude_between_winter_and_summer(self):
        """April/May (days 90–135) amplitude falls between winter and summer."""
        amp_spring = _heating_amplitude(120)
        amp_summer = _heating_amplitude(172)
        amp_winter = _heating_amplitude(355)
        assert amp_winter < amp_spring < amp_summer

    def test_summer_amplitude_reasonable(self):
        """Summer max heating should be ~20–22°F above 12Z for Oklahoma."""
        amp = _heating_amplitude(172)
        assert 18.0 < amp < 25.0

    def test_winter_amplitude_reasonable(self):
        """Winter heating should be lower but still positive."""
        amp = _heating_amplitude(355)
        assert 5.0 < amp < 15.0

    def test_symmetry_around_solstice(self):
        """Heating amplitude should be symmetric ~90 days from the peak."""
        # doy 82 (Mar 23) and doy 262 (Sep 19) are equidistant from doy 172
        amp_spring = _heating_amplitude(82)
        amp_fall = _heating_amplitude(262)
        assert abs(amp_spring - amp_fall) < 1.0


# ── _okla_surface_temp_f ───────────────────────────────────────────────────────

class TestOklaSurfaceTempF:
    """Oklahoma climatological heating curve."""

    T12Z = 65.0   # representative spring morning temp
    DOY = 120     # late April

    def test_at_12z_equals_anchor(self):
        """Temperature at 12Z is the anchor (no heating yet)."""
        assert _okla_surface_temp_f(self.T12Z, 12.0, self.DOY) == pytest.approx(self.T12Z)

    def test_peaks_at_21z(self):
        """Peak temperature is at 21Z (4pm CDT)."""
        t_peak = _okla_surface_temp_f(self.T12Z, 21.0, self.DOY)
        for h in [12, 14, 16, 18, 20, 22, 23, 24]:
            t_other = _okla_surface_temp_f(self.T12Z, float(h), self.DOY)
            assert t_peak >= t_other, f"Peak at 21Z should exceed temp at {h}Z"

    def test_monotonically_increasing_to_peak(self):
        """Temperature increases from 12Z to 21Z."""
        temps = [_okla_surface_temp_f(self.T12Z, float(h), self.DOY) for h in range(12, 22)]
        for i in range(1, len(temps)):
            assert temps[i] >= temps[i - 1], f"Temp should not drop before peak (hour {12+i}Z)"

    def test_cooling_after_peak(self):
        """Temperature decreases after 21Z."""
        t21 = _okla_surface_temp_f(self.T12Z, 21.0, self.DOY)
        t24 = _okla_surface_temp_f(self.T12Z, 24.0, self.DOY)
        assert t21 > t24

    def test_peak_delta_matches_amplitude(self):
        """Peak heating above 12Z should match the climatological amplitude."""
        amp = _heating_amplitude(self.DOY)
        t_peak = _okla_surface_temp_f(self.T12Z, 21.0, self.DOY)
        assert t_peak == pytest.approx(self.T12Z + amp, abs=0.01)

    def test_post_midnight_handled(self):
        """Hours 0–11 UTC (post-midnight) are handled as next-day hours."""
        # Hour 1 UTC = 25Z in our frame → should be in cooling phase
        t1 = _okla_surface_temp_f(self.T12Z, 1.0, self.DOY)
        t21 = _okla_surface_temp_f(self.T12Z, 21.0, self.DOY)
        assert t1 < t21


# ── compute_convective_temp_gap ────────────────────────────────────────────────

class TestConvectiveTempGap:
    def test_positive_when_cap_holding(self):
        """Gap is positive when surface is below Tc."""
        assert compute_convective_temp_gap(65.0, 80.0) == pytest.approx(15.0)

    def test_zero_at_erosion(self):
        """Gap is zero when surface equals Tc."""
        assert compute_convective_temp_gap(80.0, 80.0) == pytest.approx(0.0)

    def test_negative_after_erosion(self):
        """Gap is negative when surface exceeds Tc."""
        assert compute_convective_temp_gap(85.0, 80.0) == pytest.approx(-5.0)


# ── compute_ces_from_sounding ──────────────────────────────────────────────────

def _make_indices(cap_strength: float, mlcin: float, **kwargs):
    """Create a minimal ThermodynamicIndices for testing."""
    from datetime import datetime, timezone
    from ok_weather_model.models import ThermodynamicIndices
    from ok_weather_model.models.enums import OklahomaSoundingStation

    defaults = dict(
        valid_time=datetime(1999, 5, 3, 12, tzinfo=timezone.utc),
        station=OklahomaSoundingStation.OUN,
        MLCAPE=3000.0,
        MLCIN=mlcin,
        SBCAPE=4000.0,
        SBCIN=mlcin,
        MUCAPE=4500.0,
        LCL_height=500.0,
        LFC_height=1500.0,
        EL_height=12000.0,
        convective_temperature=80.0,
        cap_strength=cap_strength,
        lapse_rate_700_500=7.0,
        lapse_rate_850_500=6.5,
        precipitable_water=1.2,
        mixing_ratio_850=12.0,
        wet_bulb_zero=3500.0,
    )
    defaults.update(kwargs)
    return ThermodynamicIndices(**defaults)


class TestComputeCESFromSounding:
    """Cap Erosion Score computation from sounding data."""

    def test_returns_required_keys(self):
        idx = _make_indices(cap_strength=2.0, mlcin=30.0)
        result = compute_ces_from_sounding(idx, 18.0, date(1999, 5, 3))
        expected_keys = {
            "convective_temp_gap_12Z",
            "convective_temp_gap_15Z",
            "convective_temp_gap_18Z",
            "cap_erosion_time",
            "cap_behavior",
        }
        assert expected_keys == set(result.keys())

    def test_weak_cap_erodes_early(self):
        """Weak cap (low cap_strength, low MLCIN) should erode early."""
        idx = _make_indices(cap_strength=0.5, mlcin=5.0)
        result = compute_ces_from_sounding(idx, 18.0, date(1999, 5, 3))
        assert result["cap_behavior"] in (CapBehavior.EARLY_EROSION, CapBehavior.CLEAN_EROSION)
        assert result["cap_erosion_time"] is not None

    def test_strong_cap_no_erosion(self):
        """Very strong cap should show NO_EROSION under heating-only model."""
        idx = _make_indices(cap_strength=10.0, mlcin=250.0)
        result = compute_ces_from_sounding(idx, 18.0, date(1999, 5, 3))
        assert result["cap_behavior"] == CapBehavior.NO_EROSION
        assert result["cap_erosion_time"] is None

    def test_gap_decreases_through_day(self):
        """Tc gap should decrease from 12Z to 18Z as surface heats."""
        idx = _make_indices(cap_strength=3.0, mlcin=50.0)
        result = compute_ces_from_sounding(idx, 15.0, date(1999, 5, 3))
        assert result["convective_temp_gap_12Z"] > result["convective_temp_gap_15Z"]
        assert result["convective_temp_gap_15Z"] > result["convective_temp_gap_18Z"]

    def test_12z_gap_positive_when_cap_holds(self):
        """12Z gap should be positive when cap is holding at 12Z."""
        idx = _make_indices(cap_strength=3.0, mlcin=50.0)
        result = compute_ces_from_sounding(idx, 15.0, date(1999, 5, 3))
        # At 12Z, no heating has occurred yet — gap must equal T_eff - T_12Z
        assert result["convective_temp_gap_12Z"] > 0.0

    def test_erosion_time_is_time_object(self):
        """cap_erosion_time should be a datetime.time when erosion occurs."""
        idx = _make_indices(cap_strength=1.0, mlcin=10.0)
        result = compute_ces_from_sounding(idx, 18.0, date(1999, 5, 3))
        if result["cap_erosion_time"] is not None:
            assert isinstance(result["cap_erosion_time"], time)

    def test_no_erosion_for_cold_start(self):
        """
        With a very cold 12Z temp and strong cap, heating cannot break the cap
        even at peak afternoon temperature.
        """
        idx = _make_indices(cap_strength=8.0, mlcin=200.0)
        result = compute_ces_from_sounding(idx, -5.0, date(1999, 1, 15))  # cold winter day
        assert result["cap_behavior"] == CapBehavior.NO_EROSION

    def test_may3_1999_physics(self):
        """
        Ground truth case: May 3, 1999.  cap_strength=3.34°C, MLCIN=16.2 J/kg.
        Expects erosion in early-to-clean window (before ~21Z).
        """
        idx = _make_indices(cap_strength=3.34, mlcin=16.2)
        result = compute_ces_from_sounding(idx, 18.2, date(1999, 5, 3))
        assert result["cap_behavior"] in (CapBehavior.EARLY_EROSION, CapBehavior.CLEAN_EROSION)
        assert result["convective_temp_gap_12Z"] > 10.0   # substantial 12Z gap
        assert result["convective_temp_gap_18Z"] < 5.0    # nearly eroded by 18Z

    def test_heating_efficiency_constant_is_positive(self):
        """HEATING_EFFICIENCY must be positive (J/kg per °F per hour)."""
        assert HEATING_EFFICIENCY > 0.0


# ── ThermodynamicIndices optional fields ──────────────────────────────────────

class TestThermodynamicIndicesOptionalFields:
    """LFC_height and EL_height should be Optional to handle legacy stored data."""

    def test_lfc_height_can_be_none(self):
        from datetime import datetime, timezone
        from ok_weather_model.models import ThermodynamicIndices
        from ok_weather_model.models.enums import OklahomaSoundingStation

        idx = ThermodynamicIndices(
            valid_time=datetime(2000, 5, 1, 12, tzinfo=timezone.utc),
            station=OklahomaSoundingStation.OUN,
            MLCAPE=2000.0,
            MLCIN=50.0,
            SBCAPE=2500.0,
            SBCIN=50.0,
            MUCAPE=3000.0,
            LCL_height=600.0,
            LFC_height=None,
            EL_height=None,
            convective_temperature=85.0,
            cap_strength=2.5,
            lapse_rate_700_500=7.0,
            lapse_rate_850_500=6.0,
            precipitable_water=1.1,
            mixing_ratio_850=11.0,
            wet_bulb_zero=3000.0,
        )
        assert idx.LFC_height is None
        assert idx.EL_height is None

    def test_lfc_height_accepts_float(self):
        from datetime import datetime, timezone
        from ok_weather_model.models import ThermodynamicIndices
        from ok_weather_model.models.enums import OklahomaSoundingStation

        idx = ThermodynamicIndices(
            valid_time=datetime(2000, 5, 1, 12, tzinfo=timezone.utc),
            station=OklahomaSoundingStation.OUN,
            MLCAPE=2000.0,
            MLCIN=50.0,
            SBCAPE=2500.0,
            SBCIN=50.0,
            MUCAPE=3000.0,
            LCL_height=600.0,
            LFC_height=1500.0,
            EL_height=12000.0,
            convective_temperature=85.0,
            cap_strength=2.5,
            lapse_rate_700_500=7.0,
            lapse_rate_850_500=6.0,
            precipitable_water=1.1,
            mixing_ratio_850=11.0,
            wet_bulb_zero=3000.0,
        )
        assert idx.LFC_height == pytest.approx(1500.0)
