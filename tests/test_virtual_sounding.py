"""
Tests for ERA5 virtual sounding extraction.

Uses synthetic xarray Datasets with analytically known values so all
expected outputs can be verified exactly.
"""

import math
from datetime import datetime, timezone

import numpy as np
import pytest

xr = pytest.importorskip("xarray", reason="xarray required")

from ok_weather_model.processing.era5_diagnostics import (
    extract_virtual_sounding,
    _nearest_sounding_station,
    _specific_humidity_to_dewpoint_c,
)
from ok_weather_model.models.enums import OklahomaSoundingStation

# ── Synthetic dataset helpers ─────────────────────────────────────────────────

# Pressure levels matching SOUNDING_PRESSURE_LEVELS (22 levels)
SOUNDING_LEVELS = np.array([
    1000, 975, 950, 925, 900, 875, 850, 825, 800,
    775,  750, 700, 650, 600, 550, 500, 450, 400,
    350,  300, 250, 200,
], dtype=float)

LATS = np.array([34.0, 35.0, 36.0, 37.0])
LONS = np.array([-103.0, -101.0, -99.0, -97.0, -95.0])
VALID_TIME = datetime(1999, 5, 3, 12)  # naive UTC

TIMES = np.array([VALID_TIME], dtype="datetime64[ns]")
_G = 9.80665


def _make_sounding_ds(
    T_func=None,        # callable(level_hpa) → K, default standard atm
    q_func=None,        # callable(level_hpa) → kg/kg, default 0.01
    u_func=None,        # callable(level_hpa) → m/s, default 0
    v_func=None,        # callable(level_hpa) → m/s, default 10 (southerly)
    phi_func=None,      # callable(level_hpa) → m²/s², default hypsometric
) -> xr.Dataset:
    """
    Build a synthetic ERA5 Dataset with the full sounding pressure level set.
    Default profiles are physically plausible Oklahoma spring values.
    """
    nlat, nlon = len(LATS), len(LONS)
    nlev = len(SOUNDING_LEVELS)

    # Default: simple US standard atmosphere
    if T_func is None:
        def T_func(p):
            # Roughly 300 K at surface, decreasing at ~6.5 K/km
            return 300.0 - (1000.0 - p) * 0.06  # K

    if q_func is None:
        def q_func(p):
            # Moisture decreasing with height
            return max(0.002, 0.018 * (p / 1000.0) ** 3)

    if u_func is None:
        def u_func(p): return 0.0

    if v_func is None:
        def v_func(p): return 10.0  # southerly 10 m/s

    if phi_func is None:
        def phi_func(p):
            # Geopotential from hypsometric: Φ ≈ R*T*ln(p0/p), rough values
            return _G * (10000.0 * math.log(1000.0 / p))

    T_cube = np.zeros((1, nlev, nlat, nlon))
    q_cube = np.zeros((1, nlev, nlat, nlon))
    u_cube = np.zeros((1, nlev, nlat, nlon))
    v_cube = np.zeros((1, nlev, nlat, nlon))
    phi_cube = np.zeros((1, nlev, nlat, nlon))

    for li, lev in enumerate(SOUNDING_LEVELS):
        T_cube[0, li, :, :]   = T_func(lev)
        q_cube[0, li, :, :]   = q_func(lev)
        u_cube[0, li, :, :]   = u_func(lev)
        v_cube[0, li, :, :]   = v_func(lev)
        phi_cube[0, li, :, :] = phi_func(lev)

    coords = {
        "time": TIMES,
        "level": SOUNDING_LEVELS,
        "latitude": LATS,
        "longitude": LONS,
    }
    dims = ("time", "level", "latitude", "longitude")

    return xr.Dataset(
        {
            "temperature":          (dims, T_cube),
            "specific_humidity":    (dims, q_cube),
            "u_component_of_wind":  (dims, u_cube),
            "v_component_of_wind":  (dims, v_cube),
            "geopotential":         (dims, phi_cube),
        },
        coords=coords,
    )


# ── Tests for _specific_humidity_to_dewpoint_c ────────────────────────────────

class TestSpecificHumidityToDewpoint:

    def test_typical_boundary_layer_moisture(self):
        """q = 0.016 kg/kg at 850 hPa → Td ≈ 17–22 °C."""
        td = _specific_humidity_to_dewpoint_c(0.016, 850.0)
        assert 15.0 < td < 25.0

    def test_dry_air_returns_very_cold_dewpoint(self):
        """Near-zero q → dewpoint well below freezing."""
        td = _specific_humidity_to_dewpoint_c(0.0001, 500.0)
        assert td < -20.0

    def test_zero_humidity_returns_floor(self):
        td = _specific_humidity_to_dewpoint_c(0.0, 850.0)
        assert td == pytest.approx(-40.0)

    def test_dewpoint_increases_with_humidity(self):
        """Higher specific humidity → higher dewpoint at same pressure."""
        td_dry  = _specific_humidity_to_dewpoint_c(0.005, 850.0)
        td_moist = _specific_humidity_to_dewpoint_c(0.015, 850.0)
        assert td_moist > td_dry

    def test_dewpoint_below_temperature_at_typical_values(self):
        """Dewpoint must be ≤ temperature (saturation)."""
        T_c = 25.0
        td = _specific_humidity_to_dewpoint_c(0.018, 925.0)
        assert td <= T_c


# ── Tests for _nearest_sounding_station ──────────────────────────────────────

class TestNearestSoundingStation:

    def test_norman_oklahoma_maps_to_oun(self):
        """Norman, OK (OUN site) should map to OUN."""
        assert _nearest_sounding_station(35.22, -97.44) == OklahomaSoundingStation.OUN

    def test_lamont_oklahoma_maps_to_lmn(self):
        """Lamont, OK (LMN site) should map to LMN."""
        assert _nearest_sounding_station(36.71, -97.49) == OklahomaSoundingStation.LMN

    def test_amarillo_tx_maps_to_ama(self):
        """Amarillo, TX should map to AMA."""
        assert _nearest_sounding_station(35.23, -101.70) == OklahomaSoundingStation.AMA

    def test_dodge_city_ks_maps_to_ddc(self):
        """Dodge City, KS should map to DDC."""
        assert _nearest_sounding_station(37.76, -99.97) == OklahomaSoundingStation.DDC

    def test_western_oklahoma_maps_to_ama_or_ddc(self):
        """Western Oklahoma panhandle is closer to AMA or DDC than OUN."""
        station = _nearest_sounding_station(36.5, -102.0)
        assert station in (OklahomaSoundingStation.AMA, OklahomaSoundingStation.DDC)


# ── Tests for extract_virtual_sounding ───────────────────────────────────────

class TestExtractVirtualSounding:

    def test_returns_sounding_profile(self):
        """Basic call returns a non-None SoundingProfile."""
        ds = _make_sounding_ds()
        result = extract_virtual_sounding(ds, lat=35.5, lon=-97.5, valid_time=VALID_TIME)
        assert result is not None

    def test_raw_source_is_virtual(self):
        ds = _make_sounding_ds()
        result = extract_virtual_sounding(ds, 35.5, -97.5, VALID_TIME)
        assert result.raw_source == "virtual"

    def test_levels_in_decreasing_pressure_order(self):
        """Pressure must strictly decrease with index (required by SoundingProfile)."""
        ds = _make_sounding_ds()
        result = extract_virtual_sounding(ds, 35.5, -97.5, VALID_TIME)
        pressures = [lv.pressure for lv in result.levels]
        for i in range(1, len(pressures)):
            assert pressures[i] < pressures[i - 1], (
                f"Pressure not decreasing at index {i}: {pressures[i-1]} → {pressures[i]}"
            )

    def test_minimum_level_count_for_metpy(self):
        """Virtual sounding must have ≥ 10 levels for MetPy CAPE calculations."""
        ds = _make_sounding_ds()
        result = extract_virtual_sounding(ds, 35.5, -97.5, VALID_TIME)
        assert len(result.levels) >= 10

    def test_temperature_in_celsius(self):
        """Temperature values should be in °C (< 60), not Kelvin (> 200)."""
        ds = _make_sounding_ds()
        result = extract_virtual_sounding(ds, 35.5, -97.5, VALID_TIME)
        for lv in result.levels:
            assert -90.0 < lv.temperature < 60.0, f"Temperature {lv.temperature} looks like Kelvin"

    def test_dewpoint_does_not_exceed_temperature(self):
        """Dewpoint must be ≤ temperature at every level."""
        ds = _make_sounding_ds()
        result = extract_virtual_sounding(ds, 35.5, -97.5, VALID_TIME)
        for lv in result.levels:
            assert lv.dewpoint <= lv.temperature + 0.01, (
                f"Dewpoint {lv.dewpoint:.1f}°C exceeds temperature {lv.temperature:.1f}°C "
                f"at {lv.pressure:.0f} hPa"
            )

    def test_height_increases_with_altitude(self):
        """Height should increase as pressure decreases (lower pressure = higher altitude)."""
        ds = _make_sounding_ds()
        result = extract_virtual_sounding(ds, 35.5, -97.5, VALID_TIME)
        heights = [lv.height for lv in result.levels]
        for i in range(1, len(heights)):
            assert heights[i] > heights[i - 1], (
                f"Height not increasing at index {i}: {heights[i-1]:.0f} → {heights[i]:.0f} m"
            )

    def test_wind_speed_nonnegative(self):
        ds = _make_sounding_ds()
        result = extract_virtual_sounding(ds, 35.5, -97.5, VALID_TIME)
        for lv in result.levels:
            assert lv.wind_speed >= 0.0

    def test_wind_direction_in_range(self):
        ds = _make_sounding_ds()
        result = extract_virtual_sounding(ds, 35.5, -97.5, VALID_TIME)
        for lv in result.levels:
            assert 0.0 <= lv.wind_direction <= 360.0

    def test_southerly_wind_gives_correct_direction(self):
        """
        Pure southerly flow (u=0, v=10 m/s, blowing northward) should be reported
        as coming FROM the south → wind_direction ≈ 180°.
        """
        ds = _make_sounding_ds(u_func=lambda p: 0.0, v_func=lambda p: 10.0)
        result = extract_virtual_sounding(ds, 35.5, -97.5, VALID_TIME)
        for lv in result.levels:
            assert abs(lv.wind_direction - 180.0) < 2.0, (
                f"Southerly flow should give 180°, got {lv.wind_direction:.1f}° "
                f"at {lv.pressure:.0f} hPa"
            )

    def test_known_temperature_value(self):
        """Temperature at 850 hPa should match T_func(850) - 273.15."""
        T850_k = 285.0
        ds = _make_sounding_ds(T_func=lambda p: T850_k if abs(p - 850) < 1 else 273.15)
        result = extract_virtual_sounding(ds, 35.5, -97.5, VALID_TIME)
        lv_850 = next(lv for lv in result.levels if abs(lv.pressure - 850.0) < 1.0)
        assert lv_850.temperature == pytest.approx(T850_k - 273.15, abs=0.1)

    def test_nearest_station_assigned(self):
        """Station should be the nearest OklahomaSoundingStation to the requested lat/lon."""
        ds = _make_sounding_ds()
        # Norman, OK → should be OUN
        result = extract_virtual_sounding(ds, 35.2, -97.5, VALID_TIME)
        assert result.station == OklahomaSoundingStation.OUN

    def test_timezone_aware_valid_time_accepted(self):
        """Timezone-aware valid_time should work identically to naive."""
        ds = _make_sounding_ds()
        t_aware = VALID_TIME.replace(tzinfo=timezone.utc)
        result = extract_virtual_sounding(ds, 35.5, -97.5, t_aware)
        assert result is not None
        assert result.valid_time.tzinfo is not None

    def test_missing_field_returns_none(self):
        """Dataset missing geopotential → returns None gracefully."""
        ds = _make_sounding_ds()
        ds = ds.drop_vars("geopotential")
        result = extract_virtual_sounding(ds, 35.5, -97.5, VALID_TIME)
        assert result is None

    # ── Physics integration: MetPy can process the output ────────────────────

    def test_metpy_can_compute_thermodynamic_indices(self):
        """
        The virtual SoundingProfile should pass through compute_thermodynamic_indices
        without errors, producing physically plausible CAPE values.

        Uses a profile designed to have moderate instability:
          - Surface ~27°C, upper-level lapse rate steeper than moist adiabat.
        """
        pytest.importorskip("metpy", reason="metpy required for this integration test")

        from ok_weather_model.processing import (
            compute_thermodynamic_indices,
            extract_virtual_sounding,
        )

        def T_unstable(p):
            # Warm moist BL (~300 K at surface) with steep mid-level lapse rate
            return 300.0 - (1000.0 - p) * 0.07

        def q_moist(p):
            return max(0.001, 0.020 * (p / 1000.0) ** 2)

        ds = _make_sounding_ds(T_func=T_unstable, q_func=q_moist)
        profile = extract_virtual_sounding(ds, 35.5, -97.5, VALID_TIME)
        assert profile is not None
        assert len(profile.levels) >= 10

        indices = compute_thermodynamic_indices(profile)
        assert indices.MLCAPE >= 0.0
        assert indices.MLCIN >= 0.0
        assert indices.LCL_height >= 0.0
