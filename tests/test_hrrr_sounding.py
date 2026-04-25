"""
Tests for HRRR prs virtual sounding extraction.

Uses a mock Herbie instance that returns synthetic xarray Datasets — no actual
HRRR data is fetched, so these tests run fully offline.

Structure mirrors tests/test_virtual_sounding.py (ERA5 virtual sounding).
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

xr = pytest.importorskip("xarray", reason="xarray required")

from ok_weather_model.processing.hrrr_sounding import (
    extract_virtual_sounding_from_hrrr,
    find_best_hrrr_prs_run,
)
from ok_weather_model.models.enums import OklahomaSoundingStation

# ── Synthetic HRRR prs dataset helpers ───────────────────────────────────────

# Pressure levels: 14 levels spanning 200–1000 hPa (matches realistic HRRR set)
_LEVELS = np.array(
    [1000, 950, 925, 900, 850, 800, 750, 700, 650, 600, 500, 400, 300, 250, 200],
    dtype=float,
)

# OUN lat/lon (Norman, OK) — lon in 0-360 convention
_TARGET_LAT = 35.22
_TARGET_LON_360 = 360.0 - 97.44  # ≈ 262.56

_VALID_TIME = datetime(1999, 5, 3, 12, tzinfo=timezone.utc)
_FXX = 6


def _make_prs_ds(var_name: str, levels: np.ndarray, values_func):
    """
    Build a synthetic HRRR prs xarray Dataset for a single variable.

    Grid: 5×5 points centred on OUN, lon in 0-360 convention.
    Dims: (isobaricInhPa, y, x).
    """
    ny, nx = 5, 5
    lats = np.tile(
        np.linspace(_TARGET_LAT - 1.0, _TARGET_LAT + 1.0, ny)[:, None], (1, nx)
    )
    lons = np.tile(
        np.linspace(_TARGET_LON_360 - 1.0, _TARGET_LON_360 + 1.0, nx)[None, :],
        (ny, 1),
    )

    data = np.zeros((len(levels), ny, nx))
    for li, lev in enumerate(levels):
        data[li, :, :] = values_func(lev)

    return xr.Dataset(
        {var_name: (("isobaricInhPa", "y", "x"), data)},
        coords={
            "isobaricInhPa": levels,
            "latitude": (("y", "x"), lats),
            "longitude": (("y", "x"), lons),
        },
    )


def _make_mock_herbie(levels=_LEVELS):
    """
    Return a mock Herbie instance whose .xarray() responds to GRIB2 search patterns
    by returning the appropriate synthetic dataset.
    """

    def _xarray(pattern, remove_grib=False):
        if "TMP" in pattern:
            # Temperature: 300 K at surface, decreasing ~7 K per 100 hPa
            return _make_prs_ds("t", levels, lambda p: 300.0 - (1000.0 - p) * 0.07)
        elif "HGT" in pattern:
            # Geopotential height from log-pressure approximation
            return _make_prs_ds(
                "gh", levels, lambda p: 9.80665 * 10000.0 * math.log(1000.0 / max(p, 1.0))
            )
        elif "UGRD" in pattern:
            return _make_prs_ds("u", levels, lambda p: 5.0)  # westerly 5 m/s
        elif "VGRD" in pattern:
            return _make_prs_ds("v", levels, lambda p: 10.0)  # southerly 10 m/s
        elif "SPFH" in pattern:
            # Specific humidity decreasing with height
            return _make_prs_ds("q", levels, lambda p: max(0.001, 0.015 * (p / 1000.0) ** 2))
        else:
            raise ValueError(f"Unrecognised pattern: {pattern}")

    mock = MagicMock()
    mock.xarray.side_effect = _xarray
    mock.grib = "synthetic.grib2"  # non-None → run is "posted"
    return mock


# Convenience: patch Herbie and run extraction at OUN
def _extract_at_oun(fxx=_FXX, valid_time=_VALID_TIME, levels=_LEVELS):
    mock_h = _make_mock_herbie(levels)
    with patch("herbie.Herbie", return_value=mock_h):
        return extract_virtual_sounding_from_hrrr(valid_time, fxx, _TARGET_LAT, -97.44)


# ── Basic structural tests ────────────────────────────────────────────────────

class TestExtractVirtualSoundingFromHRRR:

    def test_returns_sounding_profile(self):
        """Basic call returns a non-None SoundingProfile."""
        result = _extract_at_oun()
        assert result is not None

    def test_raw_source_is_hrrr_virtual(self):
        result = _extract_at_oun()
        assert result.raw_source == "hrrr-virtual"

    def test_station_assigned_to_oun(self):
        """Nearest station for OUN lat/lon should be OUN."""
        result = _extract_at_oun()
        assert result.station == OklahomaSoundingStation.OUN

    def test_valid_time_is_timezone_aware(self):
        result = _extract_at_oun()
        assert result.valid_time.tzinfo is not None

    def test_valid_time_matches_input(self):
        result = _extract_at_oun()
        assert result.valid_time == _VALID_TIME

    def test_minimum_level_count(self):
        """Profile must have ≥ 10 levels for downstream MetPy calculations."""
        result = _extract_at_oun()
        assert len(result.levels) >= 10

    # ── Level ordering ────────────────────────────────────────────────────────

    def test_levels_in_decreasing_pressure_order(self):
        """Pressure must strictly decrease with index."""
        result = _extract_at_oun()
        pressures = [lv.pressure for lv in result.levels]
        for i in range(1, len(pressures)):
            assert pressures[i] < pressures[i - 1], (
                f"Pressure not decreasing at index {i}: {pressures[i-1]} → {pressures[i]}"
            )

    def test_height_increases_with_altitude(self):
        """Height must increase as index rises (lower pressure = higher altitude)."""
        result = _extract_at_oun()
        heights = [lv.height for lv in result.levels]
        for i in range(1, len(heights)):
            assert heights[i] > heights[i - 1], (
                f"Height not increasing at index {i}: {heights[i-1]:.0f} → {heights[i]:.0f} m"
            )

    # ── Physical sanity ───────────────────────────────────────────────────────

    def test_temperature_in_celsius_not_kelvin(self):
        """Temperature values should be in °C (< 60), not Kelvin (> 200)."""
        result = _extract_at_oun()
        for lv in result.levels:
            assert -90.0 < lv.temperature < 60.0, (
                f"Temperature {lv.temperature} looks like Kelvin"
            )

    def test_dewpoint_does_not_exceed_temperature(self):
        """Dewpoint ≤ temperature at every level (physical constraint)."""
        result = _extract_at_oun()
        for lv in result.levels:
            assert lv.dewpoint <= lv.temperature + 0.01, (
                f"Dewpoint {lv.dewpoint:.1f}°C exceeds temperature {lv.temperature:.1f}°C "
                f"at {lv.pressure:.0f} hPa"
            )

    def test_wind_speed_nonnegative(self):
        result = _extract_at_oun()
        for lv in result.levels:
            assert lv.wind_speed >= 0.0

    def test_wind_direction_in_range(self):
        result = _extract_at_oun()
        for lv in result.levels:
            assert 0.0 <= lv.wind_direction <= 360.0

    def test_southerly_wind_gives_correct_direction(self):
        """
        u=0, v=10 m/s (blowing northward) → from the south → wind_direction ≈ 180°.
        """
        mock_h = _make_mock_herbie()
        # Override UGRD / VGRD via a specialised mock
        original_side = mock_h.xarray.side_effect

        def _override(pattern, remove_grib=False):
            if "UGRD" in pattern:
                return _make_prs_ds("u", _LEVELS, lambda p: 0.0)
            if "VGRD" in pattern:
                return _make_prs_ds("v", _LEVELS, lambda p: 10.0)
            return original_side(pattern, remove_grib=remove_grib)

        mock_h.xarray.side_effect = _override

        with patch("herbie.Herbie", return_value=mock_h):
            result = extract_virtual_sounding_from_hrrr(_VALID_TIME, _FXX, _TARGET_LAT, -97.44)

        assert result is not None
        for lv in result.levels:
            assert abs(lv.wind_direction - 180.0) < 2.0, (
                f"Southerly flow → 180°, got {lv.wind_direction:.1f}° at {lv.pressure:.0f} hPa"
            )

    def test_known_temperature_value(self):
        """Temperature at 850 hPa should match T_func(850) - 273.15."""
        T850_k = 285.0
        mock_h = _make_mock_herbie()

        def _t_override(pattern, remove_grib=False):
            if "TMP" in pattern:
                return _make_prs_ds(
                    "t", _LEVELS,
                    lambda p: T850_k if abs(p - 850) < 1 else 280.0,
                )
            return mock_h.xarray.side_effect.__wrapped__(pattern, remove_grib=remove_grib)

        # Simpler: just replace the mock directly
        mock_h2 = _make_mock_herbie()
        orig = mock_h2.xarray.side_effect

        def _override2(pattern, remove_grib=False):
            if "TMP" in pattern:
                return _make_prs_ds(
                    "t", _LEVELS,
                    lambda p: T850_k if abs(p - 850) < 1 else 280.0,
                )
            return orig(pattern, remove_grib=remove_grib)

        mock_h2.xarray.side_effect = _override2

        with patch("herbie.Herbie", return_value=mock_h2):
            result = extract_virtual_sounding_from_hrrr(_VALID_TIME, _FXX, _TARGET_LAT, -97.44)

        assert result is not None
        lv_850 = next(lv for lv in result.levels if abs(lv.pressure - 850.0) < 1.0)
        assert lv_850.temperature == pytest.approx(T850_k - 273.15, abs=0.1)

    # ── Error handling ────────────────────────────────────────────────────────

    def test_returns_none_when_herbie_unavailable(self):
        """ImportError from herbie → graceful None, no exception."""
        import sys
        original = sys.modules.get("herbie")
        sys.modules["herbie"] = None  # type: ignore[assignment]
        try:
            result = extract_virtual_sounding_from_hrrr(_VALID_TIME, _FXX, _TARGET_LAT, -97.44)
            assert result is None
        finally:
            if original is None:
                sys.modules.pop("herbie", None)
            else:
                sys.modules["herbie"] = original

    def test_returns_none_when_herbie_init_fails(self):
        """Herbie constructor exception → graceful None."""
        with patch("herbie.Herbie", side_effect=RuntimeError("network error")):
            result = extract_virtual_sounding_from_hrrr(_VALID_TIME, _FXX, _TARGET_LAT, -97.44)
        assert result is None

    def test_returns_none_when_temperature_field_missing(self):
        """If TMP field unavailable, sounding cannot be built → None."""
        mock_h = _make_mock_herbie()
        orig = mock_h.xarray.side_effect

        def _skip_tmp(pattern, remove_grib=False):
            if "TMP" in pattern:
                raise RuntimeError("field not available")
            return orig(pattern, remove_grib=remove_grib)

        mock_h.xarray.side_effect = _skip_tmp

        with patch("herbie.Herbie", return_value=mock_h):
            result = extract_virtual_sounding_from_hrrr(_VALID_TIME, _FXX, _TARGET_LAT, -97.44)
        assert result is None

    def test_returns_none_when_too_few_valid_levels(self):
        """
        If fewer than 10 valid levels survive QC (e.g. all T below 150 K),
        the function should return None rather than a degenerate profile.
        """
        mock_h = _make_mock_herbie()
        orig = mock_h.xarray.side_effect

        def _bad_tmp(pattern, remove_grib=False):
            if "TMP" in pattern:
                # Temperature 100 K — fails the 150 K guard in hrrr_sounding.py
                return _make_prs_ds("t", _LEVELS, lambda p: 100.0)
            return orig(pattern, remove_grib=remove_grib)

        mock_h.xarray.side_effect = _bad_tmp

        with patch("herbie.Herbie", return_value=mock_h):
            result = extract_virtual_sounding_from_hrrr(_VALID_TIME, _FXX, _TARGET_LAT, -97.44)
        assert result is None

    # ── fxx=0 (analysis hour) ─────────────────────────────────────────────────

    def test_fxx_zero_uses_anl_suffix(self):
        """fxx=0 should use 'anl:' suffix, not '0 hour fcst:'."""
        patterns_seen: list[str] = []
        mock_h = _make_mock_herbie()
        orig = mock_h.xarray.side_effect

        def _capture(pattern, remove_grib=False):
            patterns_seen.append(pattern)
            return orig(pattern, remove_grib=remove_grib)

        mock_h.xarray.side_effect = _capture

        with patch("herbie.Herbie", return_value=mock_h):
            extract_virtual_sounding_from_hrrr(_VALID_TIME, 0, _TARGET_LAT, -97.44)

        assert any("anl:" in p for p in patterns_seen), (
            f"Expected 'anl:' in patterns for fxx=0, got: {patterns_seen}"
        )
        assert not any("hour fcst" in p for p in patterns_seen), (
            f"Unexpected 'hour fcst' in patterns for fxx=0: {patterns_seen}"
        )

    def test_fxx_nonzero_uses_fcst_suffix(self):
        """fxx=6 should use '6 hour fcst:' suffix."""
        patterns_seen: list[str] = []
        mock_h = _make_mock_herbie()
        orig = mock_h.xarray.side_effect

        def _capture(pattern, remove_grib=False):
            patterns_seen.append(pattern)
            return orig(pattern, remove_grib=remove_grib)

        mock_h.xarray.side_effect = _capture

        with patch("herbie.Herbie", return_value=mock_h):
            extract_virtual_sounding_from_hrrr(_VALID_TIME, 6, _TARGET_LAT, -97.44)

        assert any("6 hour fcst:" in p for p in patterns_seen), (
            f"Expected '6 hour fcst:' in patterns for fxx=6, got: {patterns_seen}"
        )

    # ── MetPy integration ─────────────────────────────────────────────────────

    def test_metpy_can_compute_thermodynamic_indices(self):
        """
        The virtual SoundingProfile should pass through compute_thermodynamic_indices
        without errors, producing physically plausible CAPE values.
        """
        pytest.importorskip("metpy", reason="metpy required for this integration test")
        from ok_weather_model.processing import compute_thermodynamic_indices

        result = _extract_at_oun()
        assert result is not None

        indices = compute_thermodynamic_indices(result)
        assert indices.MLCAPE >= 0.0
        assert indices.MLCIN >= 0.0
        assert indices.LCL_height >= 0.0

    def test_full_pipeline_produces_valid_feature_vector(self):
        """
        Virtual sounding → indices → kinematics → extract_features_from_indices
        should produce a 28-element feature dict with no import errors.
        """
        pytest.importorskip("metpy", reason="metpy required")
        from ok_weather_model.processing import (
            compute_thermodynamic_indices,
            compute_kinematic_profile,
        )
        from ok_weather_model.modeling import extract_features_from_indices, FEATURE_NAMES

        profile = _extract_at_oun()
        assert profile is not None

        indices = compute_thermodynamic_indices(profile)
        kinematics = compute_kinematic_profile(profile, indices)
        feats = extract_features_from_indices(indices, kinematics)

        assert set(feats.keys()) == set(FEATURE_NAMES)
        # Core thermodynamic fields must be finite (not NaN) for a virtual sounding
        assert math.isfinite(feats["MLCAPE"])
        assert math.isfinite(feats["SBCAPE"])
        assert math.isfinite(feats["SRH_0_3km"])


# ── Tests for find_best_hrrr_prs_run ─────────────────────────────────────────

class TestFindBestHRRRPrsRun:

    def test_returns_none_when_herbie_unavailable(self):
        """ImportError from herbie → None, no exception."""
        import sys
        original = sys.modules.get("herbie")
        sys.modules["herbie"] = None  # type: ignore[assignment]
        try:
            now = datetime(2024, 5, 3, 13, tzinfo=timezone.utc)
            vt  = datetime(2024, 5, 3, 18, tzinfo=timezone.utc)
            result = find_best_hrrr_prs_run(vt, now)
            assert result is None
        finally:
            if original is None:
                sys.modules.pop("herbie", None)
            else:
                sys.modules["herbie"] = original

    def test_returns_none_when_all_candidates_unposted(self):
        """All candidate runs have grib=None → None returned."""
        mock_h = MagicMock()
        mock_h.grib = None

        now = datetime(2024, 5, 3, 13, tzinfo=timezone.utc)
        vt  = datetime(2024, 5, 3, 18, tzinfo=timezone.utc)

        with patch("herbie.Herbie", return_value=mock_h):
            result = find_best_hrrr_prs_run(vt, now)
        assert result is None

    def test_returns_most_recent_run_when_available(self):
        """
        When the first candidate has grib != None, it should be returned.
        The function tries candidates in recency order (most recent run first),
        so the first valid hit should be returned.
        """
        now = datetime(2024, 5, 3, 14, 30, tzinfo=timezone.utc)
        vt  = datetime(2024, 5, 3, 18, tzinfo=timezone.utc)

        call_count = [0]

        def _make_h(run_str, **kwargs):
            call_count[0] += 1
            m = MagicMock()
            # First call succeeds; later calls would also succeed but shouldn't be reached
            m.grib = "synthetic.grib2"
            return m

        with patch("herbie.Herbie", side_effect=_make_h):
            result = find_best_hrrr_prs_run(vt, now, max_candidates=4)

        assert result is not None
        run_time, fxx = result
        # fxx must bring the run to the valid time
        assert run_time + timedelta(hours=fxx) == vt
        # Should have stopped after the first success
        assert call_count[0] == 1

    def test_skips_failed_candidates_and_returns_next(self):
        """First N candidates fail (grib=None), next one succeeds."""
        now = datetime(2024, 5, 3, 14, 30, tzinfo=timezone.utc)
        vt  = datetime(2024, 5, 3, 18, tzinfo=timezone.utc)

        call_count = [0]

        def _make_h(run_str, **kwargs):
            call_count[0] += 1
            m = MagicMock()
            m.grib = "synthetic.grib2" if call_count[0] >= 3 else None
            return m

        with patch("herbie.Herbie", side_effect=_make_h):
            result = find_best_hrrr_prs_run(vt, now, max_candidates=5)

        assert result is not None
        assert call_count[0] == 3

    def test_returned_fxx_within_bounds(self):
        """fxx of the returned run must be within [min_fxx, max_fxx]."""
        now = datetime(2024, 5, 3, 14, tzinfo=timezone.utc)
        vt  = datetime(2024, 5, 4, 12, tzinfo=timezone.utc)  # ~22h ahead

        def _make_h(run_str, **kwargs):
            m = MagicMock()
            m.grib = "synthetic.grib2"
            return m

        with patch("herbie.Herbie", side_effect=_make_h):
            result = find_best_hrrr_prs_run(vt, now, min_fxx=1, max_fxx=48)

        assert result is not None
        _, fxx = result
        assert 1 <= fxx <= 48
