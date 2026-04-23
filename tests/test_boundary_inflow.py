"""
Tests for boundary-inflow angle calculation and BoundaryObservation orientation.
"""

import math
from datetime import datetime, timezone

import pytest

from ok_weather_model.models.boundary import BoundaryObservation
from ok_weather_model.models.enums import BoundaryType, OklahomaCounty
from ok_weather_model.processing.cap_calculator import (
    compute_boundary_inflow_angle,
    BOUNDARY_CONVERGENCE_SCALE,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_boundary(
    lats: list[float],
    lons: list[float],
    orientation_angle: float | None = None,
) -> BoundaryObservation:
    return BoundaryObservation(
        valid_time=datetime(1999, 5, 3, 18, tzinfo=timezone.utc),
        boundary_type=BoundaryType.DRYLINE,
        position_lat=lats,
        position_lon=lons,
        counties_intersected=[OklahomaCounty.CANADIAN],
        orientation_angle=orientation_angle,
    )


def _make_kinematics(llj_speed: float | None, llj_direction: float | None):
    """Minimal KinematicProfile with only LLJ fields populated."""
    from ok_weather_model.models.kinematic import KinematicProfile, WindLevel
    from ok_weather_model.models.enums import OklahomaSoundingStation, HodographShape

    # Provide a single surface level so the model is valid
    sfc = WindLevel(pressure=850.0, height=0.0, u_component=0.0, v_component=0.0)
    return KinematicProfile(
        valid_time=datetime(1999, 5, 3, 12, tzinfo=timezone.utc),
        station=OklahomaSoundingStation.OUN,
        levels=[sfc],
        SRH_0_1km=200.0,
        SRH_0_3km=400.0,
        BWD_0_1km=20.0,
        BWD_0_6km=50.0,
        mean_wind_0_6km=30.0,
        hodograph_shape=HodographShape.CURVED,
        storm_motion_bunkers_right=(5.0, 10.0),
        storm_motion_bunkers_left=(-5.0, 10.0),
        LLJ_speed=llj_speed,
        LLJ_direction=llj_direction,
    )


# ── BoundaryObservation.compute_orientation_from_polyline ─────────────────────

class TestBoundaryOrientation:
    def test_ns_polyline_orientation_near_zero(self):
        """A N-S polyline (constant lon, increasing lat) should give ~0°."""
        b = _make_boundary(lats=[35.0, 36.0, 37.0], lons=[-98.0, -98.0, -98.0])
        assert b.orientation_angle == pytest.approx(0.0, abs=1.0)

    def test_ew_polyline_orientation_near_90(self):
        """An E-W polyline (constant lat, changing lon) should give ~90°."""
        b = _make_boundary(lats=[35.5, 35.5, 35.5], lons=[-100.0, -98.0, -96.0])
        assert b.orientation_angle == pytest.approx(90.0, abs=1.0)

    def test_ne_sw_polyline_orientation_near_45(self):
        """A NE-SW polyline (equal dlat and dlon) should give ~45°."""
        b = _make_boundary(lats=[35.0, 36.0], lons=[-98.0, -97.0])
        assert b.orientation_angle == pytest.approx(45.0, abs=2.0)

    def test_explicit_orientation_not_overwritten(self):
        """A manually supplied orientation_angle must not be overwritten."""
        b = _make_boundary(
            lats=[35.0, 36.0], lons=[-98.0, -98.0], orientation_angle=30.0
        )
        assert b.orientation_angle == pytest.approx(30.0)

    def test_two_point_polyline_consistent(self):
        """Two-point N-S polyline (minimum valid) should be consistent."""
        b = _make_boundary(lats=[35.0, 36.0], lons=[-98.0, -98.0])
        assert b.orientation_angle == pytest.approx(0.0, abs=1.0)


# ── compute_boundary_inflow_angle ─────────────────────────────────────────────

class TestBoundaryInflowAngle:

    # ── Perpendicular cases ───────────────────────────────────────────────────

    def test_perpendicular_inflow_efficiency_is_one(self):
        """
        N-S boundary + easterly inflow (FROM east = 90°) → inflow moving west,
        which is perfectly perpendicular to the N-S boundary.
        convergence_efficiency should be 1.0.
        """
        boundary = _make_boundary(lats=[35.0, 36.0], lons=[-98.0, -98.0])
        kin = _make_kinematics(llj_speed=30.0, llj_direction=90.0)  # from east
        result = compute_boundary_inflow_angle(boundary, kin)
        assert result["convergence_efficiency"] == pytest.approx(1.0, abs=0.01)
        assert result["angle_to_boundary_deg"] == pytest.approx(90.0, abs=1.0)

    def test_perpendicular_normal_inflow_equals_speed(self):
        """At 90° (perpendicular), normal component equals full inflow speed."""
        boundary = _make_boundary(lats=[35.0, 36.0], lons=[-98.0, -98.0])
        kin = _make_kinematics(llj_speed=25.0, llj_direction=90.0)
        result = compute_boundary_inflow_angle(boundary, kin)
        assert result["normal_inflow_component_kt"] == pytest.approx(25.0, abs=0.5)

    # ── Parallel cases ────────────────────────────────────────────────────────

    def test_parallel_inflow_efficiency_is_zero(self):
        """
        N-S boundary + southerly inflow (FROM south = 180°) → inflow moving
        north, parallel to the boundary.  convergence_efficiency should be 0.
        """
        boundary = _make_boundary(lats=[35.0, 36.0], lons=[-98.0, -98.0])
        kin = _make_kinematics(llj_speed=30.0, llj_direction=180.0)  # from south
        result = compute_boundary_inflow_angle(boundary, kin)
        assert result["convergence_efficiency"] == pytest.approx(0.0, abs=0.01)
        assert result["normal_inflow_component_kt"] == pytest.approx(0.0, abs=0.5)

    # ── 45° case ──────────────────────────────────────────────────────────────

    def test_45_degree_efficiency_is_half_sqrt2(self):
        """
        N-S boundary + SE inflow (FROM SE = 135°) → inflow moving NW at 45°
        to the boundary.  convergence_efficiency = sin(45°) ≈ 0.707.
        """
        boundary = _make_boundary(lats=[35.0, 36.0], lons=[-98.0, -98.0])
        kin = _make_kinematics(llj_speed=30.0, llj_direction=135.0)  # from SE
        result = compute_boundary_inflow_angle(boundary, kin)
        assert result["convergence_efficiency"] == pytest.approx(math.sin(math.radians(45)), abs=0.02)
        assert result["normal_inflow_component_kt"] == pytest.approx(
            30.0 * math.sin(math.radians(45)), abs=0.5
        )

    # ── Oklahoma dryline ground-truth case ────────────────────────────────────

    def test_may3_1999_dryline_scenario(self):
        """
        May 3, 1999: dryline roughly N-S (~10° orientation), LLJ from SSE (~160°),
        ~35 knots.  Expect significant convergence (efficiency > 0.5).
        """
        boundary = _make_boundary(
            lats=[34.5, 35.5, 36.5], lons=[-98.5, -98.3, -98.1],
            orientation_angle=10.0,   # slightly NNE-SSW
        )
        kin = _make_kinematics(llj_speed=35.0, llj_direction=150.0)  # SSE
        result = compute_boundary_inflow_angle(boundary, kin)
        assert result["convergence_efficiency"] >= 0.5
        assert result["normal_inflow_component_kt"] > 15.0
        assert result["inflow_source"] == "LLJ_850mb"

    # ── No LLJ fallback ───────────────────────────────────────────────────────

    def test_no_llj_returns_zero_forcing(self):
        """When LLJ is unavailable and no wind levels, returns zeros."""
        from ok_weather_model.models.kinematic import KinematicProfile, WindLevel
        from ok_weather_model.models.enums import OklahomaSoundingStation, HodographShape

        # Build a KinematicProfile with no LLJ and no sub-1km wind levels
        kin = KinematicProfile(
            valid_time=datetime(1999, 5, 3, 12, tzinfo=timezone.utc),
            station=OklahomaSoundingStation.OUN,
            levels=[WindLevel(pressure=500.0, height=5000.0, u_component=0.0, v_component=0.0)],
            SRH_0_1km=200.0,
            SRH_0_3km=400.0,
            BWD_0_1km=20.0,
            BWD_0_6km=50.0,
            mean_wind_0_6km=30.0,
            hodograph_shape=HodographShape.CURVED,
            storm_motion_bunkers_right=(5.0, 10.0),
            storm_motion_bunkers_left=(-5.0, 10.0),
            LLJ_speed=None,
            LLJ_direction=None,
        )
        boundary = _make_boundary(lats=[35.0, 36.0], lons=[-98.0, -98.0])
        result = compute_boundary_inflow_angle(boundary, kin)
        assert result["convergence_efficiency"] == pytest.approx(0.0)
        assert result["normal_inflow_component_kt"] == pytest.approx(0.0)

    # ── Result keys ───────────────────────────────────────────────────────────

    def test_returns_required_keys(self):
        boundary = _make_boundary(lats=[35.0, 36.0], lons=[-98.0, -98.0])
        kin = _make_kinematics(llj_speed=30.0, llj_direction=180.0)
        result = compute_boundary_inflow_angle(boundary, kin)
        expected = {
            "boundary_orientation_deg",
            "inflow_direction_deg",
            "inflow_speed_kt",
            "angle_to_boundary_deg",
            "convergence_efficiency",
            "normal_inflow_component_kt",
            "inflow_source",
        }
        assert set(result.keys()) == expected

    # ── Scaling constant ─────────────────────────────────────────────────────

    def test_boundary_convergence_scale_positive(self):
        assert BOUNDARY_CONVERGENCE_SCALE > 0.0

    def test_30kt_perpendicular_forces_roughly_15_jkg_hr(self):
        """30-kt perpendicular inflow should produce ~15 J/kg/hr erosion."""
        from ok_weather_model.processing.cap_calculator import _boundary_forcing_from_normal_inflow
        forcing = _boundary_forcing_from_normal_inflow(30.0)
        assert forcing == pytest.approx(-15.0, abs=1.0)
        assert forcing < 0  # must be erosive
