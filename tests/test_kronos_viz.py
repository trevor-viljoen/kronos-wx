"""
Tests for the kronos_viz 3D visualization package.

These are unit tests for the data-to-trace conversion logic — they verify
correct coordinate placement, trace structure, and geographic grounding
without opening a browser or requiring a display.
"""

import math
from datetime import datetime

import numpy as np
import pytest

plotly = pytest.importorskip("plotly", reason="plotly required for kronos_viz")

from kronos_viz.atmosphere import pressure_to_km, SOUNDING_LEVELS_KM, SOUNDING_LEVELS_HPA
from kronos_viz.scene import CapErosionScene


# ── Atmosphere utilities ──────────────────────────────────────────────────────

class TestAtmosphere:

    def test_surface_pressure_is_near_zero(self):
        """1000 hPa should be very close to 0 km."""
        assert pressure_to_km(1000.0) < 0.15

    def test_500mb_is_near_5km(self):
        """500 hPa ≈ 5.5 km in ICAO standard atmosphere."""
        assert 5.0 < pressure_to_km(500.0) < 6.0

    def test_200mb_is_near_12km(self):
        assert 11.0 < pressure_to_km(200.0) < 13.0

    def test_height_increases_as_pressure_decreases(self):
        """Lower pressure = greater height."""
        heights = [pressure_to_km(p) for p in [1000, 850, 700, 500, 300, 200]]
        for i in range(1, len(heights)):
            assert heights[i] > heights[i - 1]

    def test_sounding_levels_array_length(self):
        assert len(SOUNDING_LEVELS_KM) == len(SOUNDING_LEVELS_HPA) == 22


# ── Geographic layer ──────────────────────────────────────────────────────────

class TestBaseMap:

    def test_base_map_returns_traces(self):
        """add_base_map() should add at least one trace (state boundaries)."""
        scene = CapErosionScene()
        scene.add_base_map(show_counties=False)
        assert len(scene._traces) >= 1

    def test_state_boundary_trace_is_scatter3d(self):
        import plotly.graph_objects as go

        scene = CapErosionScene()
        scene.add_base_map(show_counties=False)
        boundary_traces = [t for t in scene._traces if isinstance(t, go.Scatter3d)]
        assert len(boundary_traces) >= 1

    def test_floor_traces_at_z_zero(self):
        """All geographic floor traces should have z ≈ 0."""
        import plotly.graph_objects as go

        scene = CapErosionScene()
        scene.add_base_map(show_counties=False)
        for trace in scene._traces:
            if isinstance(trace, go.Scatter3d):
                z_vals = [v for v in trace.z if v is not None and not (isinstance(v, float) and math.isnan(v))]
                if z_vals:
                    assert all(abs(z) < 0.2 for z in z_vals), (
                        f"Floor trace has z values far from 0: {z_vals[:5]}"
                    )


# ── Mesonet surface layer ─────────────────────────────────────────────────────

class TestMesonetLayer:

    _LONS   = [-97.5, -96.0, -98.5]
    _LATS   = [35.5,  36.0,  35.0]
    _TEMPS  = [28.0,  26.0,  30.0]
    _DEWS   = [18.0,  16.0,  20.0]
    _WDIRS  = [180.0, 195.0, 170.0]
    _WSPDS  = [15.0,  12.0,  18.0]
    _NAMES  = ["OKC", "Tulsa", "Chickasha"]

    def test_returns_two_traces(self):
        """Mesonet layer should return a marker trace and a column trace."""
        scene = CapErosionScene()
        scene.add_mesonet(
            self._LONS, self._LATS, self._TEMPS, self._DEWS,
            self._WDIRS, self._WSPDS, self._NAMES,
        )
        assert len(scene._traces) == 2

    def test_marker_x_matches_longitudes(self):
        import plotly.graph_objects as go

        scene = CapErosionScene()
        scene.add_mesonet(
            self._LONS, self._LATS, self._TEMPS, self._DEWS,
            self._WDIRS, self._WSPDS, self._NAMES,
        )
        marker_trace = next(
            t for t in scene._traces
            if isinstance(t, go.Scatter3d) and t.mode == "markers"
        )
        for actual, expected in zip(marker_trace.x, self._LONS):
            assert abs(actual - expected) < 1e-6

    def test_marker_y_matches_latitudes(self):
        import plotly.graph_objects as go

        scene = CapErosionScene()
        scene.add_mesonet(
            self._LONS, self._LATS, self._TEMPS, self._DEWS,
            self._WDIRS, self._WSPDS, self._NAMES,
        )
        marker_trace = next(
            t for t in scene._traces
            if isinstance(t, go.Scatter3d) and t.mode == "markers"
        )
        for actual, expected in zip(marker_trace.y, self._LATS):
            assert abs(actual - expected) < 1e-6


# ── ERA5 layers ───────────────────────────────────────────────────────────────

class TestERA5Layers:

    def _make_ds(self):
        """Minimal xarray Dataset matching ERA5 structure."""
        xr = pytest.importorskip("xarray")
        lats = np.array([34.0, 35.0, 36.0, 37.0])
        lons = np.array([-103.0, -101.0, -99.0, -97.0, -95.0])
        levels = np.array([700.0, 500.0])
        times = np.array([datetime(1999, 5, 3, 12)], dtype="datetime64[ns]")

        shape = (1, 2, 4, 5)
        dims = ("time", "level", "latitude", "longitude")
        return xr.Dataset(
            {
                "temperature":          (dims, np.full(shape, 275.0)),
                "u_component_of_wind":  (dims, np.full(shape, 10.0)),
                "v_component_of_wind":  (dims, np.full(shape, 5.0)),
            },
            coords={"time": times, "level": levels, "latitude": lats, "longitude": lons},
        )

    def test_era5_temperature_returns_surface_trace(self):
        import plotly.graph_objects as go

        ds = self._make_ds()
        scene = CapErosionScene()
        scene.add_era5_temperature(ds, level_mb=700, valid_time=datetime(1999, 5, 3, 12))
        surface_traces = [t for t in scene._traces if isinstance(t, go.Surface)]
        assert len(surface_traces) == 1

    def test_era5_temperature_surface_z_at_correct_height(self):
        """Surface z values should equal the standard-atmosphere height of 700 mb."""
        import plotly.graph_objects as go

        ds = self._make_ds()
        scene = CapErosionScene()
        scene.add_era5_temperature(ds, level_mb=700, valid_time=datetime(1999, 5, 3, 12))
        surf = next(t for t in scene._traces if isinstance(t, go.Surface))

        z_km_expected = pressure_to_km(700.0)
        z_flat = np.array(surf.z).ravel()
        assert np.allclose(z_flat, z_km_expected, atol=0.01)

    def test_era5_temperature_x_is_longitude(self):
        """Surface x values should be the dataset longitude values."""
        import plotly.graph_objects as go

        ds = self._make_ds()
        scene = CapErosionScene()
        scene.add_era5_temperature(ds, level_mb=700, valid_time=datetime(1999, 5, 3, 12))
        surf = next(t for t in scene._traces if isinstance(t, go.Surface))
        assert list(surf.x) == [-103.0, -101.0, -99.0, -97.0, -95.0]

    def test_era5_winds_returns_cone_trace(self):
        import plotly.graph_objects as go

        ds = self._make_ds()
        scene = CapErosionScene()
        scene.add_era5_winds(ds, level_mb=700, valid_time=datetime(1999, 5, 3, 12))
        cones = [t for t in scene._traces if isinstance(t, go.Cone)]
        assert len(cones) == 1

    def test_era5_winds_cone_z_at_correct_height(self):
        import plotly.graph_objects as go

        ds = self._make_ds()
        scene = CapErosionScene()
        scene.add_era5_winds(ds, level_mb=700, valid_time=datetime(1999, 5, 3, 12))
        cone = next(t for t in scene._traces if isinstance(t, go.Cone))
        z_expected = pressure_to_km(700.0)
        assert all(abs(z - z_expected) < 0.01 for z in cone.z)


# ── Scene assembly ────────────────────────────────────────────────────────────

class TestSceneAssembly:

    def test_build_figure_returns_figure(self):
        import plotly.graph_objects as go

        scene = CapErosionScene(title="Test")
        scene.add_base_map(show_counties=False)
        fig = scene.build_figure()
        assert isinstance(fig, go.Figure)

    def test_figure_has_correct_title(self):
        scene = CapErosionScene(title="May 3 1999")
        scene.add_base_map(show_counties=False)
        fig = scene.build_figure()
        assert "May 3 1999" in fig.layout.title.text

    def test_scene_z_axis_has_pressure_labels(self):
        scene = CapErosionScene()
        scene.add_base_map(show_counties=False)
        fig = scene.build_figure()
        tick_texts = fig.layout.scene.zaxis.ticktext
        assert any("mb" in str(t) for t in tick_texts)

    def test_x_axis_covers_oklahoma_longitudes(self):
        """X-axis range should span the Oklahoma domain (approx -103 to -94.5)."""
        scene = CapErosionScene()
        scene.add_base_map(show_counties=False)
        fig = scene.build_figure()
        x_range = fig.layout.scene.xaxis.range
        assert x_range[0] <= -100.0
        assert x_range[1] >= -97.0

    def test_save_writes_html(self, tmp_path):
        scene = CapErosionScene(title="Save test")
        scene.add_base_map(show_counties=False)
        out = tmp_path / "test.html"
        scene.save(str(out))
        assert out.exists()
        content = out.read_text()
        assert "plotly" in content.lower()
