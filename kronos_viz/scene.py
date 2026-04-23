"""
CapErosionScene — the top-level 3D visualization object.

Assembles all layers into a single Plotly figure anchored to real Oklahoma
lat/lon coordinates.  Every feature — surface observations, ERA5 pressure
surfaces, sounding profiles, boundary curtains — is placed at its actual
geographic location so spatial relationships are immediately readable.

Coordinate system
-----------------
  x = longitude (degrees E, negative for Oklahoma domain)
  y = latitude  (degrees N)
  z = height    (km above MSL, derived from pressure via ICAO standard
                 atmosphere so pressure-level labels are always shown)

Usage
-----
    from kronos_viz import CapErosionScene

    scene = CapErosionScene(title="May 3 1999 — 12Z Cap Analysis")
    scene.add_base_map()
    scene.add_era5_temperature(ds, level_mb=700, valid_time=t)
    scene.add_era5_winds(ds, level_mb=850, valid_time=t)
    scene.add_sounding(profile, lon=-97.44, lat=35.22)
    scene.add_boundary(dryline)
    scene.show()          # opens browser
    scene.save("cap_700mb.html")  # self-contained HTML
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional, Sequence

if TYPE_CHECKING:
    import xarray as xr
    from ok_weather_model.models import SoundingProfile
    from ok_weather_model.models.boundary import BoundaryObservation

from .atmosphere import Z_TICKS_KM, Z_TICK_LABELS
from .geo import DOMAIN_LAT_MAX, DOMAIN_LAT_MIN, DOMAIN_LON_MAX, DOMAIN_LON_MIN
from .layers import (
    base_map_traces,
    boundary_curtain_trace,
    era5_temperature_surface,
    era5_wind_vectors,
    mesonet_surface_trace,
    sounding_curtain_traces,
)


class CapErosionScene:
    """
    3D atmospheric visualization anchored to Oklahoma lat/lon geography.

    Layers are accumulated with add_* methods and rendered to a Plotly figure
    with show() or save().
    """

    def __init__(self, title: str = "Cap Erosion — 3D Scene"):
        self._title = title
        self._traces: list = []

    # ── Layer adders ──────────────────────────────────────────────────────────

    def add_base_map(self, show_counties: bool = True) -> "CapErosionScene":
        """Add state/county boundary floor and reference city markers."""
        self._traces.extend(base_map_traces(show_counties=show_counties))
        return self

    def add_mesonet(
        self,
        station_lons: Sequence[float],
        station_lats: Sequence[float],
        temperatures_c: Sequence[float],
        dewpoints_c: Sequence[float],
        wind_dirs: Sequence[float],
        wind_speeds_kt: Sequence[float],
        station_names: Sequence[str],
    ) -> "CapErosionScene":
        """Add Mesonet surface temperature markers and dewpoint columns."""
        self._traces.extend(mesonet_surface_trace(
            station_lons, station_lats, temperatures_c, dewpoints_c,
            wind_dirs, wind_speeds_kt, station_names,
        ))
        return self

    def add_era5_temperature(
        self,
        ds: "xr.Dataset",
        level_mb: float,
        valid_time: datetime,
        colorscale: str = "RdBu_r",
        opacity: float = 0.55,
        reference_temp_k: Optional[float] = None,
    ) -> "CapErosionScene":
        """
        Add a semi-transparent ERA5 temperature surface at `level_mb`.

        Pass `reference_temp_k` to color by anomaly — useful for visualizing
        the EML warm nose at 700 mb (warm anomaly = stronger cap).
        """
        self._traces.extend(era5_temperature_surface(
            ds, level_mb, valid_time, colorscale, opacity, reference_temp_k
        ))
        return self

    def add_era5_winds(
        self,
        ds: "xr.Dataset",
        level_mb: float,
        valid_time: datetime,
        stride: int = 2,
    ) -> "CapErosionScene":
        """Add ERA5 wind vector cones at `level_mb`."""
        self._traces.extend(era5_wind_vectors(ds, level_mb, valid_time, stride=stride))
        return self

    def add_sounding(
        self,
        profile: "SoundingProfile",
        lon: Optional[float] = None,
        lat: Optional[float] = None,
        label: Optional[str] = None,
    ) -> "CapErosionScene":
        """
        Add a sounding profile as T (red) and Td (green) vertical curtains.

        Supply lon/lat for virtual soundings at arbitrary grid points; omit
        to use the station's known coordinates.
        """
        self._traces.extend(sounding_curtain_traces(profile, lon=lon, lat=lat, label=label))
        return self

    def add_boundary(self, boundary: "BoundaryObservation") -> "CapErosionScene":
        """Add a boundary observation (dryline, front, etc.) as a vertical curtain."""
        self._traces.extend(boundary_curtain_trace(boundary))
        return self

    # ── Rendering ─────────────────────────────────────────────────────────────

    def build_figure(self):
        """Assemble and return a plotly.graph_objects.Figure."""
        import plotly.graph_objects as go

        fig = go.Figure(data=self._traces)

        fig.update_layout(
            title=dict(text=self._title, font=dict(size=16)),
            scene=dict(
                xaxis=dict(
                    title="Longitude (°E)",
                    range=[DOMAIN_LON_MIN, DOMAIN_LON_MAX],
                    backgroundcolor="rgba(230,230,255,0.3)",
                    gridcolor="white",
                    showbackground=True,
                ),
                yaxis=dict(
                    title="Latitude (°N)",
                    range=[DOMAIN_LAT_MIN, DOMAIN_LAT_MAX],
                    backgroundcolor="rgba(230,255,230,0.3)",
                    gridcolor="white",
                    showbackground=True,
                ),
                zaxis=dict(
                    title="Height (km / mb)",
                    range=[0, 14],
                    tickvals=Z_TICKS_KM,
                    ticktext=Z_TICK_LABELS,
                    backgroundcolor="rgba(200,220,255,0.2)",
                    gridcolor="rgba(100,100,200,0.3)",
                    showbackground=True,
                ),
                # Default camera: slightly elevated, looking SSW over Oklahoma
                camera=dict(
                    eye=dict(x=0.8, y=-1.8, z=0.9),
                    center=dict(x=0, y=0, z=-0.2),
                ),
                aspectratio=dict(x=2.0, y=1.0, z=0.6),
                bgcolor="rgba(245,248,255,1)",
            ),
            legend=dict(
                x=0.01,
                y=0.99,
                bgcolor="rgba(255,255,255,0.85)",
                bordercolor="grey",
                borderwidth=1,
            ),
            margin=dict(l=0, r=0, t=40, b=0),
            paper_bgcolor="white",
        )

        return fig

    def show(self) -> None:
        """Render the scene in the default browser."""
        self.build_figure().show()

    def save(self, path: str) -> None:
        """
        Save the scene as a self-contained HTML file.

        The file includes all Plotly JavaScript inline — no internet connection
        required to view it.
        """
        self.build_figure().write_html(path, include_plotlyjs="cdn")
        print(f"Scene saved → {path}")
