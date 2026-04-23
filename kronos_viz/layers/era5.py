"""
ERA5 upper-air field layers.

- era5_temperature_surface: semi-transparent 3D surface mesh of temperature
  at a chosen pressure level, colored by departure from a reference.
- era5_wind_vectors: 3D cone glyphs at ERA5 grid points showing wind direction
  and speed at a chosen level.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import numpy as np

if TYPE_CHECKING:
    import xarray as xr

from ..atmosphere import pressure_to_km


def era5_temperature_surface(
    ds: "xr.Dataset",
    level_mb: float,
    valid_time,
    colorscale: str = "RdBu_r",
    opacity: float = 0.55,
    reference_temp_k: Optional[float] = None,
) -> list:
    """
    Return a Surface trace for ERA5 temperature at `level_mb`.

    The surface is drawn at the standard-atmosphere height of that pressure
    level and colored by temperature (K or anomaly from reference).

    Args:
        ds: ERA5 xr.Dataset with dims (time, level, latitude, longitude).
        level_mb: Pressure level in hPa.
        valid_time: UTC datetime (aware or naive).
        reference_temp_k: If provided, color by (T - ref) instead of T.
            Useful for highlighting the EML warm nose (typically ~3°C warmer
            than the surrounding environment at 700 mb).
        opacity: 0–1 transparency of the surface mesh.
    """
    import plotly.graph_objects as go

    t_naive = valid_time.replace(tzinfo=None) if getattr(valid_time, "tzinfo", None) else valid_time

    T = (
        ds["temperature"]
        .sel(level=level_mb, method="nearest")
        .sel(time=t_naive, method="nearest")
    )

    lats = T.latitude.values
    lons = T.longitude.values
    T_vals = T.values  # (lat, lon)

    if reference_temp_k is not None:
        color_vals = T_vals - reference_temp_k
        colorbar_title = f"ΔT from {reference_temp_k:.0f} K"
        zmid = 0.0
    else:
        color_vals = T_vals - 273.15  # K → °C
        colorbar_title = f"T at {level_mb:.0f} mb (°C)"
        zmid = None

    z_km = pressure_to_km(level_mb)
    z_surface = np.full_like(T_vals, z_km)

    # Surface trace: x=longitude, y=latitude, z=constant height
    trace = go.Surface(
        x=lons,
        y=lats,
        z=z_surface,
        surfacecolor=color_vals,
        colorscale=colorscale,
        cmid=zmid,
        opacity=opacity,
        showscale=True,
        colorbar=dict(
            title=colorbar_title,
            x=1.08,
            len=0.4,
            y=0.7,
            thickness=12,
        ),
        hovertemplate=(
            "Lon: %{x:.2f}°  Lat: %{y:.2f}°<br>"
            f"{colorbar_title}: %{{surfacecolor:.1f}}<extra>{level_mb:.0f} mb</extra>"
        ),
        name=f"ERA5 T {level_mb:.0f} mb",
        showlegend=True,
    )
    return [trace]


def era5_wind_vectors(
    ds: "xr.Dataset",
    level_mb: float,
    valid_time,
    stride: int = 2,
    scale: float = 0.3,
    color: str = "rgba(30,30,180,0.8)",
) -> list:
    """
    Return a Cone trace for ERA5 wind vectors at `level_mb`.

    Cones are placed at every `stride`-th grid point.  The cone direction
    gives wind direction; length gives wind speed.

    Args:
        stride: Grid point skip (1 = every point, 2 = every other point, etc.)
        scale: Arrow length scaling factor.
    """
    import plotly.graph_objects as go

    t_naive = valid_time.replace(tzinfo=None) if getattr(valid_time, "tzinfo", None) else valid_time

    u = (
        ds["u_component_of_wind"]
        .sel(level=level_mb, method="nearest")
        .sel(time=t_naive, method="nearest")
    )
    v = (
        ds["v_component_of_wind"]
        .sel(level=level_mb, method="nearest")
        .sel(time=t_naive, method="nearest")
    )

    lats = u.latitude.values[::stride]
    lons = u.longitude.values[::stride]
    U = u.values[::stride, ::stride]
    V = v.values[::stride, ::stride]

    lon_grid, lat_grid = np.meshgrid(lons, lats)

    z_km = pressure_to_km(level_mb)
    wspd = np.sqrt(U**2 + V**2)
    hover = [
        f"Lon: {lon:.1f}°  Lat: {lat:.1f}°<br>"
        f"Wind: {spd:.1f} m/s at {level_mb:.0f} mb"
        for lon, lat, spd in zip(
            lon_grid.ravel(), lat_grid.ravel(), wspd.ravel()
        )
    ]

    trace = go.Cone(
        x=lon_grid.ravel().tolist(),
        y=lat_grid.ravel().tolist(),
        z=[z_km] * lon_grid.size,
        u=U.ravel().tolist(),
        v=V.ravel().tolist(),
        w=[0.0] * lon_grid.size,
        sizemode="scaled",
        sizeref=scale,
        colorscale=[[0, "lightblue"], [1, "navy"]],
        showscale=False,
        anchor="tail",
        hovertext=hover,
        hoverinfo="text",
        name=f"Wind {level_mb:.0f} mb",
        showlegend=True,
    )
    return [trace]
