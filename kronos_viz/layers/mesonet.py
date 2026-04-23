"""
Mesonet surface observation layer.

Renders each Mesonet station as a vertical column at its real lat/lon.
Column height is proportional to surface dewpoint (the primary convective
trigger variable).  Station marker is colored by 2m temperature.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

import numpy as np

if TYPE_CHECKING:
    pass


def mesonet_surface_trace(
    station_lons: Sequence[float],
    station_lats: Sequence[float],
    temperatures_c: Sequence[float],
    dewpoints_c: Sequence[float],
    wind_dirs: Sequence[float],
    wind_speeds_kt: Sequence[float],
    station_names: Sequence[str],
    column_scale: float = 0.5,
) -> list:
    """
    Build Scatter3d traces for Mesonet surface observations.

    Each station appears as:
      - A sphere at z=0 colored by 2m temperature
      - A vertical stem up to z = dewpoint/column_scale km (so wetter =
        taller column, making moisture visible from any angle)

    Args:
        column_scale: km per °C of dewpoint (default 0.5 → Td=25°C → 12.5 km tall,
                      which stands out clearly in the 3D scene).
        Coordinates: real lat/lon (WGS-84 degrees).
    """
    import plotly.graph_objects as go

    lons = list(station_lons)
    lats = list(station_lats)
    temps = list(temperatures_c)
    dews = list(dewpoints_c)
    wdirs = list(wind_dirs)
    wspds = list(wind_speeds_kt)
    names = list(station_names)

    traces = []

    # ── Surface markers colored by temperature ───────────────────────────────
    hover = [
        f"<b>{n}</b><br>T: {t:.1f}°C  Td: {d:.1f}°C<br>"
        f"Wind: {ws:.0f} kt from {wd:.0f}°"
        for n, t, d, ws, wd in zip(names, temps, dews, wspds, wdirs)
    ]

    traces.append(go.Scatter3d(
        x=lons,
        y=lats,
        z=[0.05] * len(lons),
        mode="markers",
        marker=dict(
            size=6,
            color=temps,
            colorscale="RdYlBu_r",
            cmin=10,
            cmax=40,
            colorbar=dict(
                title="2m T (°C)",
                x=1.02,
                len=0.4,
                y=0.2,
                thickness=12,
            ),
            line=dict(width=0.5, color="grey"),
        ),
        text=hover,
        hoverinfo="text",
        name="Mesonet T",
        showlegend=True,
    ))

    # ── Dewpoint moisture columns ────────────────────────────────────────────
    # Each column is a pair of points (base at z=0, top at z=Td/scale) with
    # NaN separator so all columns share one trace (fast rendering).
    col_x: list[float] = []
    col_y: list[float] = []
    col_z: list[float] = []
    col_color: list[float] = []

    for lon, lat, td in zip(lons, lats, dews):
        top_z = max(0.0, td * column_scale / 10.0)  # 10°C Td → 0.5 km
        col_x += [lon, lon, float("nan")]
        col_y += [lat, lat, float("nan")]
        col_z += [0.0, top_z, float("nan")]
        col_color += [td, td, float("nan")]

    traces.append(go.Scatter3d(
        x=col_x,
        y=col_y,
        z=col_z,
        mode="lines",
        line=dict(
            color=col_color,
            colorscale="YlGnBu",
            cmin=0,
            cmax=25,
            width=3,
        ),
        hoverinfo="skip",
        name="Dewpoint columns",
        showlegend=True,
    ))

    return traces
