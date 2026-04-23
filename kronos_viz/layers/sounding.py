"""
Sounding profile layer.

Renders a SoundingProfile as two vertical curtains at the station's real
lat/lon: temperature (red) and dewpoint (green).  The gap between them
shows the spread — when they converge, the atmosphere is saturated.

An optional "cap highlight" marks the layer between LCL and LFC in orange.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ok_weather_model.models import SoundingProfile

from ..atmosphere import pressure_to_km
from ..geo import REFERENCE_CITIES


def sounding_curtain_traces(
    profile: "SoundingProfile",
    lon: Optional[float] = None,
    lat: Optional[float] = None,
    label: Optional[str] = None,
) -> list:
    """
    Return Scatter3d traces for a sounding T / Td vertical profile.

    The sounding is drawn at the station's real lat/lon (or at the supplied
    lon/lat override for virtual soundings at arbitrary grid points).

    Temperature is offset slightly eastward (+0.15°) and dewpoint westward
    (-0.15°) so they don't overlap when viewed from above.

    Args:
        profile: SoundingProfile with .levels list.
        lon, lat: Override position (default: station coordinates from REFERENCE_CITIES).
        label: Legend label prefix (default: station name).
    """
    import plotly.graph_objects as go

    # Resolve position
    station_name = profile.station.value if hasattr(profile.station, "value") else str(profile.station)
    if lon is None or lat is None:
        # Try to find station position from geo reference
        for city, (clon, clat) in REFERENCE_CITIES.items():
            if station_name.upper() in city.upper():
                lon, lat = clon, clat
                break
        if lon is None:
            # Fall back to domain center
            lon, lat = -98.5, 35.5

    label = label or station_name
    levels = sorted(profile.levels, key=lambda lv: lv.pressure, reverse=True)  # surface first

    pressures = [lv.pressure for lv in levels]
    heights_km = [pressure_to_km(p) for p in pressures]
    temps = [lv.temperature for lv in levels]
    dews  = [lv.dewpoint for lv in levels]

    # Temperature scaling: map °C to a small longitude offset so the profile
    # reads like a traditional skew-T but is placed in real lon/lat space.
    # Scale: 1°C = 0.01° longitude offset (±2° for ±20°C range)
    t_lons = [lon + t * 0.01 for t in temps]
    td_lons = [lon + td * 0.01 for td in dews]

    traces = []

    # ── Temperature profile ───────────────────────────────────────────────────
    hover_t = [
        f"<b>{label} — T</b><br>{p:.0f} mb / {h:.2f} km<br>T: {t:.1f}°C"
        for p, h, t in zip(pressures, heights_km, temps)
    ]
    traces.append(go.Scatter3d(
        x=t_lons,
        y=[lat] * len(levels),
        z=heights_km,
        mode="lines+markers",
        line=dict(color="firebrick", width=3),
        marker=dict(size=3, color="firebrick"),
        text=hover_t,
        hoverinfo="text",
        name=f"{label} T",
        legendgroup=label,
        showlegend=True,
    ))

    # ── Dewpoint profile ──────────────────────────────────────────────────────
    hover_td = [
        f"<b>{label} — Td</b><br>{p:.0f} mb / {h:.2f} km<br>Td: {d:.1f}°C"
        for p, h, d in zip(pressures, heights_km, dews)
    ]
    traces.append(go.Scatter3d(
        x=td_lons,
        y=[lat] * len(levels),
        z=heights_km,
        mode="lines+markers",
        line=dict(color="seagreen", width=3),
        marker=dict(size=3, color="seagreen"),
        text=hover_td,
        hoverinfo="text",
        name=f"{label} Td",
        legendgroup=label,
        showlegend=True,
    ))

    # ── Station anchor ────────────────────────────────────────────────────────
    traces.append(go.Scatter3d(
        x=[lon],
        y=[lat],
        z=[0.0],
        mode="markers+text",
        marker=dict(size=8, color="black", symbol="circle"),
        text=[label],
        textposition="top center",
        textfont=dict(size=10, color="black"),
        hoverinfo="text",
        hovertext=[f"<b>{label}</b> sounding"],
        name=f"{label} station",
        legendgroup=label,
        showlegend=False,
    ))

    return traces
