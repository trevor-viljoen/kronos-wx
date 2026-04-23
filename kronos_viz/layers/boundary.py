"""
Boundary observation layer.

Renders a BoundaryObservation (dryline, outflow boundary, cold front, etc.)
as a semi-transparent vertical "curtain" that rises from the surface to the
top of the boundary layer (~850 mb / ~1.5 km).  The curtain is colored by
boundary type so different features are visually distinct.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from ok_weather_model.models.boundary import BoundaryObservation

from ..atmosphere import pressure_to_km

# Height to which boundaries are extruded (top of the boundary layer)
_BOUNDARY_TOP_KM = pressure_to_km(850.0)  # ~1.46 km

_TYPE_COLORS = {
    "dryline":          "rgba(180,120,0,0.55)",
    "outflow_boundary": "rgba(0,80,200,0.55)",
    "cold_front":       "rgba(0,0,200,0.70)",
    "warm_front":       "rgba(200,0,0,0.55)",
    "squall_line":      "rgba(120,0,180,0.65)",
}
_DEFAULT_COLOR = "rgba(80,80,80,0.5)"


def boundary_curtain_trace(boundary: "BoundaryObservation") -> list:
    """
    Return Mesh3d + Scatter3d traces for a boundary.

    The boundary polyline is extruded vertically from z=0 to
    `_BOUNDARY_TOP_KM`, forming a curtain.  The top and bottom edges are also
    drawn as Scatter3d lines so the boundary is visible from any viewing angle.

    Args:
        boundary: BoundaryObservation with position_lat / position_lon arrays.
    """
    import plotly.graph_objects as go

    btype = getattr(boundary, "boundary_type", "unknown")
    if hasattr(btype, "value"):
        btype = btype.value
    color = _TYPE_COLORS.get(str(btype).lower(), _DEFAULT_COLOR)
    label = str(btype).replace("_", " ").title()

    lats = list(boundary.position_lat)
    lons = list(boundary.position_lon)
    n = len(lats)

    if n < 2:
        return []

    top_km = _BOUNDARY_TOP_KM
    traces = []

    # ── Curtain mesh ─────────────────────────────────────────────────────────
    # Vertices: bottom ring (z=0) then top ring (z=top_km)
    x_verts = lons + lons
    y_verts = lats + lats
    z_verts = [0.0] * n + [top_km] * n

    # Quad faces as triangles: (i, i+1, i+n) and (i+1, i+n+1, i+n)
    i_idx, j_idx, k_idx = [], [], []
    for seg in range(n - 1):
        b0, b1, t0, t1 = seg, seg + 1, seg + n, seg + n + 1
        i_idx += [b0, b1]
        j_idx += [b1, t1]
        k_idx += [t0, t0]

    traces.append(go.Mesh3d(
        x=x_verts,
        y=y_verts,
        z=z_verts,
        i=i_idx,
        j=j_idx,
        k=k_idx,
        color=color,
        opacity=0.45,
        hoverinfo="skip",
        name=f"{label} curtain",
        showlegend=True,
    ))

    # ── Surface polyline (bottom edge) ───────────────────────────────────────
    traces.append(go.Scatter3d(
        x=lons,
        y=lats,
        z=[0.02] * n,
        mode="lines",
        line=dict(color=color.replace("0.55", "1.0").replace("0.45", "1.0"), width=4),
        hoverinfo="skip",
        name=f"{label} surface",
        legendgroup=label,
        showlegend=False,
    ))

    # ── Top edge ─────────────────────────────────────────────────────────────
    traces.append(go.Scatter3d(
        x=lons,
        y=lats,
        z=[top_km] * n,
        mode="lines",
        line=dict(color=color.replace("0.55", "0.8"), width=2),
        hoverinfo="skip",
        name=f"{label} top",
        legendgroup=label,
        showlegend=False,
    ))

    return traces
