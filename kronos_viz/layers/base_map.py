"""
Geographic floor layer: state and county boundaries drawn as Scatter3d lines
at z=0 so every upper-air feature projects down to its real lat/lon location.
"""

from __future__ import annotations

import numpy as np

from ..geo import get_ok_counties, get_state_boundaries, REFERENCE_CITIES


def base_map_traces(show_counties: bool = True) -> list:
    """
    Return a list of plotly trace dicts for the geographic floor (z=0).

    Includes:
      - State boundary polylines (thick, dark grey)
      - Oklahoma county lines (thin, light grey) — optional
      - Named city / station annotations as small markers
    """
    import plotly.graph_objects as go

    traces = []

    # ── State boundaries ─────────────────────────────────────────────────────
    state_lons, state_lats = get_state_boundaries()
    if state_lons:
        traces.append(go.Scatter3d(
            x=state_lons,
            y=state_lats,
            z=[0.0] * len(state_lons),
            mode="lines",
            line=dict(color="rgba(60,60,60,0.9)", width=2),
            hoverinfo="skip",
            showlegend=False,
            name="State boundaries",
        ))

    # ── County boundaries ────────────────────────────────────────────────────
    if show_counties:
        co_lons, co_lats = get_ok_counties()
        if co_lons:
            traces.append(go.Scatter3d(
                x=co_lons,
                y=co_lats,
                z=[0.0] * len(co_lons),
                mode="lines",
                line=dict(color="rgba(140,140,140,0.4)", width=1),
                hoverinfo="skip",
                showlegend=False,
                name="OK counties",
            ))

    # ── Reference cities / sounding stations ─────────────────────────────────
    city_lons = [v[0] for v in REFERENCE_CITIES.values()]
    city_lats = [v[1] for v in REFERENCE_CITIES.values()]
    city_names = list(REFERENCE_CITIES.keys())

    traces.append(go.Scatter3d(
        x=city_lons,
        y=city_lats,
        z=[0.02] * len(city_lons),  # tiny offset so markers sit above floor
        mode="markers+text",
        marker=dict(size=4, color="black", symbol="diamond"),
        text=city_names,
        textposition="top center",
        textfont=dict(size=9, color="black"),
        hovertext=city_names,
        hoverinfo="text",
        showlegend=False,
        name="Reference cities",
    ))

    return traces
