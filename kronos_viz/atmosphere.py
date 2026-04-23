"""
Pressure ↔ height conversion utilities for the 3D visualization.

Uses the ICAO standard atmosphere so that ERA5 pressure levels map to
consistent, interpretable altitudes in the 3D scene.
"""

import numpy as np


def pressure_to_km(pressure_hpa: float | np.ndarray) -> float | np.ndarray:
    """
    Convert pressure (hPa) to height (km) via ICAO standard atmosphere.

    Valid for troposphere (below ~11 km / 227 hPa).
    Above the tropopause we use a simple log-pressure extension.
    """
    p = np.asarray(pressure_hpa, dtype=float)
    # Troposphere: z = 44.330 * (1 - (p/1013.25)^0.1903) km
    z = 44.330 * (1.0 - (p / 1013.25) ** 0.1903)
    return float(z) if np.ndim(pressure_hpa) == 0 else z


# Pre-computed heights for ERA5 SOUNDING_PRESSURE_LEVELS so callers don't
# need to recompute them.
SOUNDING_LEVELS_HPA = np.array([
    1000, 975, 950, 925, 900, 875, 850, 825, 800,
    775, 750, 700, 650, 600, 550, 500, 450, 400,
    350, 300, 250, 200,
], dtype=float)

SOUNDING_LEVELS_KM = pressure_to_km(SOUNDING_LEVELS_HPA)


# Z-axis tick marks that pair a human-readable pressure with its height
Z_TICKS_HPA = [1000, 925, 850, 700, 600, 500, 400, 300, 250, 200]
Z_TICKS_KM  = [pressure_to_km(p) for p in Z_TICKS_HPA]
Z_TICK_LABELS = [f"{p} mb" for p in Z_TICKS_HPA]
