"""
HRRR-derived severe weather parameter models.

HRRRCountyPoint holds the extracted HRRR analysis fields at one Oklahoma
county centroid.  HRRRCountySnapshot is the full Oklahoma picture at a
single valid time — one point per county.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator

from .enums import OklahomaCounty


class HRRRCountyPoint(BaseModel):
    """
    HRRR analysis fields at one Oklahoma county centroid (nearest grid point).

    Parameters are extracted from the HRRR surface (sfc) product F00 analysis.
    CIN is stored as a positive magnitude (J/kg); HRRR native values are negative.
    """
    model_config = ConfigDict(frozen=False)

    county: OklahomaCounty

    # ── Instability ───────────────────────────────────────────────────────────
    MLCAPE:  float          # J/kg  (90-0 mb above ground parcel)
    MLCIN:   float          # J/kg  (positive magnitude)
    SBCAPE:  float          # J/kg  (surface-based parcel)
    SBCIN:   float          # J/kg  (positive magnitude)

    # ── Wind shear / helicity ─────────────────────────────────────────────────
    SRH_0_1km:  float       # m²/s² (storm-relative helicity 0-1km AGL)
    SRH_0_3km:  float       # m²/s² (storm-relative helicity 0-3km AGL)
    BWD_0_6km:  float       # kt    (bulk wind difference 0-6km AGL)

    # ── Thermodynamic profile ─────────────────────────────────────────────────
    lapse_rate_700_500: Optional[float] = None   # °C/km
    dewpoint_2m_F:      float  = 0.0             # °F (2m AGL)
    LCL_height_m:       Optional[float] = None   # m AGL (from HRRR cloud base)

    # ── Composite parameters (derived) ───────────────────────────────────────
    EHI:  Optional[float] = None   # Energy Helicity Index
    STP:  Optional[float] = None   # Significant Tornado Parameter

    @field_validator("MLCIN", "SBCIN", mode="before")
    @classmethod
    def ensure_positive_cin(cls, v: float) -> float:
        """CIN arrives from HRRR as a negative value; store positive magnitude."""
        return abs(v) if v is not None else 0.0


class HRRRCountySnapshot(BaseModel):
    """
    HRRR analysis snapshot for all Oklahoma county centroids at one valid time.

    ``run_time`` is the model initialization time; ``valid_time`` is the
    analysis time (run_time + fxx hours).  fxx=0 means the F00 analysis,
    which is the closest thing to a model-analysed observation.
    """
    model_config = ConfigDict(frozen=False)

    valid_time: datetime
    run_time:   datetime
    fxx:        int = 0        # forecast hour (0 = analysis)
    counties:   list[HRRRCountyPoint]

    def get(self, county: OklahomaCounty) -> Optional[HRRRCountyPoint]:
        """Return the point for a specific county, or None."""
        for pt in self.counties:
            if pt.county == county:
                return pt
        return None

    def max_field(self, field: str) -> Optional[float]:
        """Return the statewide maximum of a named field."""
        vals = [getattr(pt, field) for pt in self.counties
                if getattr(pt, field, None) is not None]
        return max(vals) if vals else None

    def as_county_dict(self, field: str) -> dict[OklahomaCounty, float]:
        """Return {county: field_value} for every county that has a value."""
        return {
            pt.county: getattr(pt, field)
            for pt in self.counties
            if getattr(pt, field, None) is not None
        }
