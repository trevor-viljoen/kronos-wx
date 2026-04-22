"""
Cap erosion specific models — the thermodynamic core of the prediction system.

The cap (convective inhibition layer / elevated mixed layer) is the primary
gating mechanism separating explosive tornado outbreaks from busts.
These models track the cap's state, budget, and trajectory toward erosion.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator

from .enums import OklahomaSoundingStation, OklahomaCounty, ErosionMechanism


class EMLCharacteristics(BaseModel):
    """
    Elevated Mixed Layer (EML) — the warm, well-mixed air mass originating
    over the Mexican Plateau / high terrain that advects eastward over Oklahoma,
    forming the cap's upper boundary.
    """
    model_config = ConfigDict(frozen=False)

    valid_time: datetime
    source_station: OklahomaSoundingStation

    base_pressure: float    # mb — pressure at EML base (bottom of warm layer)
    top_pressure: float     # mb — pressure at EML top
    depth_mb: float         # mb — derived: base - top

    # How warm the EML is relative to a dry adiabat anchored at the surface
    peak_temp_excess: float  # °C — warmest point relative to dry adiabat

    base_height_agl: float   # meters AGL at EML base

    advection_rate: Optional[float] = None  # m/s, estimated eastward movement
    source_region: Optional[str] = None     # e.g., "Mexican Plateau", "TX Panhandle"

    @field_validator("base_pressure", "top_pressure")
    @classmethod
    def pressure_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"Pressure must be > 0, got {v}")
        return v

    @field_validator("depth_mb")
    @classmethod
    def depth_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"EML depth must be > 0 mb, got {v}")
        return v


class CapErosionBudget(BaseModel):
    """
    Instantaneous cap erosion budget at a given time and location.

    Each forcing term represents the rate of change of CIN (J/kg per hour).
    Negative = eroding the cap. Positive = rebuilding/maintaining the cap.

    The sum of all terms gives net_tendency, which drives the trajectory model.
    """
    model_config = ConfigDict(frozen=False)

    valid_time: datetime
    county: OklahomaCounty
    current_CIN: float       # J/kg, positive magnitude

    # ── Erosion forcing terms (negative values = reducing CIN) ───────────────
    heating_forcing: float       # Surface heating → parcel warming → CIN reduction
    dynamic_forcing: float       # Synoptic-scale lift / QG forcing
    boundary_forcing: float      # Mesoscale boundary convergence
    lapse_rate_forcing: float    # Steepening lapse rates aloft (EML erosion)

    # ── Preservation terms (positive values = maintaining/rebuilding CIN) ────
    subsidence_forcing: float    # Synoptic-scale descent / high pressure
    EML_advection_forcing: float # New EML air advecting in from west
    cold_pool_forcing: float     # Cold pool stabilization from nearby convection

    # ── Net budget ────────────────────────────────────────────────────────────
    net_tendency: float          # J/kg per hour; negative = net erosion

    # ── Trajectory forecast ───────────────────────────────────────────────────
    projected_erosion_time: Optional[datetime] = None
    forcing_window_close: Optional[datetime] = None  # when synoptic forcing exits
    will_erode_in_window: Optional[bool] = None       # key binary forecast output

    confidence: float = 0.5

    @field_validator("current_CIN")
    @classmethod
    def cin_nonneg(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"current_CIN must be >= 0 (positive magnitude), got {v}")
        return v

    @field_validator("confidence")
    @classmethod
    def confidence_range(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"confidence must be 0.0–1.0, got {v}")
        return v

    @property
    def total_erosion_forcing(self) -> float:
        """Sum of all erosion terms (should be <= 0)."""
        return self.heating_forcing + self.dynamic_forcing + self.boundary_forcing + self.lapse_rate_forcing

    @property
    def total_preservation_forcing(self) -> float:
        """Sum of all preservation terms (should be >= 0)."""
        return self.subsidence_forcing + self.EML_advection_forcing + self.cold_pool_forcing

    @property
    def hours_to_erosion(self) -> Optional[float]:
        """Naive linear estimate of hours until CIN reaches zero, given net_tendency."""
        if self.net_tendency >= 0 or self.current_CIN == 0:
            return None
        return self.current_CIN / abs(self.net_tendency)


class CapErosionTrajectory(BaseModel):
    """
    Time-integrated cap erosion trajectory for a single county on a case day.
    Built from a sequence of CapErosionBudget snapshots.
    """
    model_config = ConfigDict(frozen=False)

    county: OklahomaCounty
    analysis_time: datetime
    budget_history: list[CapErosionBudget]

    erosion_achieved: bool
    erosion_time: Optional[datetime] = None
    erosion_location_description: Optional[str] = None
    primary_mechanism: ErosionMechanism = ErosionMechanism.UNKNOWN

    # 0.0 = confident erosion; 1.0 = confident bust (cap holds through window)
    bust_risk_score: float = 0.5

    @field_validator("bust_risk_score")
    @classmethod
    def bust_risk_range(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"bust_risk_score must be 0.0–1.0, got {v}")
        return v

    @property
    def latest_budget(self) -> Optional[CapErosionBudget]:
        return self.budget_history[-1] if self.budget_history else None

    @property
    def cin_trend(self) -> Optional[float]:
        """J/kg per hour — negative means overall erosion across history."""
        if len(self.budget_history) < 2:
            return None
        first = self.budget_history[0]
        last = self.budget_history[-1]
        dt = (last.valid_time - first.valid_time).total_seconds() / 3600.0
        if dt == 0:
            return None
        return (last.current_CIN - first.current_CIN) / dt
