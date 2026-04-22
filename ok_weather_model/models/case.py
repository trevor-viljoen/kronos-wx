"""
Master historical case model — the top-level record in the case library.

Each HistoricalCase represents a single convective day over Oklahoma,
classified by event outcome and enriched incrementally from multiple
data sources.  The design supports partial enrichment: a case can be
saved with only SPC tornado counts, then later enriched with sounding
data, then Mesonet surface analysis, then cap erosion trajectory.
"""

from datetime import date, datetime, time
from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from .enums import (
    EventClass,
    StormMode,
    CapBehavior,
    ErosionMechanism,
    JetPosition,
    TornadoRating,
    ForecastVerification,
    OklahomaCounty,
)
from .sounding import ThermodynamicIndices
from .kinematic import KinematicProfile
from .cap import EMLCharacteristics
from .boundary import BoundaryObservation


class HistoricalCase(BaseModel):
    """
    Complete record for a single convective day over Oklahoma.

    Populated incrementally:
      1. Skeleton from SPC tornado CSV (case_id, date, event_class, tornado_count)
      2. Sounding enrichment (sounding_12Z, kinematics_12Z, EML_12Z)
      3. Mesonet enrichment (convective_temp_gap series, boundaries)
      4. Cap analysis (cap_erosion_time, cap_behavior, cap_erosion_mechanism)
      5. Manual annotation (primary_bust_mechanism, research_references)
    """
    model_config = ConfigDict(frozen=False)

    # ── Identity ──────────────────────────────────────────────────────────────
    case_id: str    # Format: "YYYYMMDD_OK"
    date: date
    event_class: EventClass
    storm_mode: Optional[StormMode] = None
    cap_behavior: Optional[CapBehavior] = None

    # ── Synoptic scale ────────────────────────────────────────────────────────
    upper_trough_longitude: Optional[float] = None   # °W (negative)
    jet_streak_max_knots: Optional[float] = None
    jet_position: Optional[JetPosition] = None
    surface_low_lat: Optional[float] = None
    surface_low_lon: Optional[float] = None
    LLJ_strength_12Z: Optional[float] = None         # knots

    # ── Morning thermodynamics (12Z OUN unless noted) ─────────────────────────
    sounding_12Z: Optional[ThermodynamicIndices] = None
    kinematics_12Z: Optional[KinematicProfile] = None
    EML_12Z: Optional[EMLCharacteristics] = None

    # ── Cap evolution ─────────────────────────────────────────────────────────
    convective_temp_gap_12Z: Optional[float] = None  # °F — Tc minus 12Z surface temp
    convective_temp_gap_15Z: Optional[float] = None  # °F
    convective_temp_gap_18Z: Optional[float] = None  # °F
    cap_erosion_time: Optional[time] = None           # UTC
    cap_erosion_county: Optional[OklahomaCounty] = None
    cap_erosion_mechanism: Optional[ErosionMechanism] = None

    # ── Mesoscale boundaries ──────────────────────────────────────────────────
    dryline_longitude_18Z: Optional[float] = None    # °W (negative)
    dryline_surge_rate_mph: Optional[float] = None
    outflow_boundaries_present: bool = False
    boundary_interaction: bool = False
    boundary_interaction_time: Optional[time] = None
    boundaries: list[BoundaryObservation] = []

    # ── Convective initiation ─────────────────────────────────────────────────
    first_initiation_time: Optional[time] = None
    first_initiation_county: Optional[OklahomaCounty] = None
    initiation_mechanism: Optional[ErosionMechanism] = None
    open_warm_sector_initiation: Optional[bool] = None

    # ── Outcome ───────────────────────────────────────────────────────────────
    tornado_count: int = 0
    max_tornado_rating: Optional[TornadoRating] = None
    max_path_length_miles: Optional[float] = None
    max_hail_size_inches: Optional[float] = None
    max_wind_gust_knots: Optional[float] = None
    significant_severe: bool = False
    county_tornado_counts: dict[str, int] = {}  # OklahomaCounty.name → count

    # ── Forecast verification ─────────────────────────────────────────────────
    SPC_max_tornado_prob: Optional[float] = None     # 0.0–1.0
    SPC_risk_category: Optional[str] = None          # e.g. "MODERATE", "HIGH"
    forecast_verification: Optional[ForecastVerification] = None
    primary_bust_mechanism: Optional[str] = None

    # ── Metadata / data availability ─────────────────────────────────────────
    data_completeness_score: float = 0.0
    mesonet_data_available: bool = False
    sounding_data_available: bool = False
    radar_data_available: bool = False
    research_references: list[str] = []
    notes: Optional[str] = None

    @field_validator("case_id")
    @classmethod
    def valid_case_id(cls, v: str) -> str:
        import re
        if not re.fullmatch(r"\d{8}_OK", v):
            raise ValueError(f"case_id must match YYYYMMDD_OK format, got '{v}'")
        return v

    @field_validator("tornado_count")
    @classmethod
    def tornado_count_nonneg(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"tornado_count must be >= 0, got {v}")
        return v

    @field_validator("data_completeness_score")
    @classmethod
    def completeness_range(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"data_completeness_score must be 0.0–1.0, got {v}")
        return v

    @field_validator("SPC_max_tornado_prob")
    @classmethod
    def spc_prob_range(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (0.0 <= v <= 1.0):
            raise ValueError(f"SPC_max_tornado_prob must be 0.0–1.0, got {v}")
        return v

    @model_validator(mode="after")
    def compute_completeness_score(self) -> "HistoricalCase":
        """
        Automatically update data_completeness_score based on available fields.
        This is called on creation; callers should re-invoke after enrichment.
        """
        checks = [
            self.sounding_12Z is not None,
            self.kinematics_12Z is not None,
            self.EML_12Z is not None,
            self.mesonet_data_available,
            self.sounding_data_available,
            self.convective_temp_gap_12Z is not None,
            self.convective_temp_gap_15Z is not None,
            self.convective_temp_gap_18Z is not None,
            self.cap_erosion_time is not None,
            self.cap_behavior is not None,
        ]
        self.data_completeness_score = sum(checks) / len(checks)
        return self

    def recompute_completeness(self) -> float:
        """Re-run completeness calculation after incremental enrichment."""
        checks = [
            self.sounding_12Z is not None,
            self.kinematics_12Z is not None,
            self.EML_12Z is not None,
            self.mesonet_data_available,
            self.sounding_data_available,
            self.convective_temp_gap_12Z is not None,
            self.convective_temp_gap_15Z is not None,
            self.convective_temp_gap_18Z is not None,
            self.cap_erosion_time is not None,
            self.cap_behavior is not None,
        ]
        self.data_completeness_score = sum(checks) / len(checks)
        return self.data_completeness_score

    @classmethod
    def make_case_id(cls, case_date: date) -> str:
        return f"{case_date.strftime('%Y%m%d')}_OK"
