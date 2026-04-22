"""
Mesoscale boundary models.

Boundaries — drylines, outflow boundaries, frontal zones, differential heating
boundaries — are the primary initiation triggers when the cap is eroding.
Tracking boundary position, motion, and interactions is critical to resolving
the bust/outbreak problem.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from .enums import OklahomaCounty, BoundaryType


class BoundaryObservation(BaseModel):
    """
    A single observed or analyzed mesoscale boundary at a point in time.
    Position is defined as a polyline (list of lat/lon points).
    """
    model_config = ConfigDict(frozen=False)

    valid_time: datetime
    boundary_type: BoundaryType

    # Boundary polyline — parallel lists of lat/lon points
    position_lat: list[float]    # °N
    position_lon: list[float]    # °W (negative values)

    counties_intersected: list[OklahomaCounty]

    motion_speed: Optional[float] = None      # mph
    motion_direction: Optional[float] = None  # degrees (meteorological, direction moving toward)

    confidence: float = 0.5
    detected_by: str = "manual"  # "mesonet_windshift" | "radar" | "satellite" | "manual"

    @model_validator(mode="after")
    def lat_lon_length_match(self) -> "BoundaryObservation":
        if len(self.position_lat) != len(self.position_lon):
            raise ValueError(
                f"position_lat ({len(self.position_lat)} pts) and "
                f"position_lon ({len(self.position_lon)} pts) must have equal length"
            )
        if len(self.position_lat) < 2:
            raise ValueError("Boundary must have at least 2 points to define a line")
        return self

    @field_validator("confidence")
    @classmethod
    def confidence_range(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"confidence must be 0.0–1.0, got {v}")
        return v

    @field_validator("motion_direction")
    @classmethod
    def motion_dir_range(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (0.0 <= v <= 360.0):
            raise ValueError(f"motion_direction must be 0–360, got {v}")
        return v

    @field_validator("detected_by")
    @classmethod
    def valid_detector(cls, v: str) -> str:
        allowed = {"mesonet_windshift", "radar", "satellite", "manual"}
        if v not in allowed:
            raise ValueError(f"detected_by must be one of {allowed}, got '{v}'")
        return v


class BoundaryInteraction(BaseModel):
    """
    The intersection or convergence of two mesoscale boundaries.

    These interactions are meteorological alarm bells — when two boundaries
    interact in a thermodynamically favorable environment, initiation risk
    escalates dramatically.  The alarm_bell_flag encodes this.
    """
    model_config = ConfigDict(frozen=False)

    valid_time: datetime
    boundary_1: BoundaryObservation
    boundary_2: BoundaryObservation

    interaction_point_lat: float
    interaction_point_lon: float
    interaction_county: OklahomaCounty

    convergence_magnitude: Optional[float] = None  # m/s or qualitative

    # True when two boundaries interact in a thermodynamically favorable environment.
    # Set to True by the analysis layer when:
    #   - MLCAPE > 1000 J/kg in the interaction county
    #   - MLCIN < 100 J/kg or erosion is underway
    #   - Interaction is within the synoptic forcing window
    alarm_bell_flag: bool = False

    notes: Optional[str] = None

    @field_validator("interaction_point_lat")
    @classmethod
    def lat_range(cls, v: float) -> float:
        if not (33.0 <= v <= 37.5):
            raise ValueError(f"Oklahoma latitude must be ~33–37.5°N, got {v}")
        return v

    @field_validator("interaction_point_lon")
    @classmethod
    def lon_range(cls, v: float) -> float:
        if not (-103.5 <= v <= -94.0):
            raise ValueError(f"Oklahoma longitude must be ~-103.5 to -94°W, got {v}")
        return v
