"""
Wind profile and kinematic shear models.
"""

import math
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from .enums import OklahomaSoundingStation, HodographShape


class WindLevel(BaseModel):
    model_config = ConfigDict(frozen=False)

    pressure: float      # mb
    height: float        # meters AGL
    u_component: float   # m/s (positive = westerly)
    v_component: float   # m/s (positive = southerly)

    # Derived — computed from u/v if not provided
    wind_speed: Optional[float] = None      # knots
    wind_direction: Optional[float] = None  # degrees meteorological (0–360)

    @model_validator(mode="after")
    def derive_speed_and_direction(self) -> "WindLevel":
        """Compute speed and direction from u/v components if not already set."""
        if self.wind_speed is None:
            speed_ms = math.sqrt(self.u_component**2 + self.v_component**2)
            self.wind_speed = speed_ms * 1.94384  # m/s → knots

        if self.wind_direction is None:
            # Meteorological convention: direction wind is coming FROM
            direction = math.degrees(math.atan2(-self.u_component, -self.v_component)) % 360
            self.wind_direction = direction

        return self


class KinematicProfile(BaseModel):
    model_config = ConfigDict(frozen=False)

    valid_time: datetime
    station: OklahomaSoundingStation
    levels: list[WindLevel]

    # Storm-relative helicity
    SRH_0_1km: float              # m²/s²
    SRH_0_3km: float              # m²/s²
    SRH_effective: Optional[float] = None  # m²/s²

    # Bulk wind difference
    BWD_0_1km: float              # knots
    BWD_0_6km: float              # knots
    BWD_effective: Optional[float] = None  # knots

    # Low-level jet (850 mb)
    LLJ_speed: Optional[float] = None     # knots
    LLJ_direction: Optional[float] = None # degrees

    mean_wind_0_6km: float        # knots

    hodograph_shape: HodographShape

    # Storm motion — Bunkers right and left mover as (u, v) in m/s
    storm_motion_bunkers_right: Optional[tuple[float, float]] = None
    storm_motion_bunkers_left: Optional[tuple[float, float]] = None

    # Composite parameters
    EHI: Optional[float] = None   # Energy Helicity Index
    STP: Optional[float] = None   # Significant Tornado Parameter
    SCP: Optional[float] = None   # Supercell Composite Parameter

    @field_validator("LLJ_direction")
    @classmethod
    def llj_dir_range(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (0.0 <= v <= 360.0):
            raise ValueError(f"LLJ_direction must be 0–360, got {v}")
        return v
