"""
Thermodynamic profile models for rawinsonde and virtual sounding data.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from .enums import OklahomaSoundingStation, OklahomaCounty, ErosionMechanism


class SoundingLevel(BaseModel):
    model_config = ConfigDict(frozen=False)

    pressure: float          # mb
    height: float            # meters AGL
    temperature: float       # °C
    dewpoint: float          # °C
    wind_direction: float    # degrees (0–360)
    wind_speed: float        # knots

    @field_validator("wind_direction")
    @classmethod
    def wind_dir_range(cls, v: float) -> float:
        if not (0.0 <= v <= 360.0):
            raise ValueError(f"wind_direction must be 0–360, got {v}")
        return v

    @field_validator("pressure")
    @classmethod
    def pressure_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"pressure must be > 0 mb, got {v}")
        return v

    @field_validator("wind_speed")
    @classmethod
    def wind_speed_nonneg(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"wind_speed must be >= 0, got {v}")
        return v


class SoundingProfile(BaseModel):
    model_config = ConfigDict(frozen=False)

    station: OklahomaSoundingStation
    valid_time: datetime                 # UTC
    levels: list[SoundingLevel]
    raw_source: str                      # "wyoming" | "rap" | "virtual"

    @field_validator("raw_source")
    @classmethod
    def valid_source(cls, v: str) -> str:
        allowed = {"wyoming", "rap", "virtual"}
        if v not in allowed:
            raise ValueError(f"raw_source must be one of {allowed}, got '{v}'")
        return v

    @model_validator(mode="after")
    def levels_decreasing_pressure(self) -> "SoundingProfile":
        """Pressure levels must decrease with increasing index (higher altitude)."""
        pressures = [lev.pressure for lev in self.levels]
        if len(pressures) > 1:
            for i in range(1, len(pressures)):
                if pressures[i] >= pressures[i - 1]:
                    raise ValueError(
                        f"Pressure levels must decrease with altitude. "
                        f"Level {i} ({pressures[i]} mb) >= level {i-1} ({pressures[i-1]} mb)"
                    )
        return self


class ThermodynamicIndices(BaseModel):
    model_config = ConfigDict(frozen=False)

    valid_time: datetime
    station: OklahomaSoundingStation

    # CAPE / CIN — stored as positive magnitudes
    MLCAPE: float            # Mixed layer CAPE, J/kg
    MLCIN: float             # Mixed layer CIN magnitude, J/kg (positive)
    SBCAPE: float            # Surface-based CAPE, J/kg
    SBCIN: float             # Surface-based CIN magnitude, J/kg (positive)
    MUCAPE: float            # Most unstable CAPE, J/kg

    # Parcel trajectory heights
    LCL_height: float        # Lifting condensation level, meters AGL
    LFC_height: float        # Level of free convection, meters AGL
    EL_height: float         # Equilibrium level, meters AGL

    # Cap diagnostics
    convective_temperature: float   # °F — surface must reach this for free convection
    cap_strength: float             # °C — peak temp excess of environment over parcel (LCL→LFC)

    # EML (Elevated Mixed Layer / elevated warm nose)
    EML_base: Optional[float] = None   # mb — pressure of EML base
    EML_top: Optional[float] = None    # mb — pressure of EML top
    EML_depth: Optional[float] = None  # mb — derived: base - top

    # Lapse rates and moisture
    lapse_rate_700_500: float    # °C/km
    lapse_rate_850_500: float    # °C/km
    precipitable_water: float    # inches
    mixing_ratio_850: float      # g/kg
    wet_bulb_zero: float         # meters AGL

    @field_validator("MLCAPE", "SBCAPE", "MUCAPE", "MLCIN", "SBCIN")
    @classmethod
    def cape_cin_nonneg(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"CAPE/CIN values must be >= 0 (stored as positive magnitude), got {v}")
        return v

    @model_validator(mode="after")
    def derive_eml_depth(self) -> "ThermodynamicIndices":
        if self.EML_base is not None and self.EML_top is not None and self.EML_depth is None:
            self.EML_depth = self.EML_base - self.EML_top
        return self


class CapErosionState(BaseModel):
    model_config = ConfigDict(frozen=False)

    valid_time: datetime
    county: OklahomaCounty

    current_CIN: float       # J/kg, positive magnitude
    current_CAPE: float      # J/kg
    cap_strength: float      # °C

    # convective_temp_gap: Tc minus current surface temp
    # Positive = cap still holding; negative = convection possible
    convective_temp_gap: float   # °F

    LCL_height: float        # meters AGL
    LFC_height: float        # meters AGL

    erosion_rate: Optional[float] = None          # J/kg per hour, negative = eroding
    estimated_erosion_time: Optional[datetime] = None  # when CIN hits zero at current rate

    confidence: float = 0.5  # 0.0–1.0
    primary_erosion_mechanism: ErosionMechanism = ErosionMechanism.UNKNOWN
    notes: Optional[str] = None

    @field_validator("current_CIN", "current_CAPE")
    @classmethod
    def nonneg(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"CIN/CAPE must be >= 0, got {v}")
        return v

    @field_validator("confidence")
    @classmethod
    def confidence_range(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"confidence must be 0.0–1.0, got {v}")
        return v
