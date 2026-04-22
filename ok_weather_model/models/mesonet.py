"""
Surface observation models for Oklahoma Mesonet data.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator

from .enums import OklahomaCounty


class MesonetStation(BaseModel):
    model_config = ConfigDict(frozen=False)

    station_id: str          # 4-letter Mesonet code (e.g. "NORM")
    county: OklahomaCounty
    latitude: float          # °N
    longitude: float         # °W (negative)
    elevation: float         # meters MSL
    name: str                # full station name


class MesonetObservation(BaseModel):
    model_config = ConfigDict(frozen=False)

    station_id: str
    county: OklahomaCounty
    valid_time: datetime     # UTC

    temperature: float       # °F
    dewpoint: float          # °F
    relative_humidity: float # %
    wind_direction: float    # degrees
    wind_speed: float        # mph
    wind_gust: Optional[float] = None        # mph
    pressure: float          # mb

    solar_radiation: Optional[float] = None  # W/m²
    soil_temperature_5cm: Optional[float] = None   # °C
    soil_moisture_5cm: Optional[float] = None      # volumetric fraction 0–1
    precipitation: Optional[float] = None          # inches (5-min accumulation)

    @field_validator("wind_direction")
    @classmethod
    def wind_dir_range(cls, v: float) -> float:
        if not (0.0 <= v <= 360.0):
            raise ValueError(f"wind_direction must be 0–360, got {v}")
        return v

    @field_validator("relative_humidity")
    @classmethod
    def rh_range(cls, v: float) -> float:
        if not (0.0 <= v <= 100.0):
            raise ValueError(f"relative_humidity must be 0–100, got {v}")
        return v

    @field_validator("wind_speed")
    @classmethod
    def wind_speed_nonneg(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"wind_speed must be >= 0, got {v}")
        return v


class MesonetTimeSeries(BaseModel):
    model_config = ConfigDict(frozen=False)

    station_id: str
    county: OklahomaCounty
    start_time: datetime     # UTC
    end_time: datetime       # UTC
    interval_minutes: int = 5
    observations: list[MesonetObservation]

    # Computed tendencies (filled after loading)
    heating_rate: Optional[float] = None        # °F/hour over last N obs
    dewpoint_tendency: Optional[float] = None   # °F/hour
    pressure_tendency: Optional[float] = None   # mb/hour

    def compute_tendencies(self, window_obs: int = 12) -> None:
        """
        Compute heating rate, dewpoint tendency, and pressure tendency
        over the last `window_obs` observations (default 1 hour at 5-min interval).
        Mutates self in place.
        """
        obs = self.observations
        if len(obs) < 2:
            return
        subset = obs[-window_obs:] if len(obs) >= window_obs else obs

        dt_hours = (subset[-1].valid_time - subset[0].valid_time).total_seconds() / 3600.0
        if dt_hours == 0:
            return

        self.heating_rate = (subset[-1].temperature - subset[0].temperature) / dt_hours
        self.dewpoint_tendency = (subset[-1].dewpoint - subset[0].dewpoint) / dt_hours
        self.pressure_tendency = (subset[-1].pressure - subset[0].pressure) / dt_hours


class CountySurfaceState(BaseModel):
    """
    Spatially-averaged surface conditions across all Mesonet stations in a county
    at a given valid time.
    """
    model_config = ConfigDict(frozen=False)

    county: OklahomaCounty
    valid_time: datetime     # UTC

    mean_temperature: float  # °F
    mean_dewpoint: float     # °F
    mean_pressure: float     # mb
    dominant_wind_direction: float  # degrees (vector mean)
    mean_wind_speed: float   # mph

    # Populated when ThermodynamicIndices are available
    convective_temp_gap: Optional[float] = None   # °F — Tc minus surface temp

    # Tendency fields — populated from MesonetTimeSeries
    heating_rate_1hr: Optional[float] = None     # °F/hour
    heating_rate_3hr: Optional[float] = None     # °F/hour
    dewpoint_tendency_1hr: Optional[float] = None  # °F/hour

    data_quality_score: float = 1.0  # 0.0–1.0, fraction of stations with valid data

    @field_validator("data_quality_score")
    @classmethod
    def dqs_range(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"data_quality_score must be 0.0–1.0, got {v}")
        return v
