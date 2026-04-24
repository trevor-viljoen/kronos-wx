"""
ERA5 reanalysis client via the Copernicus Climate Data Store (CDS) API.

ERA5 fills upper-air gaps between the twice-daily sounding times (00Z/12Z),
providing hourly analysis fields at 31km resolution over the Oklahoma domain.

Setup (one-time):
    1. Register at https://cds.climate.copernicus.eu/user/register
    2. Accept the ERA5 terms of use at:
       https://cds.climate.copernicus.eu/datasets/reanalysis-era5-pressure-levels?tab=download#manage-licences
    3. Create ~/.cdsapirc with your API key (new CDS API format):

       url: https://cds.climate.copernicus.eu/api
       key: <YOUR-API-KEY>

    4. Set CDS_API_KEY=<YOUR-API-KEY> in your .env as a fallback

Dependencies: pip install cdsapi xarray cfgrib
"""

import logging
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# Oklahoma bounding box [N, W, S, E] in ERA5 format [N, W, S, E]
OK_BBOX = [37.0, -103.0, 33.5, -94.5]

# Standard pressure levels used for synoptic analysis (fast, low-volume)
PRESSURE_LEVELS = ["925", "850", "700", "500", "300", "250"]

# Extended pressure levels for virtual sounding extraction.
# Provides ≥ 20 levels from near-surface to the upper troposphere — sufficient
# for MetPy CAPE/CIN computation (which requires ≥ 10 levels).
SOUNDING_PRESSURE_LEVELS = [
    "1000", "975", "950", "925", "900", "875", "850", "825", "800",
    "775", "750", "700", "650", "600", "550", "500", "450", "400",
    "350", "300", "250", "200",
]

# ERA5 variable names
UPPER_AIR_VARS = [
    "temperature",
    "u_component_of_wind",
    "v_component_of_wind",
    "vertical_velocity",
    "geopotential",
    "specific_humidity",
]


# ── Dataset normalization ─────────────────────────────────────────────────────
# The new CDS API (2024+) returns short variable names and different dimension
# names vs the legacy API. Normalize everything to the long-name convention that
# era5_diagnostics.py expects so the processing layer never has to care.

_VAR_RENAME = {
    "t": "temperature",
    "u": "u_component_of_wind",
    "v": "v_component_of_wind",
    "w": "vertical_velocity",
    "z": "geopotential",
    "q": "specific_humidity",
}

_DIM_RENAME = {
    "valid_time":     "time",
    "pressure_level": "level",
}


def _normalize_era5_dataset(ds):
    """
    Rename CDS API short variable/dimension names to the long-name convention.
    Passes through datasets that already use the long names (idempotent).
    """
    # Rename dimensions that exist
    dim_map = {k: v for k, v in _DIM_RENAME.items() if k in ds.dims}
    if dim_map:
        ds = ds.rename(dim_map)

    # Rename variables that exist
    var_map = {k: v for k, v in _VAR_RENAME.items() if k in ds.data_vars}
    if var_map:
        ds = ds.rename(var_map)

    return ds


class ERA5Client:
    """
    Client for ECMWF ERA5 reanalysis data via CDS API.

    Requires a configured ~/.cdsapirc file (see module docstring).

    Usage::

        client = ERA5Client()
        ds = client.get_upper_air_fields(
            date(1999, 5, 3),
            pressure_levels=PRESSURE_LEVELS,
            variables=UPPER_AIR_VARS,
        )
    """

    def __init__(self, cache_dir: Optional[Path] = None):
        self._cache_dir = cache_dir or Path("data/era5_cache")
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._cds = None  # Lazy-initialized

    def _get_cds_client(self):
        """Lazily initialize the CDS API client."""
        if self._cds is None:
            try:
                import cdsapi
                self._cds = cdsapi.Client(quiet=True)
                logger.info("CDS API client initialized")
            except ImportError:
                raise ImportError(
                    "cdsapi is required for ERA5 access. Install with: pip install cdsapi"
                ) from None
            except Exception as exc:
                raise RuntimeError(
                    "Failed to initialize CDS API client. "
                    "Ensure ~/.cdsapirc is configured correctly. "
                    f"Error: {exc}"
                ) from exc
        return self._cds

    def get_upper_air_fields(
        self,
        analysis_date: date,
        pressure_levels: Optional[list[str]] = None,
        variables: Optional[list[str]] = None,
        hours: Optional[list[int]] = None,
    ):
        """
        Download ERA5 upper-air fields for the Oklahoma domain on a given date.

        Args:
            analysis_date: Date to retrieve
            pressure_levels: List of pressure levels in mb (as strings)
            variables: ERA5 variable names to retrieve
            hours: UTC hours to retrieve (default: all 24)

        Returns:
            xarray.Dataset with dimensions (time, level, latitude, longitude)
        """
        import xarray as xr

        if pressure_levels is None:
            pressure_levels = PRESSURE_LEVELS
        if variables is None:
            variables = UPPER_AIR_VARS
        if hours is None:
            hours = list(range(24))

        # Check cache
        cache_key = (
            f"era5_ua_{analysis_date.strftime('%Y%m%d')}_"
            f"{'_'.join(pressure_levels)}.nc"
        )
        cache_path = self._cache_dir / cache_key

        if cache_path.exists():
            logger.info("Loading ERA5 from cache: %s", cache_path)
            return _normalize_era5_dataset(xr.open_dataset(cache_path))

        cds = self._get_cds_client()
        hour_strings = [f"{h:02d}:00" for h in hours]

        request = {
            "product_type": "reanalysis",
            "variable": variables,
            "pressure_level": pressure_levels,
            "year": str(analysis_date.year),
            "month": f"{analysis_date.month:02d}",
            "day": f"{analysis_date.day:02d}",
            "time": hour_strings,
            "area": OK_BBOX,
            "format": "netcdf",
        }

        logger.info(
            "Requesting ERA5 upper-air fields for %s (%d pressure levels, %d hours)",
            analysis_date,
            len(pressure_levels),
            len(hours),
        )

        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            cds.retrieve("reanalysis-era5-pressure-levels", request, str(tmp_path))
            tmp_path.rename(cache_path)
            logger.info("ERA5 download complete → %s", cache_path)
            return _normalize_era5_dataset(xr.open_dataset(cache_path))
        except Exception as exc:
            tmp_path.unlink(missing_ok=True)
            raise RuntimeError(f"ERA5 download failed: {exc}") from exc

    def get_synoptic_analysis(self, analysis_date: date) -> dict:
        """
        Compute synoptic-scale diagnostics from ERA5 for a given date.

        Returns a dict with:
            jet_position: dict of pressure/lat/lon/speed at jet max
            trough_longitude: float (°W, negative)
            temp_700mb_ok: dict of min/mean/max 700mb temps over Oklahoma (°C)
            llj_analysis: dict of 850mb wind max over Oklahoma
        """
        ds = self.get_upper_air_fields(
            analysis_date,
            pressure_levels=["850", "700", "500", "300", "250"],
            variables=["temperature", "u_component_of_wind", "v_component_of_wind", "geopotential"],
            hours=[0, 6, 12, 18],
        )

        result = {}

        try:
            # 250mb jet analysis
            if "u_component_of_wind" in ds and "v_component_of_wind" in ds:
                u250 = ds["u_component_of_wind"].sel(level=250, method="nearest")
                v250 = ds["v_component_of_wind"].sel(level=250, method="nearest")
                wspd250 = np.sqrt(u250**2 + v250**2) * 1.94384  # m/s → knots

                # Max jet at 12Z
                spd_12z = wspd250.sel(time=wspd250.time[wspd250.time.dt.hour == 12][0])
                jet_max_idx = spd_12z.values.argmax()
                jet_lat_idx, jet_lon_idx = np.unravel_index(jet_max_idx, spd_12z.shape)

                result["jet_position"] = {
                    "speed_knots": float(spd_12z.values.max()),
                    "lat": float(spd_12z.latitude.values[jet_lat_idx]),
                    "lon": float(spd_12z.longitude.values[jet_lon_idx]),
                    "level_mb": 250,
                }

            # 700mb temperature over Oklahoma (proxy for EML / warm nose)
            if "temperature" in ds:
                t700 = ds["temperature"].sel(level=700, method="nearest")
                t700_12z = t700.sel(time=t700.time[t700.time.dt.hour == 12][0])
                t700_c = t700_12z.values - 273.15  # K → °C

                result["temp_700mb_ok"] = {
                    "min_c": float(t700_c.min()),
                    "mean_c": float(t700_c.mean()),
                    "max_c": float(t700_c.max()),
                }

            # 850mb LLJ analysis
            if "u_component_of_wind" in ds:
                u850 = ds["u_component_of_wind"].sel(level=850, method="nearest")
                v850 = ds["v_component_of_wind"].sel(level=850, method="nearest")
                wspd850 = np.sqrt(u850**2 + v850**2) * 1.94384

                # Max LLJ at 06Z (typical nocturnal LLJ peak)
                time_06z = wspd850.time[wspd850.time.dt.hour == 6]
                if len(time_06z) > 0:
                    spd_06z = wspd850.sel(time=time_06z[0])
                    result["llj_analysis"] = {
                        "max_speed_knots": float(spd_06z.values.max()),
                        "valid_time": "06Z",
                    }

        except Exception as exc:
            logger.warning("Error computing synoptic diagnostics: %s", exc)

        return result

    def close(self) -> None:
        pass  # cdsapi client manages its own connections

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
