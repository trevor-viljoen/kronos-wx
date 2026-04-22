"""
Oklahoma Mesonet API client.

Data source: https://api.mesonet.org/
Public API — no authentication required for most endpoints.
Rate limits apply; this client enforces them and retries on transient failures.

Reference: https://www.mesonet.org/index.php/api/main
"""

import logging
import time as time_module
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from functools import lru_cache

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..models import (
    OklahomaCounty,
    MesonetStation,
    MesonetObservation,
    MesonetTimeSeries,
    CountySurfaceState,
)

logger = logging.getLogger(__name__)

# Mesonet API base — public JSON endpoints
MESONET_BASE = "https://api.mesonet.org/public/station"
MESONET_OBS_BASE = "https://api.mesonet.org/public/obs"

# Rate limiting: Mesonet asks for no more than ~1 req/sec for bulk pulls
REQUEST_DELAY_SECONDS = 1.1


class MesonetClient:
    """
    Client for Oklahoma Mesonet observations.

    Usage::

        client = MesonetClient()
        ts = await client.get_observations("NORM", start, end)
    """

    def __init__(self, request_delay: float = REQUEST_DELAY_SECONDS):
        self._delay = request_delay
        self._http = httpx.Client(timeout=30.0)
        self._last_request_time: float = 0.0

    def _rate_limit(self) -> None:
        elapsed = time_module.monotonic() - self._last_request_time
        if elapsed < self._delay:
            time_module.sleep(self._delay - elapsed)
        self._last_request_time = time_module.monotonic()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
    )
    def _get(self, url: str, params: dict) -> dict:
        self._rate_limit()
        logger.debug("GET %s params=%s", url, params)
        response = self._http.get(url, params=params)
        # Don't retry client errors (4xx) — fail fast
        if 400 <= response.status_code < 500:
            response.raise_for_status()
        response.raise_for_status()
        return response.json()

    @lru_cache(maxsize=1)
    def get_station_metadata(self) -> list[MesonetStation]:
        """
        Pull metadata for all active Mesonet stations.
        Returns a list of MesonetStation objects.
        Cached after first call.
        """
        data = self._get(MESONET_BASE, {"format": "json"})
        stations: list[MesonetStation] = []

        for record in data.get("data", []):
            station_id = record.get("stid", "").upper()
            # Map station to county via our enum
            try:
                county = OklahomaCounty.from_mesonet_station(station_id)
            except ValueError:
                logger.debug("Station %s not mapped to a county — skipping", station_id)
                continue

            stations.append(
                MesonetStation(
                    station_id=station_id,
                    county=county,
                    latitude=float(record.get("lat", 0)),
                    longitude=float(record.get("lon", 0)),
                    elevation=float(record.get("elev", 0)),
                    name=record.get("name", station_id),
                )
            )

        logger.info("Loaded metadata for %d Mesonet stations", len(stations))
        return stations

    def get_observations(
        self,
        station_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> MesonetTimeSeries:
        """
        Fetch 5-minute observations for a single station over a time window.

        Args:
            station_id: 4-letter Mesonet code (e.g. "NORM")
            start_time: UTC start time
            end_time: UTC end time

        Returns:
            MesonetTimeSeries with parsed observations
        """
        station_id = station_id.upper()
        county = OklahomaCounty.from_mesonet_station(station_id)

        params = {
            "stid": station_id,
            "sdate": start_time.strftime("%Y%m%d%H%M"),
            "edate": end_time.strftime("%Y%m%d%H%M"),
            "vars": "TAIR,TDEW,RELH,WSPD,WDIR,WMAX,PRES,SRAD,ST05,SM05,RAIN",
            "format": "json",
        }

        data = self._get(MESONET_OBS_BASE, params)
        observations = _parse_obs_response(data, station_id, county)

        ts = MesonetTimeSeries(
            station_id=station_id,
            county=county,
            start_time=start_time,
            end_time=end_time,
            observations=observations,
        )
        ts.compute_tendencies()
        return ts

    def get_county_observations(
        self,
        county: OklahomaCounty,
        start_time: datetime,
        end_time: datetime,
    ) -> list[MesonetTimeSeries]:
        """
        Fetch observations from all stations in a county over a time window.
        Returns one MesonetTimeSeries per station.
        """
        # Find all stations in this county
        all_stations = self.get_station_metadata()
        county_stations = [s for s in all_stations if s.county == county]

        if not county_stations:
            logger.warning("No Mesonet stations found for county %s", county.name)
            return []

        result = []
        for station in county_stations:
            try:
                ts = self.get_observations(station.station_id, start_time, end_time)
                result.append(ts)
            except Exception as exc:
                logger.warning(
                    "Failed to retrieve observations for %s: %s",
                    station.station_id,
                    exc,
                )

        return result

    def get_statewide_snapshot(self, valid_time: datetime) -> list[MesonetObservation]:
        """
        Fetch a single-time observation from all stations.
        Uses a ±5 minute window around valid_time to find the nearest ob.

        Returns a list of MesonetObservation objects (one per station).
        """
        start = valid_time - timedelta(minutes=5)
        end = valid_time + timedelta(minutes=5)

        all_stations = self.get_station_metadata()
        observations: list[MesonetObservation] = []

        for station in all_stations:
            try:
                ts = self.get_observations(station.station_id, start, end)
                if ts.observations:
                    # Use observation nearest to valid_time
                    nearest = min(
                        ts.observations,
                        key=lambda o: abs((o.valid_time - valid_time).total_seconds()),
                    )
                    observations.append(nearest)
            except Exception as exc:
                logger.debug("Skipping %s in snapshot: %s", station.station_id, exc)

        logger.info(
            "Statewide snapshot at %s: %d stations returned data",
            valid_time.isoformat(),
            len(observations),
        )
        return observations

    def get_historical_case_data(self, case_date: date) -> dict[str, MesonetTimeSeries]:
        """
        Pull full-day Mesonet observations for all stations on a case date.

        Returns a dict mapping station_id → MesonetTimeSeries.
        """
        start = datetime(case_date.year, case_date.month, case_date.day, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(hours=24)

        all_stations = self.get_station_metadata()
        result: dict[str, MesonetTimeSeries] = {}

        logger.info(
            "Pulling full-day Mesonet data for %s (%d stations)",
            case_date.isoformat(),
            len(all_stations),
        )

        for station in all_stations:
            try:
                ts = self.get_observations(station.station_id, start, end)
                result[station.station_id] = ts
            except Exception as exc:
                logger.warning(
                    "Failed to pull %s for %s: %s",
                    station.station_id,
                    case_date,
                    exc,
                )

        logger.info(
            "Retrieved data for %d/%d stations on %s",
            len(result),
            len(all_stations),
            case_date.isoformat(),
        )
        return result

    def compute_county_surface_state(
        self,
        county: OklahomaCounty,
        valid_time: datetime,
        station_series: list[MesonetTimeSeries],
    ) -> Optional[CountySurfaceState]:
        """
        Average all available station observations within a county at valid_time
        to produce a CountySurfaceState.

        Handles missing stations gracefully; returns None if no data available.
        """
        window = timedelta(minutes=7)
        obs_at_time: list[MesonetObservation] = []

        for ts in station_series:
            if ts.county != county:
                continue
            for ob in ts.observations:
                if abs((ob.valid_time - valid_time).total_seconds()) <= window.total_seconds():
                    obs_at_time.append(ob)

        if not obs_at_time:
            return None

        n = len(obs_at_time)
        mean_temp = sum(o.temperature for o in obs_at_time) / n
        mean_dew = sum(o.dewpoint for o in obs_at_time) / n
        mean_pres = sum(o.pressure for o in obs_at_time) / n
        mean_wspd = sum(o.wind_speed for o in obs_at_time) / n

        # Vector-average wind direction
        import math
        u_sum = sum(math.sin(math.radians(o.wind_direction)) for o in obs_at_time)
        v_sum = sum(math.cos(math.radians(o.wind_direction)) for o in obs_at_time)
        dom_dir = math.degrees(math.atan2(u_sum / n, v_sum / n)) % 360

        # Data quality: fraction of possible stations that returned valid data
        all_county_stations = [s for s in self.get_station_metadata() if s.county == county]
        dqs = n / max(len(all_county_stations), 1)
        dqs = min(dqs, 1.0)

        return CountySurfaceState(
            county=county,
            valid_time=valid_time,
            mean_temperature=mean_temp,
            mean_dewpoint=mean_dew,
            mean_pressure=mean_pres,
            dominant_wind_direction=dom_dir,
            mean_wind_speed=mean_wspd,
            data_quality_score=dqs,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ── Private helpers ────────────────────────────────────────────────────────────

def _parse_obs_response(
    data: dict,
    station_id: str,
    county: OklahomaCounty,
) -> list[MesonetObservation]:
    """Parse the Mesonet JSON response into MesonetObservation objects."""
    observations: list[MesonetObservation] = []

    # Mesonet API returns data in 'data' list with column headers in 'head'
    headers = data.get("head", {}).get("vars", [])
    rows = data.get("data", [])

    if not headers or not rows:
        logger.debug("No observation data for %s", station_id)
        return observations

    # Build a header→index map
    col = {h: i for i, h in enumerate(headers)}

    def _get(row, key, default=None):
        idx = col.get(key)
        if idx is None:
            return default
        val = row[idx]
        if val in (None, "", "M", -999, -999.0):
            return default
        return val

    for row in rows:
        try:
            ts_str = _get(row, "TIME")
            if ts_str is None:
                continue
            valid_time = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )

            obs = MesonetObservation(
                station_id=station_id,
                county=county,
                valid_time=valid_time,
                temperature=float(_get(row, "TAIR", 0)),
                dewpoint=float(_get(row, "TDEW", 0)),
                relative_humidity=float(_get(row, "RELH", 50)),
                wind_direction=float(_get(row, "WDIR", 0)),
                wind_speed=float(_get(row, "WSPD", 0)),
                wind_gust=_float_or_none(_get(row, "WMAX")),
                pressure=float(_get(row, "PRES", 1000)),
                solar_radiation=_float_or_none(_get(row, "SRAD")),
                soil_temperature_5cm=_float_or_none(_get(row, "ST05")),
                soil_moisture_5cm=_float_or_none(_get(row, "SM05")),
                precipitation=_float_or_none(_get(row, "RAIN")),
            )
            observations.append(obs)
        except Exception as exc:
            logger.debug("Skipping malformed observation row: %s", exc)

    return observations


def _float_or_none(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
