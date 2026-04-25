"""
Oklahoma Mesonet client — MDF file-based archive fetcher.

Data source: https://www.mesonet.org/data/public/mesonet/mdf/
Public archive — no authentication required.
Each MDF file is a ~20 KB snapshot covering all ~120 stations statewide
at a 5-minute observation interval.

For case analysis we fetch 4 key snapshots per case day (12Z, 15Z, 18Z, 21Z)
rather than every 5-minute file, keeping network traffic to ~80 KB per case.
"""

import logging
import math
import time as time_module
from datetime import date, datetime, timedelta, timezone
from typing import Optional

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

# MDF archive base — files at YYYY/MM/DD/YYYYMMDDHHmm.mdf
MESONET_MDF_BASE = "https://www.mesonet.org/data/public/mesonet/mdf"

# Rate limiting: be polite to the Mesonet archive
REQUEST_DELAY_SECONDS = 1.1

# Analysis hours for a convective case day
_ANALYSIS_HOURS = (12, 15, 18, 21)


def _mdf_url(dt: datetime) -> str:
    """Build the MDF archive URL for the given UTC datetime."""
    # MDF files are on 5-minute boundaries; snap down
    minute = (dt.minute // 5) * 5
    return (
        f"{MESONET_MDF_BASE}/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/"
        f"{dt.year:04d}{dt.month:02d}{dt.day:02d}{dt.hour:02d}{minute:02d}.mdf"
    )


def _dewpoint_c(temp_c: float, rh: float) -> float:
    """August-Roche-Magnus approximation: dewpoint in °C from T(°C) and RH(%)."""
    rh = max(1.0, min(100.0, rh))
    gamma = (17.625 * temp_c / (243.04 + temp_c)) + math.log(rh / 100.0)
    return 243.04 * gamma / (17.625 - gamma)


def _c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def _ms_to_mph(ms: float) -> float:
    return ms * 2.23694


def _mm_to_in(mm: float) -> float:
    return mm / 25.4


def _parse_mdf(content: str, fallback_date: date) -> list[MesonetObservation]:
    """
    Parse an MDF fixed-width text file into MesonetObservation objects.

    MDF format (3-line header, then one row per station):
      Line 1: ``  N ! copyright``  (N = station count)
      Line 2: ``  M YYYY MM DD HH MM SS``  (M = column count, then file timestamp)
      Line 3: column names separated by whitespace
      Remaining: data rows; missing values are -998 (instrument error) or -999 (not installed)
    """
    lines = content.strip().splitlines()
    if len(lines) < 4:
        return []

    # Extract file date/time from line 2
    meta = lines[1].split()
    try:
        year, month, day = int(meta[1]), int(meta[2]), int(meta[3])
    except (IndexError, ValueError):
        year, month, day = fallback_date.year, fallback_date.month, fallback_date.day

    headers = lines[2].split()
    col = {h: i for i, h in enumerate(headers)}

    def _val(parts: list[str], key: str) -> Optional[float]:
        idx = col.get(key)
        if idx is None or idx >= len(parts):
            return None
        try:
            v = float(parts[idx])
            # MDF missing codes: -998 (instrument error), -999 (not installed),
            # -996 (comms failure), -995, etc. All valid met values are > -100.
            return None if v < -100 else v
        except (ValueError, TypeError):
            return None

    observations: list[MesonetObservation] = []
    for line in lines[3:]:
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) < 4:
            continue

        stid_idx = col.get("STID")
        if stid_idx is None or stid_idx >= len(parts):
            continue
        stid = parts[stid_idx].upper()

        try:
            county = OklahomaCounty.from_mesonet_station(stid)
        except (ValueError, AttributeError):
            continue

        # TIME column is minutes since midnight UTC for this date
        time_min = _val(parts, "TIME")
        if time_min is None:
            continue
        obs_hour = int(time_min) // 60
        obs_min = int(time_min) % 60
        valid_time = datetime(year, month, day, obs_hour % 24, obs_min, tzinfo=timezone.utc)

        tair = _val(parts, "TAIR")
        relh = _val(parts, "RELH")
        wspd = _val(parts, "WSPD")
        wdir = _val(parts, "WDIR")
        pres = _val(parts, "PRES")

        # Require the five core fields
        if any(v is None for v in (tair, relh, wspd, wdir, pres)):
            continue

        wmax = _val(parts, "WMAX")
        srad = _val(parts, "SRAD")
        ts05 = _val(parts, "TS05")
        tr05 = _val(parts, "TR05")
        rain = _val(parts, "RAIN")

        try:
            obs = MesonetObservation(
                station_id=stid,
                county=county,
                valid_time=valid_time,
                temperature=_c_to_f(tair),
                dewpoint=_c_to_f(_dewpoint_c(tair, relh)),
                relative_humidity=relh,
                wind_direction=float(wdir) % 360.0,
                wind_speed=_ms_to_mph(wspd),
                wind_gust=_ms_to_mph(wmax) if wmax is not None else None,
                pressure=float(pres),
                solar_radiation=float(srad) if srad is not None else None,
                soil_temperature_5cm=float(ts05) if ts05 is not None else None,
                soil_moisture_5cm=float(tr05) if tr05 is not None else None,
                precipitation=_mm_to_in(rain) if rain is not None and rain >= 0 else None,
            )
            observations.append(obs)
        except Exception as exc:
            logger.debug("Skipping malformed MDF row for %s: %s", stid, exc)

    return observations


def _parse_mdf_raw(content: str, fallback_date: date) -> list[dict]:
    """
    Parse an MDF file and return ALL stations as plain dicts — no OklahomaCounty
    filtering. Used for the web dashboard map where we want every active station.

    Returned keys: station_id, temp_f, dewpoint_f, wind_dir, wind_speed, wind_gust.
    Stations missing any of the five core fields are still dropped.
    """
    lines = content.strip().splitlines()
    if len(lines) < 4:
        return []

    meta = lines[1].split()
    try:
        year, month, day = int(meta[1]), int(meta[2]), int(meta[3])
    except (IndexError, ValueError):
        year, month, day = fallback_date.year, fallback_date.month, fallback_date.day

    headers = lines[2].split()
    col = {h: i for i, h in enumerate(headers)}

    def _val(parts: list[str], key: str) -> Optional[float]:
        idx = col.get(key)
        if idx is None or idx >= len(parts):
            return None
        try:
            v = float(parts[idx])
            return None if v < -100 else v
        except (ValueError, TypeError):
            return None

    result: list[dict] = []
    for line in lines[3:]:
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) < 4:
            continue
        stid_idx = col.get("STID")
        if stid_idx is None or stid_idx >= len(parts):
            continue
        stid = parts[stid_idx].upper()

        tair = _val(parts, "TAIR")
        relh = _val(parts, "RELH")
        wspd = _val(parts, "WSPD")
        wdir = _val(parts, "WDIR")
        pres = _val(parts, "PRES")
        if any(v is None for v in (tair, relh, wspd, wdir, pres)):
            continue

        wmax = _val(parts, "WMAX")
        try:
            td_c = _dewpoint_c(float(tair), float(relh))   # type: ignore[arg-type]
            result.append({
                "station_id": stid,
                "temp_f":     round(_c_to_f(float(tair)), 1),   # type: ignore[arg-type]
                "dewpoint_f": round(_c_to_f(td_c), 1),
                "wind_dir":   round(float(wdir), 0),             # type: ignore[arg-type]
                "wind_speed": round(_ms_to_mph(float(wspd)), 1), # type: ignore[arg-type]
                "wind_gust":  round(_ms_to_mph(float(wmax)), 1) if wmax is not None else None,
            })
        except Exception as exc:
            logger.debug("Skipping raw MDF row for %s: %s", stid, exc)

    return result


class MesonetClient:
    """
    Client for Oklahoma Mesonet observations via the public MDF archive.

    Usage::

        with MesonetClient() as mc:
            data = mc.get_historical_case_data(case_date)
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
    def _fetch_text(self, url: str) -> str:
        self._rate_limit()
        logger.debug("GET %s", url)
        response = self._http.get(url)
        if 400 <= response.status_code < 500:
            response.raise_for_status()
        response.raise_for_status()
        return response.text

    def get_snapshot_observations(self, dt: datetime) -> list[MesonetObservation]:
        """
        Fetch all-station observations from the MDF file at the given UTC time.
        Time is snapped down to the nearest 5-minute boundary.
        """
        url = _mdf_url(dt)
        content = self._fetch_text(url)
        obs = _parse_mdf(content, dt.date())
        logger.debug("Parsed %d observations from %s", len(obs), url)
        return obs

    def get_snapshot_with_display(
        self, dt: datetime
    ) -> tuple[list[MesonetObservation], list[dict]]:
        """
        Single MDF fetch returning both:
          - domain observations (OklahomaCounty-filtered, for dryline/moisture)
          - raw display dicts for ALL stations (no county filter, for the map)
        """
        url = _mdf_url(dt)
        content = self._fetch_text(url)
        domain_obs = _parse_mdf(content, dt.date())
        display_obs = _parse_mdf_raw(content, dt.date())
        logger.debug(
            "Parsed %d domain / %d display obs from %s",
            len(domain_obs), len(display_obs), url,
        )
        return domain_obs, display_obs

    def get_historical_case_data(self, case_date: date) -> dict[str, MesonetTimeSeries]:
        """
        Pull Mesonet snapshots at 12Z, 15Z, 18Z, and 21Z for a case date.

        Returns a dict mapping station_id → MesonetTimeSeries.
        Each TimeSeries contains the observations from all fetched snapshots
        for that station.
        """
        start = datetime(case_date.year, case_date.month, case_date.day, 12, 0, tzinfo=timezone.utc)
        end = datetime(case_date.year, case_date.month, case_date.day, 22, 0, tzinfo=timezone.utc)

        station_obs: dict[str, list[MesonetObservation]] = {}
        station_county: dict[str, OklahomaCounty] = {}

        for hour in _ANALYSIS_HOURS:
            dt = datetime(case_date.year, case_date.month, case_date.day, hour, 0, tzinfo=timezone.utc)
            try:
                snapshot = self.get_snapshot_observations(dt)
                for obs in snapshot:
                    station_obs.setdefault(obs.station_id, []).append(obs)
                    station_county[obs.station_id] = obs.county
                logger.info("Fetched %dZ MDF for %s: %d stations", hour, case_date, len(snapshot))
            except Exception as exc:
                logger.warning("Failed to fetch %dZ MDF for %s: %s", hour, case_date, exc)

        result: dict[str, MesonetTimeSeries] = {}
        for stid, obs_list in station_obs.items():
            ts = MesonetTimeSeries(
                station_id=stid,
                county=station_county[stid],
                start_time=start,
                end_time=end,
                observations=sorted(obs_list, key=lambda o: o.valid_time),
            )
            try:
                ts.compute_tendencies()
            except Exception:
                pass  # tendencies require ≥2 obs; may fail for sparse data
            result[stid] = ts

        logger.info(
            "Retrieved Mesonet data for %d stations on %s",
            len(result),
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
        Average all county station observations nearest to valid_time to produce
        a CountySurfaceState with heating tendency.  Returns None if no data.
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
        u_sum = sum(math.sin(math.radians(o.wind_direction)) for o in obs_at_time)
        v_sum = sum(math.cos(math.radians(o.wind_direction)) for o in obs_at_time)
        dom_dir = math.degrees(math.atan2(u_sum / n, v_sum / n)) % 360

        # DQS: 1 station is adequate for a county; cap at 1.0
        dqs = min(float(n), 1.0)

        # Heating rate: compare against most recent prior snapshot (within 4 hrs)
        heating_rate_1hr: Optional[float] = None
        dewpoint_tendency_1hr: Optional[float] = None
        for lookback_hrs in (1, 2, 3, 4, 5, 6, 7):
            prev_time = valid_time - timedelta(hours=lookback_hrs)
            prev_obs: list[MesonetObservation] = []
            for ts in station_series:
                if ts.county != county:
                    continue
                for ob in ts.observations:
                    if abs((ob.valid_time - prev_time).total_seconds()) <= window.total_seconds():
                        prev_obs.append(ob)
            if prev_obs:
                prev_temp = sum(o.temperature for o in prev_obs) / len(prev_obs)
                prev_dew = sum(o.dewpoint for o in prev_obs) / len(prev_obs)
                heating_rate_1hr = (mean_temp - prev_temp) / lookback_hrs
                dewpoint_tendency_1hr = (mean_dew - prev_dew) / lookback_hrs
                break

        return CountySurfaceState(
            county=county,
            valid_time=valid_time,
            mean_temperature=mean_temp,
            mean_dewpoint=mean_dew,
            mean_pressure=mean_pres,
            dominant_wind_direction=dom_dir,
            mean_wind_speed=mean_wspd,
            data_quality_score=dqs,
            heating_rate_1hr=heating_rate_1hr,
            dewpoint_tendency_1hr=dewpoint_tendency_1hr,
        )

    def get_station_metadata(self) -> list[MesonetStation]:
        """
        Return basic station metadata derived from a current MDF snapshot.
        Lat/lon/elevation are not available from MDF files; returns stubs.
        Prefer using get_historical_case_data() for analysis work.
        """
        # Fetch today's 12Z file as a proxy for the station roster
        today = datetime.now(tz=timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
        try:
            obs = self.get_snapshot_observations(today)
        except Exception:
            # Fall back to yesterday
            yesterday = today - timedelta(days=1)
            obs = self.get_snapshot_observations(yesterday)

        seen: dict[str, MesonetStation] = {}
        for o in obs:
            if o.station_id not in seen:
                seen[o.station_id] = MesonetStation(
                    station_id=o.station_id,
                    county=o.county,
                    latitude=0.0,
                    longitude=0.0,
                    elevation=0.0,
                    name=o.station_id,
                )
        return list(seen.values())

    def close(self) -> None:
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
