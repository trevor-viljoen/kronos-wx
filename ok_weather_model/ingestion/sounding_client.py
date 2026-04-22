"""
University of Wyoming Radiosonde Archive client.

Fetches rawinsonde soundings from:
    https://weather.uwyo.edu/cgi-bin/sounding.py

The Wyoming archive provides soundings in an HTML-embedded text table.
This client parses the raw HTML into SoundingProfile objects and optionally
computes derived ThermodynamicIndices and KinematicProfile via MetPy.

Station WMO IDs:
    OUN = 72357  (Norman, OK)
    LMN = 74646  (Lamont, OK — ARM site)
    AMA = 72363  (Amarillo, TX — western proximity)
    DDC = 72451  (Dodge City, KS — northern proximity)
"""

import logging
import re
import time as time_module
from datetime import date, datetime, timezone
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..models import (
    OklahomaSoundingStation,
    SoundingLevel,
    SoundingProfile,
)

logger = logging.getLogger(__name__)

WYOMING_BASE = "https://weather.uwyo.edu/wsgi/sounding"
REQUEST_DELAY = 2.0  # seconds — Wyoming server is lightly resourced

MPS_TO_KNOTS = 1.94384  # m/s → knots (new API returns m/s)

# WMO station numbers for our four stations
STATION_WMO = {
    OklahomaSoundingStation.OUN: 72357,
    OklahomaSoundingStation.LMN: 74646,
    OklahomaSoundingStation.AMA: 72363,
    OklahomaSoundingStation.DDC: 72451,
}


class SoundingClient:
    """
    Client for University of Wyoming sounding archive.

    Usage::

        client = SoundingClient()
        profile = client.get_sounding(OklahomaSoundingStation.OUN, date(1999, 5, 3), 12)
    """

    def __init__(self, request_delay: float = REQUEST_DELAY):
        self._delay = request_delay
        self._http = httpx.Client(timeout=45.0)
        self._last_request: float = 0.0

    def _rate_limit(self) -> None:
        elapsed = time_module.monotonic() - self._last_request
        if elapsed < self._delay:
            time_module.sleep(self._delay - elapsed)
        self._last_request = time_module.monotonic()

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=4, max=60),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
    )
    def _fetch_raw(self, url: str) -> str:
        self._rate_limit()
        logger.debug("Fetching Wyoming sounding: %s", url)
        response = self._http.get(url)
        response.raise_for_status()
        return response.text

    def get_sounding(
        self,
        station: OklahomaSoundingStation,
        sounding_date: date,
        hour: int,  # 0 or 12 (UTC)
    ) -> Optional[SoundingProfile]:
        """
        Retrieve a single rawinsonde sounding.

        Args:
            station: OklahomaSoundingStation enum member
            sounding_date: date of the sounding
            hour: 0 or 12 (UTC)

        Returns:
            SoundingProfile or None if the sounding is missing in the archive.
        """
        if hour not in (0, 12):
            raise ValueError(f"hour must be 0 or 12, got {hour}")

        wmo_id = STATION_WMO[station]
        url = (
            f"{WYOMING_BASE}"
            f"?datetime={sounding_date.year}-{sounding_date.month:02d}-{sounding_date.day:02d}+{hour:02d}%3A00%3A00"
            f"&type=TEXT%3ALIST"
            f"&id={wmo_id}"
            f"&src=UNKNOWN"
        )

        try:
            html = self._fetch_raw(url)
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "HTTP error fetching sounding %s %s %02dZ: %s",
                station.value, sounding_date, hour, exc
            )
            return None

        return parse_wyoming_sounding(html, station, sounding_date, hour)

    def get_sounding_range(
        self,
        station: OklahomaSoundingStation,
        start_date: date,
        end_date: date,
    ) -> list[SoundingProfile]:
        """
        Bulk retrieval of soundings (both 00Z and 12Z) over a date range.
        Missing soundings are skipped gracefully.
        """
        profiles: list[SoundingProfile] = []
        current = start_date

        while current <= end_date:
            for hour in (0, 12):
                profile = self.get_sounding(station, current, hour)
                if profile is not None:
                    profiles.append(profile)
                    logger.info(
                        "Retrieved %s %s %02dZ (%d levels)",
                        station.value, current, hour, len(profile.levels)
                    )
                else:
                    logger.debug("Missing sounding: %s %s %02dZ", station.value, current, hour)

            from datetime import timedelta
            current += timedelta(days=1)

        return profiles

    def close(self) -> None:
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ── HTML parsing ───────────────────────────────────────────────────────────────

def parse_wyoming_sounding(
    html: str,
    station: OklahomaSoundingStation,
    sounding_date: date,
    hour: int,
) -> Optional[SoundingProfile]:
    """
    Parse the University of Wyoming HTML sounding page into a SoundingProfile.

    The Wyoming page embeds a fixed-width data table inside <pre> tags.
    Column order: PRES HGHT TEMP DWPT RELH MIXR DRCT SKNT THTA THTE THTV

    Returns None if the page indicates missing data.
    """
    soup = BeautifulSoup(html, "lxml")

    # Check for "No sounding available" message
    h2_tags = soup.find_all("h2")
    for h2 in h2_tags:
        if "Can't" in h2.text or "No" in h2.text or "not" in h2.text.lower():
            logger.debug("Wyoming archive: no sounding for %s %s %02dZ", station.value, sounding_date, hour)
            return None

    pre_tags = soup.find_all("pre")
    if len(pre_tags) < 1:
        logger.warning("No <pre> data block found in Wyoming response for %s %s %02dZ", station.value, sounding_date, hour)
        return None

    # First <pre> block contains the sounding data table
    raw_text = pre_tags[0].get_text()

    levels = _parse_sounding_table(raw_text)
    if not levels:
        logger.warning(
            "Parsed 0 levels from Wyoming sounding: %s %s %02dZ",
            station.value, sounding_date, hour
        )
        return None

    valid_time = datetime(
        sounding_date.year,
        sounding_date.month,
        sounding_date.day,
        hour,
        0,
        tzinfo=timezone.utc,
    )

    return SoundingProfile(
        station=station,
        valid_time=valid_time,
        levels=levels,
        raw_source="wyoming",
    )


def _parse_sounding_table(text: str) -> list[SoundingLevel]:
    """
    Parse the fixed-width data table from Wyoming sounding text.

    Column order: PRES HGHT TEMP DWPT RELH MIXR DRCT SPED THTA THTE THTV
    (new API: SPED in m/s — converted to knots on ingest)
    (legacy API used SKNT in knots — both handled)

    Only PRES, HGHT, TEMP, DWPT, DRCT, speed are extracted.
    """
    lines = text.strip().split("\n")
    levels: list[SoundingLevel] = []

    # Detect whether wind speed column is m/s (SPED) or knots (SKNT)
    speed_in_mps = False
    for line in lines:
        if "SPED" in line:
            speed_in_mps = True
            break
        if "SKNT" in line:
            break

    # Skip header lines — data starts after the second dashed separator
    dash_count = 0
    in_data = False
    for line in lines:
        stripped = line.strip()
        if re.match(r"^[-]+", stripped):
            dash_count += 1
            if dash_count >= 2:
                in_data = True
            continue

        if not in_data:
            continue

        # Empty line marks end of data
        if not stripped:
            break

        parts = stripped.split()
        if len(parts) < 8:
            continue

        try:
            pres = float(parts[0])
            hght = float(parts[1])
            temp = float(parts[2])
            dwpt = float(parts[3])
            drct = float(parts[6])
            spd  = float(parts[7])
        except (ValueError, IndexError):
            continue

        # Skip clearly bad values
        if any(v <= -999 for v in (pres, hght, temp, dwpt)):
            continue
        if pres <= 0:
            continue

        wind_speed_kt = spd * MPS_TO_KNOTS if speed_in_mps else spd

        levels.append(
            SoundingLevel(
                pressure=pres,
                height=hght,
                temperature=temp,
                dewpoint=dwpt,
                wind_direction=drct % 360,
                wind_speed=round(wind_speed_kt, 1),
            )
        )

    return levels
