"""
Storm Prediction Center (SPC) archive client.

Data sources:
    Tornado database: https://www.spc.noaa.gov/wcm/data/1950-2023_actual_tornadoes.csv
    Day 1 outlooks: https://www.spc.noaa.gov/archive/

This client builds the skeleton HistoricalCase library from tornado reports,
automatically classifying each day's event type from count and intensity data.
"""

import io
import logging
from datetime import date, datetime, timezone
from typing import Optional

import httpx
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..models import (
    EventClass,
    TornadoRating,
    OklahomaCounty,
    HistoricalCase,
)

logger = logging.getLogger(__name__)

TORNADO_CSV_URL = "https://www.spc.noaa.gov/wcm/data/1950-2023_actual_tornadoes.csv"

# SPC state code for Oklahoma
OKLAHOMA_STATE_FIPS = 40
OKLAHOMA_STATE_ABBR = "OK"

# F/EF scale column name in SPC CSV
MAGNITUDE_COL = "mag"  # 0–5, negative = unknown


class SPCClient:
    """
    Client for SPC historical tornado data and convective outlooks.

    Usage::

        client = SPCClient()
        tornadoes = client.get_oklahoma_tornadoes(date(1994,1,1), date(2023,12,31))
        skeletons = client.build_case_skeletons(date(1994,1,1), date(2023,12,31))
    """

    def __init__(self):
        self._http = httpx.Client(timeout=120.0)
        self._tornado_df: Optional[pd.DataFrame] = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=60),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
    )
    def _load_tornado_csv(self) -> pd.DataFrame:
        """Download and parse the SPC tornado database. Cached after first load."""
        if self._tornado_df is not None:
            return self._tornado_df

        logger.info("Downloading SPC tornado database from %s", TORNADO_CSV_URL)
        response = self._http.get(TORNADO_CSV_URL)
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text), low_memory=False)

        # Normalize column names (SPC CSV has lowercase cols)
        df.columns = [c.strip().lower() for c in df.columns]

        # Parse date
        df["event_date"] = pd.to_datetime(
            df["yr"].astype(str) + "-"
            + df["mo"].astype(str).str.zfill(2) + "-"
            + df["dy"].astype(str).str.zfill(2),
            errors="coerce",
        ).dt.date

        self._tornado_df = df
        logger.info("Loaded %d tornado records from SPC database", len(df))
        return df

    def get_oklahoma_tornadoes(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        Filter SPC tornado database to Oklahoma events within date range.

        Returns a DataFrame with columns:
            event_date, mag, fat, inj, slat, slon, elat, elon, len, wid, stf, stn, f1
        """
        df = self._load_tornado_csv()

        # Oklahoma FIPS = 40; SPC 'st' column is state FIPS
        ok_mask = df["st"].astype(int) == OKLAHOMA_STATE_FIPS
        date_mask = (df["event_date"] >= start_date) & (df["event_date"] <= end_date)

        result = df[ok_mask & date_mask].copy()
        logger.info(
            "Oklahoma tornadoes %s–%s: %d events on %d days",
            start_date,
            end_date,
            len(result),
            result["event_date"].nunique(),
        )
        return result

    def build_case_skeletons(
        self,
        start_date: date,
        end_date: date,
    ) -> list[HistoricalCase]:
        """
        Group Oklahoma tornado reports by date and build skeleton HistoricalCase objects.

        EventClass classification logic:
            0 tornadoes            → NULL_BUST (when on a day with SPC prob ≥ 5%)
                                     or skip (no severe weather day)
            1–2 weak (EF0–EF1)    → WEAK_OUTBREAK
            1–2 significant (EF2+)→ ISOLATED_SIGNIFICANT
            3+ tornadoes with EF2+→ SIGNIFICANT_OUTBREAK
            no tornadoes, sig hail/wind → SIGNIFICANT_SEVERE_NO_TORNADO
        """
        tornadoes = self.get_oklahoma_tornadoes(start_date, end_date)

        cases: list[HistoricalCase] = []
        grouped = tornadoes.groupby("event_date")

        for event_date, day_df in grouped:
            tornado_count = len(day_df)
            magnitudes = day_df[MAGNITUDE_COL].tolist()

            # Convert to EF scale (SPC uses F scale pre-2007, EF post-2007)
            max_mag = max((m for m in magnitudes if m >= 0), default=0)
            max_rating = _mag_to_tornado_rating(max_mag)

            event_class = _classify_event(tornado_count, max_mag, day_df)

            # County breakdown — SPC uses county FIPS in 'f1' column
            county_counts: dict[str, int] = {}
            if "f1" in day_df.columns:
                for _, row in day_df.iterrows():
                    county = _fips_to_county(int(row.get("f1", 0)))
                    if county:
                        key = county.name
                        county_counts[key] = county_counts.get(key, 0) + 1

            max_path = _safe_float_max(day_df, "len")
            max_hail = None  # tornado CSV doesn't include hail
            max_gust = None

            if isinstance(event_date, date):
                case_date = event_date
            else:
                case_date = event_date.date()

            case = HistoricalCase(
                case_id=HistoricalCase.make_case_id(case_date),
                date=case_date,
                event_class=event_class,
                tornado_count=tornado_count,
                max_tornado_rating=max_rating,
                max_path_length_miles=max_path,
                significant_severe=(max_mag >= 2),
                county_tornado_counts=county_counts,
            )
            cases.append(case)

        logger.info(
            "Built %d case skeletons from SPC data (%s–%s)",
            len(cases),
            start_date,
            end_date,
        )
        return cases

    def get_day1_outlook(self, outlook_date: date) -> Optional[dict]:
        """
        Retrieve the archived SPC Day 1 convective outlook for a given date.
        Returns a dict with 'tornado_probs' and 'risk_category' if available.

        Note: SPC outlook archive format varies by era. Pre-2003 outlooks may
        not be available in machine-readable form.
        """
        # SPC archives Day 1 outlooks at:
        # https://www.spc.noaa.gov/archive/YYYY/day1otlk_YYYYMMDD_HHMM_torn.lyr.geojson
        # This is a future enhancement; returning None until implemented.
        logger.debug("Day 1 outlook retrieval not yet implemented for %s", outlook_date)
        return None

    def close(self) -> None:
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ── Classification helpers ─────────────────────────────────────────────────────

def _classify_event(
    tornado_count: int,
    max_magnitude: int,
    day_df: pd.DataFrame,
) -> EventClass:
    """
    Automatically classify a day's event class based on count and intensity.

    This is a first-pass classification; manual review is expected for
    ambiguous cases (e.g. QLCS days, elevated convection days).
    """
    if tornado_count == 0:
        return EventClass.NULL_BUST

    sig_count = sum(1 for m in day_df[MAGNITUDE_COL] if m >= 2)

    if tornado_count >= 10 and max_magnitude >= 2:
        return EventClass.SIGNIFICANT_OUTBREAK
    elif tornado_count >= 3 and max_magnitude >= 2:
        return EventClass.SIGNIFICANT_OUTBREAK
    elif tornado_count >= 5 and max_magnitude >= 1:
        return EventClass.WEAK_OUTBREAK
    elif sig_count >= 1 and tornado_count <= 4:
        return EventClass.ISOLATED_SIGNIFICANT
    else:
        return EventClass.WEAK_OUTBREAK


def _mag_to_tornado_rating(mag: int) -> TornadoRating:
    """Convert integer F/EF magnitude to TornadoRating enum."""
    mapping = {
        0: TornadoRating.EF0,
        1: TornadoRating.EF1,
        2: TornadoRating.EF2,
        3: TornadoRating.EF3,
        4: TornadoRating.EF4,
        5: TornadoRating.EF5,
    }
    return mapping.get(mag, TornadoRating.UNKNOWN)


# Oklahoma county FIPS mapping (state FIPS=40, county FIPS 3-digit)
# Maps to OklahomaCounty enum members
_OK_COUNTY_FIPS: dict[int, str] = {
    1: "ADAIR", 3: "ALFALFA", 5: "ATOKA", 7: "BEAVER", 9: "BECKHAM",
    11: "BLAINE", 13: "BRYAN", 15: "CADDO", 17: "CANADIAN", 19: "CARTER",
    21: "CHEROKEE", 23: "CHOCTAW", 25: "CIMARRON", 27: "CLEVELAND", 29: "COAL",
    31: "COMANCHE", 33: "COTTON", 35: "CRAIG", 37: "CREEK", 39: "CUSTER",
    41: "DELAWARE", 43: "DEWEY", 45: "ELLIS", 47: "GARFIELD", 49: "GARVIN",
    51: "GRADY", 53: "GRANT", 55: "GREER", 57: "HARMON", 59: "HARPER",
    61: "HASKELL", 63: "HUGHES", 65: "JACKSON", 67: "JEFFERSON", 69: "JOHNSTON",
    71: "KAY", 73: "KINGFISHER", 75: "KIOWA", 77: "LATIMER", 79: "LE_FLORE",
    81: "LINCOLN", 83: "LOGAN", 85: "LOVE", 87: "MAJOR", 89: "MARSHALL",
    91: "MAYES", 93: "MCCLAIN", 95: "MCCURTAIN", 97: "MCINTOSH", 99: "MURRAY",
    101: "MUSKOGEE", 103: "NOBLE", 105: "NOWATA", 107: "OKFUSKEE", 109: "OKLAHOMA",
    111: "OKMULGEE", 113: "OSAGE", 115: "OTTAWA", 117: "PAWNEE", 119: "PAYNE",
    121: "PITTSBURG", 123: "PONTOTOC", 125: "POTTAWATOMIE", 127: "PUSHMATAHA",
    129: "ROGER_MILLS", 131: "ROGERS", 133: "SEMINOLE", 135: "SEQUOYAH",
    137: "STEPHENS", 139: "TEXAS", 141: "TILLMAN", 143: "TULSA",
    145: "WAGONER", 147: "WASHINGTON", 149: "WASHITA", 151: "WOODS",
    153: "WOODWARD",
}


def _fips_to_county(county_fips: int) -> Optional[OklahomaCounty]:
    """Convert Oklahoma county FIPS code to OklahomaCounty enum member."""
    name = _OK_COUNTY_FIPS.get(county_fips)
    if name is None:
        return None
    try:
        return OklahomaCounty[name]
    except KeyError:
        return None


def _safe_float_max(df: pd.DataFrame, col: str) -> Optional[float]:
    """Return max of a numeric column, or None if all missing."""
    if col not in df.columns:
        return None
    vals = pd.to_numeric(df[col], errors="coerce").dropna()
    return float(vals.max()) if len(vals) > 0 else None
