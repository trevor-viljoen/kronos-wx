"""
Storm Prediction Center (SPC) archive client.

Data sources:
    Tornado database: https://www.spc.noaa.gov/wcm/data/1950-2023_actual_tornadoes.csv
    Day 1 outlooks: https://www.spc.noaa.gov/archive/

This client builds the skeleton HistoricalCase library from tornado reports,
automatically classifying each day's event type from count and intensity data.
"""

import io
import json
import logging
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
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

# SPC Day 1 outlook KMZ archive
_SPC_OUTLOOK_BASE = (
    "https://www.spc.noaa.gov/products/outlook/archive/{year}/"
    "day1otlk_{yyyymmdd}_{hhmm}.kmz"
)
_OUTLOOK_TIMES = ["1300", "1200", "1630", "2000", "0100"]

# Local cache: data/spc_outlooks/YYYY.json  →  {YYYYMMDD: prob_float}
_OUTLOOK_CACHE_DIR = (
    Path(__file__).resolve().parent.parent.parent / "data" / "spc_outlooks"
)

# Oklahoma representative sample points [lat, lon] — used for polygon intersection
_OK_SAMPLE_POINTS: list[tuple[float, float]] = [
    (35.47, -97.52),   # OKC
    (36.15, -95.99),   # Tulsa
    (34.60, -98.39),   # Lawton
    (36.39, -97.88),   # Enid
    (35.22, -97.44),   # Norman / OUN
    (34.90, -95.97),   # McAlester
    (35.85, -96.94),   # Cushing
    (36.78, -98.67),   # Woodward
    (34.36, -99.32),   # Altus
    (35.98, -94.93),   # Tahlequah
]

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

        # SPC 'st' column is the two-letter state abbreviation
        ok_mask = df["st"] == OKLAHOMA_STATE_ABBR
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
        """Legacy stub — use get_day1_outlook_torn_prob() for probability data."""
        prob = self.get_day1_outlook_torn_prob(outlook_date)
        if prob is None:
            return None
        return {"tornado_probs": prob, "risk_category": None}

    def get_day1_outlook_torn_prob(self, outlook_date: date) -> Optional[float]:
        """
        Return the maximum SPC Day 1 tornado probability over Oklahoma (0.0–1.0).

        Fetches the KMZ archive for the 1300Z issuance (falling back to 1200Z,
        1630Z, 2000Z, 0100Z). Results are cached in data/spc_outlooks/YYYY.json
        to avoid repeat downloads. Returns None if no outlook file is available
        (archive only covers 2003+; missing files treated as None, not 0%).
        """
        date_str = outlook_date.strftime("%Y%m%d")
        year_str = outlook_date.strftime("%Y")

        # Check local cache first
        cache = _load_outlook_cache(outlook_date.year)
        if date_str in cache:
            v = cache[date_str]
            return None if v is None else float(v)

        # Try each issuance time until one succeeds
        prob: Optional[float] = None
        for hhmm in _OUTLOOK_TIMES:
            url = _SPC_OUTLOOK_BASE.format(
                year=year_str, yyyymmdd=date_str, hhmm=hhmm
            )
            try:
                resp = self._http.get(url, follow_redirects=True, timeout=30.0)
                if resp.status_code == 200 and b"PK" in resp.content[:4]:
                    prob = _parse_kmz_ok_torn_prob(resp.content)
                    logger.debug(
                        "Outlook %s %sZ → max OK torn prob %.0f%%",
                        date_str, hhmm, (prob or 0) * 100,
                    )
                    break
                elif resp.status_code == 404:
                    continue
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                logger.debug("Outlook fetch failed %s %sZ: %s", date_str, hhmm, exc)
                continue

        # Cache and return (None = no file found, distinct from 0%)
        cache[date_str] = prob
        _save_outlook_cache(outlook_date.year, cache)
        return prob

    def build_null_bust_skeletons(
        self,
        start_date: date,
        end_date: date,
        spc_threshold: float = 0.10,
        existing_dates: Optional[set[date]] = None,
    ) -> list[HistoricalCase]:
        """
        Build NULL_BUST HistoricalCase skeletons for days where SPC Day 1
        tornado probability over Oklahoma was ≥ spc_threshold but actual
        Oklahoma tornado count was 0.

        Skips dates already present in existing_dates (to avoid duplicates).
        Archive only covers 2003-01-01 onward; earlier dates are skipped.

        Returns a list of un-enriched HistoricalCase skeletons ready for
        enrich-case or enrich-all.
        """
        tornado_df = self._load_tornado_csv()
        ok_torn = tornado_df[tornado_df["st"] == OKLAHOMA_STATE_ABBR]
        ok_tornado_dates: set[date] = set(ok_torn["event_date"].dropna().unique())

        archive_start = date(2003, 1, 1)
        effective_start = max(start_date, archive_start)

        cases: list[HistoricalCase] = []
        current = effective_start
        skipped_no_file = 0
        skipped_existing = 0
        checked = 0

        while current <= end_date:
            if existing_dates and current in existing_dates:
                current += timedelta(days=1)
                skipped_existing += 1
                continue

            if current in ok_tornado_dates:
                current += timedelta(days=1)
                continue

            checked += 1
            prob = self.get_day1_outlook_torn_prob(current)

            if prob is None:
                skipped_no_file += 1
                current += timedelta(days=1)
                continue

            if prob >= spc_threshold:
                case_date = current
                case = HistoricalCase(
                    case_id=HistoricalCase.make_case_id(case_date),
                    date=case_date,
                    event_class=EventClass.NULL_BUST,
                    tornado_count=0,
                    max_tornado_rating=None,
                    significant_severe=False,
                    SPC_max_tornado_prob=prob,
                )
                cases.append(case)
                logger.info(
                    "NULL_BUST candidate: %s  SPC torn prob %.0f%%",
                    case_date.isoformat(), prob * 100,
                )

            current += timedelta(days=1)

        logger.info(
            "Bust scan %s–%s: checked %d no-tornado days, found %d NULL_BUST "
            "candidates, %d no-file days, %d skipped (already in DB)",
            effective_start, end_date, checked, len(cases),
            skipped_no_file, skipped_existing,
        )
        return cases

    def close(self) -> None:
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ── SPC outlook KMZ parsing ───────────────────────────────────────────────────


def _point_in_ring(lat: float, lon: float, ring: list[tuple[float, float]]) -> bool:
    """Ray-casting point-in-polygon. ring is [(lon, lat), ...] pairs."""
    n = len(ring)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i]   # lon, lat
        xj, yj = ring[j]
        if (yi > lat) != (yj > lat):
            if lon < (xj - xi) * (lat - yi) / (yj - yi) + xi:
                inside = not inside
        j = i
    return inside


def _parse_kmz_ok_torn_prob(kmz_bytes: bytes) -> Optional[float]:
    """
    Parse a SPC Day 1 KMZ file and return max tornado probability (0.0–1.0)
    over any Oklahoma sample point.  Returns 0.0 if the file parses but no
    probability polygons overlap Oklahoma; None if the KML is unreadable.

    KMZ structure: ZIP → doc.kml (or *.kml)
    KML structure: Folders named "10 %" etc. containing Placemark Polygons.
    Coordinates are lon,lat,alt space-separated tuples.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(kmz_bytes)) as zf:
            kml_names = [n for n in zf.namelist() if n.endswith(".kml")]
            if not kml_names:
                return None
            kml_bytes = zf.read(kml_names[0])
    except (zipfile.BadZipFile, KeyError):
        return None

    try:
        root = ET.fromstring(kml_bytes)
    except ET.ParseError:
        return None

    # Strip XML namespaces for simpler matching
    ns_prefix = ""
    if root.tag.startswith("{"):
        ns_prefix = root.tag.split("}")[0] + "}"

    def tag(name: str) -> str:
        return f"{ns_prefix}{name}"

    max_prob: float = 0.0

    # Walk all Folders and Placemarks looking for probability names
    for elem in root.iter():
        local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

        if local not in ("Folder", "Placemark"):
            continue

        # Extract probability from name or description
        prob_val: Optional[float] = None
        name_el = elem.find(f".//{tag('name')}")
        if name_el is not None and name_el.text:
            prob_val = _parse_prob_text(name_el.text)
        if prob_val is None:
            desc_el = elem.find(f".//{tag('description')}")
            if desc_el is not None and desc_el.text:
                prob_val = _parse_prob_text(desc_el.text)

        if prob_val is None or prob_val < 0.05:
            continue

        # Extract polygon coordinates
        for coords_el in elem.iter(tag("coordinates")):
            if coords_el.text is None:
                continue
            ring = _parse_kml_coordinates(coords_el.text)
            if len(ring) < 3:
                continue
            for lat, lon in _OK_SAMPLE_POINTS:
                if _point_in_ring(lat, lon, ring):
                    max_prob = max(max_prob, prob_val)
                    break

    return max_prob


def _parse_prob_text(text: str) -> Optional[float]:
    """Extract probability float from strings like '10 %', '0.10', '10%', 'DN=10'."""
    import re
    text = text.strip()
    # DN=10 or DN: 10
    m = re.search(r"DN\s*[=:]\s*(\d+)", text, re.IGNORECASE)
    if m:
        return int(m.group(1)) / 100.0
    # '30 %' or '30%'
    m = re.search(r"(\d+)\s*%", text)
    if m:
        v = int(m.group(1))
        return v / 100.0 if v <= 1 else v / 100.0
    # '0.10' or '0.30'
    m = re.fullmatch(r"0?\.\d+", text)
    if m:
        return float(text)
    # bare integer like '10' (from <name>10</name>)
    m = re.fullmatch(r"(\d+)", text.strip())
    if m:
        v = int(m.group(1))
        if 2 <= v <= 60:
            return v / 100.0
    return None


def _parse_kml_coordinates(text: str) -> list[tuple[float, float]]:
    """Parse KML coordinates string into (lon, lat) pairs."""
    ring: list[tuple[float, float]] = []
    for token in text.strip().split():
        parts = token.split(",")
        if len(parts) >= 2:
            try:
                ring.append((float(parts[0]), float(parts[1])))
            except ValueError:
                pass
    return ring


def _load_outlook_cache(year: int) -> dict[str, Optional[float]]:
    """Load cached outlook probabilities for a year."""
    path = _OUTLOOK_CACHE_DIR / f"{year}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_outlook_cache(year: int, cache: dict[str, Optional[float]]) -> None:
    """Persist outlook probability cache for a year."""
    _OUTLOOK_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _OUTLOOK_CACHE_DIR / f"{year}.json"
    path.write_text(json.dumps(cache, separators=(",", ":")))


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
