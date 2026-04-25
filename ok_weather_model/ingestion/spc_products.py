"""
SPC real-time products: Mesoscale Discussions and Day 1 convective outlook.

Mesoscale Discussions (MDs) are issued by SPC meteorologists when they are
actively watching a developing severe weather situation.  They precede Tornado
Watches by 20–60 minutes and contain explicit expert reasoning that
complements KRONOS-WX's automated diagnostics.

Day 1 outlook: highest categorical level and max tornado probability over
the Oklahoma bounding box, extracted from SPC's GeoJSON products.

Both products are suitable for polling every 10–15 minutes.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ── URLs ──────────────────────────────────────────────────────────────────────

_MD_INDEX_URL = "https://www.spc.noaa.gov/products/md/"
_MD_BASE_URL  = "https://www.spc.noaa.gov"

_D1_CAT_URL   = "https://www.spc.noaa.gov/products/outlook/day1otlk_cat.nolyr.geojson"
_D1_TORN_URL  = "https://www.spc.noaa.gov/products/outlook/day1otlk_torn.nolyr.geojson"
_NWS_ALERT_URL = "https://api.weather.gov/alerts/active?area=OK"
_NWS_UA        = "kronos-wx/0.1 trevor.viljoen@gmail.com"

# Oklahoma bounding box for outlook intersection check (rough)
_OK_LAT_MIN, _OK_LAT_MAX = 33.5, 37.0
_OK_LON_MIN, _OK_LON_MAX = -103.0, -94.5

# ── Oklahoma mention terms ────────────────────────────────────────────────────
# Lowercase; checked against full MD text.
_OK_TERMS = frozenset({
    "oklahoma",
    "central ok", "northern ok", "southern ok", "western ok", "eastern ok",
    "south-central ok", "north-central ok",
    "tulsa", "oklahoma city", "enid", "lawton", "norman",
})


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class NWSAlert:
    """
    One active NWS alert product for Oklahoma.

    Priority order for display (highest first):
        Tornado Warning > Tornado Watch > Severe Thunderstorm Warning > other
    """
    event:        str             # e.g. "Tornado Watch"
    headline:     str
    expires_utc:  Optional[datetime]
    area_desc:    str             # comma-separated county names
    watch_number: Optional[int] = None  # parsed from headline for watches

    @property
    def priority(self) -> int:
        if "Tornado Warning" in self.event:    return 4
        if "Tornado Watch"   in self.event:    return 3
        if "Severe Thunderstorm Warning" in self.event: return 2
        return 1

    @property
    def expires_label(self) -> str:
        if self.expires_utc is None:
            return ""
        return self.expires_utc.strftime("%H:%MZ")


@dataclass
class MesoscaleDiscussion:
    """
    One SPC Mesoscale Discussion.

    ``body_lines`` holds the first few lines of the discussion text body,
    suitable for display in a terminal panel.
    """
    number:            int
    url:               str
    areas_affected:    str = ""
    concerning:        str = ""
    issued_utc:        Optional[datetime] = None
    body_lines:        list[str] = field(default_factory=list)
    mentions_oklahoma: bool = False


@dataclass
class SPCOutlook:
    """
    Current SPC Day 1 convective outlook summary for Oklahoma.
    """
    category:                str            # TSTM / MRGL / SLGT / ENH / MDT / HIGH / NONE
    max_tornado_prob:         Optional[float]  # 0.02, 0.05, 0.10 …
    sig_tornado_hatched:      bool = False  # CIG1 / SIGN hatch intersects Oklahoma
    issued_utc:               Optional[datetime] = None
    valid_label:              str = ""


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_active_mds(timeout: float = 12.0) -> list[MesoscaleDiscussion]:
    """
    Fetch currently active SPC Mesoscale Discussions.

    Returns a list filtered to those mentioning Oklahoma.  If no active MDs
    mention Oklahoma, returns up to 3 of the most recent active MDs.
    Returns [] on network error or when no MDs are active.
    """
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            return _fetch_mds_with_client(client)
    except Exception as exc:
        logger.warning("SPC MD fetch failed: %s", exc)
        return []


def fetch_active_watches_warnings(timeout: float = 12.0) -> list[NWSAlert]:
    """
    Fetch active NWS watch and warning products for Oklahoma via the NWS API.

    Returns Tornado Warnings, Tornado Watches, and Severe Thunderstorm
    Warnings sorted by priority (highest first), then by expiry.
    Returns [] on network error.
    """
    _WANTED = frozenset({
        "Tornado Warning",
        "Tornado Watch",
        "Severe Thunderstorm Warning",
    })

    try:
        with httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": _NWS_UA},
        ) as client:
            resp = client.get(_NWS_ALERT_URL)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.warning("NWS alerts fetch failed: %s", exc)
        return []

    now_utc = datetime.now(tz=timezone.utc)
    alerts: list[NWSAlert] = []

    for feature in data.get("features", []):
        props = feature.get("properties", {})
        event = props.get("event", "")
        if event not in _WANTED:
            continue

        # Parse expiry
        expires_utc: Optional[datetime] = None
        raw_exp = props.get("expires") or props.get("ends")
        if raw_exp:
            try:
                expires_utc = datetime.fromisoformat(raw_exp).astimezone(timezone.utc)
            except ValueError:
                pass

        # Skip already-expired products
        if expires_utc and expires_utc < now_utc:
            continue

        headline  = props.get("headline", "")
        area_desc = props.get("areaDesc", "")

        # Extract watch number from headline ("Tornado Watch Number 141")
        watch_number: Optional[int] = None
        m = re.search(r"Watch\s+(?:Number\s+)?(\d+)", headline, re.IGNORECASE)
        if m:
            watch_number = int(m.group(1))

        alerts.append(NWSAlert(
            event=event,
            headline=headline,
            expires_utc=expires_utc,
            area_desc=area_desc,
            watch_number=watch_number,
        ))

    # Deduplicate watches by number (multiple WFOs issue the same watch)
    seen_watch_nums: set[int] = set()
    deduped: list[NWSAlert] = []
    for a in alerts:
        if a.watch_number is not None:
            if a.watch_number in seen_watch_nums:
                continue
            seen_watch_nums.add(a.watch_number)
        deduped.append(a)

    deduped.sort(key=lambda a: (-a.priority, a.expires_utc or now_utc))
    return deduped


def fetch_spc_outlook(timeout: float = 12.0) -> Optional[SPCOutlook]:
    """
    Fetch the current SPC Day 1 convective outlook for Oklahoma.

    Returns an SPCOutlook with the highest categorical level and maximum
    tornado probability contour that intersects the Oklahoma domain.
    Returns None on network error.
    """
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            return _fetch_outlook_with_client(client)
    except Exception as exc:
        logger.warning("SPC outlook fetch failed: %s", exc)
        return None


# ── Internal helpers ──────────────────────────────────────────────────────────

def _fetch_mds_with_client(client: httpx.Client) -> list[MesoscaleDiscussion]:
    try:
        resp = client.get(_MD_INDEX_URL)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("SPC MD index unreachable: %s", exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")

    # Collect links to individual MD pages
    md_links: list[tuple[int, str]] = []
    seen: set[int] = set()
    for a in soup.find_all("a", href=True):
        m = re.search(r"/products/md/md(\d+)\.html", a["href"])
        if m:
            num = int(m.group(1))
            if num not in seen:
                seen.add(num)
                url = _MD_BASE_URL + a["href"]
                md_links.append((num, url))

    if not md_links:
        logger.debug("SPC MD index: no active MDs found")
        return []

    # Fetch up to 6 MDs (cap to avoid hammering SPC servers)
    results: list[MesoscaleDiscussion] = []
    for num, url in md_links[:6]:
        md = _fetch_single_md(client, num, url)
        if md is not None:
            results.append(md)

    return [md for md in results if md.mentions_oklahoma]


def _fetch_single_md(
    client: httpx.Client,
    number: int,
    url: str,
) -> Optional[MesoscaleDiscussion]:
    try:
        resp = client.get(url)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("MD %04d fetch failed: %s", number, exc)
        return None

    soup = BeautifulSoup(resp.text, "lxml")

    # SPC MD text lives in a <pre> element
    pre = soup.find("pre")
    if pre is None:
        logger.debug("MD %04d: no <pre> block found", number)
        return None

    raw = pre.get_text()
    lines = [ln.strip() for ln in raw.splitlines()]

    areas_affected = ""
    concerning     = ""
    issued_utc:    Optional[datetime] = None
    body_lines:    list[str] = []

    # Track parse state: skip the 3-line header (title / office / time),
    # then collect body after the "Valid" line, stop at footer junk.
    header_lines_seen = 0
    past_valid = False
    _FOOTER_PREFIXES = ("ATTN", "LAT...LON", "LAT...LON", "MOST PROBABLE",
                        "WATCH COUNTY", "&&", "$$")

    for line in lines:
        if not line:
            continue

        upper = line.upper()

        # Skip the 3-line MD header (title, office, time)
        if header_lines_seen < 3:
            header_lines_seen += 1
            continue

        # Areas affected / Concerning keyword lines
        if upper.startswith("AREAS AFFECTED"):
            parts = line.split("...", 1)
            areas_affected = parts[1].strip() if len(parts) > 1 else ""
            continue

        if upper.startswith("CONCERNING"):
            parts = line.split("...", 1)
            concerning = parts[1].strip() if len(parts) > 1 else ""
            continue

        # "Valid HHMMZ - HHMMZ" marks start of the discussion body
        if upper.startswith("VALID"):
            past_valid = True
            continue

        # Stop at footer junk
        if any(upper.startswith(p) for p in _FOOTER_PREFIXES):
            break

        # Collect body lines
        if past_valid and len(body_lines) < 50:
            body_lines.append(line)

    # Check only the content fields — not the raw text, which always contains
    # "NWS Storm Prediction Center Norman OK" in the header, causing every MD
    # to match regardless of the actual affected area.
    content_lower = " ".join(
        [areas_affected, concerning] + body_lines
    ).lower()
    mentions_ok = any(term in content_lower for term in _OK_TERMS)

    return MesoscaleDiscussion(
        number=number,
        url=url,
        areas_affected=areas_affected,
        concerning=concerning,
        issued_utc=issued_utc,
        body_lines=body_lines,
        mentions_oklahoma=mentions_ok,
    )


# ── Day 1 outlook ─────────────────────────────────────────────────────────────

# Categorical level ordering (highest = most dangerous)
_CAT_RANK = {
    "HIGH":  6,
    "MDT":   5,
    "ENH":   4,
    "SLGT":  3,
    "MRGL":  2,
    "TSTM":  1,
    "NONE":  0,
}


def _fetch_outlook_with_client(client: httpx.Client) -> Optional[SPCOutlook]:
    # Categorical outlook
    try:
        cat_resp = client.get(_D1_CAT_URL)
        cat_resp.raise_for_status()
        cat_data = cat_resp.json()
    except Exception as exc:
        logger.warning("SPC D1 categorical fetch failed: %s", exc)
        return None

    best_cat = "NONE"
    valid_label = ""

    for feature in cat_data.get("features", []):
        props = feature.get("properties", {})
        # LABEL has the short code (TSTM/MRGL/SLGT/ENH/MDT/HIGH);
        # LABEL2 is the long description ("Marginal Risk") — do not use for ranking
        label = (props.get("LABEL") or "").upper().strip()
        if not label:
            continue
        # Check if any polygon vertex is inside OK bounding box (fast proxy)
        if _geojson_intersects_ok(feature.get("geometry", {})):
            if _CAT_RANK.get(label, 0) > _CAT_RANK.get(best_cat, 0):
                best_cat = label
        valid_label = props.get("VALID", "") or valid_label

    # Tornado probability + significant hatch
    max_torn_prob: Optional[float] = None
    sig_hatched = False
    try:
        torn_resp = client.get(_D1_TORN_URL)
        torn_resp.raise_for_status()
        torn_data = torn_resp.json()
        for feature in torn_data.get("features", []):
            props = feature.get("properties", {})
            label = (props.get("LABEL") or "").strip()
            if not label:
                continue

            geom = feature.get("geometry", {})

            # Significant tornado hatch: CIG1 (current) or SIGN (legacy)
            if label.upper() in ("CIG1", "SIGN"):
                if _geojson_intersects_ok(geom):
                    sig_hatched = True
                continue

            try:
                raw = float(label.replace("%", "").strip())
                # LABEL is already a decimal (0.02, 0.05 …); only divide if
                # it looks like a whole-number percentage (e.g. "5", "15")
                prob = raw / 100.0 if raw > 1.0 else raw
            except ValueError:
                continue
            if _geojson_intersects_ok(geom) and (
                max_torn_prob is None or prob > max_torn_prob
            ):
                max_torn_prob = prob
    except Exception as exc:
        logger.debug("SPC D1 tornado prob fetch failed: %s", exc)

    return SPCOutlook(
        category=best_cat,
        max_tornado_prob=max_torn_prob,
        sig_tornado_hatched=sig_hatched,
        valid_label=valid_label,
    )


def _geojson_intersects_ok(geometry: dict) -> bool:
    """
    Fast approximate check: does any coordinate in a GeoJSON geometry fall
    within the Oklahoma bounding box?
    """
    coords_list = _extract_coords(geometry)
    for lon, lat in coords_list:
        if (
            _OK_LAT_MIN <= lat <= _OK_LAT_MAX
            and _OK_LON_MIN <= lon <= _OK_LON_MAX
        ):
            return True
    return False


def _extract_coords(geometry: dict) -> list[tuple[float, float]]:
    """Flatten all [lon, lat] pairs from a GeoJSON geometry."""
    gtype = geometry.get("type", "")
    coords = geometry.get("coordinates", [])

    if gtype == "Point":
        return [tuple(coords[:2])]  # type: ignore[return-value]
    if gtype in ("LineString", "MultiPoint"):
        return [tuple(c[:2]) for c in coords]  # type: ignore[return-value]
    if gtype in ("Polygon", "MultiLineString"):
        return [tuple(c[:2]) for ring in coords for c in ring]  # type: ignore[return-value]
    if gtype == "MultiPolygon":
        return [tuple(c[:2]) for poly in coords for ring in poly for c in ring]  # type: ignore[return-value]
    return []
