"""
All classification enumerations for the Oklahoma severe weather prediction model.
"""

from enum import Enum


class EventClass(str, Enum):
    SIGNIFICANT_OUTBREAK = "SIGNIFICANT_OUTBREAK"
    ISOLATED_SIGNIFICANT = "ISOLATED_SIGNIFICANT"
    WEAK_OUTBREAK = "WEAK_OUTBREAK"
    SIGNIFICANT_SEVERE_NO_TORNADO = "SIGNIFICANT_SEVERE_NO_TORNADO"
    NULL_BUST = "NULL_BUST"
    SURPRISING_OUTBREAK = "SURPRISING_OUTBREAK"


class StormMode(str, Enum):
    SUPERCELL_DOMINANT = "SUPERCELL_DOMINANT"
    SUPERCELL_TO_LINEAR = "SUPERCELL_TO_LINEAR"
    LINEAR_QLCS = "LINEAR_QLCS"
    MULTICELL_CLUSTER = "MULTICELL_CLUSTER"
    ELEVATED_CONVECTION = "ELEVATED_CONVECTION"
    MIXED_MODE = "MIXED_MODE"


class CapBehavior(str, Enum):
    CLEAN_EROSION = "CLEAN_EROSION"
    LATE_EROSION = "LATE_EROSION"
    NO_EROSION = "NO_EROSION"
    EARLY_EROSION = "EARLY_EROSION"
    BOUNDARY_FORCED = "BOUNDARY_FORCED"
    RECONSTITUTED = "RECONSTITUTED"


class ErosionMechanism(str, Enum):
    HEATING = "HEATING"
    BOUNDARY = "BOUNDARY"
    DYNAMIC = "DYNAMIC"
    COMBINED = "COMBINED"
    UNKNOWN = "UNKNOWN"


class HodographShape(str, Enum):
    CURVED = "CURVED"
    STRAIGHT = "STRAIGHT"
    HYBRID = "HYBRID"


class JetPosition(str, Enum):
    LEFT_EXIT = "LEFT_EXIT"
    RIGHT_ENTRANCE = "RIGHT_ENTRANCE"
    LEFT_ENTRANCE = "LEFT_ENTRANCE"
    RIGHT_EXIT = "RIGHT_EXIT"
    NONE = "NONE"


class ForecastVerification(str, Enum):
    OVERFORECAST = "OVERFORECAST"
    UNDERFORECAST = "UNDERFORECAST"
    VERIFIED = "VERIFIED"


class TornadoRating(str, Enum):
    EF0 = "EF0"
    EF1 = "EF1"
    EF2 = "EF2"
    EF3 = "EF3"
    EF4 = "EF4"
    EF5 = "EF5"
    UNKNOWN = "UNKNOWN"


class BoundaryType(str, Enum):
    DRYLINE = "DRYLINE"
    OUTFLOW = "OUTFLOW"
    OLD_MCS_REMNANT = "OLD_MCS_REMNANT"
    FRONTAL = "FRONTAL"
    DIFFERENTIAL_HEATING = "DIFFERENTIAL_HEATING"


class OklahomaSoundingStation(str, Enum):
    OUN = "OUN"  # Norman, OK — WMO 72357
    LMN = "LMN"  # Lamont, OK — WMO 74646
    AMA = "AMA"  # Amarillo, TX — WMO 72363  (western proximity sounding)
    DDC = "DDC"  # Dodge City, KS — WMO 72451 (northern proximity sounding)


class OklahomaCounty(Enum):
    """
    All 77 Oklahoma counties with embedded metadata.

    Each enum's attributes:
        county_seat        — county seat city name
        mesonet_station_id — primary Oklahoma Mesonet station ID (4-letter code)
        lat                — approximate county centroid latitude (°N)
        lon                — approximate county centroid longitude (negative = °W)
        region             — geographic region: PANHANDLE | WESTERN | CENTRAL | EASTERN

    Station assignments reflect the primary Mesonet station per county.
    Some counties have multiple Mesonet sites; only one is listed here.
    Verify against https://www.mesonet.org/index.php/station/list
    """

    # ── PANHANDLE ──────────────────────────────────────────────────────────────
    CIMARRON    = ("Boise City",   "BOIS", 36.73, -102.51, "PANHANDLE")
    TEXAS       = ("Guymon",       "GUYM", 36.74, -101.49, "PANHANDLE")
    BEAVER      = ("Beaver",       "BEAV", 36.82, -100.51, "PANHANDLE")

    # ── WESTERN ───────────────────────────────────────────────────────────────
    HARPER      = ("Buffalo",      "BUFF", 36.78,  -99.67, "WESTERN")
    WOODS       = ("Alva",         "ALVA", 36.75,  -98.77, "WESTERN")
    ALFALFA     = ("Cherokee",     "CHER", 36.72,  -98.32, "WESTERN")
    GRANT       = ("Medford",      "MEDF", 36.79,  -97.69, "WESTERN")
    WOODWARD    = ("Woodward",     "WOOD", 36.43,  -99.40, "WESTERN")
    MAJOR       = ("Fairview",     "FAIR", 36.28,  -98.65, "WESTERN")
    ELLIS       = ("Arnett",       "GAGE", 36.26,  -99.74, "WESTERN")
    DEWEY       = ("Taloga",       "VICI", 36.02,  -99.29, "WESTERN")
    ROGER_MILLS = ("Cheyenne",     "CHEY", 35.57, -100.05, "WESTERN")
    BECKHAM     = ("Sayre",        "BESS", 35.30,  -99.64, "WESTERN")
    CUSTER      = ("Arapaho",      "BUTL", 35.57,  -99.03, "WESTERN")
    KIOWA       = ("Hobart",       "HOBA", 35.02,  -99.09, "WESTERN")
    WASHITA     = ("Cordell",      "CORD", 35.30,  -99.02, "WESTERN")
    GREER       = ("Mangum",       "MANG", 34.93,  -99.57, "WESTERN")
    HARMON      = ("Hollis",       "HOLL", 34.73,  -99.85, "WESTERN")
    JACKSON     = ("Altus",        "ALTU", 34.64,  -99.36, "WESTERN")
    TILLMAN     = ("Frederick",    "FRED", 34.37,  -98.97, "WESTERN")
    COMANCHE    = ("Lawton",       "LAHO", 34.61,  -98.49, "WESTERN")
    COTTON      = ("Walters",      "WALT", 34.36,  -98.31, "WESTERN")

    # ── CENTRAL ───────────────────────────────────────────────────────────────
    KAY         = ("Newkirk",      "NEWK", 36.83,  -97.05, "CENTRAL")
    NOBLE       = ("Perry",        "PERK", 36.30,  -97.22, "CENTRAL")
    GARFIELD    = ("Enid",         "NINN", 36.38,  -97.89, "CENTRAL")
    KINGFISHER  = ("Kingfisher",   "KING", 35.86,  -97.94, "CENTRAL")
    BLAINE      = ("Watonga",      "WATF", 35.85,  -98.52, "CENTRAL")
    CANADIAN    = ("El Reno",      "BBOW", 35.54,  -97.97, "CENTRAL")
    LOGAN       = ("Guthrie",      "GUTH", 35.87,  -97.42, "CENTRAL")
    OKLAHOMA    = ("Oklahoma City","OKCE", 35.47,  -97.52, "CENTRAL")
    CADDO       = ("Anadarko",     "ACME", 35.15,  -98.24, "CENTRAL")
    CLEVELAND   = ("Norman",       "NORM", 35.22,  -97.44, "CENTRAL")
    GRADY       = ("Chickasha",    "CHIC", 35.05,  -97.97, "CENTRAL")
    MCCLAIN     = ("Purcell",      "PURC", 34.97,  -97.36, "CENTRAL")
    GARVIN      = ("Pauls Valley", "BYAR", 34.80,  -97.28, "CENTRAL")
    STEPHENS    = ("Duncan",       "DUNC", 34.51,  -97.95, "CENTRAL")
    JEFFERSON   = ("Waurika",      "WATO", 34.17,  -97.98, "CENTRAL")
    MURRAY      = ("Sulphur",      "SULP", 34.51,  -96.97, "CENTRAL")
    CARTER      = ("Ardmore",      "ARDM", 34.17,  -97.12, "CENTRAL")
    LOVE        = ("Marietta",     "MARE", 33.98,  -97.31, "CENTRAL")
    MARSHALL    = ("Madill",       "MADI", 34.11,  -96.77, "CENTRAL")
    JOHNSTON    = ("Tishomingo",   "TISH", 34.23,  -96.68, "CENTRAL")
    PONTOTOC    = ("Ada",          "BURN", 34.77,  -96.68, "CENTRAL")
    SEMINOLE    = ("Wewoka",       "SLAP", 35.05,  -96.57, "CENTRAL")
    POTTAWATOMIE = ("Shawnee",     "SHAW", 35.13,  -96.93, "CENTRAL")

    # ── EASTERN ───────────────────────────────────────────────────────────────
    OSAGE       = ("Pawhuska",     "BREC", 36.62,  -96.38, "EASTERN")
    CRAIG       = ("Vinita",       "COOK", 36.64,  -95.16, "EASTERN")
    NOWATA      = ("Nowata",       "NOWA", 36.69,  -95.63, "EASTERN")
    WASHINGTON  = ("Bartlesville", "WYNO", 36.73,  -95.98, "EASTERN")
    OTTAWA      = ("Miami",        "MIAM", 36.87,  -94.82, "EASTERN")
    ROGERS      = ("Claremore",    "BRIS", 36.31,  -95.61, "EASTERN")
    MAYES       = ("Pryor Creek",  "PRYO", 36.27,  -95.32, "EASTERN")
    DELAWARE    = ("Jay",          "JAYX", 36.42,  -94.80, "EASTERN")
    PAWNEE      = ("Pawnee",       "BURB", 36.33,  -96.80, "EASTERN")
    PAYNE       = ("Stillwater",   "STIL", 36.12,  -96.99, "EASTERN")
    LINCOLN     = ("Chandler",     "CHAN", 35.70,  -96.89, "EASTERN")
    CREEK       = ("Sapulpa",      "SKIA", 35.97,  -96.27, "EASTERN")
    TULSA       = ("Tulsa",        "BIXB", 36.15,  -95.99, "EASTERN")
    WAGONER     = ("Wagoner",      "WAGO", 35.96,  -95.37, "EASTERN")
    CHEROKEE    = ("Tahlequah",    "TAHL", 35.97,  -94.87, "EASTERN")
    ADAIR       = ("Stilwell",     "ADAX", 35.88,  -94.67, "EASTERN")
    SEQUOYAH    = ("Sallisaw",     "SALL", 35.50,  -94.78, "EASTERN")
    LE_FLORE    = ("Poteau",       "POTA", 34.97,  -94.62, "EASTERN")
    HASKELL     = ("Stigler",      "STIG", 35.25,  -95.17, "EASTERN")
    LATIMER     = ("Wilburton",    "WILB", 34.92,  -95.30, "EASTERN")
    PITTSBURG   = ("McAlester",    "MCAL", 34.93,  -95.78, "EASTERN")
    COAL        = ("Coalgate",     "COAL", 34.57,  -96.22, "EASTERN")
    ATOKA       = ("Atoka",        "ATOY", 34.38,  -96.13, "EASTERN")
    HUGHES      = ("Holdenville",  "HENR", 35.08,  -96.10, "EASTERN")
    OKFUSKEE    = ("Okemah",       "OKEM", 35.43,  -96.31, "EASTERN")
    OKMULGEE    = ("Okmulgee",     "OKMU", 35.63,  -95.97, "EASTERN")
    MUSKOGEE    = ("Muskogee",     "MUSK", 35.75,  -95.37, "EASTERN")
    MCINTOSH    = ("Eufaula",      "EUFA", 35.29,  -95.58, "EASTERN")
    BRYAN       = ("Durant",       "DURC", 33.99,  -96.37, "EASTERN")
    CHOCTAW     = ("Hugo",         "HUGO", 34.01,  -95.54, "EASTERN")
    PUSHMATAHA  = ("Antlers",      "ANTL", 34.23,  -95.62, "EASTERN")
    MCCURTAIN   = ("Idabel",       "IDAB", 34.08,  -94.87, "EASTERN")

    def __init__(self, county_seat, mesonet_station_id, lat, lon, region):
        self.county_seat = county_seat
        self.mesonet_station_id = mesonet_station_id
        self.lat = lat
        self.lon = lon
        self.region = region

    @classmethod
    def from_mesonet_station(cls, station_id: str) -> "OklahomaCounty":
        """Look up county by Mesonet station ID."""
        for member in cls:
            if member.mesonet_station_id == station_id:
                return member
        raise ValueError(f"No county found for Mesonet station: {station_id}")

    @classmethod
    def by_region(cls, region: str) -> list["OklahomaCounty"]:
        """Return all counties in a geographic region."""
        return [m for m in cls if m.region == region]

    def __str__(self) -> str:
        return self.name

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        """
        Pydantic v2 schema: serialize as the enum name (string) and accept
        name strings or list/tuple values on deserialization.

        JSON serialization stores e.g. "CLEVELAND" instead of the raw tuple,
        which round-trips cleanly across JSON with no ambiguity.
        """
        from pydantic_core import core_schema

        def _validate(value):
            if isinstance(value, cls):
                return value
            if isinstance(value, str):
                try:
                    return cls[value]           # name lookup: "CLEVELAND"
                except KeyError:
                    pass
                # Legacy: value as county_seat string
                for m in cls:
                    if m.county_seat == value:
                        return m
            if isinstance(value, (list, tuple)):
                tup = tuple(value)
                for m in cls:
                    if m.value == tup:
                        return m
            raise ValueError(f"Cannot convert {value!r} to OklahomaCounty")

        return core_schema.no_info_plain_validator_function(
            _validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: v.name,
                info_arg=False,
            ),
        )
