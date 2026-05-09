"""
Configuration for KRONOS-WX.

All paths and environment-specific settings are centralized here.
Copy .env.example to .env and fill in your API keys before running.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = Path(os.getenv("DATA_DIR", ROOT_DIR / "data"))
LOG_DIR = Path(os.getenv("LOG_DIR", ROOT_DIR / "logs"))

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── CDS / ERA5 ────────────────────────────────────────────────────────────────
# CDS_API_KEY format: "<UID>:<API-KEY>"
# Also accepted: CDSAPI_KEY (native cdsapi env var) + CDSAPI_URL
CDS_API_KEY = os.getenv("CDS_API_KEY", "")
CDSAPI_URL  = os.getenv("CDSAPI_URL", "https://cds.climate.copernicus.eu/api")

# ── NWS API ───────────────────────────────────────────────────────────────────
# api.weather.gov requires a contact email in the User-Agent header.
# Set this to your own email address when self-hosting.
NWS_CONTACT_EMAIL = os.getenv("NWS_CONTACT_EMAIL", "")

# ── Web Push ──────────────────────────────────────────────────────────────────
# VAPID contact URI sent to push services. Must be mailto: or https:.
VAPID_CONTACT = os.getenv("VAPID_CONTACT", "mailto:kronos@localhost")

# ── Date range ────────────────────────────────────────────────────────────────
# Mesonet became operational in 1994; use this as the start of the case library
CASE_LIBRARY_START_YEAR = int(os.getenv("CASE_LIBRARY_START_YEAR", 1994))
CASE_LIBRARY_END_YEAR = int(os.getenv("CASE_LIBRARY_END_YEAR", 2024))

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ── Rate limiting ─────────────────────────────────────────────────────────────
MESONET_REQUEST_DELAY = float(os.getenv("MESONET_REQUEST_DELAY", 1.1))
WYOMING_REQUEST_DELAY = float(os.getenv("WYOMING_REQUEST_DELAY", 2.0))

# ── Ground truth validation case ─────────────────────────────────────────────
VALIDATION_CASE_ID = "19990503_OK"  # May 3, 1999 Oklahoma tornado outbreak

# ── War Room Plugin ──────────────────────────────────────────────────────────
# Configuration for local news stream integration via bridge to tablo-web.
WAR_ROOM_ENABLED = os.getenv("WAR_ROOM_ENABLED", "false").lower() == "true"
WAR_ROOM_TABLO_WEB_HOST = os.getenv("WAR_ROOM_TABLO_WEB_HOST", "")
WAR_ROOM_KOCO_ID = os.getenv("WAR_ROOM_KOCO_ID", "")
WAR_ROOM_KFOR_ID = os.getenv("WAR_ROOM_KFOR_ID", "")
WAR_ROOM_KWTV_ID = os.getenv("WAR_ROOM_KWTV_ID", "")
