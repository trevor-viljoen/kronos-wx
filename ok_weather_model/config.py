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
# Configure via ~/.cdsapirc OR via environment variable
# CDS_API_KEY format: "<UID>:<API-KEY>"
CDS_API_KEY = os.getenv("CDS_API_KEY", "")

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
