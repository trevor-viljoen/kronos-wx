# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests
python -m pytest tests/

# Run a single test file
python -m pytest tests/test_cap_calculator.py

# Run a single test by name
python -m pytest tests/test_cap_calculator.py::TestComputeCESFromSounding::test_may3_1999_physics

# Install dependencies
pip install -r requirements.txt

# CLI entry point
python main.py --help
python main.py build-case-skeleton --start-year 1999 --end-year 1999
python main.py enrich-case 19990503_OK
python main.py enrich-all 1994 2024
python main.py compute-ces --start-year 1994 --end-year 2024
python main.py analyze-cap-behavior 19990503_OK
python main.py build-bust-database
```

## Architecture

KRONOS-WX is an Oklahoma severe weather case library and analysis system. The pipeline has four stages:

**1. Ingestion** (`ok_weather_model/ingestion/`)
- `SPCClient` — downloads the SPC tornado database and groups Oklahoma tornado days into `HistoricalCase` skeletons
- `SoundingClient` — fetches rawinsonde soundings from the University of Wyoming archive (OUN/LMN/AMA/DDC stations, 00Z and 12Z)
- `MesonetClient` — pulls Oklahoma Mesonet surface observations
- `ERA5Client` — fetches reanalysis fields via `cdsapi` (CDS API key required in `.env`)

**2. Processing** (`ok_weather_model/processing/`)
- `sounding_parser.py` — converts raw `SoundingProfile` → `ThermodynamicIndices` + `KinematicProfile` using MetPy; computes CAPE/CIN, LCL/LFC/EL, cap strength, EML detection, lapse rates, composite parameters (STP, SCP, EHI)
- `cap_calculator.py` — the central diagnostic; implements the Cap Erosion Budget framework (instantaneous balance sheet of erosion/preservation forcings) and the Cap Erosion Score (CES), a sounding-only heating model that estimates when Tc will be reached
- `era5_diagnostics.py` — bridges ERA5 grids to cap diagnostics: `compute_thermal_advection()` computes `ADV(T) = -(u·∂T/∂x + v·∂T/∂y)` in K/hr via xarray differentiation; `compute_synoptic_cap_forcing()` aggregates 700/500mb advection + omega into a `dynamic_cap_forcing_jkg_hr` term for the budget; `extract_virtual_sounding()` extracts a `SoundingProfile(raw_source="virtual")` at any lat/lon grid point from ERA5 pressure-level fields

**3. Storage** (`ok_weather_model/storage/database.py`)
- Two-tier: SQLite for `HistoricalCase` metadata and indexed queries; Parquet (via pyarrow) for `MesonetTimeSeries` and `SoundingProfile` level data
- `Database.save_case()` / `load_case()` use full JSON serialization via Pydantic for round-trip fidelity; all indexed columns are denormalized for fast SQL queries

**4. Models** (`ok_weather_model/models/`)
- All models are frozen Pydantic `BaseModel`s. Key types:
  - `SoundingProfile` / `SoundingLevel` — raw radiosonde data
  - `ThermodynamicIndices` — derived CAPE/CIN/cap diagnostics (MLCIN stored as positive magnitude)
  - `CapErosionBudget` / `CapErosionTrajectory` — per-hour and full-day cap analysis
  - `HistoricalCase` — the top-level case record aggregating all data
  - `OklahomaCounty` — enum with embedded metadata (county seat, Mesonet station ID, lat/lon, region)

## Key Domain Concepts

**Cap strength** (°C): max temperature excess of environment over a surface parcel between LCL and LFC. The cap is the EML warm nose at ~600–700mb that suppresses convection until the afternoon.

**Convective temperature (Tc)** (°F): surface temperature at which surface-based CIN drops to near zero (free convection imminent). The CES model estimates when daytime heating will reach Tc.

**Cap Erosion Score (CES)**: sounding-only model in `compute_ces_from_sounding()`. Derives an effective surface temperature needed to break the cap:
```
T_eff = T_12Z + (cap_strength × 4.5) + (MLCIN / 8.0)
```
Then steps through an Oklahoma climatological heating curve (sinusoidal 12Z→21Z peak, linear cool-down) to find the erosion hour. `HEATING_EFFICIENCY = 8.0` J/kg per °F per hour is the key empirical constant.

**CapBehavior** classifications: `EARLY_EROSION` (before 18Z), `CLEAN_EROSION` (18Z–21Z, peak storm window), `LATE_EROSION` (after 21Z), `NO_EROSION`, `BOUNDARY_FORCED`, `RECONSTITUTED`.

**Ground truth validation case**: May 3, 1999 Oklahoma tornado outbreak (`VALIDATION_CASE_ID = "19990503_OK"`). Published benchmark values for OUN 12Z sounding are documented in `sounding_parser.py`.

**5. Visualization** (`kronos_viz/`)
- Standalone package — no reverse dependency on the pipeline. Imports `ok_weather_model` types but the core package never imports `kronos_viz`.
- `CapErosionScene` — builder pattern; accumulate layers with `add_*` methods, render with `show()` or `save("file.html")`.
- Coordinate system: **x = longitude, y = latitude** (real WGS-84 degrees), **z = height in km** derived from pressure via ICAO standard atmosphere. Every feature is at its actual geographic position.
- Layers: `add_base_map()` (state/county outlines at z=0), `add_mesonet()` (surface T markers + dewpoint columns), `add_era5_temperature()` (semi-transparent pressure-level surface), `add_era5_winds()` (3D cone glyphs), `add_sounding()` (T/Td curtains at station lon/lat), `add_boundary()` (vertical curtain extruded to 850 mb).
- Requires `plotly` (`pip install plotly`). County boundaries fetched from Census TIGER GeoJSON at first use (cached in-process); state outlines from Plotly's Folium dataset with hardcoded simplified fallback.

```python
from kronos_viz import CapErosionScene

scene = CapErosionScene(title="May 3 1999 — 12Z")
scene.add_base_map()
scene.add_era5_temperature(ds, level_mb=700, valid_time=t)
scene.add_era5_winds(ds, level_mb=850, valid_time=t)
scene.add_sounding(profile, lon=-97.44, lat=35.22)
scene.add_boundary(dryline)
scene.save("cap_analysis.html")  # self-contained HTML, no server needed
```

## Configuration

Copy `.env.example` to `.env`. ERA5 data requires a Copernicus CDS account and `CDS_API_KEY` in `.env` (format: `<UID>:<API-KEY>`). Wyoming sounding and Mesonet clients work without API keys but respect rate limits (`WYOMING_REQUEST_DELAY=2.0s`, `MESONET_REQUEST_DELAY=1.1s`).

Data is stored under `./data/` (SQLite DB + Parquet files). Logs go to `./logs/`.
