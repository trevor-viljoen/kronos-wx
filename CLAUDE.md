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
python main.py classify-boundary-forced --start-year 1994 --end-year 2024
python main.py classify-boundary-forced --dry-run   # preview without writing

# Forecast models
python main.py train-models                          # train severity classifier + count regressor
python main.py evaluate-models                       # leave-one-year-out cross-validation
python main.py predict-day 19990503_OK               # apply models to a historical case

# Real-time situational awareness
python main.py analyze-now                        # one-shot: current cap + HRRR county risk
python main.py analyze-now --mode kinematics      # weight shear/SRH in analogue scoring
python main.py watch-now                          # continuous: alerts on tier changes / trend flips
python main.py watch-now --interval 10 --min-tier HIGH   # tighter alert threshold
```

## Architecture

KRONOS-WX is an Oklahoma severe weather case library and analysis system. The pipeline has four stages:

**1. Ingestion** (`ok_weather_model/ingestion/`)
- `SPCClient` — downloads the SPC tornado database and groups Oklahoma tornado days into `HistoricalCase` skeletons
- `SoundingClient` — fetches rawinsonde soundings from the University of Wyoming archive (OUN/LMN/AMA/DDC stations, 00Z and 12Z)
- `MesonetClient` — pulls Oklahoma Mesonet surface observations; `get_snapshot_observations()` fetches a single 5-min network snapshot for real-time use
- `ERA5Client` — fetches reanalysis fields via `cdsapi` (CDS API key required in `.env`)
- `HRRRClient` — fetches HRRR F00 analysis fields from the NOAA AWS archive (s3://noaa-hrrr-bdp-pds/) using `herbie`; available from 2016-07-15 onward. `get_county_snapshot(valid_time)` returns a `HRRRCountySnapshot` with 14 severe weather fields extracted at all 77 Oklahoma county centroids. Fields: MLCAPE, MLCIN, SBCAPE, SBCIN, SRH 0-1km/3km, BWD 0-6km, lapse rate 700-500mb, dewpoint 2m, LCL height, EHI, STP.

**2. Processing** (`ok_weather_model/processing/`)
- `sounding_parser.py` — converts raw `SoundingProfile` → `ThermodynamicIndices` + `KinematicProfile` using MetPy; computes CAPE/CIN, LCL/LFC/EL, cap strength, EML detection, lapse rates, composite parameters (STP, SCP, EHI)
- `cap_calculator.py` — the central diagnostic; implements the Cap Erosion Budget framework (instantaneous balance sheet of erosion/preservation forcings) and the Cap Erosion Score (CES), a sounding-only heating model that estimates when Tc will be reached
- `era5_diagnostics.py` — bridges ERA5 grids to cap diagnostics: `compute_thermal_advection()` computes `ADV(T) = -(u·∂T/∂x + v·∂T/∂y)` in K/hr via xarray differentiation; `compute_synoptic_cap_forcing()` aggregates 700/500mb advection + omega into a `dynamic_cap_forcing_jkg_hr` term for the budget; `extract_virtual_sounding()` extracts a `SoundingProfile(raw_source="virtual")` at any lat/lon grid point from ERA5 pressure-level fields
- `dryline_detector.py` — detects dryline position from Mesonet surface dewpoint gradient; returns a `BoundaryObservation` polyline with confidence score; `compute_dryline_surge_rate()` estimates eastward movement in mph between two snapshots
- `risk_zone.py` — scores all 77 Oklahoma counties into risk tiers (EXTREME, HIGH, DANGEROUS_CAPPED, MODERATE, MARGINAL, LOW). Primary path: `compute_risk_zones_from_hrrr()` uses per-county HRRR data directly. Fallback: `compute_risk_zones()` interpolates linearly between OUN (35.2°N) and LMN (36.7°N) soundings. Dryline proximity boosts tier for counties within ±50 miles.

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
  - `HRRRCountyPoint` / `HRRRCountySnapshot` — HRRR analysis at a single county centroid / all 77 counties at one valid time

**Real-time analysis** (`main.py`)
- `analyze-now`: fetches latest OUN+LMN soundings, Mesonet snapshot, dryline, two HRRR snapshots (sounding-hour baseline + most-recent current). Outputs: multi-station comparison, DANGEROUS_CAPPED warning, dryline table, CES projection, county risk zones, per-county drill-down, environment tendency table (ΔMLCIN/ΔCAPE/ΔSRH per threat county), historical analogues.
- `watch-now`: polls on a timer, diffs risk tiers and tendency direction each cycle, prints alerts only on tier changes or trend flips. Quiet cycles print a one-line status. First cycle always prints full risk snapshot.
- `DANGEROUS_CAPPED` flag (`_dangerous_capped_flag`): fires when MLCIN ≥ 80 J/kg AND (SRH 0-1km > 150 OR EHI > 2.5 OR SRH 0-3km > 300 OR shear > 50kt) — the April 23, 2026 boundary-forced miss pattern.
- Analogue `mode` options: `cap` (default, weights MLCIN/cap/Tc-gap), `kinematics` (weights SRH/shear/EHI), `full` (blended).

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

**5. Forecast Models** (`ok_weather_model/modeling/`)
- `features.py` — `extract_features(case)` and `extract_features_from_indices(indices, kinematics, ctg)` produce a 23-feature dict (NaN for missing optional fields). `build_feature_matrix()` builds `(X, y)` DataFrames for training.
- `severity_classifier.py` — `SeverityClassifier`: RandomForestClassifier (balanced weights, 300 trees) predicting SIGNIFICANT_OUTBREAK vs WEAK_OUTBREAK. `predict_proba()` returns `{'significant': p, 'weak': 1-p}`.
- `tornado_regressor.py` — `TornadoRegressor`: GradientBoostingRegressor on log1p(tornado_count). `predict()` returns expected count + 80% prediction interval.
- `registry.py` — `save_model(name, obj)` / `load_model(name)` via joblib under `data/models/`.
- LOYO cross-validation (30 year folds): severity ROC-AUC 0.649, tornado count MAE ~3.2 tornadoes. Models integrate into `analyze-now` automatically when artifacts exist.
- Feature importances: kinematics (SRH, BWD, SCP, mean wind) dominate over thermodynamics — consistent with supercell climatology.

**6. Visualization** (`kronos_viz/`)
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

## Development Workflow

After completing any feature or fix:

1. **Verify** — run the affected CLI command or test to confirm the change works end-to-end. For pipeline changes, prefer a real data smoke-test over a unit test alone.
2. **Commit** — one focused commit per logical change; message explains *why*, not just *what*.
3. **Push** — `git push` after every commit unless the user says otherwise.
4. **Close GitHub issues** — if the work closes an open issue, close it with `gh issue close <N> --comment "..."`. Do this every session.
5. **Update CLAUDE.md** — if new CLI commands, architectural patterns, or domain concepts were added, update the relevant section immediately. Don't defer.
6. **Open new GitHub issues** for any work discovered but not done in the current session, and for any new ideas or architectural thoughts raised in conversation. Use `gh issue create --title "..." --body "..." --label <labels>` and immediately add to the project with `gh project item-add 1 --owner trevor-viljoen --url <issue-url>`. Nothing gets discussed and dropped — if it's worth saying, it's worth an issue.

The GitHub issue list (trevor-viljoen/kronos-wx) is the canonical backlog. Keep it current. When starting a session, check open issues to orient on priorities.

### Issue labels
- `enhancement` — new features and improvements
- `architecture` — design decisions with cross-cutting impact (message bus, API shape, data models)
- `infrastructure` — deployment, containers, DevOps
- `long-term` — roadmap items not scheduled for near-term implementation

### Architectural direction (roadmap)
The long-term target is a containerized field deployment (issue #9):
- **`kronos-worker`** — background fetch loop (HRRR, Mesonet, sounding); publishes to MQTT
- **`kronos-api`** — FastAPI REST + WebSocket server (issue #7); bridges MQTT to HTTP clients
- **`mosquitto`** — MQTT message bus (issue #8); retained messages for current state, QoS 1 for alerts
- **`kronos-llm`** — optional Ollama container for on-device LLM narration (issue #10)

All services share a `./data` volume. Coordination is via MQTT topics (`kronos/risk/#`, `kronos/alert/#`, `kronos/sensor/#`), not direct DB calls between services. Podman rootless is the target runtime.

## Configuration

Copy `.env.example` to `.env`. ERA5 data requires a Copernicus CDS account and `CDS_API_KEY` in `.env` (format: `<UID>:<API-KEY>`). Wyoming sounding and Mesonet clients work without API keys but respect rate limits (`WYOMING_REQUEST_DELAY=2.0s`, `MESONET_REQUEST_DELAY=1.1s`).

Data is stored under `./data/` (SQLite DB + Parquet files). Logs go to `./logs/`.
