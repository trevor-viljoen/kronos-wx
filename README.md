# KRONOS-WX

**Resolving the outbreak/bust duality through temporal cap erosion analysis**

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-Research%20%2F%20Alpha-orange)
[![PayPal](https://img.shields.io/badge/Donate-PayPal-0070ba?logo=paypal)](https://paypal.me/trevorviljoen)
[![Sponsor](https://img.shields.io/badge/Sponsor-GitHub-ea4aaa?logo=github-sponsors)](https://github.com/sponsors/trevor-viljoen)

KRONOS-WX is a research-grade data system for studying why some Oklahoma severe
weather setups produce tornado outbreaks while identical-looking setups produce
nothing.  It builds a 30-year historical case library, computes thermodynamic
and kinematic parameters from rawinsonde and surface data, and models the
real-time erosion of the convective cap — the single atmospheric layer most
responsible for both enabling and suppressing tornadoes.

The name draws from Kronos, the Greek personification of time: this system's
central thesis is that severe weather forecasting is fundamentally a **timing
problem**.

---

## The Problem

On any given spring afternoon in Oklahoma, you might have 4,000 J/kg of
potential energy loaded into the atmosphere — more than enough to produce
violent tornadoes.  That energy does nothing if it can't be released.  The
atmosphere places a lid on it: a layer of warm, stable air aloft called the
**capping inversion** that prevents surface air from rising freely.

On outbreak days, that cap erodes.  On bust days, it doesn't.

The frustrating part: the morning sounding on a bust day often looks nearly
identical to the morning sounding before a major outbreak.  Both have enormous
CAPE.  Both have a cap.  Both have wind shear sufficient for supercells.  The
difference is what happens over the next six hours — whether the cap gives way
before the synoptic forcing window closes.

Operational forecast models handle this poorly.  They resolve large-scale
dynamics but struggle with the mesoscale processes — surface heating rates,
elevated mixed layer movement, dryline surge timing, and boundary interactions
— that determine whether the cap actually erodes.  The result is a pattern
familiar to Oklahoma forecasters: overconfident outbreak forecasts that verify
as busts, and busts that verify as outbreaks.

KRONOS-WX is an attempt to study that problem systematically, using Oklahoma's
unique observational network to build a case library that can eventually
support pattern recognition and probabilistic timing forecasts.

---

## The Approach

### Oklahoma as the Natural Laboratory

Oklahoma is uniquely suited for this research.  The state sits at the
intersection of cold, dry air from the Rockies; warm, moist air from the Gulf;
and a cap formed by elevated mixed layer air transported from the Mexican
Plateau.  The clash happens here, repeatedly, every spring — and it has been
watched by the **Oklahoma Mesonet** since 1994, a network of 120 automated
weather stations covering all 77 counties at 5-minute resolution.  No other
region has anything like it.

### Historical Case Library

KRONOS-WX builds a classified record of every significant convective day over
Oklahoma from 1994 to the present.  Each case is tagged with:

- **Event class** (significant outbreak through null bust)
- **Storm mode** (supercell dominant, linear, mixed, etc.)
- **Cap behavior** (clean erosion, late erosion, no erosion, boundary-forced)
- Morning thermodynamic and kinematic parameters from rawinsonde soundings
- Hourly surface conditions across all 77 counties from Mesonet
- Dryline position, outflow boundary locations, and boundary interaction flags
- SPC outlook probabilities vs. actual outcomes (forecast verification)

### Cap Erosion Budget

The analytical core of the system is a **cap erosion budget**: a balance
sheet approach to tracking CIN (Convective Inhibition, the energy required to
break the cap) over time.  Each forcing term is estimated from available data:

| Erosion forcings           | Preservation forcings         |
|----------------------------|-------------------------------|
| Surface heating            | Synoptic-scale subsidence     |
| Synoptic-scale lift (QG)   | Eastward EML advection        |
| Mesoscale boundary forcing | Cold pool stabilization       |
| Lapse rate steepening      |                               |

The net tendency determines whether CIN will reach zero before the forcing
window closes.

### The Temporal Race Condition

The system frames cap erosion as a **race condition**: the cap must erode
before the synoptic forcing window exits the region.  If the upper-level trough
passes and the jet exits before the surface heats sufficiently, the cap wins.
If the surface heats fast enough — or if a mesoscale boundary provides
supplemental lifting — the cap loses and convection initiates.

This framing makes the bust/outbreak question answerable: given the current
erosion rate, the remaining CIN, and the time left in the forcing window,
will the cap erode in time?

### The Six Case Classes

| Class | Description |
|-------|-------------|
| `SIGNIFICANT_OUTBREAK` | 3+ tornadoes, at least one EF2+ |
| `ISOLATED_SIGNIFICANT` | 1–2 significant tornadoes |
| `WEAK_OUTBREAK` | 5+ weak tornadoes, no significant |
| `SIGNIFICANT_SEVERE_NO_TORNADO` | Major hail/wind event, no tornadoes |
| `NULL_BUST` | SPC probability ≥ 5%, zero tornadoes |
| `SURPRISING_OUTBREAK` | SPC probability < 5%, major outbreak |

---

## Data Sources

| Source | Data Type | Coverage | Resolution | Access |
|--------|-----------|----------|------------|--------|
| Oklahoma Mesonet | Surface obs | 1994–present | 5-min, 77 stations | Public API |
| Univ. of Wyoming | Rawinsonde soundings | 1994–present | 00Z/12Z, 4 stations | Public scrape |
| NOAA SPC | Tornado reports + outlooks | 1950–present | Event-level | Public CSV |
| ECMWF ERA5 | Reanalysis upper air | 1940–present | Hourly, 31km | CDS API (free) |
| NOAA NCEI | Storm Data narratives | 1994–present | Event-level | Public |
| NEXRAD Level II | Radar volumetric scans | 1994–present | ~5 min, 4 stations | AWS S3 |

---

## Architecture

```
Raw Data Sources
      │
      ▼
Ingestion Layer
  mesonet_client.py   — Oklahoma Mesonet 5-min observations
  sounding_client.py  — University of Wyoming rawinsonde archive
  spc_client.py       — SPC tornado reports and convective outlooks
  era5_client.py      — ECMWF ERA5 reanalysis via CDS API
      │
      ▼
Processing Layer
  sounding_parser.py  — MetPy thermodynamic and kinematic computations
  cap_calculator.py   — CIN budget, erosion trajectory, bust risk
      │
      ▼
Pydantic Models (typed, validated, serializable)
  HistoricalCase, SoundingProfile, ThermodynamicIndices,
  KinematicProfile, CapErosionBudget, BoundaryObservation, ...
      │
      ▼
Storage Layer
  SQLite             — case metadata, indices, parameters
  Parquet            — Mesonet time series, sounding level data
      │
      ▼
Historical Case Library (1994–present)
      │
      ▼
[Analysis Engine — Phase 2]      [Forecast Module — Phase 3]
```

---

## Core Concepts

**Capping Inversion / EML** — A layer of abnormally warm, well-mixed air
aloft (typically 700–600mb) that acts as a lid on surface convection.  It
originates over the high terrain of Mexico and the Texas Panhandle, then
advects eastward over Oklahoma at low levels.  Without it, convection would
fire randomly throughout the day.  With it, energy builds until the cap
finally fails.

**CIN (Convective Inhibition)** — The activation energy required to break
the cap.  Think of it as the energy a surface air parcel must spend fighting
through the stable layer to reach the level of free convection.  Measured in
J/kg.  A value of 200 J/kg is a very strong cap; 25 J/kg is marginal.  Zero
means free convection is possible.

**Cap Erosion Budget** — The system's primary diagnostic.  Each hour, the
budget sums all forcings working to reduce CIN (surface heating, synoptic
lift, boundary convergence) against forcings working to rebuild it
(subsidence, EML reinforcement, cold pools).  The net tendency determines
whether the cap is winning or losing the race.

**The Temporal Race Condition** — The synoptic forcing window is finite.
The upper-level trough and its associated lift will exit Oklahoma by some
time — often late evening.  If the cap doesn't erode before that time,
the forcing disappears and convection may never initiate.  This is the
central timing question KRONOS-WX is designed to answer.

**Virtual Sounding Network** — Oklahoma has only four operational rawinsonde
sites, launched twice daily.  Between launch times and between stations, the
atmosphere is unsampled.  KRONOS-WX bridges this gap by combining Mesonet
surface data with ERA5 reanalysis upper air fields to construct "virtual
soundings" — pseudo-radiosonde profiles at arbitrary times and locations
across the 77-county network.

**The Six Case Classes** — See table above.  The classification scheme is
designed to separate the bust problem from the outbreak problem: the system
studies not just what happened, but what was forecast to happen.

---

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/kronos-wx.git
cd kronos-wx

# Create a virtual environment (Python 3.11+ required)
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env as needed (see ERA5 setup below)
```

### ERA5 Setup (required for reanalysis data)

ERA5 requires a free API key from the Copernicus Climate Data Store:

1. Register at https://cds.climate.copernicus.eu/user/register
2. Accept the ERA5 terms of use
3. Find your UID and API key at https://cds.climate.copernicus.eu/user
4. Create `~/.cdsapirc`:

```
url: https://cds.climate.copernicus.eu/api/v2
key: <YOUR-UID>:<YOUR-API-KEY>
verify: 0
```

No API keys are required for Mesonet, Wyoming sounding, or SPC data.

---

## Quick Start

**Step 1 — Build the case skeleton from SPC tornado data:**

```bash
python main.py build-case-skeleton --start-year 1994 --end-year 2023
```

```
Building case skeleton from SPC data
Downloading SPC tornado data 1994–2023... done
Built 847 case skeletons from SPC data

Case Library Summary (1994–2023)
┌──────────────────────────────────┬───────┬───────────────┬──────────────────┐
│ Event Class                      │ Cases │ Avg Tornadoes │ Avg Completeness │
├──────────────────────────────────┼───────┼───────────────┼──────────────────┤
│ WEAK_OUTBREAK                    │   312 │           3.1 │              2%  │
│ NULL_BUST                        │   198 │           0.0 │              2%  │
│ ISOLATED_SIGNIFICANT             │   156 │           1.4 │              2%  │
│ SIGNIFICANT_OUTBREAK             │    98 │          14.7 │              2%  │
│ SIGNIFICANT_SEVERE_NO_TORNADO    │    83 │           0.0 │              2%  │
└──────────────────────────────────┴───────┴───────────────┴──────────────────┘
```

**Step 2 — Enrich the May 3, 1999 ground truth case:**

```bash
python main.py enrich-case 1999-05-03
```

```
Enriching 19990503_OK
Sounding: 82 levels
  MLCAPE=4712 J/kg  MLCIN=73 J/kg  cap=4.8°C  SRH0-3=487 m²/s²
Mesonet: 71 stations
  12Z Tc-gap: +18.4°F
  15Z Tc-gap: +9.1°F
  18Z Tc-gap: +2.3°F

Case 19990503_OK saved. Completeness: 94%
```

**Step 3 — Bulk enrich with sounding data:**

```bash
python main.py enrich-all 1994 2023
```

```
Found 464 total cases. Enriching 463 (skipping 1 already enriched).
Enriching cases... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 0:30:12
Enriched: 435  No sounding: 28  Errors: 0
```

**Step 4 — Compute Cap Erosion Score for all cases:**

```bash
python main.py compute-ces --start-year 1994 --end-year 2023
```

```
Found 436 enriched cases. Processing 436 (skipping 0 already done).
Computing CES... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 0:00:08

Processed: 436  Skipped (no sounding): 0  Errors: 0

Cap Behavior Distribution (1994–2023)
┏━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Cap Behavior  ┃ Cases ┃ Description                               ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ NO_EROSION    │   280 │ Cap held through 02Z — bust candidate     │
│ EARLY_EROSION │   150 │ Eroded before 18Z — early initiation      │
│ CLEAN_EROSION │     6 │ Eroded 18Z–21Z — peak storm window        │
│ NOT_COMPUTED  │    28 │ No sounding data available                 │
└───────────────┴───────┴───────────────────────────────────────────┘
```

The Cap Erosion Score uses the Oklahoma climatological heating model: the
`cap_strength` (°C warm-nose excess) and `MLCIN` (mixed-layer inhibition)
from the 12Z sounding determine the effective surface temperature needed to
drive convection.  Cases classified `NO_EROSION` required dynamic forcing
(QG lift, dryline surge) beyond surface heating — quantifying that forcing
from ERA5 reanalysis is the next analysis phase.

**Step 5 — Analyze a single case in detail:**

```bash
python main.py analyze-cap-behavior 1999-05-03
```

```
Cap Analysis — 19990503_OK
┌──────────────────────┬──────────────────────┐
│ Metric               │ Value                │
├──────────────────────┼──────────────────────┤
│ Event Class          │ SIGNIFICANT_OUTBREAK │
│ Cap Behavior         │ CLEAN_EROSION        │
│ Primary Mechanism    │ COMBINED             │
│ Erosion Achieved     │ YES                  │
│ Erosion Time         │ 21:00 UTC            │
│ Bust Risk Score      │ 0.09                 │
│ 12Z MLCAPE           │ 4712 J/kg            │
│ 12Z MLCIN            │ 73 J/kg              │
│ 12Z Cap Strength     │ 4.8°C                │
│ 12Z Tc Gap           │ +18.4°F              │
│ 15Z Tc Gap           │ +9.1°F               │
│ 18Z Tc Gap           │ +2.3°F               │
│ Forcing Window Close │ 00:00 UTC            │
└──────────────────────┴──────────────────────┘
```

---

## Key CLI Commands

| Command | Description | Example |
|---------|-------------|---------|
| `build-case-skeleton` | Initialize case library from SPC data | `python main.py build-case-skeleton` |
| `enrich-case CASE_REF` | Add sounding + Mesonet data to one case | `python main.py enrich-case 1999-05-03` |
| `enrich-all YEAR YEAR` | Bulk enrichment with resume support | `python main.py enrich-all 1994 2023` |
| `compute-ces` | Cap Erosion Score from sounding data (no Mesonet needed) | `python main.py compute-ces --start-year 1994 --end-year 2023` |
| `analyze-cap-behavior CASE_REF` | Compute cap erosion trajectory (requires Mesonet) | `python main.py analyze-cap-behavior 19990503_OK` |
| `build-bust-database` | Identify bust and alarm-bell cases | `python main.py build-bust-database --spc-threshold 0.10` |

`CASE_REF` accepts either `YYYYMMDD_OK` (case ID) or `YYYY-MM-DD` (date).

---

## The Case Library

Each `HistoricalCase` record contains:

- **Identity** — date, event class, storm mode, cap behavior classification
- **Synoptic scale** — trough longitude, jet streak intensity/position, surface low
- **Morning thermodynamics** — full `ThermodynamicIndices` from 12Z OUN sounding
  (MLCAPE, MLCIN, SBCAPE, SBCIN, MUCAPE, LCL/LFC/EL heights, cap strength,
  convective temperature, EML characteristics, lapse rates, precipitable water)
- **Kinematics** — SRH 0-1/0-3km, BWD 0-1/0-6km, Bunkers storm motion,
  hodograph shape, STP, SCP, EHI
- **Cap evolution** — Tc gap at 12Z/15Z/18Z, erosion time, erosion county,
  primary mechanism
- **Boundaries** — dryline position, outflow boundaries, boundary interactions
- **Outcome** — tornado count and ratings, path lengths, county-level breakdown
- **Forecast verification** — SPC probabilities vs. actual outcome

**Querying the library:**

```python
from ok_weather_model.storage import Database
from ok_weather_model.models import EventClass, CapBehavior

db = Database()

# All significant outbreaks
outbreaks = db.get_cases_by_class(EventClass.SIGNIFICANT_OUTBREAK)

# Bust days with late cap erosion
late_busts = db.query_parameter_space({
    "event_class": "NULL_BUST",
    "cap_behavior": "LATE_EROSION",
})

# High-completeness cases for ML
rich_cases = db.query_parameter_space({
    "min_completeness": 0.8,
    "start_date": "2000-01-01",
})
```

**Data completeness scoring:**  
Each case receives a completeness score from 0.0 to 1.0 based on 10 criteria
(sounding available, Mesonet available, Tc gap computed at each time step, etc.).
Target for production analysis: completeness ≥ 0.8.

**Scale:** 30 years (1994–2024) × ~30 Oklahoma severe weather days/year ≈ 900 cases.

---

## Validation

**May 3, 1999 is the ground truth benchmark** for all MetPy calculations.

This case is among the best-documented tornado outbreaks in history: 74 tornadoes,
including the Bridge Creek–Moore F5, with published proximity soundings, Mesonet
analyses, and post-event research by SPC and university researchers.

Expected values from the published literature:

| Parameter | Expected | Source |
|-----------|----------|--------|
| SBCAPE (12Z OUN) | 4500–5000 J/kg | Doswell et al. 1999 |
| SBCIN (12Z OUN) | 50–150 J/kg | Multiple |
| SRH 0–3km | 400–500 m²/s² | Thompson & Edwards 2000 |
| BWD 0–6km | 50–60 kts | Brooks et al. |
| LCL height | 500–800 m AGL | Multiple |
| Tc gap (12Z) | ~15–20°F | Oklahoma Mesonet archive |

Any computed MetPy value that falls outside these ranges indicates a parsing
or unit error and must be investigated before proceeding to bulk processing.

---

## Roadmap

### Phase 1 — Foundation (Current)
- [x] Pydantic model architecture (all 77-county enums, 15 model classes)
- [x] Data ingestion pipeline (Mesonet, Wyoming sounding, SPC, ERA5)
- [x] Historical case library construction from SPC tornado data
- [x] MetPy-based thermodynamic and kinematic computation
- [x] Cap erosion budget framework
- [ ] Ground truth validation against May 3, 1999 published values
- [ ] 30-year bulk enrichment (1994–2024)

### Phase 2 — Analysis Engine
- [ ] Cap Erosion Score (CES) — normalized composite parameter
- [ ] Bust/outbreak divergence signature identification
- [ ] Parameter interaction matrix (which combinations predict busts?)
- [ ] Historical analog matching
- [ ] Mesonet-based mesoscale boundary detection (wind shift analysis)
- [ ] Dryline surge rate computation from Mesonet network

### Phase 3 — Forecast Module
- [ ] Real-time Mesonet data ingestion
- [ ] Virtual sounding network (77 county pseudo-soundings)
- [ ] CIN trajectory forecasting with confidence intervals
- [ ] County-level initiation probability maps
- [ ] Timing confidence corridors (when will the cap erode here?)
- [ ] Alarm bell detection system for boundary interactions
- [ ] Operational alert interface

---

## Contributing

Fork the repository and submit pull requests.

**Branch naming:**
- `feature/` — new capabilities
- `fix/` — bug corrections
- `data/` — new data sources or ingestion improvements
- `analysis/` — analytical methods and scoring

**Requirements:**
- All new data sources require a corresponding Pydantic model with validators
- All MetPy calculations require unit tests validated against known cases
- Case classifications require a documentation comment explaining the reasoning
- Datetime objects must be timezone-aware (UTC storage)

---

## Scientific References

Full bibliography in `docs/whitepaper.md`.  Foundational references:

- Brooks, H.E., C.A. Doswell III, and J. Cooper, 1994: On the environments of
  tornadic and nontornadic mesocyclones. *Wea. Forecasting*, **9**, 606–618.

- Rasmussen, E.N., and D.O. Blanchard, 1998: A baseline climatology of
  sounding-derived supercell and tornado forecast parameters. *Wea. Forecasting*,
  **13**, 1148–1164.

- Thompson, R.L., R. Edwards, J.A. Hart, K.L. Elmore, and P. Markowski, 2003:
  Close proximity soundings within supercell environments obtained from the
  Rapid Update Cycle. *Wea. Forecasting*, **18**, 1243–1261.

- Markowski, P., and Y. Richardson, 2010: *Mesoscale Meteorology in Midlatitudes.*
  Wiley-Blackwell.

- Doswell, C.A. III, H.E. Brooks, and R.A. Maddox, 1996: Flash flood forecasting:
  An ingredients-based methodology. *Wea. Forecasting*, **11**, 560–581.

- Bunkers, M.J., B.A. Klimowski, J.W. Zeitler, R.L. Thompson, and M.L. Weisman,
  2000: Predicting supercell motion using a new hodograph technique. *Wea.
  Forecasting*, **15**, 61–79.

---

## License

MIT License. See `LICENSE` for details.

---

## Support This Project

KRONOS-WX is independent research. If you find it useful or want to help fund
data storage, compute time, and continued development:

- **PayPal:** [paypal.me/trevorviljoen](https://paypal.me/trevorviljoen)
- **GitHub Sponsors:** [github.com/sponsors/trevor-viljoen](https://github.com/sponsors/trevor-viljoen)

---

## Acknowledgments

- **Oklahoma Mesonet** — operated by the Oklahoma Climatological Survey at the
  University of Oklahoma and Oklahoma State University. The Mesonet is the
  primary observational backbone of this system.

- **NOAA Storm Prediction Center**, Norman, Oklahoma — SPC tornado database,
  convective outlook archive, and decades of foundational research on severe
  weather climatology.

- **University of Wyoming Department of Atmospheric Science** — for maintaining
  the public rawinsonde archive that makes historical sounding analysis possible.

- **ECMWF / Copernicus Climate Change Service** — ERA5 reanalysis data, available
  free of charge via the Climate Data Store.
