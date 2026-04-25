# KRONOS-WX Dashboard Design Specification

## Terminal target

Minimum 130 columns, 40 rows. Optimal 160+ × 50+.

## Layout

```
┌─ KRONOS-WX ─────────────── HH:MMZ ──────────────────────────────────────────┐
│ Header (clock)                                                                │
├─ Risk Zones ────────────────┬─ Environment ──────┬─ Surface / Dryline ──────┤
│ DataTable:                  │ OUN │ LMN column    │ Moisture return          │
│ County │ Tier │ CAPE │ CIN  │ CAPE, CIN, Cap     │ Td mean, S/N gradient    │
│ SRH-1  │ SRH-3│ EHI  │ STP  │ SRH, Shear, EHI   │ Gulf coverage (%)        │
│                             │ CES projection     │ Dryline position & surge  │
│ (refreshes every 15 min)    │ Model forecast     │ (refreshes every 5 min)  │
│                             │ (refreshes 60 min) │                           │
├─ Tendency ─────── baseline → current ────────────────────────────────────────┤
│ County │ Tier │ ΔCIN │ ΔCAPE │ ΔSRH-1 │ ΔSRH-3 │ ΔEHI │ Trend               │
│ (refreshes every 15 min, shows top 10 elevated counties)                     │
├─ SPC Products ───────────────────────────────────────────────────────────────┤
│ D1 Outlook: RISK_CAT  torn X%  │ Tornado Warnings │ Watches │ MDs           │
│ (refreshes every 15 min)                                                     │
├─ Alert Log ──────────────────────────────────────────────────────────────────┤
│ HH:MMZ  Scrolling feed: tier changes, new NWS alerts, new MDs               │
└─ [R] Refresh  [Q] Quit ──────────────────────────────────────────────────────┘
```

## Design tokens

| Token           | Value     | Usage                          |
|-----------------|-----------|--------------------------------|
| bg-deep         | `#050510` | Screen background               |
| bg-panel        | `#080820` | Panel background (subtle)       |
| border-base     | `#1e3a5f` | Default panel border            |
| border-warn     | `#5f3a1e` | SPC / warning panel border      |
| border-alert    | `#5f1e1e` | Active tornado warning border   |
| text-primary    | `#d0d8e8` | Default text                    |
| text-dim        | `#5a7090` | Timestamps, secondary labels    |
| header-bg       | `#0e2040` | Header / footer background      |
| accent          | `#7ec8e3` | Panel titles, section headers   |

## Tier colors

| Tier               | Style              | Condition summary              |
|--------------------|--------------------|--------------------------------|
| EXTREME            | `bold bright_red`  | CIN≈0, CAPE≥1500, EHI≥3.5      |
| HIGH               | `bold red`         | CIN<100, CAPE≥1000, EHI≥2.0    |
| DANGEROUS_CAPPED   | `bold magenta`     | CIN≥80 but violent kinematics  |
| MODERATE           | `yellow`           | CIN<200, CAPE≥500, SRH1≥100    |
| MARGINAL           | `green`            | CIN<250, CAPE≥200, SRH1≥50     |
| LOW                | `dim white`        | Minimal threat                 |

## CAPE/CIN color thresholds

| Value     | CAPE color    | CIN color     |
|-----------|---------------|---------------|
| MLCAPE    | ≥3000 bright_red / ≥2000 red / ≥1000 yellow / else white |
| MLCIN     | ≥200 bright_red / ≥100 red / ≥50 yellow / else green |

## Tendency trend scoring

Each HRRR update delta generates a composite score per county:

```
+1 for ΔCIN ≤ -10 (cap eroding)
+1 for ΔCIN ≤ -30 (cap eroding fast)
+1 for ΔSRH-1 ≥ 20 (shear increasing)
+1 for ΔCAPE ≥ 200 (instability increasing)
-1 for ΔCIN ≥ +10 (cap rebuilding)
-1 for ΔSRH-1 ≤ -20 (shear decreasing)
```

Score → arrow:
- ≥ 3  `▲▲` bright_green  (strongly improving)
- 2    `▲`  green          (improving)
- 0–1  `→`  yellow         (steady)
- ≤ -1 `▼`  red            (degrading)

## Model predictions (environment panel footer)

Loaded from `data/models/` at startup. If artifacts exist:
- **Severity**: `XX% SIGNIFICANT` (colored red/yellow/green by threshold)
- **Count**: `N expected  (80% PI: lo–hi)`

If models not trained: dim placeholder message with link to `train-models`.

## Panel refresh intervals

| Panel       | Source  | Interval |
|-------------|---------|----------|
| Risk zones  | HRRR    | 15 min   |
| Environment | Wyoming | 60 min   |
| Dryline     | Mesonet | 5 min    |
| Tendency    | HRRR    | 15 min   |
| SPC         | SPC/NWS | 15 min   |

## Keyboard bindings

| Key | Action                  |
|-----|-------------------------|
| R   | Force-refresh all panels|
| Q   | Quit                    |

## Textual widgets

| Panel       | Widget              | Notes                                   |
|-------------|---------------------|-----------------------------------------|
| Risk zones  | `DataTable`         | `cursor_type="none"`, sorted by tier    |
| Environment | `Static`            | Rich markup, OUN+LMN dual columns       |
| Dryline     | `Static`            | Rich markup                             |
| Tendency    | `Static`            | Rich markup with trend arrows           |
| SPC         | `Static`            | Rich markup, categorized by alert type  |
| Alert log   | `RichLog`           | `highlight=True, wrap=True`             |

All `Static` and `DataTable` panels use `border_title` (set at runtime) for
clean panel labels. The border renders the title in the border line itself,
avoiding a wasted first row of every panel.
