---
name: KRONOS-WX Web Dashboard
version: alpha
description: >
  Real-time severe weather situational awareness dashboard for Oklahoma.
  Dark tactical aesthetic — deep navy backgrounds, precise monospace data,
  color-coded threat tiers from dim green (MARGINAL) to vivid red (EXTREME).

colors:
  deep:           "#050510"
  panel:          "#080820"
  panel-raised:   "#0d1530"
  border:         "#1e3a5f"
  border-warn:    "#5f3a1e"
  border-alert:   "#5f1e1e"
  header:         "#0e2040"
  accent:         "#7ec8e3"
  text-primary:   "#d0d8e8"
  text-dim:       "#5a7090"
  text-muted:     "#2a4060"

  tier-extreme:         "#ff2222"
  tier-extreme-bg:      "#2a0505"
  tier-high:            "#ff6600"
  tier-high-bg:         "#2a1205"
  tier-dangerous:       "#cc44ff"
  tier-dangerous-bg:    "#1a0530"
  tier-moderate:        "#ffcc00"
  tier-moderate-bg:     "#1a1400"
  tier-marginal:        "#44aa44"
  tier-marginal-bg:     "#051205"
  tier-low:             "#4a5568"
  tier-low-bg:          "#080810"

  cape-high:      "#ff4444"
  cape-mid:       "#ff8800"
  cape-low:       "#ffcc00"
  cin-high:       "#ff4444"
  cin-mid:        "#ff8800"
  cin-low:        "#ffcc00"
  cin-ok:         "#44aa44"

  warning-tornado:       "#ff2222"
  warning-watch:         "#ff6600"
  warning-svr:           "#ffcc00"
  warning-md:            "#7ec8e3"

typography:
  body:
    fontFamily: "Inter, system-ui, -apple-system, sans-serif"
    fontSize: "14px"
    fontWeight: 400
    lineHeight: 1.5
  mono:
    fontFamily: "JetBrains Mono, Fira Code, ui-monospace, monospace"
    fontSize: "13px"
    fontWeight: 400
    lineHeight: 1.4
  heading:
    fontFamily: "Inter, system-ui, sans-serif"
    fontSize: "20px"
    fontWeight: 600
    lineHeight: 1.2
  panel-title:
    fontFamily: "Inter, system-ui, sans-serif"
    fontSize: "11px"
    fontWeight: 600
    letterSpacing: "0.08em"

spacing:
  xs:  "4px"
  sm:  "8px"
  md:  "16px"
  lg:  "24px"
  xl:  "32px"
  2xl: "48px"

rounded:
  sm:   "4px"
  md:   "6px"
  lg:   "10px"
  xl:   "16px"
  full: "9999px"

components:
  panel:
    backgroundColor: "{colors.panel}"
    textColor: "{colors.text-primary}"
    rounded: "{rounded.lg}"
    padding: "{spacing.md}"

  panel-header:
    backgroundColor: "{colors.panel-raised}"
    textColor: "{colors.accent}"
    typography: "{typography.panel-title}"
    padding: "{spacing.sm} {spacing.md}"

  badge-extreme:
    backgroundColor: "{colors.tier-extreme-bg}"
    textColor: "{colors.tier-extreme}"
    rounded: "{rounded.sm}"
    padding: "{spacing.xs} {spacing.sm}"

  badge-high:
    backgroundColor: "{colors.tier-high-bg}"
    textColor: "{colors.tier-high}"
    rounded: "{rounded.sm}"
    padding: "{spacing.xs} {spacing.sm}"

  badge-dangerous:
    backgroundColor: "{colors.tier-dangerous-bg}"
    textColor: "{colors.tier-dangerous}"
    rounded: "{rounded.sm}"
    padding: "{spacing.xs} {spacing.sm}"

  badge-moderate:
    backgroundColor: "{colors.tier-moderate-bg}"
    textColor: "{colors.tier-moderate}"
    rounded: "{rounded.sm}"
    padding: "{spacing.xs} {spacing.sm}"

  badge-marginal:
    backgroundColor: "{colors.tier-marginal-bg}"
    textColor: "{colors.tier-marginal}"
    rounded: "{rounded.sm}"
    padding: "{spacing.xs} {spacing.sm}"

  alert-card-warning:
    backgroundColor: "#2a0505"
    textColor: "{colors.warning-tornado}"
    rounded: "{rounded.md}"
    padding: "{spacing.sm} {spacing.md}"

  alert-card-watch:
    backgroundColor: "#2a1205"
    textColor: "{colors.warning-watch}"
    rounded: "{rounded.md}"
    padding: "{spacing.sm} {spacing.md}"

  alert-card-md:
    backgroundColor: "#071520"
    textColor: "{colors.warning-md}"
    rounded: "{rounded.md}"
    padding: "{spacing.sm} {spacing.md}"
---

## Overview

KRONOS-WX is a real-time severe weather situational awareness dashboard for
Oklahoma. The visual language is derived from operational meteorology: dark
backgrounds preserve night vision, threat tiers escalate from muted green
through orange to vivid red, and monospace fonts give sounding data a
structured, instrument-readout feel.

The map is the hero of the interface — a live Leaflet choropleth with county
risk-tier fills, multiple overlay layers, and a boundary tracking system that
elevates initiation risk when watch corridors and meteorological boundaries
intersect.

## Colors

### Background Depth

Three layered background values create visual depth without harsh contrast:
`deep` (#050510) for the screen background, `panel` (#080820) for card
surfaces, and `panel-raised` (#0d1530) for panel headers and elevated
elements.

### Tier Spectrum

The six risk tiers map to a perceptual severity spectrum. Each tier has a
foreground color (used for text and map fills) and a background color (used
for badge surfaces). The spectrum intentionally avoids pure hue transitions —
DANGEROUS_CAPPED uses magenta to distinguish boundary-forced events from the
instability-driven red/orange of EXTREME and HIGH.

| Tier             | Foreground   | Connotation                              |
|------------------|--------------|------------------------------------------|
| EXTREME          | #ff2222      | Near-certain significant tornadoes       |
| HIGH             | #ff6600      | Likely significant with initiation       |
| DANGEROUS_CAPPED | #cc44ff      | Boundary-forced; violent despite cap     |
| MODERATE         | #ffcc00      | Some potential if cap erodes             |
| MARGINAL         | #44aa44      | Isolated storm potential                 |
| LOW              | #4a5568      | No meaningful threat                     |

### Alert Hierarchy

Active products use a separate alert color set that maps to warning priority:
tornado warnings (red), watches (orange), MDs (cyan accent). These colors
are used for polygon fills on the map and card backgrounds in the alerts panel.

## Typography

Body text uses Inter at 14px — readable at monitor distance, modern without
being stylized. Data values in the environment and tendency panels use
JetBrains Mono at 13px, giving numeric columns the alignment and instrument
feel of a terminal but with better rendering at small sizes.

Panel titles use Inter at 11px, 600 weight, wide letter-spacing — they
read as labels, not headings.

## Layout

```
┌─ Header ──────────────────────────────────────────────────────────────┐
│  KRONOS-WX · HH:MMZ · [SPC badge] · Last HRRR: HH:MMZ                │
├─ Map (2fr) ─────────────────────────────┬─ Right Column (1fr) ────────┤
│                                         │  EnvironmentPanel           │
│  Leaflet + CartoDB Dark Matter          │  OUN / LMN dual-col         │
│  County choropleth (risk tiers)         │  CES + model forecast       │
│  [Overlay toggles — see below]          ├─────────────────────────────┤
│  Hover → tooltip, click → drawer        │  SPCPanel                   │
│  [Legend] Tier / Boundary / Radar key   │  Warnings · Watches · MDs   │
├─ ⚡ INITIATION CANDIDATES banner (cond.) ┴─────────────────────────────┤
│  Orange bar; lists counties with cap-break probability ≥ 45%          │
│  Visible only when: in_watch AND convergence_score > 0                │
├─ TendencyTable ──────────────┬─ AnaloguePanel ─┬─ AlertFeed ──────────┤
│  County · Tier · Δ fields    │  Historical      │  Scrolling log       │
│  · Trend arrow               │  analogues       │  tier/warning events │
└──────────────────────────────┴──────────────────┴──────────────────────┘
```

On narrow viewports (<1200px) the right column drops below the map.

## Map Overlay System

Overlays are toggled independently via the overlay control panel (top-right of
map). Active overlays persist for the session. Each toggle may show/hide one
or more Leaflet layers.

| Key          | Label         | Description                                        |
|--------------|---------------|----------------------------------------------------|
| `counties`   | Counties      | County tier choropleth fill                        |
| `spc_risk`   | SPC Risk      | SPC Day 1 categorical outlook polygon (filled)     |
| `spc_torn`   | SPC Torn      | SPC tornado probability polygon                    |
| `spc_wind`   | SPC Wind      | SPC wind probability polygon                       |
| `spc_hail`   | SPC Hail      | SPC hail probability polygon                       |
| `alerts`     | Alerts        | NWS warning/watch polygons; click for full text    |
| `watches`    | Watches       | Watch county corridor fills (orange/red)           |
| `mesonet`    | Mesonet       | Surface obs: dewpoint label + wind arrow per stn   |
| `dryline`    | Dryline       | Detected dryline polyline (brown, dashed)          |
| `radar`      | Radar         | Animated MRMS radar; choice of RainViewer or RIDGE2|
| `boundaries` | Boundaries    | WPC fronts + mesonet outflow boundaries (polylines)|
| `satellite`  | Satellite     | GOES-East VIS or IR imagery (IEM tile service)     |

### SPC Hatching

SPC significant tornado areas use SVG `<pattern>` fills injected into Leaflet's
SVG layer (`SvgDefs` component). Three densities: CIG1 (sparse, 12px), CIG2
(medium, 8px), CIG3 (dense, 5px). `SIGN` label maps to CIG1. Rendered correctly
on top of filled probability polygons.

### Radar Modes

- **RainViewer MRMS**: animated multi-frame tile sequence via `/api/radar/frames` proxy.
  `RainViewerRadar` component sequences frames on a fixed interval.
- **NOAA RIDGE2**: per-station tile proxy at `/api/radar/tile/{station}/{time_iso}/{z}/{x}/{y}`.
  Station selector (KTLX, KINX, KVNX, KFDR) shown when mode = RIDGE2.
  Backend caches tiles with 2000-entry LRU; supports disconnect cancellation.

## Boundary Tracking System

### Sources

| Source                | Type(s)                                         | Detected by        |
|-----------------------|-------------------------------------------------|--------------------|
| WPC Day 1 Fronts      | COLD, WARM, STATIONARY, OCCLUDED, TROUGH, DRY  | `wpc_*`            |
| Mesonet outflow       | OUTFLOW_BOUNDARY                               | `mesonet_wind_pressure` |
| Dryline detector      | DRYLINE                                        | `mesonet_dewpoint` |

Dryline rendering is exclusively owned by the Dryline toggle; BoundaryLayer
filters out DRYLINE-type entries to prevent double-rendering.

### Boundary Polyline Styles

| Detected by               | Color    | Style  |
|---------------------------|----------|--------|
| wpc_cold_front            | #4488ff  | solid  |
| wpc_warm_front            | #ff4444  | solid  |
| wpc_stationary_front      | #9944ff  | dashed |
| wpc_occluded_front        | #cc44ff  | solid  |
| wpc_trough                | #cc8822  | dashed |
| mesonet_wind_pressure     | #aaaaaa  | dashed |

### Convergence Score

Each county receives a `convergence_score` ∈ [0, 1] — a distance-decayed,
boundary-type-weighted sum of proximity to all active boundaries.

Boundary type weights: cold front 1.2, dryline 1.0, stationary 0.9, trough 0.8,
occluded 0.8, warm front 0.6, mesonet outflow 1.0.

Effective CIN used in tier scoring:
```
effective_cin = cin * max(0.3, 1.0 - 0.5 * convergence_score)
```

### Alarm Bell

Fires when two boundaries intersect at a point where the nearest county has
MLCAPE ≥ 1000 J/kg AND MLCIN < 100 J/kg. Alarm bell hard-caps effective CIN
at 50 J/kg. Displayed as a ⚡ marker on the map at the intersection point.

### Watch + Boundary Elevation

When a county is inside an active Tornado or SVR Watch (`in_watch = true`):
- Effective CIN reduced by additional 15% (×0.85)
- `cap_break_prob` boosted by +0.15 if `convergence_score > 0`

### Cap-Break Probability

Physics-informed initiation probability ∈ [0, 1]:
```
0.35 × cin_score       (effective CIN inverse — higher CIN = lower score)
0.30 × cape_score      (MLCAPE ≥ 3000 → 1.0, scales down)
0.20 × srh_score       (SRH 0-1km ≥ 400 → 1.0)
0.15 × convergence_score
+0.15 bonus if in_watch AND convergence_score > 0
```

### Initiation Candidates Banner

Shown when at least one county satisfies:
`in_watch AND convergence_score > 0 AND cap_break_prob ≥ 0.45`

Orange banner above the tendency table; lists counties with probability
percentages. Clicking a county name opens the CountyDrawer.

## Mesonet Station Overlay

Each station plots:
- Dewpoint in °F (label), color: green (≥60), yellow (≥50), gray (<50)
- Wind arrow pointing in the direction the wind is **traveling** (TO direction):
  `toDir = (meteorological_from_dir + 180) % 360`
- Arrow color: blue (< 20 kt), orange (20–29 kt), red (≥ 30 kt)
- Station ID on hover tooltip

## County Drawer

Slides in from the right when a county is clicked. Sections:

1. **Header** — county name + tier badge
2. **Coordinates** — lat/lon display
3. **Instability** — MLCAPE, MLCIN, SBCAPE, SBCIN (color-coded)
4. **Kinematics** — SRH 0-1, SRH 0-3, Shear 0-6, Dewpoint
5. **Composites** — EHI, STP, LCL height, lapse rate
6. **Initiation** — cap-break probability banner (color thresholded at 45%/65%);
   WATCH and ⚡ ALARM badges when applicable; Convergence % data cell

## Satellite Imagery

GOES-East tiles served by IEM's tile cache (updated ~5 min):
- VIS: `goes-east-vis-1km` product, 1km resolution
- IR: `goes-east-ir-4km` product, 4km resolution

Both products use standard XYZ tile coordinates (no TMS Y-axis flip).
Sub-selector buttons (VIS / IR) appear in the overlay controls when satellite
is enabled. Switching product remounts the TileLayer (`key={satProduct}`)
for a clean tile flush. Max zoom: 8 (IEM limitation).

## Elevation & Depth

Panels are elevated from the background using border color only (no shadows,
which wash out on dark backgrounds). The map panel has no border — it occupies
the full content area and bleeds to the panel edge.

The county drawer slides in from the right at z-index 1000, with a translucent
`panel` background at 80% opacity.

## Map Legend

The legend is dynamic: entries appear only when the corresponding data is
present in the current state.

Fixed entries:
- Tier color swatch row (EXTREME → LOW)
- Radar color scale (when radar is active)

Conditional entries:
- One row per `detected_by` type present in the boundaries list
- ⚡ Alarm bell entry when any boundary interaction carries `alarm_bell_flag`

## Panel Refresh Intervals

| Panel          | Source        | Interval |
|----------------|---------------|----------|
| Risk zones     | HRRR          | 15 min   |
| Boundaries     | WPC + Mesonet | 15 min   |
| Environment    | Wyoming sounding | 60 min |
| Dryline        | Mesonet       | 5 min    |
| Mesonet obs    | Mesonet       | 5 min    |
| SPC products   | SPC/NWS       | 15 min   |
| Radar (RainViewer) | RainViewer proxy | 2 min |
| Satellite      | IEM tile cache | ~5 min (tile TTL) |

## Components

### TierBadge

A pill badge for displaying a risk tier inline. Uses the `badge-{tier}` tokens.
Text is the tier name in title-cased abbreviated form. Uppercase, 600 weight,
11px, letter-spaced.

### DataCell (CountyDrawer)

A labeled value cell with optional unit and color override. Numeric values
use `toFixed(2)` for EHI/STP, `toFixed(0)` for all others. Null values render
as `—`.

### AlertCard

Used in the SPC panel for active warnings, watches, and MDs. Each card has a
colored left border (2px, tier color) and a tinted background. County list is
truncated with "+N more" if longer than 8 counties.

## Do's and Don'ts

**Do** use `text-dim` for timestamps, labels, and secondary information.
**Do** use `mono` typography for all numeric meteorological values.
**Do** tint map polygon fills at 60–70% opacity so CartoDB base tiles show through.
**Do** make wind arrows point in the direction of travel (FROM + 180°).

**Don't** use white text on the dark panel backgrounds — use `text-primary` (#d0d8e8).
**Don't** add drop shadows; use border contrast for elevation instead.
**Don't** mix tier colors for non-tier content — the severity spectrum should
remain unambiguous at a glance.
**Don't** render drylines in BoundaryLayer — the Dryline toggle owns that path exclusively.
