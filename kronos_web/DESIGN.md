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
risk-tier fills, NWS warning/watch polygon overlays, the SPC Day 1 outlook
polygon, and the detected dryline polyline.

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
┌─ Header ─────────────────────────────────────────────────────────┐
│  KRONOS-WX · HH:MMZ · [SPC badge] · Last HRRR: HH:MMZ            │
├─ Map (2fr) ──────────────────────────┬─ Right Column (1fr) ──────┤
│                                      │  EnvironmentPanel         │
│  Leaflet + CartoDB Dark Matter       │  OUN / LMN dual-col       │
│  County choropleth (risk tiers)      │  CES + model forecast     │
│  NWS warning/watch polygons          ├───────────────────────────┤
│  SPC Day1 outlook polygon            │  SPCPanel                 │
│  Dryline polyline overlay            │  Warnings · Watches · MDs │
│  Hover → tooltip, click → drawer    │                           │
├─ TendencyTable ──────────────────────┴───────────────────────────┤
│  County · Tier · ΔCIN · ΔCAPE · ΔSRH-1 · ΔSRH-3 · ΔEHI · Trend  │
├─ AlertFeed ────────────────────────────────────────────────────────┤
│  Scrolling log · tier changes · new warnings · dryline events     │
└───────────────────────────────────────────────────────────────────┘
```

On narrow viewports (<1200px) the right column drops below the map.

## Elevation & Depth

Panels are elevated from the background using border color only (no shadows,
which wash out on dark backgrounds). The map panel has no border — it occupies
the full content area and bleeds to the panel edge.

The county drawer slides in from the right at z-index 1000, with a translucent
`panel` background backdrop at 80% opacity.

## Components

### TierBadge

A pill badge for displaying a risk tier inline. Uses the `badge-{tier}` tokens.
Text is the tier name in title-cased abbreviated form (EXTREME, HIGH, MOD, etc.).
Uppercase, 600 weight, 11px, letter-spaced.

### ValueCell

A table cell for a numeric meteorological value. The cell background is
transparent; the value text color is chosen from the CAPE/CIN/SRH color ramps
defined in the colors section. A subtle right-aligned layout keeps columns
visually aligned in the environment panel.

### AlertCard

Used in the SPC panel for active warnings, watches, and MDs. Each card has a
colored left border (2px, tier color) and a tinted background. County list is
truncated with "+N more" if longer than 8 counties.

## Do's and Don'ts

**Do** use `text-dim` for timestamps, labels, and secondary information.
**Do** use `mono` typography for all numeric meteorological values.
**Do** tint map polygon fills at 60–70% opacity so CartoDB base tiles show through.

**Don't** use white text on the dark panel backgrounds — use `text-primary` (#d0d8e8).
**Don't** add drop shadows; use border contrast for elevation instead.
**Don't** mix tier colors for non-tier content — the severity spectrum should
remain unambiguous at a glance.
