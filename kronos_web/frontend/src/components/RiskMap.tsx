import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { MapContainer, TileLayer, GeoJSON, Polyline, Marker, Tooltip, useMap } from 'react-leaflet'
import L from 'leaflet'
import type { DashboardState, Tier, CountyPoint, StationObs, BoundaryData, BoundaryInteractionData } from '../types/api'

// ── Tier → map fill color + opacity ──────────────────────────────────────────
const TIER_STYLE: Record<Tier, { color: string; fillOpacity: number }> = {
  EXTREME:          { color: '#ff2222', fillOpacity: 0.70 },
  HIGH:             { color: '#ff6600', fillOpacity: 0.65 },
  DANGEROUS_CAPPED: { color: '#cc44ff', fillOpacity: 0.65 },
  MODERATE:         { color: '#ffcc00', fillOpacity: 0.55 },
  MARGINAL:         { color: '#44aa44', fillOpacity: 0.45 },
  LOW:              { color: 'transparent', fillOpacity: 0 },
}

const ALERT_STYLE: Record<string, { color: string; fillOpacity: number }> = {
  'Tornado Warning':             { color: '#ff2222', fillOpacity: 0.35 },
  'Tornado Watch':               { color: '#33cc66', fillOpacity: 0.20 },
  'Severe Thunderstorm Warning': { color: '#ffcc00', fillOpacity: 0.20 },
}

// Traditional SPC categorical colors (official palette)
const SPC_CAT_STYLE: Record<string, { color: string; fillOpacity: number }> = {
  HIGH:  { color: '#ff00ff', fillOpacity: 0.50 },
  MDT:   { color: '#ff0000', fillOpacity: 0.45 },
  ENH:   { color: '#ff8c00', fillOpacity: 0.38 },
  SLGT:  { color: '#ffff00', fillOpacity: 0.30 },
  MRGL:  { color: '#008b00', fillOpacity: 0.25 },
  TSTM:  { color: '#c1e9c1', fillOpacity: 0.15 },  // light green, not blue
}

const SPC_CAT_DESC: Record<string, string> = {
  HIGH:  'High Risk — Particularly dangerous situation likely',
  MDT:   'Moderate Risk — Significant severe weather expected',
  ENH:   'Enhanced Risk — Numerous severe storms likely',
  SLGT:  'Slight Risk — Isolated to scattered severe',
  MRGL:  'Marginal Risk — Isolated severe possible',
  TSTM:  'General Thunder — Non-severe convection likely',
}

const LEGEND_TIERS: Array<{ tier: Tier; label: string }> = [
  { tier: 'EXTREME',          label: 'EXTREME' },
  { tier: 'HIGH',             label: 'HIGH' },
  { tier: 'DANGEROUS_CAPPED', label: 'DANGEROUS CAPPED' },
  { tier: 'MODERATE',         label: 'MODERATE' },
  { tier: 'MARGINAL',         label: 'MARGINAL' },
]

const OK_BOUNDS: L.LatLngBoundsExpression = [[33.5, -103.1], [37.1, -94.4]]

// ── Overlay keys ──────────────────────────────────────────────────────────────
type OverlayKey = 'counties' | 'spc' | 'tornado' | 'wind' | 'hail' | 'warnings' | 'watches' | 'mesonet' | 'dryline' | 'boundaries' | 'satellite' | 'radar'

const OVERLAY_LABELS: Record<OverlayKey, string> = {
  counties:   'County Tiers',
  spc:        'SPC Cat.',
  tornado:    'Torn. Prob.',
  wind:       'Wind Prob.',
  hail:       'Hail Prob.',
  warnings:   'Warnings',
  watches:    'Watches',
  mesonet:    'Mesonet',
  dryline:    'Dryline',
  boundaries: 'Boundaries',
  satellite:  'Satellite',
  radar:      'Radar',
}

// ── GOES-East satellite tile layers ──────────────────────────────────────────
// IEM tile service: updated every ~5 min, free public access
const GOES_VIS_URL  = 'https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/goes-east-vis-1km/{z}/{x}/{y}.png'
const GOES_IR_URL   = 'https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/goes-east-ir-4km/{z}/{x}/{y}.png'

type SatProduct = 'vis' | 'ir'
const SAT_LABELS: Record<SatProduct, string> = { vis: 'VIS', ir: 'IR' }

type MesonetRegion = 'ok' | 'all'
const MESONET_REGION_LABELS: Record<MesonetRegion, string> = { ok: 'OK', all: 'All' }
// Oklahoma bounding box (generous, includes border stations)
const OK_MESONET_BOUNDS = { minLat: 33.5, maxLat: 37.1, minLon: -103.1, maxLon: -94.3 }

// ── Fit Oklahoma bounds on first render ───────────────────────────────────────
function FitBounds() {
  const map = useMap()
  const fitted = useRef(false)
  useEffect(() => {
    if (!fitted.current) {
      map.fitBounds(OK_BOUNDS, { padding: [10, 10] })
      fitted.current = true
    }
  }, [map])
  return null
}

// ── Inject SVG defs (CIG hatch patterns) into Leaflet's overlay SVG ──────────
// CIG1 = sparse (12px), CIG2 = medium (8px), CIG3 = dense (5px)
// SIGN is the legacy label — same density as CIG1
function SvgDefs() {
  const map = useMap()
  useEffect(() => {
    const inject = () => {
      const svg = map.getPanes().overlayPane?.querySelector<SVGElement>('svg')
      if (!svg || svg.querySelector('#cig-hatch-1')) return
      const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs')
      defs.innerHTML = `
        <pattern id="cig-hatch-1" x="0" y="0" width="12" height="12" patternUnits="userSpaceOnUse">
          <line x1="0" y1="12" x2="12" y2="0" stroke="#000" stroke-width="2" stroke-opacity="0.50"/>
        </pattern>
        <pattern id="cig-hatch-2" x="0" y="0" width="8" height="8" patternUnits="userSpaceOnUse">
          <line x1="0" y1="8" x2="8" y2="0" stroke="#000" stroke-width="2" stroke-opacity="0.55"/>
        </pattern>
        <pattern id="cig-hatch-3" x="0" y="0" width="5" height="5" patternUnits="userSpaceOnUse">
          <line x1="0" y1="5" x2="5" y2="0" stroke="#000" stroke-width="2" stroke-opacity="0.60"/>
        </pattern>`
      svg.insertBefore(defs, svg.firstChild)
    }
    inject()
    map.on('zoomend', inject)
    return () => { map.off('zoomend', inject) }
  }, [map])
  return null
}

const CIG_PATTERN: Record<string, string> = {
  CIG1: 'url(#cig-hatch-1)',
  CIG2: 'url(#cig-hatch-2)',
  CIG3: 'url(#cig-hatch-3)',
  SIGN: 'url(#cig-hatch-1)',  // legacy label
}

// ── County GeoJSON layer ──────────────────────────────────────────────────────
interface CountyLayerProps {
  geojson: GeoJSON.FeatureCollection
  countyData: Map<string, CountyPoint>
  tierMap: Record<string, Tier>
  onCountyClick: (name: string) => void
}

function CountyLayer({ geojson, countyData, tierMap, onCountyClick }: CountyLayerProps) {
  const keyRef = useRef(0)
  const prevTierMap = useRef<Record<string, Tier>>({})

  if (JSON.stringify(tierMap) !== JSON.stringify(prevTierMap.current)) {
    prevTierMap.current = tierMap
    keyRef.current++
  }

  const styleFeature = (feature?: GeoJSON.Feature): L.PathOptions => {
    const name = normalizeCountyName(feature?.properties?.NAME ?? '')
    const tier = tierMap[name] as Tier | undefined
    if (!tier || tier === 'LOW') {
      return { fillColor: 'transparent', fillOpacity: 0, color: 'rgba(30,58,95,0.5)', weight: 0.7 }
    }
    const s = TIER_STYLE[tier]
    return { fillColor: s.color, fillOpacity: s.fillOpacity, color: s.color, weight: 0.8, opacity: 0.6 }
  }

  const onEachFeature = (feature: GeoJSON.Feature, layer: L.Layer) => {
    const rawName = feature.properties?.NAME ?? ''
    const name    = normalizeCountyName(rawName)
    const pt      = countyData.get(name)
    const tier    = tierMap[name]

    ;(layer as L.Path).bindTooltip(buildTooltip(rawName, tier, pt), {
      sticky: true,
      className: 'county-tooltip',
      opacity: 1,
    })

    layer.on('click', () => onCountyClick(name))
    layer.on('mouseover', function (this: L.Path) {
      if (tier && tier !== 'LOW') this.setStyle({ weight: 2, opacity: 1 })
    })
    layer.on('mouseout', function (this: L.Path) {
      this.setStyle(styleFeature(feature))
    })
  }

  return (
    <GeoJSON
      key={keyRef.current}
      data={geojson}
      style={styleFeature}
      onEachFeature={onEachFeature}
    />
  )
}

function buildTooltip(rawName: string, tier?: Tier, pt?: CountyPoint): string {
  const tierColor = tier ? (TIER_STYLE[tier]?.color ?? '#fff') : 'rgba(255,255,255,0.4)'
  const tierLabel = tier && tier !== 'LOW' ? tier.replace('_', ' ') : 'LOW'
  const vals = pt
    ? `CAPE ${pt.MLCAPE.toFixed(0)} · CIN ${pt.MLCIN.toFixed(0)}<br/>` +
      `SRH-1 ${pt.SRH_0_1km.toFixed(0)} · SRH-3 ${pt.SRH_0_3km.toFixed(0)}<br/>` +
      `EHI ${pt.EHI != null ? pt.EHI.toFixed(2) : '—'} · Td ${pt.dewpoint_2m_F.toFixed(0)}°F`
    : 'Loading…'
  return `
    <div class="county-tooltip">
      <div class="county-name">${rawName} Co.</div>
      <div style="margin-bottom:4px">
        <span class="tier-badge ${tier ?? 'LOW'}" style="color:${tierColor}">${tierLabel}</span>
      </div>
      <div class="county-vals">${vals}</div>
    </div>`
}

function normalizeCountyName(raw: string): string {
  return raw.toUpperCase().replace(/\s+/g, '_').replace(/-/g, '_')
}

// ── NWS alert polygon layer ───────────────────────────────────────────────────
interface AlertLayerProps {
  geojson: GeoJSON.FeatureCollection
}

function AlertLayer({ geojson }: AlertLayerProps) {
  const styleAlert = (feature?: GeoJSON.Feature): L.PathOptions => {
    const event = feature?.properties?.event ?? ''
    const s = ALERT_STYLE[event] ?? { color: '#ffffff', fillOpacity: 0.15 }
    return {
      fillColor: s.color,
      fillOpacity: s.fillOpacity,
      color: s.color,
      weight: 2,
      opacity: 0.9,
      dashArray: event === 'Tornado Warning' ? undefined : '6 4',
    }
  }

  const onEachAlert = (feature: GeoJSON.Feature, layer: L.Layer) => {
    const props      = feature.properties ?? {}
    const event      = props.event ?? ''
    const expires    = props.expires
      ? new Date(props.expires).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })
      : ''
    const area       = props.areaDesc ?? ''
    const headline   = props.headline ?? ''
    const desc       = props.description ?? ''
    const instr      = props.instruction ?? ''
    const color      = event.includes('Tornado') ? '#ff2222' : '#ffcc00'

    ;(layer as L.Path).bindTooltip(
      `<strong style="color:${color}">${event}</strong><br/>${area.slice(0, 60)}<br/>exp ${expires}`,
      { sticky: true, opacity: 1 }
    )

    const body = [desc, instr].filter(Boolean).join('\n\n— PROTECTIVE ACTION —\n\n')
    ;(layer as L.Path).bindPopup(
      `<div class="alert-popup-wrap">
        <div class="alert-popup-event" style="color:${color}">${event}</div>
        ${headline ? `<div class="alert-popup-headline">${headline}</div>` : ''}
        <div class="alert-popup-meta">Areas: ${area}</div>
        ${expires ? `<div class="alert-popup-meta">Expires: ${expires}</div>` : ''}
        <pre class="alert-popup-body">${body || 'No text available.'}</pre>
      </div>`,
      { maxWidth: 420, maxHeight: 500, className: 'alert-popup' }
    )
  }

  return <GeoJSON key={Math.random()} data={geojson} style={styleAlert} onEachFeature={onEachAlert} />
}

// ── SPC outlook layer ─────────────────────────────────────────────────────────
interface OutlookLayerProps {
  geojson: GeoJSON.FeatureCollection
}

function OutlookLayer({ geojson }: OutlookLayerProps) {
  const styleOutlook = (feature?: GeoJSON.Feature): L.PathOptions => {
    const label = (feature?.properties?.LABEL2 ?? feature?.properties?.LABEL ?? '').toUpperCase()
    const s = SPC_CAT_STYLE[label] ?? { color: '#888888', fillOpacity: 0.10 }
    return {
      fillColor: s.color,
      fillOpacity: s.fillOpacity,
      color: s.color,
      weight: 1.5,
      opacity: 0.7,
      dashArray: '4 6',
    }
  }

  const onEachOutlook = (feature: GeoJSON.Feature, layer: L.Layer) => {
    const label = (feature?.properties?.LABEL2 ?? feature?.properties?.LABEL ?? '').toUpperCase()
    if (!label) return
    const style = SPC_CAT_STYLE[label]
    const desc  = SPC_CAT_DESC[label] ?? `SPC D1: ${label}`
    const color = style?.color ?? '#888'
    ;(layer as L.Path).bindTooltip(
      `<strong style="color:${color}">SPC D1 — ${label}</strong><br/><span style="font-size:11px">${desc}</span>`,
      { sticky: true, opacity: 1 },
    )
  }

  return <GeoJSON key="outlook" data={geojson} style={styleOutlook} onEachFeature={onEachOutlook} />
}

// ── SPC probabilistic threat layer (tornado / wind / hail) ───────────────────
// Standard SPC probability contour colors
const SPC_PROB_COLORS: Record<string, string> = {
  '0.02': '#008b00',
  '0.05': '#8b4513',
  '0.10': '#ffff00',
  '0.15': '#ff8c00',
  '0.30': '#ff0000',
  '0.45': '#ff00ff',
  '0.60': '#8b0000',
  'SIGN': '#000000',   // significant severe hatch — rendered as dark overlay
  // Also handle integer-percent labels some files use
  '2':  '#008b00',
  '5':  '#8b4513',
  '10': '#ffff00',
  '15': '#ff8c00',
  '30': '#ff0000',
  '45': '#ff00ff',
  '60': '#8b0000',
}

const THREAT_LABEL: Record<string, string> = {
  tornado: 'Tornado',
  wind:    'Damaging Wind',
  hail:    'Large Hail',
}

interface ThreatLayerProps {
  geojson: GeoJSON.FeatureCollection
  threatType: 'tornado' | 'wind' | 'hail'
}

function ThreatLayer({ geojson, threatType }: ThreatLayerProps) {
  const style = (feature?: GeoJSON.Feature): L.PathOptions => {
    // Use official SPC fill/stroke colors embedded in the GeoJSON
    const fill    = feature?.properties?.fill   ?? '#888888'
    const stroke  = feature?.properties?.stroke ?? fill
    const label   = (feature?.properties?.LABEL ?? '').toString().toUpperCase()
    const pattern = CIG_PATTERN[label]  // CIG1/CIG2/CIG3/SIGN → url(#cig-hatch-N)
    if (pattern) {
      return {
        fillColor:   pattern,
        fillOpacity: 1,
        color:       '#000',
        weight:      2,
        opacity:     0.75,
        dashArray:   '5 4',
      }
    }
    return {
      fillColor:   fill,
      fillOpacity: 0.38,
      color:       stroke,
      weight:      1.5,
      opacity:     0.85,
    }
  }

  const onEach = (feature: GeoJSON.Feature, layer: L.Layer) => {
    const label2 = (feature?.properties?.LABEL2 ?? '').toString()
    const raw    = (feature?.properties?.LABEL  ?? '').toString().toUpperCase()
    if (!raw) return
    const stroke  = feature?.properties?.stroke ?? '#888'
    const threat  = THREAT_LABEL[threatType]
    const isCig   = CIG_PATTERN[raw] != null
    const cigDesc = raw === 'CIG3' ? 'Sig. — Highest Conditional Intensity'
                  : raw === 'CIG2' ? 'Sig. — Elevated Conditional Intensity'
                  : isCig          ? 'Sig. — Conditional Intensity (10%+)'
                  : (label2 || raw)
    ;(layer as L.Path).bindTooltip(
      `<strong style="color:${isCig ? '#fff' : stroke}">D1 ${threat}</strong><br/><span style="font-size:11px">${cigDesc}</span>`,
      { sticky: true, opacity: 1 },
    )
  }

  return (
    <GeoJSON
      key={`${threatType}-${geojson.features.length}`}
      data={geojson}
      style={style}
      onEachFeature={onEach}
    />
  )
}

// ── Tornado/SVR watch county overlay ─────────────────────────────────────────
function parseWatchCounties(areaDesc: string): Set<string> {
  const names = new Set<string>()
  for (const part of areaDesc.split(/[,;]/)) {
    const noState = part.trim().replace(/,?\s+[A-Z]{2}\s*$/, '').trim()
    const name    = noState.replace(/\s+County\s*$/i, '').trim().toUpperCase()
    if (name) names.add(name)
  }
  return names
}

interface WatchLayerProps {
  alerts: DashboardState['spc']['alerts']
  countiesGeoJSON: GeoJSON.FeatureCollection
}

function WatchLayer({ alerts, countiesGeoJSON }: WatchLayerProps) {
  // Build county → watch metadata map
  const watchMap = useMemo(() => {
    const m = new Map<string, { color: string; label: string; num?: number }>()
    for (const a of alerts) {
      if (a.event !== 'Tornado Watch' && a.event !== 'Severe Thunderstorm Watch') continue
      const color = a.event === 'Tornado Watch' ? '#33cc66' : '#ffcc00'
      const label = a.event === 'Tornado Watch' ? 'TORNADO WATCH' : 'SVR TSTM WATCH'
      for (const county of parseWatchCounties(a.area_desc)) {
        m.set(county, { color, label, num: a.watch_number ?? undefined })
      }
    }
    return m
  }, [alerts])

  const filteredGJ = useMemo(() => ({
    ...countiesGeoJSON,
    features: countiesGeoJSON.features.filter(f =>
      watchMap.has((f.properties?.NAME ?? '').toUpperCase())
    ),
  }), [watchMap, countiesGeoJSON])

  if (watchMap.size === 0 || filteredGJ.features.length === 0) return null

  const style = (feature?: GeoJSON.Feature): L.PathOptions => {
    const w = watchMap.get((feature?.properties?.NAME ?? '').toUpperCase())
    if (!w) return { fillOpacity: 0, opacity: 0, weight: 0 }
    return {
      fillColor:    w.color,
      fillOpacity:  0.22,
      color:        w.color,
      weight:       2.5,
      opacity:      0.95,
      dashArray:    '8 5',
    }
  }

  const onEach = (feature: GeoJSON.Feature, layer: L.Layer) => {
    const w = watchMap.get((feature?.properties?.NAME ?? '').toUpperCase())
    if (!w) return
    const numStr = w.num != null ? ` #${w.num}` : ''
    ;(layer as L.Path).bindTooltip(
      `<strong style="color:${w.color}">${w.label}${numStr}</strong><br/>${feature.properties?.NAME} Co.`,
      { sticky: true, opacity: 1 },
    )
  }

  return (
    <GeoJSON
      key={[...watchMap.keys()].sort().join(',')}
      data={filteredGJ as GeoJSON.FeatureCollection}
      style={style}
      onEachFeature={onEach}
    />
  )
}

// ── Mesonet station plot layer ────────────────────────────────────────────────
interface MesonetLayerProps {
  observations: StationObs[]
}

function windArrowSvg(dir: number, speed: number): string {
  // dir is meteorological FROM direction (0° = FROM north, 270° = FROM west).
  // Arrow should point in the direction the wind is traveling TO,
  // so rotate by dir + 180°.
  const toDir = (dir + 180) % 360
  const color = speed >= 30 ? '#ff4444' : speed >= 20 ? '#ff8800' : '#7ec8e3'
  return `<svg width="16" height="16" viewBox="-8 -8 16 16" xmlns="http://www.w3.org/2000/svg"
    style="transform:rotate(${toDir}deg);display:block">
    <line x1="0" y1="6" x2="0" y2="-6" stroke="${color}" stroke-width="1.5"/>
    <polygon points="0,-8 -3,-2 3,-2" fill="${color}"/>
  </svg>`
}

function dewColor(td: number): string {
  if (td >= 65) return '#00e676'
  if (td >= 55) return '#66bb6a'
  if (td >= 45) return '#aed581'
  return '#78909c'
}

const COMPASS = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW']
function toCompass(deg: number): string {
  return COMPASS[Math.round(deg / 22.5) % 16]
}

function StationMarker({ obs }: { obs: StationObs }) {
  const markerRef = useRef<L.Marker>(null)
  const pressTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  const icon = useMemo(() => L.divIcon({
    className: 'station-plot',
    html: `
      <div class="sta-temp">${obs.temp_f.toFixed(0)}</div>
      <div class="sta-wind">${windArrowSvg(obs.wind_dir, obs.wind_speed)}</div>
      <div class="sta-dew" style="color:${dewColor(obs.dewpoint_f)}">${obs.dewpoint_f.toFixed(0)}</div>
    `,
    iconSize: [28, 48],
    iconAnchor: [14, 24],
  }), [obs])

  const spread = obs.temp_f - obs.dewpoint_f
  const spreadColor = spread <= 5 ? '#00e676' : spread <= 15 ? '#ffcc00' : '#ff6644'

  // Attach native touch listeners for long-press tooltip on mobile.
  // react-leaflet's eventHandlers only covers Leaflet events, not DOM touch events.
  useEffect(() => {
    const el = markerRef.current?.getElement()
    if (!el) return
    let timer: ReturnType<typeof setTimeout> | null = null
    const start = () => { timer = setTimeout(() => markerRef.current?.openTooltip(), 500) }
    const cancel = () => { if (timer) { clearTimeout(timer); timer = null } }
    el.addEventListener('touchstart', start, { passive: true })
    el.addEventListener('touchend',   cancel)
    el.addEventListener('touchmove',  cancel, { passive: true })
    return () => {
      el.removeEventListener('touchstart', start)
      el.removeEventListener('touchend',   cancel)
      el.removeEventListener('touchmove',  cancel)
    }
  }, []) // marker element is stable after mount

  return (
    <Marker ref={markerRef} position={[obs.lat, obs.lon]} icon={icon}>
      <Tooltip className="sta-tooltip" sticky>
        <div className="sta-tip-id">{obs.station_id}</div>
        <div className="sta-tip-row">
          <span className="sta-tip-label">Temp</span>
          <span>{obs.temp_f.toFixed(1)}°F</span>
        </div>
        <div className="sta-tip-row">
          <span className="sta-tip-label">Dwpt</span>
          <span style={{ color: dewColor(obs.dewpoint_f) }}>{obs.dewpoint_f.toFixed(1)}°F</span>
        </div>
        <div className="sta-tip-row">
          <span className="sta-tip-label">Sprd</span>
          <span style={{ color: spreadColor }}>{spread.toFixed(1)}°F</span>
        </div>
        <div className="sta-tip-row">
          <span className="sta-tip-label">Wind</span>
          <span>{toCompass(obs.wind_dir)} {obs.wind_speed.toFixed(0)} mph</span>
        </div>
        {obs.wind_gust != null && (
          <div className="sta-tip-row">
            <span className="sta-tip-label">Gust</span>
            <span style={{ color: obs.wind_gust >= 30 ? '#ff4444' : 'inherit' }}>
              {obs.wind_gust.toFixed(0)} mph
            </span>
          </div>
        )}
      </Tooltip>
    </Marker>
  )
}

function MesonetLayer({ observations }: MesonetLayerProps) {
  return (
    <>
      {observations.map(obs => (
        <StationMarker key={obs.station_id} obs={obs} />
      ))}
    </>
  )
}

// ── Boundary polylines (WPC fronts + outflow + dryline) ──────────────────────

// Visual styling per detected_by type
const BOUNDARY_STYLE: Record<string, { color: string; weight: number; dashArray?: string; opacity: number }> = {
  wpc_cold_front:       { color: '#4488ff', weight: 3, opacity: 0.9 },
  wpc_warm_front:       { color: '#ff4444', weight: 3, opacity: 0.9 },
  wpc_stationary_front: { color: '#9944ff', weight: 3, dashArray: '8 4', opacity: 0.85 },
  wpc_occluded_front:   { color: '#cc44ff', weight: 3, opacity: 0.85 },
  wpc_trough:           { color: '#cc8822', weight: 2, dashArray: '6 4', opacity: 0.80 },
  wpc_dryline:          { color: '#ff8800', weight: 3, dashArray: '10 5', opacity: 0.90 },
  mesonet_windshift:    { color: '#aaaaaa', weight: 2, dashArray: '4 4', opacity: 0.80 },
  mesonet_td_gradient:  { color: '#ff8800', weight: 4, dashArray: '12 6', opacity: 0.90 },
  mesonet_wind_pressure: { color: '#aaaaaa', weight: 2, dashArray: '4 4', opacity: 0.80 },
}

const BOUNDARY_LABEL: Record<string, string> = {
  wpc_cold_front:       'Cold Front',
  wpc_warm_front:       'Warm Front',
  wpc_stationary_front: 'Stationary Front',
  wpc_occluded_front:   'Occluded Front',
  wpc_trough:           'Trough',
  wpc_dryline:          'Dryline (WPC)',
  mesonet_td_gradient:  'Dryline (Mesonet)',
  mesonet_windshift:    'Outflow Boundary',
  mesonet_wind_pressure: 'Outflow Boundary',
}

interface BoundaryLayerProps {
  boundaries: BoundaryData[]
  interactions: BoundaryInteractionData[]
}

function BoundaryLayer({ boundaries, interactions }: BoundaryLayerProps) {
  const alarmCounties = useMemo(
    () => new Set(interactions.filter(ix => ix.alarm_bell_flag).map(ix => ix.interaction_county)),
    [interactions]
  )

  return (
    <>
      {boundaries.map((b, idx) => {
        if (!b.position_lat?.length) return null
        const style = BOUNDARY_STYLE[b.detected_by] ?? { color: '#ffffff', weight: 2, opacity: 0.7 }
        const label = BOUNDARY_LABEL[b.detected_by] ?? b.detected_by
        const positions: L.LatLngExpression[] = b.position_lat.map((lat, i) => [lat, b.position_lon[i]])
        return (
          <Polyline
            key={`boundary-${idx}`}
            positions={positions}
            pathOptions={style}
          >
            <Tooltip sticky>
              {label} — conf: {(b.confidence * 100).toFixed(0)}%
              {b.motion_direction != null ? ` | moving ${b.motion_direction.toFixed(0)}°` : ''}
            </Tooltip>
          </Polyline>
        )
      })}

      {/* Alarm bell markers at boundary interaction points */}
      {interactions.filter(ix => ix.alarm_bell_flag).map((ix, idx) => {
        const icon = L.divIcon({
          className: '',
          html: '<div style="font-size:18px;line-height:1;text-shadow:0 0 4px #000">⚡</div>',
          iconSize: [20, 20],
          iconAnchor: [10, 10],
        })
        return (
          <Marker
            key={`alarm-${idx}`}
            position={[ix.interaction_point_lat, ix.interaction_point_lon]}
            icon={icon}
          />
        )
      })}
    </>
  )
}

// ── Tornado warning flash banner ──────────────────────────────────────────────
interface FlashBannerProps {
  alerts: DashboardState['spc']['alerts']
}

function FlashBanner({ alerts }: FlashBannerProps) {
  const [visible, setVisible] = useState(false)
  const prevCount = useRef(0)

  const tornadoWarnings = alerts.filter(a => a.event === 'Tornado Warning')

  useEffect(() => {
    if (tornadoWarnings.length > prevCount.current) {
      setVisible(true)
      const t = setTimeout(() => setVisible(false), 8000)
      prevCount.current = tornadoWarnings.length
      return () => clearTimeout(t)
    }
    prevCount.current = tornadoWarnings.length
  }, [tornadoWarnings.length])

  if (!visible || tornadoWarnings.length === 0) return null

  return (
    <div className="warning-flash">
      <span className="flash-icon">⚠</span>
      TORNADO WARNING
      <span className="flash-count">{tornadoWarnings.length} active</span>
    </div>
  )
}

// ── Overlay toggle controls ───────────────────────────────────────────────────
interface OverlayControlsProps {
  overlays: Record<OverlayKey, boolean>
  onToggle: (key: OverlayKey) => void
  radarStation: string
  onRadarStation: (id: string) => void
  satProduct: SatProduct
  onSatProduct: (p: SatProduct) => void
  mesonetRegion: MesonetRegion
  onMesonetRegion: (r: MesonetRegion) => void
}

function OBtn({ label, active, onClick, title }: { label: string; active: boolean; onClick: () => void; title?: string }) {
  return (
    <button className={`overlay-btn ${active ? 'active' : ''}`} onClick={onClick} title={title}>
      {label}
    </button>
  )
}

function OverlayControls({ overlays, onToggle, radarStation, onRadarStation, satProduct, onSatProduct, mesonetRegion, onMesonetRegion }: OverlayControlsProps) {
  return (
    <div className="overlay-panel">

      {/* Risk analysis row */}
      <div className="overlay-group">
        <span className="overlay-group-label">RISK</span>
        <OBtn label="Tiers"  active={overlays.counties}   onClick={() => onToggle('counties')} />
        <OBtn label="Bounds" active={overlays.boundaries} onClick={() => onToggle('boundaries')} />
        <OBtn label="Dryline" active={overlays.dryline}   onClick={() => onToggle('dryline')} />
      </div>

      {/* SPC products row */}
      <div className="overlay-group">
        <span className="overlay-group-label">SPC</span>
        <OBtn label="Cat"  active={overlays.spc}     onClick={() => onToggle('spc')} />
        <OBtn label="Torn" active={overlays.tornado} onClick={() => onToggle('tornado')} />
        <OBtn label="Wind" active={overlays.wind}    onClick={() => onToggle('wind')} />
        <OBtn label="Hail" active={overlays.hail}    onClick={() => onToggle('hail')} />
      </div>

      {/* Alerts row */}
      <div className="overlay-group">
        <span className="overlay-group-label">ALERTS</span>
        <OBtn label="Warn"  active={overlays.warnings} onClick={() => onToggle('warnings')} />
        <OBtn label="Watch" active={overlays.watches}  onClick={() => onToggle('watches')} />
      </div>

      {/* Surface obs row */}
      <div className="overlay-group">
        <span className="overlay-group-label">OBS</span>
        <OBtn label="Meso" active={overlays.mesonet} onClick={() => onToggle('mesonet')} />
        {overlays.mesonet && (
          <>
            <span className="overlay-sep" />
            <OBtn label="OK"  active={mesonetRegion === 'ok'}  onClick={() => onMesonetRegion('ok')}  title="Oklahoma only" />
            <OBtn label="All" active={mesonetRegion === 'all'} onClick={() => onMesonetRegion('all')} title="All stations (TX/NM/CO)" />
          </>
        )}
        <OBtn label="SAT" active={overlays.satellite} onClick={() => onToggle('satellite')} />
        {overlays.satellite && (
          <>
            <span className="overlay-sep" />
            <OBtn label="VIS" active={satProduct === 'vis'} onClick={() => onSatProduct('vis')} title="GOES-East Visible" />
            <OBtn label="IR"  active={satProduct === 'ir'}  onClick={() => onSatProduct('ir')}  title="GOES-East Infrared" />
          </>
        )}
      </div>

      {/* Radar row */}
      <div className="overlay-group">
        <span className="overlay-group-label">RADAR</span>
        <OBtn label="On" active={overlays.radar} onClick={() => onToggle('radar')} />
        {overlays.radar && (
          <>
            <span className="overlay-sep" />
            {RADAR_STATIONS.map(stn => (
              <OBtn key={stn.id} label={stn.label} active={radarStation === stn.id}
                onClick={() => onRadarStation(stn.id)} title={stn.desc} />
            ))}
          </>
        )}
      </div>

    </div>
  )
}

// ── Radar ─────────────────────────────────────────────────────────────────────
// Composite: IEM NEXRAD N0Q composite (mesonet.agron.iastate.edu) — no API key, same
//            source as satellite tiles. Animated via ?t={unix_seconds} param.
// Station:   NOAA RIDGE2 WMS per-station with TIME stepping (proxied through backend).

const RADAR_STATIONS = [
  { id: 'mrms', label: 'NEXRAD', desc: 'Composite' },
  { id: 'ktlx', label: 'KTLX',  desc: 'OKC/Norman' },
  { id: 'kvnx', label: 'KVNX',  desc: 'Vance/Enid' },
  { id: 'kinx', label: 'KINX',  desc: 'Tulsa'       },
  { id: 'kfdr', label: 'KFDR',  desc: 'Altus/SW OK' },
]

const RADAR_INTERVAL = 600
const RADAR_OPACITY  = 0.70
const RADAR_FRAMES   = 12
const RADAR_STEP_MS  = 5 * 60 * 1000

// IEM NEXRAD N0Q composite — only serves current data; ?t= is a cache-buster
const IEM_NEXRAD_URL = 'https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/nexrad-n0q-900913/{z}/{x}/{y}.png'
// Refresh interval for composite live tile (ms)
const MRMS_REFRESH_MS = 2 * 60 * 1000

function buildRadarTimes() {
  const base = Math.floor(Date.now() / RADAR_STEP_MS) * RADAR_STEP_MS
  return Array.from({ length: RADAR_FRAMES }, (_, i) => {
    const ts = base - (RADAR_FRAMES - 1 - i) * RADAR_STEP_MS
    return { ts, iso: new Date(ts).toISOString() }
  })
}

// Inside MapContainer: tile layer only (no HUD, no flyTo)
interface RadarTilesProps {
  station:    string
  radarTimes: { ts: number; iso: string }[]
  current:    number
  liveTick:   number   // increments every 2 min to refresh composite URL
}

function RadarTiles({ station, radarTimes, current, liveTick }: RadarTilesProps) {
  if (station === 'mrms') {
    // IEM only serves current data — show a single live tile, refresh every 2 min
    return (
      <TileLayer
        key="mrms-live"
        url={`${IEM_NEXRAD_URL}?t=${liveTick}`}
        opacity={RADAR_OPACITY}
        zIndex={5}
        attribution='NEXRAD &copy; <a href="https://mesonet.agron.iastate.edu/">IEM</a>'
      />
    )
  }

  // Per-station RIDGE2 — proxy through backend (NOAA CORS blocked)
  const t = radarTimes[current]
  if (!t) return null
  const timeEnc = encodeURIComponent(t.iso)
  return (
    <TileLayer
      key={station}
      url={`/api/radar/tile/${station}/${timeEnc}/{z}/{x}/{y}`}
      opacity={RADAR_OPACITY}
      zIndex={5}
      attribution="NOAA NEXRAD RIDGE2"
    />
  )
}

// Outside MapContainer: HUD rendered in RiskMap's container div
interface RadarHUDProps {
  station:     string
  radarTimes:  { ts: number; iso: string }[]
  current:     number
  playing:     boolean
  onToggle:    () => void
  onScrub:     (i: number) => void
}

function RadarHUD({ station, radarTimes, current, playing, onToggle, onScrub }: RadarHUDProps) {
  const isMrms = station === 'mrms'
  const count  = radarTimes.length
  const tsMs   = radarTimes[current]?.ts ?? 0
  const ts = tsMs ? new Date(tsMs).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : ''
  const stn = RADAR_STATIONS.find(s => s.id === station)

  return (
    <div style={{
      position: 'absolute',
      bottom: 32,
      left: '50%',
      transform: 'translateX(-50%)',
      zIndex: 1000,
      background: 'rgba(0,0,0,0.80)',
      border: '1px solid rgba(255,255,255,0.12)',
      padding: '5px 12px',
      display: 'flex',
      alignItems: 'center',
      gap: 10,
      fontFamily: 'var(--font-mono)',
      fontSize: 11,
      color: '#ccc',
      pointerEvents: 'all',
      userSelect: 'none',
      whiteSpace: 'nowrap',
    }}>
      {isMrms ? (
        <span style={{ color: '#ff4444', fontWeight: 600, letterSpacing: 1 }}>● LIVE</span>
      ) : (
        <>
          <button onClick={onToggle} style={{ background: 'none', border: 'none', color: '#ccc', cursor: 'pointer', fontSize: 13, padding: 0 }}>
            {playing ? '⏸' : '▶'}
          </button>
          <input
            type="range" min={0} max={Math.max(0, count - 1)} value={current}
            onChange={e => onScrub(Number(e.target.value))}
            style={{ width: 110, accentColor: '#4af' }}
          />
          <span style={{ minWidth: 48, color: '#eee' }}>{ts}</span>
        </>
      )}
      {stn && (
        <span style={{ color: isMrms ? '#88aaff' : '#ffcc66' }}>
          {stn.label}
          <span style={{ color: '#555', marginLeft: 4 }}>({stn.desc})</span>
        </span>
      )}
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────
interface Props {
  state: DashboardState | null
  onCountyClick: (name: string) => void
}

export function RiskMap({ state, onCountyClick }: Props) {
  const [countiesGeoJSON, setCountiesGeoJSON] = useState<GeoJSON.FeatureCollection | null>(null)
  const [overlays, setOverlays] = useState<Record<OverlayKey, boolean>>({
    counties:   true,
    spc:        true,
    tornado:    true,
    wind:       false,
    hail:       false,
    warnings:   true,
    watches:    true,
    mesonet:    false,
    dryline:    true,
    boundaries: true,
    satellite:  false,
    radar:      false,
  })
  const [satProduct, setSatProduct] = useState<SatProduct>('vis')
  const [radarStation, setRadarStation] = useState('mrms')
  const [mesonetRegion, setMesonetRegion] = useState<MesonetRegion>('ok')

  // ── Radar state (lifted so HUD can live outside MapContainer) ──────────────
  const [radarTimes,   setRadarTimes]   = useState(() => buildRadarTimes())
  const [radarCurrent, setRadarCurrent] = useState(RADAR_FRAMES - 1)
  const [radarPlaying, setRadarPlaying] = useState(true)
  // liveTick increments every 2 min to bust the composite tile cache
  const [liveTick,     setLiveTick]     = useState(() => Math.floor(Date.now() / MRMS_REFRESH_MS))

  // Refresh per-station time window every 5 min
  useEffect(() => {
    if (!overlays.radar || radarStation === 'mrms') return
    const refresh = () => { setRadarTimes(buildRadarTimes()); setRadarCurrent(RADAR_FRAMES - 1) }
    const id = setInterval(refresh, 5 * 60 * 1000)
    return () => clearInterval(id)
  }, [overlays.radar, radarStation])

  // Live composite refresh every 2 min
  useEffect(() => {
    if (!overlays.radar || radarStation !== 'mrms') return
    const id = setInterval(() => setLiveTick(Math.floor(Date.now() / MRMS_REFRESH_MS)), MRMS_REFRESH_MS)
    return () => clearInterval(id)
  }, [overlays.radar, radarStation])

  // Animation ticker — per-station only
  useEffect(() => {
    if (!overlays.radar || radarStation === 'mrms' || !radarPlaying) return
    const id = setInterval(() => setRadarCurrent(p => (p + 1) % RADAR_FRAMES), RADAR_INTERVAL)
    return () => clearInterval(id)
  }, [overlays.radar, radarStation, radarPlaying])

  // Reset to latest frame when switching stations
  const handleRadarStation = useCallback((id: string) => {
    setRadarStation(id)
    setRadarCurrent(RADAR_FRAMES - 1)
  }, [])

  useEffect(() => {
    fetch('/api/counties.geojson')
      .then(r => r.json())
      .then(setCountiesGeoJSON)
      .catch(console.error)
  }, [])

  const toggleOverlay = (key: OverlayKey) =>
    setOverlays(prev => ({ ...prev, [key]: !prev[key] }))

  const countyData = useMemo<Map<string, CountyPoint>>(() => {
    const m = new Map<string, CountyPoint>()
    for (const pt of state?.hrrr_counties ?? []) {
      m.set(pt.county, pt)
    }
    return m
  }, [state?.hrrr_counties])

  const tierMap     = state?.tier_map    ?? {}
  const dryline     = state?.dryline
  const boundaries  = state?.boundaries ?? []
  const interactions = state?.boundary_interactions ?? []
  const alertGJ   = state?.alert_geojson
  const outlookGJ = state?.outlook_geojson
  const tornGJ    = state?.torn_geojson
  const windGJ    = state?.wind_geojson
  const hailGJ    = state?.hail_geojson
  const mesoObsAll = state?.mesonet_obs ?? []
  const mesoObs = mesonetRegion === 'ok'
    ? mesoObsAll.filter(o =>
        o.lat >= OK_MESONET_BOUNDS.minLat && o.lat <= OK_MESONET_BOUNDS.maxLat &&
        o.lon >= OK_MESONET_BOUNDS.minLon && o.lon <= OK_MESONET_BOUNDS.maxLon
      )
    : mesoObsAll

  const drylinePositions = useMemo<L.LatLngExpression[]>(() => {
    if (!dryline?.position_lat?.length) return []
    return dryline.position_lat.map((lat, i) => [lat, dryline.position_lon[i]])
  }, [dryline])

  const activeTiers = LEGEND_TIERS.filter(({ tier }) =>
    Object.values(tierMap).includes(tier)
  )

  return (
    <div style={{ position: 'relative', height: '100%', width: '100%' }}>
      <MapContainer
        style={{ height: '100%', width: '100%' }}
        bounds={OK_BOUNDS}
        boundsOptions={{ padding: [10, 10] }}
        zoomControl={true}
      >
        <FitBounds />
        <SvgDefs />

        {/* CartoDB Dark Matter base tiles */}
        <TileLayer
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
          attribution='&copy; <a href="https://carto.com/">CARTO</a>'
          subdomains="abcd"
          maxZoom={19}
        />

        {/* GOES-East satellite imagery.
            goes_east_vis uses TMS Y-convention; goes_east uses standard XYZ. */}
        {overlays.satellite && (
          <TileLayer
            key={satProduct}
            url={satProduct === 'vis' ? GOES_VIS_URL : GOES_IR_URL}
            attribution='GOES-East &copy; <a href="https://mesonet.agron.iastate.edu/">IEM</a>'
            opacity={0.75}
            maxZoom={8}
          />
        )}

        {/* SPC Day1 outlook (bottom-most overlay) */}
        {overlays.spc && outlookGJ && <OutlookLayer geojson={outlookGJ} />}

        {/* SPC probabilistic threat overlays */}
        {overlays.tornado && tornGJ && <ThreatLayer geojson={tornGJ} threatType="tornado" />}
        {overlays.wind    && windGJ && <ThreatLayer geojson={windGJ} threatType="wind" />}
        {overlays.hail    && hailGJ && <ThreatLayer geojson={hailGJ} threatType="hail" />}

        {/* Radar tiles (inside MapContainer only) */}
        {overlays.radar && (
          <RadarTiles
            station={radarStation}
            radarTimes={radarTimes}
            current={radarCurrent}
            liveTick={liveTick}
          />
        )}

        {/* County choropleth */}
        {overlays.counties && countiesGeoJSON && (
          <CountyLayer
            geojson={countiesGeoJSON}
            countyData={countyData}
            tierMap={tierMap}
            onCountyClick={onCountyClick}
          />
        )}

        {/* NWS warning polygons */}
        {overlays.warnings && alertGJ && <AlertLayer geojson={alertGJ} />}

        {/* Watch county overlay (county-based; NWS watch features lack polygon geometry) */}
        {overlays.watches && countiesGeoJSON && state?.spc?.alerts && (
          <WatchLayer alerts={state.spc.alerts} countiesGeoJSON={countiesGeoJSON} />
        )}

        {/* Mesonet station plots */}
        {overlays.mesonet && mesoObs.length > 0 && (
          <MesonetLayer observations={mesoObs} />
        )}

        {/* Dryline polyline (legacy — rendered separately for backwards compat) */}
        {overlays.dryline && drylinePositions.length > 0 && (
          <Polyline
            positions={drylinePositions}
            pathOptions={{ color: '#ff8800', weight: 4, opacity: 0.9, dashArray: '12 6' }}
          />
        )}

        {/* Multi-boundary layer: WPC fronts + outflow (dryline excluded — owned by Dryline toggle) */}
        {overlays.boundaries && (
          <BoundaryLayer
            boundaries={boundaries.filter(b => b.boundary_type !== 'DRYLINE')}
            interactions={interactions}
          />
        )}
      </MapContainer>

      {/* Overlay toggle controls */}
      <OverlayControls
        overlays={overlays}
        onToggle={toggleOverlay}
        radarStation={radarStation}
        onRadarStation={handleRadarStation}
        satProduct={satProduct}
        onSatProduct={setSatProduct}
        mesonetRegion={mesonetRegion}
        onMesonetRegion={setMesonetRegion}
      />

      {/* Radar HUD — outside MapContainer so position:absolute anchors to this div */}
      {overlays.radar && (
        <RadarHUD
          station={radarStation}
          radarTimes={radarTimes}
          current={radarCurrent}
          playing={radarPlaying}
          onToggle={() => setRadarPlaying(p => !p)}
          onScrub={i => { setRadarCurrent(i); setRadarPlaying(false) }}
        />
      )}

      {/* Legend — always visible when the map has loaded */}
      {state && (
        <div className="map-legend">
          {activeTiers.length > 0
            ? activeTiers.map(({ tier, label }) => (
                <div key={tier} className="legend-row">
                  <div className="legend-swatch" style={{ background: TIER_STYLE[tier].color, opacity: 0.8 }} />
                  <span>{label.replace('_', ' ')}</span>
                </div>
              ))
            : (
                <div className="legend-row" style={{ color: 'var(--color-text-dim)', fontSize: 10 }}>
                  No elevated risk
                </div>
              )
          }
          {overlays.watches && state?.spc?.alerts?.some(a => a.event === 'Tornado Watch') && (
            <div className="legend-row">
              <div className="legend-swatch" style={{ background: '#33cc66', opacity: 0.5, border: '2px dashed #33cc66' }} />
              <span style={{ color: '#33cc66' }}>Tor Watch</span>
            </div>
          )}
          {overlays.watches && state?.spc?.alerts?.some(a => a.event === 'Severe Thunderstorm Watch') && (
            <div className="legend-row">
              <div className="legend-swatch" style={{ background: '#ffcc00', opacity: 0.5, border: '2px dashed #ffcc00' }} />
              <span style={{ color: '#ffcc00' }}>SVR Watch</span>
            </div>
          )}
          {overlays.dryline && drylinePositions.length > 0 && (
            <div className="legend-row">
              <div className="legend-swatch" style={{ background: 'transparent', border: '2px dashed #ff8800' }} />
              <span>Dryline</span>
            </div>
          )}
          {overlays.boundaries && boundaries.some(b => b.boundary_type !== 'DRYLINE') && (
            <>
              {boundaries.some(b => b.detected_by === 'wpc_cold_front') && (
                <div className="legend-row">
                  <div className="legend-swatch" style={{ background: 'transparent', border: '2px solid #4488ff' }} />
                  <span>Cold Front</span>
                </div>
              )}
              {boundaries.some(b => b.detected_by === 'wpc_warm_front') && (
                <div className="legend-row">
                  <div className="legend-swatch" style={{ background: 'transparent', border: '2px solid #ff4444' }} />
                  <span>Warm Front</span>
                </div>
              )}
              {boundaries.some(b => b.detected_by === 'wpc_stationary_front') && (
                <div className="legend-row">
                  <div className="legend-swatch" style={{ background: 'transparent', border: '2px dashed #9944ff' }} />
                  <span>Stationary Front</span>
                </div>
              )}
              {boundaries.some(b => b.detected_by === 'wpc_occluded_front') && (
                <div className="legend-row">
                  <div className="legend-swatch" style={{ background: 'transparent', border: '2px solid #cc44ff' }} />
                  <span>Occluded Front</span>
                </div>
              )}
              {boundaries.some(b => b.detected_by === 'wpc_trough') && (
                <div className="legend-row">
                  <div className="legend-swatch" style={{ background: 'transparent', border: '2px dashed #cc8822' }} />
                  <span>Trough</span>
                </div>
              )}
              {boundaries.some(b => b.detected_by === 'mesonet_wind_pressure' || b.detected_by === 'mesonet_windshift') && (
                <div className="legend-row">
                  <div className="legend-swatch" style={{ background: 'transparent', border: '2px dashed #aaaaaa' }} />
                  <span>Outflow</span>
                </div>
              )}
              {interactions.some(ix => ix.alarm_bell_flag) && (
                <div className="legend-row">
                  <span style={{ fontSize: '14px', lineHeight: 1 }}>⚡</span>
                  <span>Boundary Alarm</span>
                </div>
              )}
            </>
          )}
          {overlays.spc && outlookGJ && (
            <>
              {(['HIGH','MDT','ENH','SLGT','MRGL'] as const).map(cat => {
                const s = SPC_CAT_STYLE[cat]
                const hasFeature = outlookGJ.features.some(f =>
                  (f?.properties?.LABEL2 ?? f?.properties?.LABEL ?? '').toUpperCase() === cat
                )
                if (!hasFeature) return null
                return (
                  <div key={cat} className="legend-row">
                    <div className="legend-swatch" style={{ background: s.color, opacity: 0.7 }} />
                    <span style={{ color: 'var(--color-text-dim)' }}>SPC {cat}</span>
                  </div>
                )
              })}
            </>
          )}
        </div>
      )}

      {/* Tornado warning flash banner */}
      {state?.spc?.alerts && <FlashBanner alerts={state.spc.alerts} />}

      {/* Loading state */}
      {!state?.hrrr_valid && (
        <div style={{
          position: 'absolute', inset: 0, zIndex: 600,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          background: 'rgba(5,5,16,0.7)',
          pointerEvents: 'none',
        }}>
          <div className="connecting">
            <div className="pulse" />
            Awaiting HRRR data…
          </div>
        </div>
      )}
    </div>
  )
}
