import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { MapContainer, TileLayer, GeoJSON, Polyline, Marker, useMap } from 'react-leaflet'
import L from 'leaflet'
import type { DashboardState, Tier, CountyPoint, StationObs } from '../types/api'

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
type OverlayKey = 'counties' | 'spc' | 'tornado' | 'wind' | 'hail' | 'warnings' | 'watches' | 'mesonet' | 'dryline' | 'radar'

const OVERLAY_LABELS: Record<OverlayKey, string> = {
  counties: 'County Tiers',
  spc:      'SPC Cat.',
  tornado:  'Torn. Prob.',
  wind:     'Wind Prob.',
  hail:     'Hail Prob.',
  warnings: 'Warnings',
  watches:  'Watches',
  mesonet:  'Mesonet',
  dryline:  'Dryline',
  radar:    'Radar',
}

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
    const props   = feature.properties ?? {}
    const event   = props.event ?? ''
    const expires = props.expires ? new Date(props.expires).toISOString().slice(11, 16) + 'Z' : ''
    const area    = props.areaDesc ?? ''
    ;(layer as L.Path).bindTooltip(
      `<strong style="color:#ff4444">${event}</strong><br/>${area.slice(0, 60)}<br/>exp ${expires}`,
      { sticky: true, opacity: 1 }
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
    const fill   = feature?.properties?.fill   ?? '#888888'
    const stroke = feature?.properties?.stroke ?? fill
    const label  = (feature?.properties?.LABEL ?? '').toString().toUpperCase()
    const isMeta = label === 'SIGN' || label.startsWith('CIG')
    return {
      fillColor:   fill,
      fillOpacity: isMeta ? 0.15 : 0.38,
      color:       stroke,
      weight:      1.5,
      opacity:     isMeta ? 0.60 : 0.85,
      dashArray:   isMeta ? '4 4' : undefined,
    }
  }

  const onEach = (feature: GeoJSON.Feature, layer: L.Layer) => {
    const label2 = (feature?.properties?.LABEL2 ?? '').toString()
    const raw    = (feature?.properties?.LABEL  ?? '').toString()
    if (!raw) return
    const stroke = feature?.properties?.stroke ?? '#888'
    const threat = THREAT_LABEL[threatType]
    ;(layer as L.Path).bindTooltip(
      `<strong style="color:${stroke}">D1 ${threat}</strong><br/><span style="font-size:11px">${label2 || raw}</span>`,
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
  const color = speed >= 30 ? '#ff4444' : speed >= 20 ? '#ff8800' : '#7ec8e3'
  return `<svg width="16" height="16" viewBox="-8 -8 16 16" xmlns="http://www.w3.org/2000/svg"
    style="transform:rotate(${dir}deg);display:block">
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

function MesonetLayer({ observations }: MesonetLayerProps) {
  const markers = useMemo(() => {
    return observations.map(obs => {
      const icon = L.divIcon({
        className: 'station-plot',
        html: `
          <div class="sta-temp">${obs.temp_f.toFixed(0)}</div>
          <div class="sta-wind">${windArrowSvg(obs.wind_dir, obs.wind_speed)}</div>
          <div class="sta-dew" style="color:${dewColor(obs.dewpoint_f)}">${obs.dewpoint_f.toFixed(0)}</div>
        `,
        iconSize: [28, 48],
        iconAnchor: [14, 24],
      })
      return { obs, icon }
    })
  }, [observations])

  return (
    <>
      {markers.map(({ obs, icon }) => (
        <Marker key={obs.station_id} position={[obs.lat, obs.lon]} icon={icon} />
      ))}
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
}

function OverlayControls({ overlays, onToggle, radarStation, onRadarStation }: OverlayControlsProps) {
  return (
    <div className="overlay-controls">
      {(Object.keys(OVERLAY_LABELS) as OverlayKey[]).map(key => (
        <button
          key={key}
          className={`overlay-btn ${overlays[key] ? 'active' : ''}`}
          onClick={() => onToggle(key)}
        >
          {OVERLAY_LABELS[key]}
        </button>
      ))}
      {overlays.radar && (
        <div className="radar-site-selector">
          {RADAR_STATIONS.map(stn => (
            <button
              key={stn.id}
              className={`overlay-btn ${radarStation === stn.id ? 'active' : ''}`}
              onClick={() => onRadarStation(stn.id)}
              title={stn.desc}
            >
              {stn.label}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Radar ─────────────────────────────────────────────────────────────────────
// MRMS:    animated RainViewer composite (~2-min frames from public API)
// Station: NOAA RIDGE2 WMS per-station with TIME stepping

const RADAR_STATIONS = [
  { id: 'mrms', label: 'MRMS',  desc: 'Composite'  },
  { id: 'ktlx', label: 'KTLX', desc: 'OKC/Norman'  },
  { id: 'kvnx', label: 'KVNX', desc: 'Vance/Enid'  },
  { id: 'kinx', label: 'KINX', desc: 'Tulsa'        },
  { id: 'kfdr', label: 'KFDR', desc: 'Altus/SW OK'  },
]

const RADAR_INTERVAL = 600
const RADAR_OPACITY  = 0.70
const RAINVIEWER_API = '/api/radar/frames'   // backend proxy — cached, shared across all clients
const RIDGE2_FRAMES  = 12
const RIDGE2_STEP_MS = 5 * 60 * 1000

interface RadarFrame { time: number; path: string }

function buildRidge2Times() {
  const base = Math.floor(Date.now() / RIDGE2_STEP_MS) * RIDGE2_STEP_MS
  return Array.from({ length: RIDGE2_FRAMES }, (_, i) => {
    const ts = base - (RIDGE2_FRAMES - 1 - i) * RIDGE2_STEP_MS
    return { ts, iso: new Date(ts).toISOString() }
  })
}

// Inside MapContainer: tile layer only (no HUD, no flyTo)
interface RadarTilesProps {
  station:      string
  mrmsFrames:   RadarFrame[]
  ridge2Times:  { ts: number; iso: string }[]
  current:      number
}

function RadarTiles({ station, mrmsFrames, ridge2Times, current }: RadarTilesProps) {
  if (station === 'mrms') {
    const frame = mrmsFrames[current]
    if (!frame) return null
    return (
      <TileLayer
        url={`https://tilecache.rainviewer.com${frame.path}/512/{z}/{x}/{y}/4/1_1.png`}
        tileSize={512}
        zoomOffset={-1}
        opacity={RADAR_OPACITY}
        zIndex={5}
        attribution=""
      />
    )
  }
  const t = ridge2Times[current]
  if (!t) return null
  // Proxy through our backend — NOAA opengeo.ncep.noaa.gov is CORS/ORB blocked.
  // key={station} only: frame changes let react-leaflet call setUrl() which
  // triggers Leaflet's _abortLoading() on in-flight tile requests.
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
  station:      string
  mrmsFrames:   RadarFrame[]
  ridge2Times:  { ts: number; iso: string }[]
  current:      number
  playing:      boolean
  onToggle:     () => void
  onScrub:      (i: number) => void
}

function RadarHUD({ station, mrmsFrames, ridge2Times, current, playing, onToggle, onScrub }: RadarHUDProps) {
  const isMrms = station === 'mrms'
  const count  = isMrms ? mrmsFrames.length : ridge2Times.length
  const tsMs   = isMrms
    ? (mrmsFrames[current]?.time ?? 0) * 1000
    : (ridge2Times[current]?.ts  ?? 0)
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
      <button onClick={onToggle} style={{ background: 'none', border: 'none', color: '#ccc', cursor: 'pointer', fontSize: 13, padding: 0 }}>
        {playing ? '⏸' : '▶'}
      </button>
      <input
        type="range" min={0} max={Math.max(0, count - 1)} value={current}
        onChange={e => onScrub(Number(e.target.value))}
        style={{ width: 110, accentColor: '#4af' }}
      />
      <span style={{ minWidth: 48, color: '#eee' }}>{ts}</span>
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
    counties: true,
    spc:      true,
    tornado:  true,
    wind:     false,
    hail:     false,
    warnings: true,
    watches:  true,
    mesonet:  false,
    dryline:  true,
    radar:    false,
  })
  const [radarStation, setRadarStation] = useState('mrms')

  // ── Radar state (lifted so HUD can live outside MapContainer) ──────────────
  const [mrmsFrames,  setMrmsFrames]  = useState<RadarFrame[]>([])
  const [ridge2Times, setRidge2Times] = useState(() => buildRidge2Times())
  const [radarCurrent, setRadarCurrent] = useState(0)
  const [radarPlaying, setRadarPlaying] = useState(true)

  // Load MRMS frames from RainViewer when radar is on
  useEffect(() => {
    if (!overlays.radar || radarStation !== 'mrms') return
    const load = () => {
      fetch(RAINVIEWER_API)
        .then(r => r.json())
        .then(data => {
          const past: RadarFrame[] = data?.radar?.past ?? []
          if (past.length > 0) { setMrmsFrames(past); setRadarCurrent(past.length - 1) }
        })
        .catch(() => {})
    }
    load()
    const id = setInterval(load, 5 * 60 * 1000)
    return () => clearInterval(id)
  }, [overlays.radar, radarStation])

  // Refresh RIDGE2 times every 5 min
  useEffect(() => {
    if (!overlays.radar || radarStation === 'mrms') return
    const refresh = () => { setRidge2Times(buildRidge2Times()); setRadarCurrent(RIDGE2_FRAMES - 1) }
    refresh()
    const id = setInterval(refresh, 5 * 60 * 1000)
    return () => clearInterval(id)
  }, [overlays.radar, radarStation])

  // Animation ticker
  useEffect(() => {
    if (!overlays.radar || !radarPlaying) return
    const count = radarStation === 'mrms' ? mrmsFrames.length : RIDGE2_FRAMES
    if (count === 0) return
    const id = setInterval(() => setRadarCurrent(p => (p + 1) % count), RADAR_INTERVAL)
    return () => clearInterval(id)
  }, [overlays.radar, radarPlaying, radarStation, mrmsFrames.length])

  // Reset frame index when switching stations
  const handleRadarStation = useCallback((id: string) => {
    setRadarStation(id)
    setRadarCurrent(id === 'mrms' ? Math.max(0, mrmsFrames.length - 1) : RIDGE2_FRAMES - 1)
  }, [mrmsFrames.length])

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

  const tierMap   = state?.tier_map    ?? {}
  const dryline   = state?.dryline
  const alertGJ   = state?.alert_geojson
  const outlookGJ = state?.outlook_geojson
  const tornGJ    = state?.torn_geojson
  const windGJ    = state?.wind_geojson
  const hailGJ    = state?.hail_geojson
  const mesoObs   = state?.mesonet_obs ?? []

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

        {/* CartoDB Dark Matter base tiles */}
        <TileLayer
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
          attribution='&copy; <a href="https://carto.com/">CARTO</a>'
          subdomains="abcd"
          maxZoom={19}
        />

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
            mrmsFrames={mrmsFrames}
            ridge2Times={ridge2Times}
            current={radarCurrent}
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

        {/* Dryline polyline */}
        {overlays.dryline && drylinePositions.length > 0 && (
          <Polyline
            positions={drylinePositions}
            pathOptions={{ color: '#ff8800', weight: 4, opacity: 0.9, dashArray: '12 6' }}
          />
        )}
      </MapContainer>

      {/* Overlay toggle controls */}
      <OverlayControls
        overlays={overlays}
        onToggle={toggleOverlay}
        radarStation={radarStation}
        onRadarStation={handleRadarStation}
      />

      {/* Radar HUD — outside MapContainer so position:absolute anchors to this div */}
      {overlays.radar && (mrmsFrames.length > 0 || radarStation !== 'mrms') && (
        <RadarHUD
          station={radarStation}
          mrmsFrames={mrmsFrames}
          ridge2Times={ridge2Times}
          current={radarCurrent}
          playing={radarPlaying}
          onToggle={() => setRadarPlaying(p => !p)}
          onScrub={i => { setRadarCurrent(i); setRadarPlaying(false) }}
        />
      )}

      {/* Legend */}
      {activeTiers.length > 0 && (
        <div className="map-legend">
          {activeTiers.map(({ tier, label }) => (
            <div key={tier} className="legend-row">
              <div className="legend-swatch" style={{ background: TIER_STYLE[tier].color, opacity: 0.8 }} />
              <span>{label.replace('_', ' ')}</span>
            </div>
          ))}
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
