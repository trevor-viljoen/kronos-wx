import { useEffect, useMemo, useRef, useState } from 'react'
import { MapContainer, TileLayer, GeoJSON, Polyline, useMap } from 'react-leaflet'
import L from 'leaflet'
import type { DashboardState, Tier, CountyPoint } from '../types/api'

// Tier → map fill color + opacity
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
  'Tornado Watch':               { color: '#ff6600', fillOpacity: 0.20 },
  'Severe Thunderstorm Warning': { color: '#ffcc00', fillOpacity: 0.20 },
}

const SPC_CAT_STYLE: Record<string, { color: string; fillOpacity: number }> = {
  HIGH:  { color: '#ff2222', fillOpacity: 0.30 },
  MDT:   { color: '#ff5533', fillOpacity: 0.28 },
  ENH:   { color: '#ff8800', fillOpacity: 0.25 },
  SLGT:  { color: '#ffcc00', fillOpacity: 0.20 },
  MRGL:  { color: '#44aa44', fillOpacity: 0.18 },
  TSTM:  { color: '#4444aa', fillOpacity: 0.12 },
}

const LEGEND_TIERS: Array<{ tier: Tier; label: string }> = [
  { tier: 'EXTREME',          label: 'EXTREME' },
  { tier: 'HIGH',             label: 'HIGH' },
  { tier: 'DANGEROUS_CAPPED', label: 'DANGEROUS CAPPED' },
  { tier: 'MODERATE',         label: 'MODERATE' },
  { tier: 'MARGINAL',         label: 'MARGINAL' },
]

const OK_BOUNDS: L.LatLngBoundsExpression = [[33.5, -103.1], [37.1, -94.4]]

// ── Fit bounds on load ────────────────────────────────────────────────────────
function FitBounds() {
  const map = useMap()
  useEffect(() => {
    map.fitBounds(OK_BOUNDS, { padding: [10, 10] })
  }, [map])
  return null
}

// ── County GeoJSON layer that re-renders when tierMap changes ─────────────────
interface CountyLayerProps {
  geojson: GeoJSON.FeatureCollection
  countyData: Map<string, CountyPoint>
  tierMap: Record<string, Tier>
  onCountyClick: (name: string) => void
}

function CountyLayer({ geojson, countyData, tierMap, onCountyClick }: CountyLayerProps) {
  const keyRef = useRef(0)
  const prevTierMap = useRef<Record<string, Tier>>({})

  // Force re-render when tier assignments change
  if (JSON.stringify(tierMap) !== JSON.stringify(prevTierMap.current)) {
    prevTierMap.current = tierMap
    keyRef.current++
  }

  const styleFeature = (feature?: GeoJSON.Feature): L.PathOptions => {
    const name = normalizeCountyName(feature?.properties?.NAME ?? '')
    const tier = tierMap[name] as Tier | undefined
    if (!tier || tier === 'LOW') {
      return {
        fillColor: 'transparent',
        fillOpacity: 0,
        color: 'rgba(30,58,95,0.5)',
        weight: 0.7,
      }
    }
    const s = TIER_STYLE[tier]
    return {
      fillColor: s.color,
      fillOpacity: s.fillOpacity,
      color: s.color,
      weight: 0.8,
      opacity: 0.6,
    }
  }

  const onEachFeature = (feature: GeoJSON.Feature, layer: L.Layer) => {
    const rawName = feature.properties?.NAME ?? ''
    const name    = normalizeCountyName(rawName)
    const pt      = countyData.get(name)
    const tier    = tierMap[name]

    const tooltipContent = buildTooltip(rawName, tier, pt)

    ;(layer as L.Path).bindTooltip(tooltipContent, {
      sticky: true,
      className: 'county-tooltip',
      opacity: 1,
    })

    layer.on('click', () => onCountyClick(name))
    layer.on('mouseover', function (this: L.Path) {
      if (tier && tier !== 'LOW') {
        this.setStyle({ weight: 2, opacity: 1 })
      }
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
  // Census TIGER NAME field is uppercase (e.g. "OKLAHOMA") — match to enum
  return raw.toUpperCase().replace(/\s+/g, '_').replace(/-/g, '_')
}

// ── NWS alert polygon layer ────────────────────────────────────────────────────
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

// ── SPC outlook layer ──────────────────────────────────────────────────────────
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
      weight: 1,
      opacity: 0.5,
      dashArray: '4 6',
    }
  }

  return <GeoJSON key="outlook" data={geojson} style={styleOutlook} />
}

// ── Main component ─────────────────────────────────────────────────────────────
interface Props {
  state: DashboardState | null
  onCountyClick: (name: string) => void
}

export function RiskMap({ state, onCountyClick }: Props) {
  const [countiesGeoJSON, setCountiesGeoJSON] = useState<GeoJSON.FeatureCollection | null>(null)

  useEffect(() => {
    fetch('/api/counties.geojson')
      .then(r => r.json())
      .then(setCountiesGeoJSON)
      .catch(console.error)
  }, [])

  const countyData = useMemo<Map<string, CountyPoint>>(() => {
    const m = new Map<string, CountyPoint>()
    for (const pt of state?.hrrr_counties ?? []) {
      m.set(pt.county, pt)
    }
    return m
  }, [state?.hrrr_counties])

  const tierMap    = state?.tier_map    ?? {}
  const dryline    = state?.dryline
  const alertGJ    = state?.alert_geojson
  const outlookGJ  = state?.outlook_geojson

  // Dryline polyline positions
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
        center={[35.5, -98.5]}
        zoom={7}
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
        {outlookGJ && <OutlookLayer geojson={outlookGJ} />}

        {/* County choropleth */}
        {countiesGeoJSON && (
          <CountyLayer
            geojson={countiesGeoJSON}
            countyData={countyData}
            tierMap={tierMap}
            onCountyClick={onCountyClick}
          />
        )}

        {/* NWS warning / watch polygons (on top) */}
        {alertGJ && <AlertLayer geojson={alertGJ} />}

        {/* Dryline polyline */}
        {drylinePositions.length > 0 && (
          <Polyline
            positions={drylinePositions}
            pathOptions={{
              color: '#ff8800',
              weight: 3,
              opacity: 0.9,
              dashArray: '10 6',
            }}
          />
        )}
      </MapContainer>

      {/* Legend */}
      {activeTiers.length > 0 && (
        <div className="map-legend">
          {activeTiers.map(({ tier, label }) => (
            <div key={tier} className="legend-row">
              <div
                className="legend-swatch"
                style={{ background: TIER_STYLE[tier].color, opacity: 0.8 }}
              />
              <span>{label.replace('_', ' ')}</span>
            </div>
          ))}
          {drylinePositions.length > 0 && (
            <div className="legend-row">
              <div className="legend-swatch" style={{
                background: 'transparent',
                border: '2px dashed #ff8800',
              }} />
              <span>Dryline</span>
            </div>
          )}
        </div>
      )}

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
