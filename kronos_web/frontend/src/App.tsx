import { useMemo, useState } from 'react'
import { useSSE } from './hooks/useSSE'
import type { DashboardState, Tier } from './types/api'
import { Header }            from './components/Header'
import { RiskMap }           from './components/RiskMap'
import { EnvironmentPanel }  from './components/EnvironmentPanel'
import { SPCPanel }          from './components/SPCPanel'
import { TendencyTable }     from './components/TendencyTable'
import { AlertFeed }         from './components/AlertFeed'
import { CountyDrawer }      from './components/CountyDrawer'
import { AnaloguePanel }     from './components/AnaloguePanel'

export default function App() {
  const state = useSSE<DashboardState>('/api/stream')
  const [selectedCounty, setSelectedCounty] = useState<string | null>(null)

  const selectedPt = useMemo(() => {
    if (!selectedCounty || !state) return null
    return state.hrrr_counties.find(p => p.county === selectedCounty) ?? null
  }, [selectedCounty, state])

  const selectedTier = selectedCounty
    ? ((state?.tier_map?.[selectedCounty] ?? null) as Tier | null)
    : null

  const env = state?.environment

  return (
    <div className="app-root">
      <Header state={state} />

      {!state ? (
        <div className="connecting" style={{ flex: 1 }}>
          <div className="pulse" />
          Connecting to KRONOS-WX…
        </div>
      ) : (
        <div className="app-body">
          <div className="main-row">
            {/* Live map */}
            <div className="map-panel">
              <RiskMap state={state} onCountyClick={setSelectedCounty} />
            </div>

            {/* Right column */}
            <div className="right-col">
              <EnvironmentPanel
                oun={env?.oun ?? null}
                lmn={env?.lmn ?? null}
                fwd={env?.fwd ?? null}
                ces={state.ces}
                model={state.model_forecast}
                hour={env?.fetched_hour ?? null}
              />
              <SPCPanel spc={state.spc} />
            </div>
          </div>

          {/* Initiation candidates banner */}
          {(state.initiation_candidates ?? []).length > 0 && (
            <div style={{
              background: 'rgba(255, 140, 0, 0.12)',
              border: '1px solid rgba(255, 140, 0, 0.5)',
              borderRadius: 4, padding: '6px 12px',
              display: 'flex', alignItems: 'center', gap: 10,
              fontSize: 12, margin: '4px 0',
            }}>
              <span style={{ fontWeight: 700, color: '#ffa500', whiteSpace: 'nowrap' }}>
                ⚡ INITIATION CANDIDATES
              </span>
              <span style={{ color: 'var(--color-text-dim)' }}>
                {state.initiation_candidates.map((c, i) => {
                  const pt = state.hrrr_counties.find(p => p.county === c)
                  const prob = pt?.cap_break_prob != null ? ` ${(pt.cap_break_prob * 100).toFixed(0)}%` : ''
                  return (
                    <span key={c}>
                      {i > 0 && ' · '}
                      <button
                        onClick={() => setSelectedCounty(c)}
                        style={{ background: 'none', border: 'none', cursor: 'pointer',
                          color: '#ffa500', fontWeight: 600, padding: 0, fontSize: 12 }}
                      >
                        {c.replace(/_/g, ' ')}
                      </button>
                      <span style={{ color: 'var(--color-text-dim)' }}>{prob}</span>
                    </span>
                  )
                })}
              </span>
            </div>
          )}

          {/* Bottom rows */}
          <div className="bottom-row">
            <TendencyTable rows={state.tendency} hrrrValid={state.hrrr_valid} />
            <AnaloguePanel analogues={state.analogues ?? []} />
            <AlertFeed entries={state.alert_log} />
          </div>
        </div>
      )}

      {/* County slide-in drawer */}
      <CountyDrawer
        countyName={selectedCounty}
        countyData={selectedPt}
        tier={selectedTier}
        onClose={() => setSelectedCounty(null)}
      />
    </div>
  )
}
