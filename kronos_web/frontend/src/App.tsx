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
                ces={state.ces}
                model={state.model_forecast}
                hour={env?.fetched_hour ?? null}
              />
              <SPCPanel spc={state.spc} />
            </div>
          </div>

          {/* Bottom rows */}
          <div className="bottom-row">
            <TendencyTable rows={state.tendency} hrrrValid={state.hrrr_valid} />
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
