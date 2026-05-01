import { useMemo, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { useSSE } from './hooks/useSSE'
import { useIsMobile } from './hooks/useIsMobile'
import type { DashboardState, Tier, CountyPoint } from './types/api'
import { Header }            from './components/Header'
import { RiskMap }           from './components/RiskMap'
import { EnvironmentPanel }  from './components/EnvironmentPanel'
import { SPCPanel }          from './components/SPCPanel'
import { TendencyTable }     from './components/TendencyTable'
import { AlertFeed }         from './components/AlertFeed'
import { CountyDrawer }      from './components/CountyDrawer'
import { AnaloguePanel }     from './components/AnaloguePanel'
import { NarrativePanel }   from './components/NarrativePanel'

type MobileTab = 'env' | 'tendency' | 'alerts'

function InitiationBanner({ state, onCountyClick }: {
  state: DashboardState
  onCountyClick: (c: string) => void
}) {
  const candidates = state.initiation_candidates ?? []
  return (
    <AnimatePresence>
      {candidates.length > 0 && (
    <motion.div
      initial={{ opacity: 0, y: -8 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -8 }}
      transition={{ duration: 0.3 }}
      style={{
        background: 'rgba(255, 140, 0, 0.12)',
        border: '1px solid rgba(255, 140, 0, 0.5)',
        borderRadius: 4, padding: '6px 12px',
        display: 'flex', alignItems: 'center', gap: 10,
        fontSize: 12, margin: '4px 0', flexShrink: 0,
      }}
    >
      <span style={{ fontWeight: 700, color: '#ffa500', whiteSpace: 'nowrap' }}>
        ⚡ INITIATION CANDIDATES
      </span>
      <span style={{ color: 'var(--color-text-dim)' }}>
        {candidates.map((c, i) => {
          const pt = state.hrrr_counties.find(p => p.county === c)
          const prob = pt?.cap_break_prob != null ? ` ${(pt.cap_break_prob * 100).toFixed(0)}%` : ''
          return (
            <span key={c}>
              {i > 0 && ' · '}
              <button
                onClick={() => onCountyClick(c)}
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
    </motion.div>
      )}
    </AnimatePresence>
  )
}

function MobileLayout({ state, selectedCounty, setSelectedCounty, selectedPt, selectedTier }: {
  state: DashboardState
  selectedCounty: string | null
  setSelectedCounty: (c: string | null) => void
  selectedPt: CountyPoint | null
  selectedTier: Tier | null
}) {
  const [activeTab, setActiveTab] = useState<MobileTab>('env')
  const env = state.environment

  const tabs: { key: MobileTab; label: string }[] = [
    { key: 'env',      label: 'ENV / SPC' },
    { key: 'tendency', label: 'TENDENCY' },
    { key: 'alerts',   label: 'ALERTS' },
  ]

  return (
    <div className="mobile-root">
      <div className="mobile-map">
        <RiskMap state={state} onCountyClick={setSelectedCounty} />
      </div>

      <InitiationBanner state={state} onCountyClick={setSelectedCounty} />

      {/* Tab bar */}
      <div className="mobile-tab-bar">
        {tabs.map(t => (
          <button
            key={t.key}
            className={`mobile-tab-btn${activeTab === t.key ? ' active' : ''}`}
            onClick={() => setActiveTab(t.key)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div className="mobile-tab-content">
        <AnimatePresence mode="wait">
          <motion.div
            key={activeTab}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.15 }}
            style={{ display: 'contents' }}
          >
            {activeTab === 'env' && (
              <>
                <NarrativePanel lines={state.situation_brief ?? []} updatedAt={state.updated_at} />
                <EnvironmentPanel
                  oun={env?.oun ?? null}
                  lmn={env?.lmn ?? null}
                  fwd={env?.fwd ?? null}
                  ces={state.ces}
                  model={state.model_forecast}
                  hour={env?.fetched_hour ?? null}
                />
                <SPCPanel spc={state.spc} />
              </>
            )}
            {activeTab === 'tendency' && (
              <>
                <TendencyTable rows={state.tendency} hrrrValid={state.hrrr_valid} />
                <AnaloguePanel analogues={state.analogues ?? []} oun={env?.oun ?? null} />
              </>
            )}
            {activeTab === 'alerts' && (
              <AlertFeed entries={state.alert_log} />
            )}
          </motion.div>
        </AnimatePresence>
      </div>

      <CountyDrawer
        countyName={selectedCounty}
        countyData={selectedPt}
        tier={selectedTier}
        onClose={() => setSelectedCounty(null)}
      />
    </div>
  )
}

export default function App() {
  const state = useSSE<DashboardState>('/api/stream')
  const isMobile = useIsMobile()
  const [selectedCounty, setSelectedCounty] = useState<string | null>(null)

  const selectedPt = useMemo(() => {
    if (!selectedCounty || !state) return null
    return state.hrrr_counties.find(p => p.county === selectedCounty) ?? null
  }, [selectedCounty, state])

  const selectedTier = selectedCounty
    ? ((state?.tier_map?.[selectedCounty] ?? null) as Tier | null)
    : null

  const env = state?.environment

  if (isMobile) {
    return (
      <div className="app-root">
        <Header state={state} />
        {!state ? (
          <div className="connecting" style={{ flex: 1 }}>
            <div className="pulse" />
            Connecting to KRONOS-WX…
          </div>
        ) : (
          <MobileLayout
            state={state}
            selectedCounty={selectedCounty}
            setSelectedCounty={setSelectedCounty}
            selectedPt={selectedPt}
            selectedTier={selectedTier}
          />
        )}
      </div>
    )
  }

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

          <InitiationBanner state={state} onCountyClick={setSelectedCounty} />

          {/* Bottom rows */}
          <div className="bottom-row">
            <NarrativePanel lines={state.situation_brief ?? []} updatedAt={state.updated_at} />
            <TendencyTable rows={state.tendency} hrrrValid={state.hrrr_valid} />
            <AnaloguePanel analogues={state.analogues ?? []} oun={env?.oun ?? null} />
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
