import { useEffect, useState, useMemo } from 'react'
import type { DashboardState } from '../types/api'

interface Props {
  state: DashboardState | null
}

const TICKER_COLOR: Record<string, string> = {
  'Tornado Warning':             '#ff2222',
  'Tornado Watch':               '#33cc66',
  'Severe Thunderstorm Warning': '#ffcc00',
  'Severe Thunderstorm Watch':   '#ffcc00',
}

function abbrev(areaDesc: string, max = 4): string {
  const parts = areaDesc.replace(/;/g, ',').split(',')
    .map(p => p.trim().replace(/,?\s*[A-Z]{2}$/, '').replace(/\s+County\s*$/i, '').trim())
    .filter(Boolean)
  if (parts.length <= max) return parts.join(', ')
  return parts.slice(0, max).join(', ') + ` +${parts.length - max}`
}

function HeaderTicker({ state }: { state: DashboardState | null }) {
  const items = useMemo(() => {
    const alerts = state?.spc?.alerts ?? []
    const mds    = state?.spc?.mds    ?? []
    const out: { text: string; color: string }[] = []
    for (const a of alerts) {
      const color   = TICKER_COLOR[a.event] ?? 'var(--color-text-dim)'
      const num     = a.watch_number ? ` #${a.watch_number}` : ''
      const expires = a.expires_label ? `  exp ${a.expires_label}` : ''
      out.push({ text: `${a.event.toUpperCase()}${num}  ${abbrev(a.area_desc)}${expires}`, color })
    }
    for (const md of mds) {
      const prob = md.prob_watch != null ? `  ${md.prob_watch}% WATCH` : ''
      out.push({ text: `MD #${md.number}  ${md.concerning.slice(0, 55)}${prob}`, color: '#cc88ff' })
    }
    return out
  }, [state?.spc])

  if (items.length === 0) return <span className="header-spacer" />

  const doubled     = [...items, ...items]
  const durationS   = Math.max(10, items.length * 7)

  return (
    <div className="header-ticker">
      <div className="header-ticker-track" style={{ animationDuration: `${durationS}s` }}>
        {doubled.map((item, i) => (
          <span key={i} className="header-ticker-item">
            <span style={{ color: item.color }}>◆</span>
            <span style={{ color: item.color }}>{item.text}</span>
          </span>
        ))}
      </div>
    </div>
  )
}

export function Header({ state }: Props) {
  const [clock, setClock] = useState('')

  useEffect(() => {
    const tick = () => {
      const now = new Date()
      const h = now.getUTCHours().toString().padStart(2, '0')
      const m = now.getUTCMinutes().toString().padStart(2, '0')
      setClock(`${h}:${m}Z`)
    }
    tick()
    const id = setInterval(tick, 1000)
    return () => clearInterval(id)
  }, [])

  const outlook   = state?.spc?.outlook
  const cat       = outlook?.category ?? 'NONE'
  const prob      = outlook?.max_tornado_prob
  const sig       = outlook?.sig_tornado_hatched
  const torActive = (state?.spc?.alerts ?? []).some(a => a.event === 'Tornado Warning')

  return (
    <header className={`app-header${torActive ? ' tor-active' : ''}`}>
      <span className="header-title">KRONOS&#x2011;WX</span>
      <span className="header-clock">{clock}</span>

      {outlook && (
        <span
          className={`spc-badge ${cat}`}
          title={`SPC Day 1: ${cat}${prob != null ? ` — torn ${Math.round(prob * 100)}%` : ''}`}
        >
          {cat}
          {prob != null && prob > 0 && (
            <span style={{ marginLeft: 8, fontFamily: 'var(--font-mono)', fontSize: 11 }}>
              {Math.round(prob * 100)}%
            </span>
          )}
          {sig && <span style={{ marginLeft: 8, color: '#ff2222', fontSize: 13 }}>SIG</span>}
        </span>
      )}

      {torActive && (
        <span style={{
          fontFamily: 'var(--font-display)',
          fontSize: 15,
          letterSpacing: '0.12em',
          padding: '1px 10px',
          background: '#1c0000',
          border: '1px solid rgba(255,34,34,0.5)',
          color: '#ff2222',
          animation: 'pulse 1.2s ease-in-out infinite',
          flexShrink: 0,
        }}>
          TOR WARNING ACTIVE
        </span>
      )}

      <HeaderTicker state={state} />

      <span className="header-meta">
        {state?.hrrr_valid ? `HRRR ${state.hrrr_valid}` : 'HRRR loading…'}
      </span>
      <span className="header-meta" style={{ color: 'var(--color-text-muted)' }}>
        {state?.updated_at
          ? `upd ${new Date(state.updated_at).toISOString().slice(11, 16)}Z`
          : ''}
      </span>
    </header>
  )
}
