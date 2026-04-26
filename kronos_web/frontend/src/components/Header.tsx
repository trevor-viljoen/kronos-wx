import { useEffect, useState } from 'react'
import type { DashboardState } from '../types/api'

interface Props {
  state: DashboardState | null
}

const CAT_COLOR: Record<string, string> = {
  HIGH: '#ff2222', MDT: '#ff5533', ENH: '#ff8800',
  SLGT: '#ffcc00', MRGL: '#44aa44', TSTM: '#5a7090', NONE: '#2a4060',
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

  const outlook    = state?.spc?.outlook
  const cat        = outlook?.category ?? 'NONE'
  const prob       = outlook?.max_tornado_prob
  const sig        = outlook?.sig_tornado_hatched
  const torActive  = (state?.spc?.alerts ?? []).some(a => a.event === 'Tornado Warning')

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
        }}>
          TOR WARNING ACTIVE
        </span>
      )}

      <span className="header-spacer" />

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
