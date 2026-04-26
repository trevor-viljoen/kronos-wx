import { useState, useMemo } from 'react'
import type { SPCData, AlertData, MDData } from '../types/api'
import { useCollapse } from '../hooks/useCollapse'

interface Props {
  spc: SPCData | null
}

function abbrevCounties(areaDesc: string, max = 6): string {
  const parts = areaDesc
    .replace(/;/g, ',')
    .split(',')
    .map(p => p.trim().replace(/,?\s*[A-Z]{2}$/, '').trim())
    .filter(Boolean)
  if (parts.length <= max) return parts.join(', ')
  return parts.slice(0, max).join(', ') + ` +${parts.length - max}`
}

// ── Shared popup ──────────────────────────────────────────────────────────────

interface PopupProps {
  title: string
  titleColor: string
  meta: { label: string; value: string; valueColor?: string }[]
  body: string
  onClose: () => void
}

function AlertPopup({ title, titleColor, meta, body, onClose }: PopupProps) {
  return (
    <div className="md-popup-overlay" onClick={onClose}>
      <div className="md-popup" onClick={e => e.stopPropagation()}>
        <div className="md-popup-header">
          <span style={{ color: titleColor, fontFamily: 'var(--font-display)', fontSize: 16, letterSpacing: '0.1em' }}>
            {title}
          </span>
          <button className="md-popup-close" onClick={onClose}>✕</button>
        </div>
        {meta.map(({ label, value, valueColor }) => value ? (
          <div key={label} className="md-popup-meta">
            <span style={{ color: 'var(--color-text-dim)' }}>{label}:</span>{' '}
            <span style={valueColor ? { color: valueColor, fontWeight: 700 } : undefined}>{value}</span>
          </div>
        ) : null)}
        <pre className="md-popup-body">
          {body || 'No text available.'}
        </pre>
      </div>
    </div>
  )
}

// ── Alert cards ───────────────────────────────────────────────────────────────

function AlertCard({ a, color, label }: { a: AlertData; color: string; label: string }) {
  const [open, setOpen] = useState(false)
  const cardClass =
    a.event === 'Tornado Warning'              ? 'tornado-warning' :
    a.event === 'Tornado Watch'                ? 'tornado-watch'   :
    a.event === 'Severe Thunderstorm Warning'  ? 'svr-warning'     :
    a.event === 'Severe Thunderstorm Watch'    ? 'svr-watch'       : 'md-card'

  const num = a.watch_number ? ` #${a.watch_number}` : ''

  return (
    <>
      <div
        className={`alert-card ${cardClass}`}
        style={{ cursor: 'pointer' }}
        onClick={() => setOpen(true)}
      >
        <div className="event-label" style={{ color }}>{label}{num}</div>
        <div className="area">{abbrevCounties(a.area_desc, 6)}</div>
        {a.expires_label && (
          <div className="expires">expires {a.expires_label}</div>
        )}
        <div style={{ fontSize: 10, color: 'var(--color-text-muted)', marginTop: 3 }}>
          click to read ↗
        </div>
      </div>

      {open && (
        <AlertPopup
          title={`${a.event}${num}`}
          titleColor={color}
          meta={[
            { label: 'Areas', value: a.area_desc },
            { label: 'Expires', value: a.expires_label },
            { label: 'Headline', value: a.headline },
          ]}
          body={a.description}
          onClose={() => setOpen(false)}
        />
      )}
    </>
  )
}

function MDCard({ md }: { md: MDData }) {
  const [open, setOpen] = useState(false)
  const probPct   = md.prob_watch ?? null
  const probColor = probPct != null
    ? probPct >= 60 ? '#ff2222' : probPct >= 40 ? '#ff6600' : '#ffcc00'
    : 'var(--color-text-dim)'

  return (
    <>
      <div
        className="alert-card md-card"
        style={{ cursor: 'pointer' }}
        onClick={() => setOpen(true)}
      >
        <div className="event-label" style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: 'var(--warning-md)' }}>MD #{md.number}</span>
          {probPct != null && (
            <span className="mono" style={{ fontSize: 11, color: probColor }}>
              {probPct}% watch
            </span>
          )}
        </div>
        {md.concerning && (
          <div className="area" style={{ marginTop: 2 }}>{md.concerning.slice(0, 80)}</div>
        )}
        <div style={{ fontSize: 10, color: 'var(--color-text-muted)', marginTop: 3 }}>
          click to read ↗
        </div>
      </div>

      {open && (
        <AlertPopup
          title={`Mesoscale Discussion #${md.number}`}
          titleColor="var(--warning-md)"
          meta={[
            { label: 'Areas', value: md.areas_affected },
            { label: 'Concerning', value: md.concerning },
            { label: 'Watch probability', value: probPct != null ? `${probPct}%` : '', valueColor: probColor },
          ]}
          body={md.body_lines.join('\n')}
          onClose={() => setOpen(false)}
        />
      )}
    </>
  )
}

// ── Ticker ────────────────────────────────────────────────────────────────────

const TICKER_COLOR: Record<string, string> = {
  'Tornado Warning':             'var(--warning-tornado)',
  'Tornado Watch':               'var(--warning-watch)',
  'Severe Thunderstorm Warning': 'var(--warning-svr)',
  'Severe Thunderstorm Watch':   'var(--warning-svr)',
}

function AlertTicker({ alerts, mds }: { alerts: AlertData[]; mds: MDData[] }) {
  const items = useMemo(() => {
    const out: { text: string; color: string }[] = []
    for (const a of alerts) {
      const color = TICKER_COLOR[a.event] ?? 'var(--color-text-dim)'
      const num   = a.watch_number ? ` #${a.watch_number}` : ''
      const area  = abbrevCounties(a.area_desc, 4)
      out.push({ text: `${a.event.toUpperCase()}${num} · ${area}`, color })
    }
    for (const md of mds) {
      const prob = md.prob_watch != null ? ` · ${md.prob_watch}% watch` : ''
      out.push({ text: `MD #${md.number} · ${md.concerning.slice(0, 60)}${prob}`, color: 'var(--warning-md)' })
    }
    return out
  }, [alerts, mds])

  if (items.length === 0) return null

  // Duplicate items so the scroll loop is seamless
  const doubled = [...items, ...items]
  // Speed: ~60px/s base, capped — wider content scrolls at same apparent speed
  const durationS = Math.max(8, items.length * 6)

  return (
    <div className="alert-ticker">
      <div
        className="alert-ticker-track"
        style={{ animationDuration: `${durationS}s` }}
      >
        {doubled.map((item, i) => (
          <span key={i} className="alert-ticker-item">
            <span style={{ color: item.color, fontWeight: 700 }}>◆</span>
            <span style={{ color: item.color }}>{item.text}</span>
          </span>
        ))}
      </div>
    </div>
  )
}

// ── Panel ─────────────────────────────────────────────────────────────────────

export function SPCPanel({ spc }: Props) {
  const { collapsed, toggle } = useCollapse('spc')
  const outlook     = spc?.outlook
  const alerts      = spc?.alerts ?? []
  const mds         = spc?.mds    ?? []
  const torWarnings = alerts.filter(a => a.event === 'Tornado Warning')
  const torWatches  = alerts.filter(a => a.event === 'Tornado Watch')
  const svrWarnings = alerts.filter(a => a.event === 'Severe Thunderstorm Warning')
  const svrWatches  = alerts.filter(a => a.event === 'Severe Thunderstorm Watch')

  const cat      = outlook?.category ?? 'NONE'
  const prob     = outlook?.max_tornado_prob
  const sig      = outlook?.sig_tornado_hatched
  const hasAlerts = torWarnings.length + torWatches.length + svrWarnings.length + svrWatches.length + mds.length > 0

  return (
    <div className={`panel spc-panel ${torWarnings.length > 0 ? 'alert' : 'warn'}${collapsed ? ' collapsed' : ''}`}>
      <div className="panel-header">
        <span className="panel-title">SPC Products</span>
        <span className="panel-subtitle">{new Date().toISOString().slice(11, 16)}Z</span>
        <button className="panel-collapse-btn" onClick={toggle}>{collapsed ? '▸' : '▾'}</button>
      </div>
      {!collapsed && <AlertTicker alerts={alerts} mds={mds} />}
      <div className="panel-body" style={{ padding: '8px 12px', overflowY: 'auto' }}>

        {/* D1 outlook */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
          <span style={{ fontFamily: 'var(--font-display)', fontSize: 12, letterSpacing: '0.1em', color: 'var(--color-text-dim)' }}>D1</span>
          <span className={`spc-badge ${cat}`}>{cat}</span>
          {prob != null && prob > 0 && (
            <span className="mono" style={{ fontSize: 12, color: 'var(--color-text-dim)' }}>
              torn {Math.round(prob * 100)}%
            </span>
          )}
          {sig && (
            <span style={{ fontFamily: 'var(--font-display)', fontSize: 13, letterSpacing: '0.1em', color: '#ff2222' }}>SIG TOR</span>
          )}
        </div>

        {torWarnings.map((a, i) => (
          <AlertCard key={i} a={a} color="var(--warning-tornado)" label="TORNADO WARNING" />
        ))}
        {torWatches.map((a, i) => (
          <AlertCard key={i} a={a} color="var(--warning-watch)" label="TORNADO WATCH" />
        ))}
        {svrWatches.map((a, i) => (
          <AlertCard key={i} a={a} color="var(--warning-svr)" label="SVR THUNDERSTORM WATCH" />
        ))}
        {svrWarnings.map((a, i) => (
          <AlertCard key={i} a={a} color="var(--warning-svr)" label="SVR THUNDERSTORM WARNING" />
        ))}
        {mds.map((md, i) => (
          <MDCard key={i} md={md} />
        ))}

        {!hasAlerts && (
          <div style={{ color: 'var(--color-text-muted)', fontSize: 11 }}>
            No active warnings, watches, or MDs
          </div>
        )}
      </div>
    </div>
  )
}
