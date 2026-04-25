import type { SPCData, AlertData, MDData } from '../types/api'

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

function TornadoWarning({ a }: { a: AlertData }) {
  return (
    <div className="alert-card tornado-warning">
      <div className="event-label" style={{ color: 'var(--warning-tornado)' }}>
        🌪 TORNADO WARNING
      </div>
      <div className="area">{abbrevCounties(a.area_desc, 5)}</div>
      <div className="expires">expires {a.expires_label}</div>
    </div>
  )
}

function TornadoWatch({ a }: { a: AlertData }) {
  const num = a.watch_number ? ` #${a.watch_number}` : ''
  return (
    <div className="alert-card tornado-watch">
      <div className="event-label" style={{ color: 'var(--warning-watch)' }}>
        TORNADO WATCH{num}
      </div>
      <div className="area">{abbrevCounties(a.area_desc, 8)}</div>
      <div className="expires">expires {a.expires_label}</div>
    </div>
  )
}

function SvrWarning({ alerts }: { alerts: AlertData[] }) {
  const counties = abbrevCounties(
    alerts.map(a => a.area_desc).join(', '), 6
  )
  return (
    <div className="alert-card svr-warning">
      <div className="event-label" style={{ color: 'var(--warning-svr)' }}>
        {alerts.length} SVR THUNDERSTORM WARNING{alerts.length > 1 ? 'S' : ''}
      </div>
      <div className="area">{counties}</div>
    </div>
  )
}

function MDCard({ md }: { md: MDData }) {
  const probPct = md.prob_watch != null ? md.prob_watch : null
  const probColor = probPct != null
    ? probPct >= 60 ? '#ff2222' : probPct >= 40 ? '#ff6600' : '#ffcc00'
    : 'var(--color-text-dim)'
  return (
    <div className="alert-card md-card">
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
    </div>
  )
}

export function SPCPanel({ spc }: Props) {
  const outlook       = spc?.outlook
  const alerts        = spc?.alerts ?? []
  const mds           = spc?.mds    ?? []
  const torWarnings   = alerts.filter(a => a.event === 'Tornado Warning')
  const torWatches    = alerts.filter(a => a.event === 'Tornado Watch')
  const svrWarnings   = alerts.filter(a => a.event === 'Severe Thunderstorm Warning')

  const cat      = outlook?.category ?? 'NONE'
  const prob     = outlook?.max_tornado_prob
  const sig      = outlook?.sig_tornado_hatched
  const hasAlerts = torWarnings.length + torWatches.length + svrWarnings.length + mds.length > 0

  return (
    <div className={`panel spc-panel ${torWarnings.length > 0 ? 'alert' : 'warn'}`}>
      <div className="panel-header">
        <span className="panel-title">SPC Products</span>
        <span className="panel-subtitle">{new Date().toISOString().slice(11, 16)}Z</span>
      </div>
      <div className="panel-body" style={{ padding: '8px 12px', overflowY: 'auto' }}>

        {/* D1 outlook row */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
          <span style={{ fontSize: 11, color: 'var(--color-text-dim)', letterSpacing: '0.06em' }}>D1</span>
          <span className={`spc-badge ${cat}`}>{cat}</span>
          {prob != null && prob > 0 && (
            <span className="mono" style={{ fontSize: 12, color: 'var(--color-text-dim)' }}>
              torn {Math.round(prob * 100)}%
            </span>
          )}
          {sig && (
            <span style={{ color: '#ff2222', fontSize: 12, fontWeight: 700 }}>⚠ SIG TOR</span>
          )}
        </div>

        {/* Active products */}
        {torWarnings.map((a, i) => <TornadoWarning key={i} a={a} />)}
        {torWatches.map((a, i)  => <TornadoWatch   key={i} a={a} />)}
        {svrWarnings.length > 0 && <SvrWarning alerts={svrWarnings} />}
        {mds.map((md, i)        => <MDCard key={i} md={md} />)}

        {!hasAlerts && (
          <div style={{ color: 'var(--color-text-muted)', fontSize: 12 }}>
            No active warnings, watches, or MDs
          </div>
        )}
      </div>
    </div>
  )
}
