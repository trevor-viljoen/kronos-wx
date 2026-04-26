import type { AnalogueEntry } from '../types/api'

interface Props {
  analogues: AnalogueEntry[]
}

const EC_LABEL: Record<string, string> = {
  SIGNIFICANT_OUTBREAK: 'SIG OUTBREAK',
  ISOLATED_SIGNIFICANT: 'ISOLATED SIG',
  MARGINAL_EVENT:       'MARGINAL',
  NULL_BUST:            'NULL BUST',
  ACTIVE_NULL:          'ACTIVE NULL',
}

const EC_COLOR: Record<string, string> = {
  SIGNIFICANT_OUTBREAK: 'var(--tier-extreme)',
  ISOLATED_SIGNIFICANT: 'var(--tier-high)',
  MARGINAL_EVENT:       'var(--tier-marginal)',
  NULL_BUST:            'var(--color-text-dim)',
  ACTIVE_NULL:          'var(--color-text-dim)',
}

const CB_LABEL: Record<string, string> = {
  CLEAN_EROSION:   'CLEAN',
  EARLY_EROSION:   'EARLY',
  LATE_EROSION:    'LATE',
  NO_EROSION:      'NONE',
  BOUNDARY_FORCED: 'BOUNDARY',
  RECONSTITUTED:   'RECON',
}

const CB_COLOR: Record<string, string> = {
  CLEAN_EROSION:   '#44cc44',
  EARLY_EROSION:   '#88cc44',
  LATE_EROSION:    '#ffcc00',
  NO_EROSION:      '#ff4444',
  BOUNDARY_FORCED: '#44aaff',
  RECONSTITUTED:   '#ff8800',
}

function fmt(v: number | null, dec = 0): string {
  if (v == null) return '—'
  return v.toFixed(dec)
}

export function AnaloguePanel({ analogues }: Props) {
  if (analogues.length === 0) {
    return (
      <div className="panel anal-panel">
        <div className="panel-header">
          <span className="panel-title">Analogues</span>
        </div>
        <div className="panel-body" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <div className="connecting"><div className="pulse" />Computing…</div>
        </div>
      </div>
    )
  }

  return (
    <div className="panel anal-panel">
      <div className="panel-header">
        <span className="panel-title">Historical Analogues</span>
        <span className="panel-subtitle">cap similarity · top {analogues.length}</span>
      </div>
      <div className="panel-body" style={{ padding: '0', overflowX: 'auto' }}>
        <table className="anal-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Event</th>
              <th title="Number of tornadoes">TOR</th>
              <th title="MLCAPE J/kg">CAPE</th>
              <th title="MLCIN J/kg">CIN</th>
              <th title="12Z Tc gap °F">Tc gap</th>
              <th title="Cap behavior">Cap</th>
              <th title="Analogue distance (lower = closer)">Dist</th>
            </tr>
          </thead>
          <tbody>
            {analogues.map(a => {
              const ecColor  = EC_COLOR[a.event_class ?? ''] ?? 'var(--color-text)'
              const cbColor  = CB_COLOR[a.cap_behavior ?? ''] ?? 'var(--color-text-dim)'
              const ecLabel  = EC_LABEL[a.event_class ?? ''] ?? (a.event_class ?? '—')
              const cbLabel  = CB_LABEL[a.cap_behavior ?? ''] ?? (a.cap_behavior ?? '—')
              const tcColor  = a.tc_gap_12Z == null ? undefined
                             : a.tc_gap_12Z < 0 ? '#44cc44'
                             : a.tc_gap_12Z < 10 ? '#ffcc00'
                             : '#ff4444'

              return (
                <tr key={a.case_id}>
                  <td className="mono">
                    <a
                      href={a.spc_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="anal-link"
                    >
                      {a.date}
                    </a>
                  </td>
                  <td style={{ color: ecColor, fontSize: 10, fontWeight: 600 }}>{ecLabel}</td>
                  <td className="mono">{a.tornado_count}</td>
                  <td className="mono">{fmt(a.MLCAPE)}</td>
                  <td className="mono">{fmt(a.MLCIN)}</td>
                  <td className="mono" style={{ color: tcColor }}>
                    {a.tc_gap_12Z != null ? `${a.tc_gap_12Z >= 0 ? '+' : ''}${a.tc_gap_12Z.toFixed(1)}°F` : '—'}
                  </td>
                  <td style={{ color: cbColor, fontSize: 10, fontWeight: 600 }}>{cbLabel}</td>
                  <td className="mono val-dim">{a.distance.toFixed(3)}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
