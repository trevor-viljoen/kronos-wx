import type { TendencyRow, Tier } from '../types/api'
import { useCollapse } from '../hooks/useCollapse'

interface Props {
  rows: TendencyRow[]
  hrrrValid: string | null
}

const TIER_COLOR: Record<Tier, string> = {
  EXTREME:          'var(--tier-extreme)',
  HIGH:             'var(--tier-high)',
  DANGEROUS_CAPPED: 'var(--tier-dangerous)',
  MODERATE:         'var(--tier-moderate)',
  MARGINAL:         'var(--tier-marginal)',
  LOW:              'var(--tier-low)',
}

const TIER_BG: Record<Tier, string> = {
  EXTREME:          'rgba(42,5,5,0.4)',
  HIGH:             'rgba(42,18,5,0.4)',
  DANGEROUS_CAPPED: 'rgba(26,5,48,0.4)',
  MODERATE:         'rgba(26,20,0,0.4)',
  MARGINAL:         'rgba(5,18,5,0.3)',
  LOW:              'transparent',
}

function DeltaCell({ value, invert = false }: { value: number; invert?: boolean }) {
  const v = invert ? -value : value
  const cls = v > 50 ? 'val-ok' : v > 10 ? 'val-ok' : v < -50 ? 'val-high' : v < -10 ? 'val-mid' : ''
  const sign = value >= 0 ? '+' : ''
  return (
    <td className={`mono ${cls}`}>
      {sign}{value.toFixed(0)}
    </td>
  )
}

function CinCell({ value }: { value: number }) {
  // For CIN, erosion (negative delta) is good
  const cls = value <= -30 ? 'val-ok' : value <= -10 ? 'val-ok' : value >= 10 ? 'val-high' : ''
  const sign = value >= 0 ? '+' : ''
  return <td className={`mono ${cls}`}>{sign}{value.toFixed(0)}</td>
}

export function TendencyTable({ rows, hrrrValid }: Props) {
  const { collapsed, toggle } = useCollapse('tendency')

  if (!rows.length) {
    return (
      <div className={`panel tend-panel${collapsed ? ' collapsed' : ''}`}>
        <div className="panel-header">
          <span className="panel-title">Tendency</span>
          {hrrrValid && <span className="panel-subtitle">HRRR {hrrrValid}</span>}
          <button className="panel-collapse-btn" onClick={toggle}>{collapsed ? '▸' : '▾'}</button>
        </div>
        <div className="panel-body" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <span style={{ color: 'var(--color-text-muted)', fontSize: 12 }}>
            No MODERATE+ counties
          </span>
        </div>
      </div>
    )
  }

  return (
    <div className={`panel tend-panel${collapsed ? ' collapsed' : ''}`}>
      <div className="panel-header">
        <span className="panel-title">Tendency</span>
        {hrrrValid && (
          <span className="panel-subtitle" style={{ marginLeft: 'auto' }}>
            HRRR {hrrrValid}
          </span>
        )}
        <button className="panel-collapse-btn" onClick={toggle}>{collapsed ? '▸' : '▾'}</button>
      </div>
      <div className="panel-body" style={{ padding: 0, overflowX: 'auto' }}>
        <table className="tend-table">
          <thead>
            <tr>
              <th style={{ textAlign: 'left' }}>County</th>
              <th style={{ textAlign: 'left' }}>Tier</th>
              <th>ΔCIN</th>
              <th>ΔCAPE</th>
              <th>ΔSRH-1</th>
              <th>ΔSRH-3</th>
              <th>ΔEHI</th>
              <th>Trend</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(row => (
              <tr key={row.county} style={{ background: TIER_BG[row.tier] }}>
                <td style={{ color: TIER_COLOR[row.tier], fontWeight: 500 }}>
                  {row.county.replace(/_/g, ' ')}
                </td>
                <td>
                  <span className={`tier-badge ${row.tier}`} style={{ fontSize: 10 }}>
                    {row.tier.replace('_CAPPED', '').replace('_', ' ')}
                  </span>
                </td>
                <CinCell value={row.d_cin} />
                <DeltaCell value={row.d_cape} />
                <DeltaCell value={row.d_srh1} />
                <DeltaCell value={row.d_srh3} />
                <td className="mono" style={{ color: 'var(--color-text-dim)' }}>
                  {row.d_ehi >= 0 ? '+' : ''}{row.d_ehi.toFixed(2)}
                </td>
                <td className={`trend-arrow trend-${row.trend_level}`} style={{ textAlign: 'center', fontSize: 16 }}>
                  {row.trend}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
