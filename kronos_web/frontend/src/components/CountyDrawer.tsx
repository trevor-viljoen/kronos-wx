import type { CountyPoint, Tier } from '../types/api'

interface Props {
  countyName: string | null
  countyData: CountyPoint | null
  tier: Tier | null
  onClose: () => void
}

const TIER_COLOR: Record<string, string> = {
  EXTREME:          'var(--tier-extreme)',
  HIGH:             'var(--tier-high)',
  DANGEROUS_CAPPED: 'var(--tier-dangerous)',
  MODERATE:         'var(--tier-moderate)',
  MARGINAL:         'var(--tier-marginal)',
  LOW:              'var(--tier-low)',
}

function capColor(v: number): string {
  if (v >= 3000) return 'var(--tier-extreme)'
  if (v >= 2000) return '#ff6600'
  if (v >= 1000) return 'var(--tier-moderate)'
  return 'var(--color-text)'
}

function cinColor(v: number): string {
  if (v >= 200) return 'var(--tier-extreme)'
  if (v >= 100) return '#ff6600'
  if (v >= 50)  return 'var(--tier-moderate)'
  return 'var(--tier-marginal)'
}

function srhColor(v: number): string {
  if (v >= 400) return 'var(--tier-extreme)'
  if (v >= 250) return '#ff6600'
  if (v >= 150) return 'var(--tier-moderate)'
  return 'var(--color-text)'
}

interface DataCellProps {
  label: string
  value: string | number | null
  unit?: string
  color?: string
}

function DataCell({ label, value, unit, color }: DataCellProps) {
  const display = value == null ? '—' : typeof value === 'number' ? value.toFixed(
    label.includes('EHI') || label.includes('STP') ? 2 : 0
  ) : value

  return (
    <div className="data-cell">
      <div className="label">{label}</div>
      <div className="value" style={color ? { color } : undefined}>
        {display}
        {unit && <span className="unit">{unit}</span>}
      </div>
    </div>
  )
}

export function CountyDrawer({ countyName, countyData: pt, tier, onClose }: Props) {
  const isOpen = countyName != null
  const displayName = countyName?.replace(/_/g, ' ') ?? ''
  const tierColor = tier ? TIER_COLOR[tier] : 'var(--color-text)'

  return (
    <div className={`county-drawer ${isOpen ? 'open' : ''}`}>
      {isOpen && (
        <>
          <div className="drawer-header">
            <div>
              <div style={{ fontWeight: 700, fontSize: 16 }}>{displayName}</div>
              {tier && (
                <span className={`tier-badge ${tier}`} style={{ marginTop: 4, display: 'inline-block' }}>
                  {tier.replace('_CAPPED', '').replace(/_/g, ' ')}
                </span>
              )}
            </div>
            <button className="drawer-close" onClick={onClose}>✕</button>
          </div>

          <div className="drawer-body">
            {pt ? (
              <>
                <div style={{ fontSize: 11, color: 'var(--color-text-dim)', marginBottom: 8 }}>
                  {pt.lat.toFixed(2)}°N, {Math.abs(pt.lon).toFixed(2)}°W
                </div>

                <div style={{ fontSize: 11, fontWeight: 600, letterSpacing: '0.06em',
                  textTransform: 'uppercase', color: 'var(--color-text-dim)', marginBottom: 4 }}>
                  Instability
                </div>
                <div className="data-grid">
                  <DataCell label="MLCAPE" value={pt.MLCAPE} unit="J/kg" color={capColor(pt.MLCAPE)} />
                  <DataCell label="MLCIN"  value={pt.MLCIN}  unit="J/kg" color={cinColor(pt.MLCIN)} />
                  <DataCell label="SBCAPE" value={pt.SBCAPE} unit="J/kg" />
                  <DataCell label="SBCIN"  value={pt.SBCIN}  unit="J/kg" />
                </div>

                <div style={{ fontSize: 11, fontWeight: 600, letterSpacing: '0.06em',
                  textTransform: 'uppercase', color: 'var(--color-text-dim)', margin: '12px 0 4px' }}>
                  Kinematics
                </div>
                <div className="data-grid">
                  <DataCell label="SRH 0-1" value={pt.SRH_0_1km} unit="m²/s²" color={srhColor(pt.SRH_0_1km)} />
                  <DataCell label="SRH 0-3" value={pt.SRH_0_3km} unit="m²/s²" color={srhColor(pt.SRH_0_3km)} />
                  <DataCell label="Shear 0-6" value={pt.BWD_0_6km} unit="kt" />
                  <DataCell label="Dewpoint" value={pt.dewpoint_2m_F} unit="°F" />
                </div>

                <div style={{ fontSize: 11, fontWeight: 600, letterSpacing: '0.06em',
                  textTransform: 'uppercase', color: 'var(--color-text-dim)', margin: '12px 0 4px' }}>
                  Composites
                </div>
                <div className="data-grid">
                  <DataCell label="EHI" value={pt.EHI} />
                  <DataCell label="STP" value={pt.STP} />
                  <DataCell label="LCL" value={pt.LCL_height_m} unit="m" />
                  <DataCell label="Lapse" value={pt.lapse_rate} unit="°C/km" />
                </div>
              </>
            ) : (
              <div className="connecting">
                <div className="pulse" /> No HRRR data yet
              </div>
            )}
          </div>
        </>
      )}
    </div>
  )
}
