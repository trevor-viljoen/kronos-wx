import type { SoundingData, CESData, ModelForecast } from '../types/api'

interface Props {
  oun: SoundingData | null
  lmn: SoundingData | null
  ces: CESData | null
  model: ModelForecast | null
  hour: number | null
}

function capeCls(v: number | null): string {
  if (v == null) return ''
  if (v >= 3000) return 'val-high'
  if (v >= 2000) return 'val-mid'
  if (v >= 1000) return 'val-low-bad'
  return ''
}

function cinCls(v: number | null): string {
  if (v == null) return ''
  if (v >= 200) return 'val-high'
  if (v >= 100) return 'val-mid'
  if (v >= 50)  return 'val-low-bad'
  return 'val-ok'
}

function srhCls(v: number | null): string {
  if (v == null) return ''
  if (v >= 400) return 'val-high'
  if (v >= 250) return 'val-mid'
  if (v >= 150) return 'val-low-bad'
  return ''
}

function ehiCls(v: number | null): string {
  if (v == null) return ''
  if (v >= 4.0) return 'val-high'
  if (v >= 2.5) return 'val-mid'
  if (v >= 1.5) return 'val-low-bad'
  return ''
}

function fmt(v: number | null | undefined, decimals = 0): string {
  if (v == null) return '—'
  return v.toFixed(decimals)
}

interface RowProps {
  label: string
  oun: number | null | undefined
  lmn: number | null | undefined
  decimals?: number
  colorFn?: (v: number | null) => string
}

function Row({ label, oun, lmn, decimals = 0, colorFn }: RowProps) {
  const ounCls = colorFn ? colorFn(oun ?? null) : ''
  return (
    <tr>
      <td>{label}</td>
      <td className={`mono ${ounCls}`}>{fmt(oun, decimals)}</td>
      <td className="mono val-dim">{fmt(lmn, decimals)}</td>
    </tr>
  )
}

const CB_LABEL: Record<string, string> = {
  CLEAN_EROSION:   'CLEAN EROSION',
  EARLY_EROSION:   'EARLY EROSION',
  LATE_EROSION:    'LATE EROSION',
  NO_EROSION:      'NO EROSION',
  BOUNDARY_FORCED: 'BOUNDARY FORCED',
  RECONSTITUTED:   'RECONSTITUTED',
}

function sigBarColor(pct: number): string {
  if (pct >= 60) return '#ff2222'
  if (pct >= 40) return '#ff6600'
  if (pct >= 20) return '#ffcc00'
  return '#44aa44'
}

export function EnvironmentPanel({ oun, lmn, ces, model, hour }: Props) {
  if (!oun) {
    return (
      <div className="panel env-panel">
        <div className="panel-header">
          <span className="panel-title">Environment</span>
        </div>
        <div className="panel-body" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <div className="connecting"><div className="pulse" />Loading sounding…</div>
        </div>
      </div>
    )
  }

  return (
    <div className="panel env-panel" style={{ overflowY: 'auto' }}>
      <div className="panel-header">
        <span className="panel-title">Environment</span>
        <span className="panel-subtitle">
          OUN{lmn ? ' / LMN' : ''}{hour != null ? ` · ${hour.toString().padStart(2, '0')}Z` : ''}
        </span>
      </div>

      <div className="panel-body" style={{ padding: '8px 12px' }}>
        <table className="env-table">
          <thead>
            <tr>
              <th></th>
              <th>OUN</th>
              <th>{lmn ? 'LMN' : '—'}</th>
            </tr>
          </thead>
          <tbody>
            <Row label="MLCAPE (J/kg)"   oun={oun.MLCAPE}          lmn={lmn?.MLCAPE}          colorFn={capeCls} />
            <Row label="MLCIN  (J/kg)"   oun={oun.MLCIN}           lmn={lmn?.MLCIN}           colorFn={cinCls} />
            <Row label="Cap    (°C)"     oun={oun.cap_strength}     lmn={lmn?.cap_strength}    decimals={1} />
            <Row label="LCL    (m)"      oun={oun.LCL_height}       lmn={lmn?.LCL_height} />
            <Row label="LFC    (m)"      oun={oun.LFC_height}       lmn={lmn?.LFC_height} />
            <tr><td colSpan={3} style={{ padding: '2px 0' }} /></tr>
            <Row label="SRH 0-1 (m²/s²)" oun={oun.SRH_0_1km}      lmn={lmn?.SRH_0_1km}      colorFn={srhCls} />
            <Row label="SRH 0-3 (m²/s²)" oun={oun.SRH_0_3km}      lmn={lmn?.SRH_0_3km}      colorFn={srhCls} />
            <Row label="Shear 0-6 (kt)"  oun={oun.BWD_0_6km}       lmn={lmn?.BWD_0_6km} />
            <Row label="EHI"             oun={oun.EHI}              lmn={lmn?.EHI}             decimals={2} colorFn={ehiCls} />
            <Row label="STP"             oun={oun.STP}              lmn={lmn?.STP}             decimals={2} />
          </tbody>
        </table>

        {/* CES projection */}
        {ces && (
          <div className="ces-block">
            <div className="ces-label">Cap Erosion Projection</div>
            <span className={`cb-badge cb-${ces.cap_behavior}`}>
              {CB_LABEL[ces.cap_behavior] ?? ces.cap_behavior}
            </span>
            {ces.erosion_hour != null && (
              <div className="mono" style={{ fontSize: 12, color: 'var(--color-text-dim)', marginTop: 3 }}>
                Erosion: {ces.erosion_hour.toString().padStart(2, '0')}Z
              </div>
            )}
            {ces.tc_gap_12Z != null && (
              <div className="mono" style={{ fontSize: 11, color: 'var(--color-text-muted)', marginTop: 2 }}>
                12Z Tc gap: <span style={{ color: ces.tc_gap_12Z < 0 ? '#44aa44' : ces.tc_gap_12Z < 10 ? '#ffcc00' : '#ff4444' }}>
                  {ces.tc_gap_12Z >= 0 ? '+' : ''}{ces.tc_gap_12Z.toFixed(1)}°F
                </span>
              </div>
            )}
          </div>
        )}

        {/* Model forecast */}
        <div className="model-block">
          <div className="ces-label">Model Forecast</div>
          {model ? (
            <>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 4 }}>
                <span style={{ fontSize: 12, color: 'var(--color-text-dim)' }}>Severity</span>
                <span className="mono" style={{ fontSize: 12, color: sigBarColor(model.sig_pct) }}>
                  {model.sig_pct.toFixed(0)}% SIGNIFICANT
                </span>
              </div>
              <div className="sig-bar-wrap">
                <div className="sig-bar" style={{
                  width: `${model.sig_pct}%`,
                  background: sigBarColor(model.sig_pct),
                }} />
              </div>
              <div className="mono" style={{ fontSize: 11, color: 'var(--color-text-dim)', marginTop: 4 }}>
                Count: {model.count_exp.toFixed(0)} expected
                <span style={{ color: 'var(--color-text-muted)' }}>
                  {' '}(80% PI: {model.count_lo.toFixed(0)}–{model.count_hi.toFixed(0)})
                </span>
              </div>
            </>
          ) : (
            <div style={{ fontSize: 11, color: 'var(--color-text-muted)', marginTop: 4 }}>
              Models not trained — run train-models
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
