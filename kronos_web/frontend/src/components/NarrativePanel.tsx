import { AnimatePresence, motion } from 'framer-motion'
import type { BriefLine } from '../types/api'
import { useCollapse } from '../hooks/useCollapse'

interface Props {
  lines: BriefLine[]
  updatedAt: string | null
}

const SEVERITY_COLOR: Record<string, string> = {
  critical:  'var(--tier-extreme)',
  elevated:  'var(--tier-high)',
  favorable: '#44cc44',
  neutral:   'var(--color-text)',
}

function timeLabel(updatedAt: string | null): string {
  if (!updatedAt) return ''
  try {
    const d = new Date(updatedAt)
    const hh = String(d.getUTCHours()).padStart(2, '0')
    const mm = String(d.getUTCMinutes()).padStart(2, '0')
    return `${hh}:${mm}Z`
  } catch {
    return ''
  }
}

export function NarrativePanel({ lines, updatedAt }: Props) {
  const { collapsed, toggle } = useCollapse('narrative')

  return (
    <div className={`panel narrative-panel${collapsed ? ' collapsed' : ''}`}>
      <div className="panel-header">
        <span className="panel-title">Situation Brief</span>
        <span className="panel-subtitle">{timeLabel(updatedAt)}</span>
        <button className="panel-collapse-btn" onClick={toggle}>{collapsed ? '▸' : '▾'}</button>
      </div>
      <div className="panel-body narrative-body">
        {lines.length === 0 ? (
          <div className="connecting"><div className="pulse" />Awaiting data…</div>
        ) : (
          <AnimatePresence initial={false}>
            {lines.map((line, i) => (
              <motion.div
                key={line.label}
                className="brief-line"
                initial={{ opacity: 0, x: -8 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ duration: 0.2, delay: i * 0.04 }}
              >
                <span className="brief-label">{line.label}</span>
                <span className="brief-value" style={{ color: SEVERITY_COLOR[line.severity] ?? 'var(--color-text)' }}>
                  {line.value}
                </span>
                {line.detail && (
                  <span className="brief-detail">{line.detail}</span>
                )}
              </motion.div>
            ))}
          </AnimatePresence>
        )}
      </div>
    </div>
  )
}
