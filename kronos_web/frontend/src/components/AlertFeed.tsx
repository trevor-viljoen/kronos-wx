import { useEffect, useRef } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import type { AlertLogEntry } from '../types/api'
import { useCollapse } from '../hooks/useCollapse'

interface Props {
  entries: AlertLogEntry[]
}

function msgColor(msg: string): string {
  if (msg.includes('TORNADO WARNING') || msg.includes('🌪')) return 'var(--warning-tornado)'
  if (msg.includes('TORNADO WATCH')   || msg.includes('📋')) return 'var(--warning-watch)'
  if (msg.includes('MD #'))            return 'var(--warning-md)'
  if (msg.includes('▲'))               return 'var(--tier-marginal)'
  if (msg.includes('▼'))               return 'var(--warning-watch)'
  if (msg.includes('Dryline'))         return '#ff8800'
  return 'var(--color-text)'
}

export function AlertFeed({ entries }: Props) {
  const { collapsed, toggle } = useCollapse('alertfeed')
  const bottomRef = useRef<HTMLDivElement>(null)
  const prevLen   = useRef(0)

  useEffect(() => {
    if (entries.length > prevLen.current) {
      bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
    }
    prevLen.current = entries.length
  }, [entries.length])

  return (
    <div className={`panel feed-panel${collapsed ? ' collapsed' : ''}`}>
      <div className="panel-header">
        <span className="panel-title">Alert Log</span>
        <span className="panel-subtitle">{entries.length} entries</span>
        <button className="panel-collapse-btn" onClick={toggle}>{collapsed ? '▸' : '▾'}</button>
      </div>
      <div className="panel-body" style={{ padding: '4px 12px', overflowY: 'auto' }}>
        {entries.length === 0 ? (
          <div style={{ color: 'var(--color-text-muted)', fontSize: 12, padding: '4px 0' }}>
            No alerts yet…
          </div>
        ) : (
          <AnimatePresence initial={false}>
            {entries.map((e, i) => (
              <motion.div
                key={`${e.ts}-${i}`}
                className="feed-entry"
                initial={{ opacity: 0, x: -12 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ duration: 0.25, ease: 'easeOut' }}
              >
                <span className="feed-ts">{e.ts}</span>
                <span className="feed-msg mono" style={{ color: msgColor(e.msg) }}>
                  {e.msg}
                </span>
              </motion.div>
            ))}
          </AnimatePresence>
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  )
}
