import { useEffect, useRef } from 'react'
import type { AlertLogEntry } from '../types/api'

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
  const bottomRef = useRef<HTMLDivElement>(null)
  const prevLen   = useRef(0)

  useEffect(() => {
    if (entries.length > prevLen.current) {
      bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
    }
    prevLen.current = entries.length
  }, [entries.length])

  return (
    <div className="panel feed-panel">
      <div className="panel-header">
        <span className="panel-title">Alert Log</span>
        <span className="panel-subtitle">{entries.length} entries</span>
      </div>
      <div className="panel-body" style={{ padding: '4px 12px', overflowY: 'auto' }}>
        {entries.length === 0 ? (
          <div style={{ color: 'var(--color-text-muted)', fontSize: 12, padding: '4px 0' }}>
            No alerts yet…
          </div>
        ) : (
          entries.map((e, i) => (
            <div key={i} className="feed-entry">
              <span className="feed-ts">{e.ts}</span>
              <span className="feed-msg mono" style={{ color: msgColor(e.msg) }}>
                {e.msg}
              </span>
            </div>
          ))
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  )
}
