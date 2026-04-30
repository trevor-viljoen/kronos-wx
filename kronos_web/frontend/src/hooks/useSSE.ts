import { useEffect, useRef, useState } from 'react'

// Poll /api/state as a backstop when SSE is unreliable (mobile proxies, etc.)
const POLL_MS = 30_000

export function useSSE<T>(url: string): T | null {
  const [state, setState] = useState<T | null>(null)
  const retryRef = useRef<number>(1000)
  const esRef    = useRef<EventSource | null>(null)

  useEffect(() => {
    let cancelled = false
    const restUrl = url.replace('/stream', '/state')

    const fetchRest = () =>
      fetch(restUrl)
        .then(r => r.ok ? r.json() : null)
        .then(data => { if (!cancelled && data && !data.ping) setState(data as T) })
        .catch(() => {})

    // Populate state immediately via REST — no blank screen while SSE negotiates
    fetchRest()

    // Poll every 30s as a fallback; SSE pushes override whenever they arrive
    const pollId = setInterval(fetchRest, POLL_MS)

    const connect = () => {
      if (cancelled) return
      const es = new EventSource(url)
      esRef.current = es

      es.onmessage = (e) => {
        if (cancelled) return
        try {
          const data = JSON.parse(e.data)
          if (!data.ping) {
            setState(data as T)
            retryRef.current = 1000  // reset backoff on success
          }
        } catch {
          // ignore parse errors
        }
      }

      es.onerror = () => {
        es.close()
        esRef.current = null
        if (!cancelled) {
          fetchRest()  // fetch immediately on drop rather than waiting for next poll
          const delay = Math.min(retryRef.current, 30_000)
          retryRef.current = Math.min(delay * 2, 30_000)
          setTimeout(connect, delay)
        }
      }
    }

    connect()

    return () => {
      cancelled = true
      clearInterval(pollId)
      esRef.current?.close()
      esRef.current = null
    }
  }, [url])

  return state
}
