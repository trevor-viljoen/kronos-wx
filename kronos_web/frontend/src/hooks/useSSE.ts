import { useEffect, useRef, useState } from 'react'

export function useSSE<T>(url: string): T | null {
  const [state, setState] = useState<T | null>(null)
  const retryRef = useRef<number>(1000)
  const esRef    = useRef<EventSource | null>(null)

  useEffect(() => {
    let cancelled = false

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
          const delay = Math.min(retryRef.current, 30_000)
          retryRef.current = Math.min(delay * 2, 30_000)
          setTimeout(connect, delay)
        }
      }
    }

    connect()

    return () => {
      cancelled = true
      esRef.current?.close()
      esRef.current = null
    }
  }, [url])

  return state
}
