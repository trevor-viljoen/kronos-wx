import { useState, useCallback } from 'react'

const STORAGE_KEY = 'kronos-panel-collapsed'

function readStore(): Record<string, boolean> {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) ?? '{}')
  } catch {
    return {}
  }
}

function writeStore(store: Record<string, boolean>) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(store))
  } catch {}
}

export function useCollapse(id: string, defaultCollapsed = false) {
  const [collapsed, setCollapsed] = useState<boolean>(() => {
    const store = readStore()
    return id in store ? store[id] : defaultCollapsed
  })

  const toggle = useCallback(() => {
    setCollapsed(prev => {
      const next = !prev
      const store = readStore()
      store[id] = next
      writeStore(store)
      return next
    })
  }, [id])

  return { collapsed, toggle }
}
