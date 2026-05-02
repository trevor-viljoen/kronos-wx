import { useState, useEffect, useCallback } from 'react'

type PushState = 'unsupported' | 'denied' | 'subscribed' | 'unsubscribed'

function urlBase64ToUint8Array(base64: string): ArrayBuffer {
  const pad = '='.repeat((4 - (base64.length % 4)) % 4)
  const b64 = (base64 + pad).replace(/-/g, '+').replace(/_/g, '/')
  const raw = atob(b64)
  const arr = new Uint8Array(raw.length)
  for (let i = 0; i < raw.length; i++) arr[i] = raw.charCodeAt(i)
  return arr.buffer
}

export function usePushNotifications() {
  const [state, setState] = useState<PushState>('unsubscribed')
  const [loading, setLoading] = useState(false)

  // Detect support and existing subscription on mount
  useEffect(() => {
    if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
      setState('unsupported')
      return
    }
    if (Notification.permission === 'denied') {
      setState('denied')
      return
    }
    navigator.serviceWorker.ready.then(reg => {
      reg.pushManager.getSubscription().then(sub => {
        setState(sub ? 'subscribed' : 'unsubscribed')
      })
    }).catch(() => {})
  }, [])

  const subscribe = useCallback(async () => {
    if (!('serviceWorker' in navigator)) return
    setLoading(true)
    try {
      // Register service worker
      const reg = await navigator.serviceWorker.register('/sw.js')
      await navigator.serviceWorker.ready

      // Fetch VAPID public key
      const resp = await fetch('/api/push/vapid')
      if (!resp.ok) throw new Error('VAPID key unavailable')
      const { publicKey } = await resp.json()

      // Request notification permission
      const perm = await Notification.requestPermission()
      if (perm !== 'granted') {
        setState(perm === 'denied' ? 'denied' : 'unsubscribed')
        return
      }

      // Subscribe to push
      const sub = await reg.pushManager.subscribe({
        userVisibleOnly:      true,
        applicationServerKey: urlBase64ToUint8Array(publicKey),
      })

      // Send subscription to backend
      await fetch('/api/push/subscribe', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(sub.toJSON()),
      })
      setState('subscribed')
    } catch (e) {
      console.warn('Push subscribe failed:', e)
    } finally {
      setLoading(false)
    }
  }, [])

  const unsubscribe = useCallback(async () => {
    setLoading(true)
    try {
      const reg = await navigator.serviceWorker.ready
      const sub = await reg.pushManager.getSubscription()
      if (sub) {
        await fetch('/api/push/subscribe', {
          method:  'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ endpoint: sub.endpoint }),
        })
        await sub.unsubscribe()
      }
      setState('unsubscribed')
    } catch (e) {
      console.warn('Push unsubscribe failed:', e)
    } finally {
      setLoading(false)
    }
  }, [])

  return { state, loading, subscribe, unsubscribe }
}
