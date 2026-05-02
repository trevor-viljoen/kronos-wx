// KRONOS-WX service worker — handles Web Push notifications
self.addEventListener('push', event => {
  let title = 'KRONOS-WX'
  let body  = 'Alert'
  try {
    const data = event.data?.json() ?? {}
    title = data.title ?? title
    body  = data.body  ?? body
  } catch {}

  event.waitUntil(
    self.registration.showNotification(title, {
      body,
      icon:    '/favicon.ico',
      badge:   '/favicon.ico',
      tag:     'kronos-alert',   // replaces prior notification of same type
      vibrate: [200, 100, 200],
      requireInteraction: false,
    })
  )
})

self.addEventListener('notificationclick', event => {
  event.notification.close()
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
      const existing = list.find(c => c.url && 'focus' in c)
      return existing ? existing.focus() : clients.openWindow('/')
    })
  )
})
