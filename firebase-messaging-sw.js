// firebase-messaging-sw.js
// Place this file in your Flask `static/` folder.
// Flask must serve it at the ROOT path (/firebase-messaging-sw.js),
// so add this route to app.py (already included in the updated app.py):
//   @app.route('/firebase-messaging-sw.js')
//   def sw(): return app.send_static_file('firebase-messaging-sw.js'), 200, {'Content-Type':'application/javascript'}

importScripts('https://www.gstatic.com/firebasejs/10.12.0/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/10.12.0/firebase-messaging-compat.js');

// ── REPLACE with your Firebase project config ──────────────────────────────
firebase.initializeApp({
  apiKey:            "REPLACE_API_KEY",
  authDomain:        "REPLACE_AUTH_DOMAIN",
  projectId:         "REPLACE_PROJECT_ID",
  storageBucket:     "REPLACE_STORAGE_BUCKET",
  messagingSenderId: "REPLACE_MESSAGING_SENDER_ID",
  appId:             "REPLACE_APP_ID",
});
// ───────────────────────────────────────────────────────────────────────────

const messaging = firebase.messaging();

// Handle background messages (app not in foreground)
messaging.onBackgroundMessage(function(payload) {
  const n = payload.notification || {};
  const data = payload.data || {};

  const title = n.title || data.title || 'ApnaDukaan';
  const body  = n.body  || data.body  || '';
  const icon  = n.icon  || '/static/icon-192.png';
  const link  = data.link || '/';

  self.registration.showNotification(title, {
    body,
    icon,
    badge: '/static/icon-72.png',
    data: { link },
    vibrate: [200, 100, 200],
    requireInteraction: false,
  });
});

// Open / focus the app when notification is clicked
self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  const link = (event.notification.data || {}).link || '/';
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(wins) {
      for (var w of wins) {
        if (w.url.includes(self.location.origin) && 'focus' in w) {
          w.focus();
          w.navigate(link);
          return;
        }
      }
      if (clients.openWindow) return clients.openWindow(link);
    })
  );
});
