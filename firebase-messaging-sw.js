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
  apiKey:            "AIzaSyCYsPGQqOu7cqbG5IZoESu0Ha9Q4IHYpEM",
  authDomain:        "apnadukaan-d47a8.firebaseapp.com",
  projectId:         "apnadukaan-d47a88",
  storageBucket:     "apnadukaan-d47a8.firebasestorage.app",
  messagingSenderId: "703142953576",
  appId:             "1:703142953576:web:ed949411b5b1d8a8e771dd",
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
