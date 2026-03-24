// 升级缓存版本：播放器壳升级到本地 Plyr 资源后，需要让旧离线壳整体失效。
const SHELL_CACHE_NAME = 'mobile-markdown-shell-v10';
const SHELL_ASSETS = [
    '/',
    '/index.html',
    '/mobile-markdown.html',
    '/manifest.json',
    '/lib/plyr.css',
    '/lib/plyr.min.js',
    '/lib/plyr.svg',
    '/lib/plyr-blank.mp4',
    '/lib/task-audit-ledger.js',
];

self.addEventListener('install', (event) => {
    event.waitUntil((async () => {
        const cache = await caches.open(SHELL_CACHE_NAME);
        await cache.addAll(SHELL_ASSETS);
        await self.skipWaiting();
    })());
});

self.addEventListener('activate', (event) => {
    event.waitUntil((async () => {
        const cacheNames = await caches.keys();
        await Promise.all(
            cacheNames
                .filter((cacheName) => cacheName !== SHELL_CACHE_NAME)
                .map((cacheName) => caches.delete(cacheName))
        );
        await self.clients.claim();
    })());
});

self.addEventListener('fetch', (event) => {
    if (event.request.method !== 'GET') {
        return;
    }
    const requestUrl = new URL(event.request.url);
    if (requestUrl.origin !== self.location.origin) {
        return;
    }

    if (event.request.mode === 'navigate') {
        event.respondWith((async () => {
            try {
                return await fetch(event.request);
            } catch (_error) {
                const cache = await caches.open(SHELL_CACHE_NAME);
                return (await cache.match('/index.html')) || Response.error();
            }
        })());
        return;
    }

    if (requestUrl.pathname.startsWith('/icons/')) {
        event.respondWith((async () => {
            const cached = await caches.match(event.request);
            if (cached) {
                return cached;
            }
            const response = await fetch(event.request);
            const cache = await caches.open(SHELL_CACHE_NAME);
            cache.put(event.request, response.clone());
            return response;
        })());
    }
});


function resolveNotificationTargetUrl(data) {
    const fallback = '/mobile-markdown.html';
    const raw = data && typeof data.url === 'string' ? data.url.trim() : '';
    if (!raw) {
        return fallback;
    }
    try {
        const resolved = new URL(raw, self.location.origin);
        if (resolved.origin !== self.location.origin) {
            return fallback;
        }
        return resolved.toString();
    } catch (_error) {
        return fallback;
    }
}

self.addEventListener('notificationclick', (event) => {
    const notification = event.notification;
    const data = notification && notification.data && typeof notification.data === 'object'
        ? notification.data
        : {};
    const targetUrl = resolveNotificationTargetUrl(data);
    notification.close();
    event.waitUntil((async () => {
        const clientList = await self.clients.matchAll({
            type: 'window',
            includeUncontrolled: true,
        });
        const matchedClient = clientList.find((client) => {
            try {
                return new URL(client.url).origin === self.location.origin;
            } catch (_error) {
                return false;
            }
        });
        if (matchedClient) {
            if (typeof matchedClient.focus === 'function') {
                await matchedClient.focus();
            }
            if (typeof matchedClient.postMessage === 'function') {
                matchedClient.postMessage({
                    type: 'openTaskFromNotification',
                    taskId: String(data.taskId || ''),
                    status: String(data.status || ''),
                    url: targetUrl,
                });
                return;
            }
            if (typeof matchedClient.navigate === 'function') {
                await matchedClient.navigate(targetUrl);
                return;
            }
        }
        if (self.clients.openWindow) {
            await self.clients.openWindow(targetUrl);
        }
    })());
});
