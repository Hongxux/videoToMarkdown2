const SHELL_CACHE_NAME = 'mobile-markdown-shell-v1';
const SHELL_ASSETS = [
    '/',
    '/index.html',
    '/mobile-markdown.html',
    '/manifest.json',
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
