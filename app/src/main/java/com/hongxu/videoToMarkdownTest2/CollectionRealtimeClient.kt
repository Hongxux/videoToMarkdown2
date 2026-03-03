package com.hongxu.videoToMarkdownTest2

import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject
import java.util.concurrent.atomic.AtomicBoolean

class CollectionRealtimeClient(
    private val wsEndpoint: String,
    private val userId: String,
    private val onTaskUpdate: (taskId: String, status: String, statusMessage: String?) -> Unit
) {
    private val okHttpClient = OkHttpClient.Builder().build()
    private val connected = AtomicBoolean(false)
    private var webSocket: WebSocket? = null
    private var desiredCollectionId: String? = null
    private var activeCollectionId: String? = null

    @Synchronized
    fun connectOrUpdate(collectionId: String?) {
        val normalized = collectionId?.trim()?.takeIf { it.isNotEmpty() }
        desiredCollectionId = normalized
        if (normalized == null) {
            disconnect()
            return
        }
        if (webSocket == null) {
            val request = Request.Builder()
                .url("$wsEndpoint?userId=${userId.trim()}")
                .build()
            webSocket = okHttpClient.newWebSocket(request, createListener())
            return
        }
        if (connected.get()) {
            pushCollectionSubscription(normalized)
        }
    }

    @Synchronized
    fun disconnect() {
        val socket = webSocket
        val subscribed = activeCollectionId
        if (socket != null && subscribed != null && connected.get()) {
            val unsubscribe = JSONObject()
                .put("action", "unsubscribeCollection")
                .put("collectionId", subscribed)
            socket.send(unsubscribe.toString())
        }
        desiredCollectionId = null
        activeCollectionId = null
        connected.set(false)
        socket?.close(1000, "collection detail closed")
        webSocket = null
    }

    private fun createListener(): WebSocketListener {
        return object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                connected.set(true)
                desiredCollectionId?.let { pushCollectionSubscription(it) }
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                val payload = runCatching { JSONObject(text) }.getOrNull() ?: return
                if (payload.optString("type") != "taskUpdate") {
                    return
                }
                val taskId = payload.optString("taskId").trim()
                if (taskId.isEmpty()) {
                    return
                }
                val status = payload.optString("status").trim()
                val statusMessage = payload.optString("message").trim().ifEmpty { null }
                onTaskUpdate(taskId, status, statusMessage)
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                connected.set(false)
                activeCollectionId = null
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                connected.set(false)
                activeCollectionId = null
            }
        }
    }

    @Synchronized
    private fun pushCollectionSubscription(nextCollectionId: String) {
        val socket = webSocket ?: return
        val current = activeCollectionId
        if (current != null && current != nextCollectionId) {
            val unsubscribe = JSONObject()
                .put("action", "unsubscribeCollection")
                .put("collectionId", current)
            socket.send(unsubscribe.toString())
            activeCollectionId = null
        }
        if (activeCollectionId == nextCollectionId) {
            return
        }
        val subscribe = JSONObject()
            .put("action", "subscribeCollection")
            .put("collectionId", nextCollectionId)
        socket.send(subscribe.toString())
        activeCollectionId = nextCollectionId
    }
}
