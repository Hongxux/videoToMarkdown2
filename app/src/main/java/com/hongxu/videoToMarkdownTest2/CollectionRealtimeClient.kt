package com.hongxu.videoToMarkdownTest2

import android.content.Context
import org.json.JSONObject

class CollectionRealtimeClient(
    context: Context,
    private val wsEndpoint: String,
    private val userId: String,
    private val onTaskUpdate: (taskId: String, status: String, statusMessage: String?) -> Unit
) {
    @Volatile
    private var desiredCollectionId: String? = null

    private val reliableClient = ReliableTaskWebSocketClient(
        context = context.applicationContext,
        wsEndpoint = wsEndpoint,
        userId = userId,
        clientLabel = "CollectionRealtimeClient",
        streamKeyProvider = {
            desiredCollectionId?.let { collectionId ->
                ReliableTaskWebSocketClient.buildStableStreamKey(
                    "android",
                    "collection",
                    "realtime",
                    userId,
                    collectionId
                )
            }
        },
        buildReplayActions = {
            desiredCollectionId?.let { collectionId ->
                listOf(
                    JSONObject()
                        .put("action", "subscribeCollection")
                        .put("collectionId", collectionId)
                )
            } ?: emptyList()
        },
        onJsonMessage = ::handlePayload
    )

    @Synchronized
    fun connectOrUpdate(collectionId: String?) {
        val normalized = collectionId?.trim()?.takeIf { it.isNotEmpty() }
        val previous = desiredCollectionId
        desiredCollectionId = normalized
        if (normalized == null) {
            disconnect()
            return
        }
        reliableClient.connect()
        if (previous != null && previous != normalized) {
            reliableClient.refreshConnection("collection changed")
        }
    }

    @Synchronized
    fun disconnect() {
        desiredCollectionId = null
        reliableClient.disconnect("collection detail closed")
    }

    private fun handlePayload(payload: JSONObject) {
        if (payload.optString("type").trim() != "taskUpdate") {
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
}