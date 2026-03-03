package com.hongxu.videoToMarkdownTest2

import android.net.Uri
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject
import java.util.concurrent.atomic.AtomicBoolean

data class TaskRealtimeUpdate(
    val taskId: String,
    val status: String,
    val message: String,
    val progress: Double,
    val resultPath: String,
    val errorMessage: String
)

class TaskRealtimeClient(
    private val wsEndpoint: String,
    private val userId: String,
    private val onTaskUpdate: (TaskRealtimeUpdate) -> Unit
) {
    private val okHttpClient = OkHttpClient.Builder().build()
    private val connected = AtomicBoolean(false)
    private var webSocket: WebSocket? = null

    @Synchronized
    fun connect() {
        if (webSocket != null) {
            return
        }
        val normalizedUserId = userId.trim()
        if (normalizedUserId.isEmpty()) {
            return
        }
        val request = Request.Builder()
            .url("$wsEndpoint?userId=${Uri.encode(normalizedUserId)}")
            .build()
        webSocket = okHttpClient.newWebSocket(request, createListener())
    }

    @Synchronized
    fun disconnect() {
        connected.set(false)
        webSocket?.close(1000, "app background")
        webSocket = null
    }

    private fun createListener(): WebSocketListener {
        return object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                connected.set(true)
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                val payload = runCatching { JSONObject(text) }.getOrNull() ?: return
                if (payload.optString("type").trim() != "taskUpdate") {
                    return
                }
                val taskId = payload.optString("taskId").trim()
                if (taskId.isEmpty()) {
                    return
                }
                onTaskUpdate(
                    TaskRealtimeUpdate(
                        taskId = taskId,
                        status = payload.optString("status").trim(),
                        message = payload.optString("message").trim(),
                        progress = payload.optDouble("progress", 0.0),
                        resultPath = payload.optString("resultPath").trim(),
                        errorMessage = payload.optString("errorMessage").trim()
                    )
                )
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                connected.set(false)
                this@TaskRealtimeClient.webSocket = null
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                connected.set(false)
                this@TaskRealtimeClient.webSocket = null
            }
        }
    }
}
