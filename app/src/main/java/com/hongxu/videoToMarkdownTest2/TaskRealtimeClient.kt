package com.hongxu.videoToMarkdownTest2

import android.content.Context
import org.json.JSONObject

data class TaskRealtimeUpdate(
    val taskId: String,
    val status: String,
    val message: String,
    val progress: Double,
    val resultPath: String,
    val errorMessage: String
)

data class TaskMetaSyncEvent(
    val taskId: String,
    val pathKey: String,
    val changeKind: String,
    val anchorId: String
)

class TaskRealtimeClient(
    context: Context,
    private val wsEndpoint: String,
    private val userId: String,
    private val onTaskUpdate: (TaskRealtimeUpdate) -> Unit,
    private val onMetaSync: (TaskMetaSyncEvent) -> Unit = {}
) {
    private val subscribedTaskIds = linkedSetOf<String>()
    private val reliableClient = ReliableTaskWebSocketClient(
        context = context.applicationContext,
        wsEndpoint = wsEndpoint,
        userId = userId,
        clientLabel = "TaskRealtimeClient",
        streamKeyProvider = {
            ReliableTaskWebSocketClient.buildStableStreamKey(
                "android",
                "task",
                "realtime",
                userId
            )
        },
        buildReplayActions = {
            synchronized(subscribedTaskIds) {
                subscribedTaskIds.map { taskId ->
                    JSONObject()
                        .put("action", "subscribe")
                        .put("taskId", taskId)
                }
            }
        },
        onJsonMessage = ::handlePayload
    )

    fun connect() {
        reliableClient.connect()
    }

    fun disconnect() {
        reliableClient.disconnect("app background")
    }

    @Synchronized
    fun subscribeTask(taskId: String) {
        val normalizedTaskId = taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            return
        }
        val added = synchronized(subscribedTaskIds) {
            subscribedTaskIds.add(normalizedTaskId)
        }
        if (!added) {
            return
        }
        reliableClient.sendAction(
            JSONObject()
                .put("action", "subscribe")
                .put("taskId", normalizedTaskId)
        )
    }

    @Synchronized
    fun unsubscribeTask(taskId: String) {
        val normalizedTaskId = taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            return
        }
        synchronized(subscribedTaskIds) {
            subscribedTaskIds.remove(normalizedTaskId)
        }
        reliableClient.sendAction(
            JSONObject()
                .put("action", "unsubscribe")
                .put("taskId", normalizedTaskId)
        )
    }

    private fun handlePayload(payload: JSONObject) {
        when (payload.optString("type").trim()) {
            "taskUpdate" -> {
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
            "taskMetaSync" -> {
                val taskId = payload.optString("taskId").trim()
                if (taskId.isEmpty()) {
                    return
                }
                onMetaSync(
                    TaskMetaSyncEvent(
                        taskId = taskId,
                        pathKey = payload.optString("pathKey").trim(),
                        changeKind = payload.optString("changeKind").trim(),
                        anchorId = payload.optString("anchorId").trim()
                    )
                )
            }
        }
    }
}