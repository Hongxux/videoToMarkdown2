package com.hongxu.videoToMarkdownTest2

import android.content.Context
import android.net.Uri
import android.os.SystemClock
import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject
import java.util.ArrayDeque
import java.util.LinkedHashSet
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max
import kotlin.math.min

private data class ReliableSocketState(
    val socket: WebSocket,
    val connectionId: Long,
    val streamKey: String
)

private data class RecentMessageWindow(
    val orderedIds: ArrayDeque<String> = ArrayDeque(),
    val ids: MutableSet<String> = LinkedHashSet()
)

private class ReliableMessageCursorStore(context: Context) {
    private val preferences = context.applicationContext.getSharedPreferences(
        PREFS_NAME,
        Context.MODE_PRIVATE
    )
    private val recentWindows = ConcurrentHashMap<String, RecentMessageWindow>()

    fun lastReceivedMessageId(userId: String, streamKey: String): String? {
        return preferences.getString(buildPreferenceKey(userId, streamKey), null)
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
    }

    fun isDuplicate(userId: String, streamKey: String, messageId: String): Boolean {
        val tracker = recentWindows.computeIfAbsent(buildTrackerKey(userId, streamKey)) {
            RecentMessageWindow().also { window ->
                lastReceivedMessageId(userId, streamKey)?.let { seed ->
                    window.orderedIds.addLast(seed)
                    window.ids.add(seed)
                }
            }
        }
        synchronized(tracker) {
            return tracker.ids.contains(messageId)
        }
    }

    fun recordReceived(userId: String, streamKey: String, messageId: String) {
        val tracker = recentWindows.computeIfAbsent(buildTrackerKey(userId, streamKey)) {
            RecentMessageWindow()
        }
        synchronized(tracker) {
            if (tracker.ids.add(messageId)) {
                tracker.orderedIds.addLast(messageId)
                while (tracker.orderedIds.size > MAX_RECENT_MESSAGE_IDS) {
                    val removed = tracker.orderedIds.removeFirst()
                    tracker.ids.remove(removed)
                }
            }
        }
        preferences.edit()
            .putString(buildPreferenceKey(userId, streamKey), messageId)
            .apply()
    }

    private fun buildPreferenceKey(userId: String, streamKey: String): String {
        return "cursor_${buildTrackerKey(userId, streamKey)}"
    }

    private fun buildTrackerKey(userId: String, streamKey: String): String {
        return "${userId.trim()}::${streamKey.trim()}"
    }

    private companion object {
        private const val PREFS_NAME = "reliable_task_websocket_cursor"
        private const val MAX_RECENT_MESSAGE_IDS = 64
    }
}

class ReliableTaskWebSocketClient(
    context: Context,
    private val wsEndpoint: String,
    userId: String,
    private val clientLabel: String,
    private val streamKeyProvider: () -> String?,
    private val buildReplayActions: () -> List<JSONObject> = { emptyList() },
    private val onJsonMessage: (JSONObject) -> Unit
) {
    private val normalizedUserId = userId.trim()
    private val cursorStore = ReliableMessageCursorStore(context.applicationContext)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val nextConnectionId = AtomicLong(0L)
    private val lastInboundAtMs = AtomicLong(0L)
    private val lastPongAtMs = AtomicLong(0L)

    @Volatile
    private var desiredConnected = false

    @Volatile
    private var reconnectJob: Job? = null

    @Volatile
    private var heartbeatJob: Job? = null

    @Volatile
    private var socketState: ReliableSocketState? = null

    @Volatile
    private var reconnectAttempt = 0

    @Synchronized
    fun connect() {
        if (normalizedUserId.isEmpty()) {
            Log.w(logTag(), "skip connect because userId is empty")
            return
        }
        desiredConnected = true
        openSocketIfNeededLocked(resetBackoff = socketState == null && reconnectJob == null)
    }

    @Synchronized
    fun disconnect(reason: String = "client disconnect") {
        desiredConnected = false
        reconnectAttempt = 0
        cancelReconnectLocked()
        stopHeartbeatLocked()
        val socket = socketState?.socket
        socketState = null
        if (socket != null) {
            runCatching {
                socket.close(CLOSE_CODE_CLIENT_DISCONNECT, reason)
            }
        }
    }

    @Synchronized
    fun refreshConnection(reason: String = "client refresh") {
        if (!desiredConnected) {
            return
        }
        reconnectAttempt = 0
        cancelReconnectLocked()
        stopHeartbeatLocked()
        val socket = socketState?.socket
        socketState = null
        if (socket == null) {
            openSocketIfNeededLocked(resetBackoff = true)
            return
        }
        runCatching {
            socket.close(CLOSE_CODE_CLIENT_REFRESH, reason)
        }
        runCatching {
            socket.cancel()
        }
        scheduleReconnect(reason)
    }

    fun sendAction(payload: JSONObject): Boolean {
        return sendRaw(payload.toString())
    }

    fun shutdown() {
        disconnect("client shutdown")
        scope.cancel()
    }

    private fun sendAck(messageId: String) {
        val ack = JSONObject()
            .put("action", "ack")
            .put("messageId", messageId)
        sendRaw(ack.toString())
    }

    private fun sendPing(streamKey: String): Boolean {
        val ping = JSONObject()
            .put("action", "ping")
            .put("clientTime", System.currentTimeMillis())
        val lastReceivedMessageId = cursorStore.lastReceivedMessageId(normalizedUserId, streamKey)
        if (!lastReceivedMessageId.isNullOrBlank()) {
            ping.put("lastReceivedMessageId", lastReceivedMessageId)
        }
        return sendRaw(ping.toString())
    }

    private fun sendRaw(payload: String): Boolean {
        val socket = synchronized(this) { socketState?.socket } ?: return false
        return runCatching { socket.send(payload) }.getOrDefault(false)
    }

    @Synchronized
    private fun openSocketIfNeededLocked(resetBackoff: Boolean = false) {
        if (!desiredConnected || socketState != null || reconnectJob != null) {
            return
        }
        val streamKey = normalizedStreamKey() ?: return
        if (resetBackoff) {
            reconnectAttempt = 0
        }
        val connectionId = nextConnectionId.incrementAndGet()
        val request = Request.Builder()
            .url(buildSocketUrl(streamKey))
            .build()
        val socket = sharedOkHttpClient.newWebSocket(request, createListener(connectionId, streamKey))
        socketState = ReliableSocketState(
            socket = socket,
            connectionId = connectionId,
            streamKey = streamKey
        )
    }

    private fun createListener(connectionId: Long, streamKey: String): WebSocketListener {
        return object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                if (!isCurrentConnection(connectionId, webSocket)) {
                    runCatching { webSocket.cancel() }
                    return
                }
                reconnectAttempt = 0
                val now = SystemClock.elapsedRealtime()
                lastInboundAtMs.set(now)
                lastPongAtMs.set(now)
                startHeartbeat(connectionId)
                buildReplayActions().forEach { payload ->
                    sendAction(payload)
                }
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                if (!isCurrentConnection(connectionId, webSocket)) {
                    return
                }
                val now = SystemClock.elapsedRealtime()
                lastInboundAtMs.set(now)
                val payload = runCatching { JSONObject(text) }.getOrNull() ?: return
                if (payload.optString("type").trim().equals("pong", ignoreCase = true)) {
                    lastPongAtMs.set(now)
                    return
                }
                val messageId = extractMessageId(payload)
                val requiresAck = payload.optBoolean("requiresAck", !messageId.isNullOrBlank())
                if (messageId != null && cursorStore.isDuplicate(normalizedUserId, streamKey, messageId)) {
                    if (requiresAck) {
                        sendAck(messageId)
                    }
                    return
                }
                val delivered = runCatching {
                    onJsonMessage(payload)
                }.onFailure { error ->
                    Log.e(
                        logTag(),
                        "failed to deliver websocket payload: streamKey=$streamKey, messageId=$messageId",
                        error
                    )
                }.isSuccess
                if (!delivered) {
                    return
                }
                if (messageId != null) {
                    cursorStore.recordReceived(normalizedUserId, streamKey, messageId)
                    if (requiresAck) {
                        sendAck(messageId)
                    }
                }
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                handleSocketTerminated(connectionId, "failure", t)
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                handleSocketTerminated(
                    connectionId = connectionId,
                    reason = "closed(code=$code, reason=$reason)",
                    failure = null
                )
            }
        }
    }

    private fun handleSocketTerminated(connectionId: Long, reason: String, failure: Throwable?) {
        var shouldReconnect = false
        synchronized(this) {
            val current = socketState
            if (current == null || current.connectionId != connectionId) {
                return
            }
            socketState = null
            stopHeartbeatLocked()
            shouldReconnect = desiredConnected
        }
        if (failure != null) {
            Log.w(logTag(), "websocket terminated: $reason", failure)
        } else {
            Log.w(logTag(), "websocket terminated: $reason")
        }
        if (shouldReconnect) {
            scheduleReconnect(reason)
        }
    }

    private fun startHeartbeat(connectionId: Long) {
        synchronized(this) {
            stopHeartbeatLocked()
            heartbeatJob = scope.launch {
                while (isActive) {
                    delay(HEARTBEAT_INTERVAL_MS)
                    val state = synchronized(this@ReliableTaskWebSocketClient) {
                        socketState?.takeIf {
                            it.connectionId == connectionId && desiredConnected
                        }
                    } ?: break
                    val lastSignalAt = max(lastInboundAtMs.get(), lastPongAtMs.get())
                    val idleMs = SystemClock.elapsedRealtime() - lastSignalAt
                    if (idleMs >= HEARTBEAT_TIMEOUT_MS) {
                        Log.w(
                            logTag(),
                            "heartbeat timeout; force reconnect: streamKey=${state.streamKey}, idleMs=$idleMs"
                        )
                        forceReconnect(state, "heartbeat timeout")
                        break
                    }
                    if (!sendPing(state.streamKey)) {
                        Log.w(
                            logTag(),
                            "heartbeat ping send failed; force reconnect: streamKey=${state.streamKey}"
                        )
                        forceReconnect(state, "heartbeat ping send failed")
                        break
                    }
                }
            }
        }
    }

    private fun forceReconnect(state: ReliableSocketState, reason: String) {
        var shouldReconnect = false
        synchronized(this) {
            val current = socketState
            if (current == null || current.connectionId != state.connectionId) {
                return
            }
            socketState = null
            stopHeartbeatLocked()
            shouldReconnect = desiredConnected
        }
        runCatching {
            state.socket.close(CLOSE_CODE_HEARTBEAT_TIMEOUT, reason)
        }
        runCatching {
            state.socket.cancel()
        }
        if (shouldReconnect) {
            scheduleReconnect(reason)
        }
    }

    private fun scheduleReconnect(reason: String) {
        var reconnectDelayMs = 0L
        synchronized(this) {
            if (!desiredConnected || reconnectJob != null) {
                return
            }
            reconnectDelayMs = computeReconnectDelayMs(reconnectAttempt)
            reconnectAttempt += 1
            reconnectJob = scope.launch {
                if (reconnectDelayMs > 0L) {
                    delay(reconnectDelayMs)
                }
                synchronized(this@ReliableTaskWebSocketClient) {
                    reconnectJob = null
                    openSocketIfNeededLocked()
                }
            }
        }
        Log.w(
            logTag(),
            "schedule websocket reconnect in ${reconnectDelayMs}ms: reason=$reason"
        )
    }

    @Synchronized
    private fun stopHeartbeatLocked() {
        heartbeatJob?.cancel()
        heartbeatJob = null
    }

    @Synchronized
    private fun cancelReconnectLocked() {
        reconnectJob?.cancel()
        reconnectJob = null
    }

    @Synchronized
    private fun isCurrentConnection(connectionId: Long, webSocket: WebSocket): Boolean {
        val current = socketState ?: return false
        return current.connectionId == connectionId && current.socket == webSocket
    }

    private fun normalizedStreamKey(): String? {
        return streamKeyProvider()
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
    }

    private fun buildSocketUrl(streamKey: String): String {
        val builder = Uri.parse(wsEndpoint).buildUpon()
        builder.appendQueryParameter("userId", normalizedUserId)
        builder.appendQueryParameter("streamKey", streamKey)
        val lastReceivedMessageId = cursorStore.lastReceivedMessageId(normalizedUserId, streamKey)
        if (!lastReceivedMessageId.isNullOrBlank()) {
            builder.appendQueryParameter("lastReceivedMessageId", lastReceivedMessageId)
        }
        return builder.build().toString()
    }

    private fun extractMessageId(payload: JSONObject): String? {
        MESSAGE_ID_KEYS.forEach { key ->
            val raw = payload.opt(key)
            val normalized = when (raw) {
                null,
                JSONObject.NULL -> ""
                else -> raw.toString()
            }.trim()
            if (normalized.isNotEmpty()) {
                return normalized
            }
        }
        return null
    }

    private fun computeReconnectDelayMs(attempt: Int): Long {
        val cappedShift = min(attempt.coerceAtLeast(0), MAX_BACKOFF_SHIFT)
        return INITIAL_RECONNECT_DELAY_MS shl cappedShift
    }

    private fun logTag(): String {
        return if (clientLabel.length <= MAX_LOG_TAG_LENGTH) {
            clientLabel
        } else {
            clientLabel.take(MAX_LOG_TAG_LENGTH)
        }
    }

    companion object {
        private const val CLOSE_CODE_CLIENT_DISCONNECT = 1000
        private const val CLOSE_CODE_CLIENT_REFRESH = 4002
        private const val CLOSE_CODE_HEARTBEAT_TIMEOUT = 4001
        private const val HEARTBEAT_INTERVAL_MS = 15_000L
        private const val HEARTBEAT_TIMEOUT_MS = 45_000L
        private const val INITIAL_RECONNECT_DELAY_MS = 1_000L
        private const val MAX_BACKOFF_SHIFT = 5
        private const val MAX_LOG_TAG_LENGTH = 23
        private val MESSAGE_ID_KEYS = listOf("messageId", "message_id")

        private val sharedOkHttpClient: OkHttpClient by lazy {
            OkHttpClient.Builder()
                .connectTimeout(10L, TimeUnit.SECONDS)
                .readTimeout(0L, TimeUnit.MILLISECONDS)
                .build()
        }

        fun buildStableStreamKey(vararg parts: String): String {
            val normalized = parts.mapNotNull { part ->
                part.trim()
                    .takeIf { it.isNotEmpty() }
                    ?.replace(STREAM_KEY_SANITIZE_REGEX, "_")
                    ?.trim('_')
                    ?.takeIf { it.isNotEmpty() }
            }
            return normalized.joinToString(":").ifEmpty { "default" }
        }

        private val STREAM_KEY_SANITIZE_REGEX = Regex("[^A-Za-z0-9._-]+")
    }
}