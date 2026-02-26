package com.hongxu.videoToMarkdownTest2

import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * 对齐后端 AST 协议的段落节点。
 */
data class SemanticNode(
    val id: String,
    val text: String,
    val type: String = "paragraph",
    val originalMarkdown: String? = null,
    val relevanceScore: Float,
    val bridgeText: String? = null,
    val reasoning: String? = null,
    val insightTerms: List<String> = emptyList(),
    val insightsTags: List<String> = emptyList()
) {
    /**
     * 兼容后端不同字段命名：
     * - insight_terms
     * - insights_tags
     */
    fun resolvedInsightTerms(): List<String> {
        return (insightTerms + insightsTags)
            .map { it.trim() }
            .filter { it.isNotBlank() }
            .distinct()
    }
}

/**
 * 段落交互事件模型。
 */
sealed class ParagraphGestureEvent {
    data class SwipeLeft(
        val nodeId: String,
        val offsetX: Float,
        val threshold: Float
    ) : ParagraphGestureEvent()

    data class SwipeRight(
        val nodeId: String,
        val offsetX: Float,
        val threshold: Float,
        val hasBridge: Boolean
    ) : ParagraphGestureEvent()

    data class DoubleTap(
        val nodeId: String
    ) : ParagraphGestureEvent()

    data class Settle(
        val nodeId: String,
        val finalOffsetX: Float
    ) : ParagraphGestureEvent()
}

/**
 * 阅读器埋点事件。
 */
data class ReaderTelemetryEvent(
    val nodeId: String,
    val eventType: String,
    val relevanceScore: Float,
    val timestampMs: Long = System.currentTimeMillis(),
    val payload: Map<String, String> = emptyMap()
)

/**
 * 词句级高亮选择状态。
 */
data class TokenSelection(
    val token: String,
    val start: Int,
    val end: Int
)

/**
 * 三维解析卡片模型。
 */
data class TokenInsightCard(
    val token: String,
    val title: String,
    val markdown: String
)

/**
 * 与 java-orchestrator 的 /api/mobile/tasks/{taskId}/meta 契约对齐。
 */
data class MobileTaskMetaPayload(
    val taskId: String,
    val pathKey: String,
    val favorites: Map<String, Boolean>,
    val deleted: Map<String, Boolean>,
    val comments: Map<String, List<String>>,
    val tokenLike: Map<String, Boolean>,
    val tokenAnnotations: Map<String, String>,
    val taskTitle: String
)

/**
 * 与 java-orchestrator 的 TaskMetaUpdateRequest 对齐。
 */
data class MobileTaskMetaUpdateRequest(
    val path: String?,
    val taskTitle: String?,
    val favorites: Map<String, Boolean>,
    val deleted: Map<String, Boolean>,
    val comments: Map<String, List<String>>,
    val tokenLike: Map<String, Boolean>,
    val tokenAnnotations: Map<String, String>
)

/**
 * Telemetry 单事件模型。
 */
data class MobileTelemetryEvent(
    val nodeId: String,
    val eventType: String,
    val relevanceScore: Float,
    val timestampMs: Long,
    val payload: Map<String, String>
)

/**
 * 任务元数据 API。
 */
interface MobileMarkdownMetaApi {
    suspend fun fetchTaskMeta(taskId: String, pathHint: String?): MobileTaskMetaPayload

    suspend fun updateTaskMeta(taskId: String, request: MobileTaskMetaUpdateRequest): MobileTaskMetaPayload
}

/**
 * 后端 telemetry 上报 API。
 */
interface MobileMarkdownTelemetryApi {
    suspend fun ingestTaskTelemetry(
        taskId: String,
        pathHint: String?,
        events: List<MobileTelemetryEvent>
    )
}

/**
 * 支持主动 flush 的 telemetry API。
 */
interface FlushableMobileMarkdownTelemetryApi : MobileMarkdownTelemetryApi {
    suspend fun flush(reason: String)

    fun flushAsync(reason: String)
}

/**
 * 微批队列配置。
 */
data class TelemetryQueueConfig(
    val batchSize: Int = 50,
    val periodicFlushMs: Long = 5_000L
)

/**
 * 直接通过 HTTP 对接 java-orchestrator。
 *
 * 默认对齐现有网页端 API_BASE = /api/mobile。
 */
class HttpMobileMarkdownMetaApi(
    private val apiBaseUrl: String
) : MobileMarkdownMetaApi {

    override suspend fun fetchTaskMeta(taskId: String, pathHint: String?): MobileTaskMetaPayload {
        return withContext(Dispatchers.IO) {
            val encodedTask = URLEncoder.encode(taskId, StandardCharsets.UTF_8)
            val query = if (!pathHint.isNullOrBlank()) {
                "?path=" + URLEncoder.encode(pathHint, StandardCharsets.UTF_8)
            } else {
                ""
            }
            val url = URL("$apiBaseUrl/tasks/$encodedTask/meta$query")
            val connection = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 8_000
                readTimeout = 8_000
                setRequestProperty("Accept", "application/json")
            }
            connection.useAndReadPayload()
        }
    }

    override suspend fun updateTaskMeta(taskId: String, request: MobileTaskMetaUpdateRequest): MobileTaskMetaPayload {
        return withContext(Dispatchers.IO) {
            val encodedTask = URLEncoder.encode(taskId, StandardCharsets.UTF_8)
            val url = URL("$apiBaseUrl/tasks/$encodedTask/meta")
            val body = JSONObject().apply {
                put("path", request.path ?: "")
                if (!request.taskTitle.isNullOrBlank()) {
                    put("taskTitle", request.taskTitle)
                }
                put("favorites", JSONObject().apply {
                    request.favorites.forEach { (key, value) ->
                        if (value) {
                            put(key, true)
                        }
                    }
                })
                put("deleted", JSONObject().apply {
                    request.deleted.forEach { (key, value) ->
                        if (value) {
                            put(key, true)
                        }
                    }
                })
                put("comments", JSONObject().apply {
                    request.comments.forEach { (key, values) ->
                        val arr = JSONArray()
                        values.forEach { comment ->
                            if (comment.isNotBlank()) {
                                arr.put(comment)
                            }
                        }
                        if (arr.length() > 0) {
                            put(key, arr)
                        }
                    }
                })
                put("tokenLike", JSONObject().apply {
                    request.tokenLike.forEach { (key, value) ->
                        if (key.isNotBlank() && value) {
                            put(key, true)
                        }
                    }
                })
                put("tokenAnnotations", JSONObject().apply {
                    request.tokenAnnotations.forEach { (key, value) ->
                        val normalizedKey = key.trim()
                        val normalizedValue = value.trim()
                        if (normalizedKey.isNotBlank() && normalizedValue.isNotBlank()) {
                            put(normalizedKey, normalizedValue)
                        }
                    }
                })
            }.toString()

            val connection = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "PUT"
                connectTimeout = 8_000
                readTimeout = 8_000
                doOutput = true
                setRequestProperty("Content-Type", "application/json")
                setRequestProperty("Accept", "application/json")
            }

            OutputStreamWriter(connection.outputStream, StandardCharsets.UTF_8).use { writer ->
                writer.write(body)
            }
            connection.useAndReadPayload()
        }
    }

    /**
     * 统一处理 HTTP 返回与 JSON 解析。
     */
    private fun HttpURLConnection.useAndReadPayload(): MobileTaskMetaPayload {
        return try {
            val code = responseCode
            val stream = if (code in 200..299) {
                inputStream
            } else {
                errorStream
            }
            val text = stream?.use {
                BufferedReader(InputStreamReader(it, StandardCharsets.UTF_8)).readText()
            }.orEmpty()
            if (code !in 200..299) {
                throw IllegalStateException("HTTP $code: $text")
            }
            parseMobileTaskMetaPayload(text)
        } finally {
            disconnect()
        }
    }
}

/**
 * 通过微批队列对接 /api/mobile/tasks/{taskId}/telemetry。
 *
 * 关键策略：
 * 1. 前端事件先入内存队列（可扩展为 Room 持久队列）。
 * 2. 队列达到 batchSize（默认 50）立即发送。
 * 3. 支持外部在锁屏/退出文章时主动 flush。
 */
class HttpMobileMarkdownTelemetryApi(
    private val apiBaseUrl: String,
    private val queueConfig: TelemetryQueueConfig = TelemetryQueueConfig()
) : FlushableMobileMarkdownTelemetryApi {
    private companion object {
        const val TAG = "MobileTelemetry"
    }

    private data class PendingTelemetry(
        val taskId: String,
        val pathHint: String?,
        val event: MobileTelemetryEvent
    )

    private val queueMutex = Mutex()
    private val pendingQueue = ArrayDeque<PendingTelemetry>()
    private val senderScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val batchSeq = AtomicLong(0L)

    init {
        if (queueConfig.periodicFlushMs > 0L) {
            senderScope.launch {
                while (isActive) {
                    delay(queueConfig.periodicFlushMs)
                    runCatching {
                        flush(reason = "periodic_flush")
                    }.onFailure { error ->
                        Log.w(TAG, "Periodic telemetry flush failed: ${error.message}", error)
                    }
                }
            }
        }
    }

    override suspend fun ingestTaskTelemetry(
        taskId: String,
        pathHint: String?,
        events: List<MobileTelemetryEvent>
    ) {
        if (events.isEmpty()) {
            return
        }
        var shouldFlush = false
        queueMutex.withLock {
            events.forEach { event ->
                pendingQueue.addLast(
                    PendingTelemetry(
                        taskId = taskId,
                        pathHint = pathHint,
                        event = event
                    )
                )
            }
            shouldFlush = pendingQueue.size >= queueConfig.batchSize
        }
        if (shouldFlush) {
            flushAsync(reason = "batch_size_reached")
        }
    }

    override suspend fun flush(reason: String) {
        val drained = queueMutex.withLock {
            if (pendingQueue.isEmpty()) {
                return
            }
            val copy = pendingQueue.toList()
            pendingQueue.clear()
            copy
        }

        val grouped = drained.groupBy { it.taskId to (it.pathHint ?: "") }
        try {
            grouped.forEach { (taskAndPath, items) ->
                sendBatch(
                    taskId = taskAndPath.first,
                    pathHint = taskAndPath.second,
                    events = items.map { it.event },
                    reason = reason,
                    sequence = batchSeq.incrementAndGet()
                )
            }
        } catch (error: Exception) {
            // 发送失败回滚到队首，避免事件丢失。
            queueMutex.withLock {
                drained.asReversed().forEach { item ->
                    pendingQueue.addFirst(item)
                }
            }
            Log.w(
                TAG,
                "Telemetry flush failed and events were re-queued: reason=$reason, size=${drained.size}",
                error
            )
        }
    }

    override fun flushAsync(reason: String) {
        senderScope.launch {
            runCatching {
                flush(reason)
            }
        }
    }

    /**
     * 页面销毁时可调用，释放后台协程。
     */
    fun close() {
        senderScope.cancel()
    }

    private suspend fun sendBatch(
        taskId: String,
        pathHint: String?,
        events: List<MobileTelemetryEvent>,
        reason: String,
        sequence: Long
    ) {
        withContext(Dispatchers.IO) {
            val globalIngestUrl = resolveGlobalTelemetryIngestUrl()
            val globalIngestBody = buildGlobalIngestBody(
                taskId = taskId,
                pathHint = pathHint,
                events = events,
                reason = reason,
                sequence = sequence
            )
            val encodedTask = URLEncoder.encode(taskId, StandardCharsets.UTF_8)
            val taskScopedUrl = URL("$apiBaseUrl/tasks/$encodedTask/telemetry")
            val taskScopedBody = buildTaskScopedBody(pathHint = pathHint, events = events)

            var globalFailure: Exception? = null
            if (globalIngestUrl != null) {
                runCatching {
                    sendJsonRequest(
                        url = globalIngestUrl,
                        body = globalIngestBody,
                        reason = reason,
                        sequence = sequence,
                        taskId = taskId,
                        endpointTag = "global_telemetry_ingest"
                    )
                }.onFailure { error ->
                    globalFailure = error as? Exception ?: IllegalStateException(
                        error.message ?: "global ingest failed",
                        error
                    )
                    Log.w(
                        TAG,
                        "Global telemetry ingest failed, fallback to task endpoint: reason=$reason, seq=$sequence, taskId=$taskId",
                        error
                    )
                }
            }
            if (globalIngestUrl == null || globalFailure != null) {
                try {
                    sendJsonRequest(
                        url = taskScopedUrl,
                        body = taskScopedBody,
                        reason = reason,
                        sequence = sequence,
                        taskId = taskId,
                        endpointTag = "task_scoped_telemetry"
                    )
                } catch (error: Exception) {
                    globalFailure?.let { error.addSuppressed(it) }
                    throw error
                }
            }
        }
    }

    private fun buildTaskScopedBody(
        pathHint: String?,
        events: List<MobileTelemetryEvent>
    ): String {
        return JSONObject().apply {
            put("path", pathHint ?: "")
            put("events", buildTelemetryEventsJson(events))
        }.toString()
    }

    private fun buildGlobalIngestBody(
        taskId: String,
        pathHint: String?,
        events: List<MobileTelemetryEvent>,
        reason: String,
        sequence: Long
    ): String {
        return JSONObject().apply {
            put("taskId", taskId)
            put("path", pathHint ?: "")
            put("flushReason", reason)
            put("batchSeq", sequence)
            put("batchSize", events.size)
            put("events", buildTelemetryEventsJson(events))
        }.toString()
    }

    private fun buildTelemetryEventsJson(events: List<MobileTelemetryEvent>): JSONArray {
        return JSONArray().apply {
            events.forEach { event ->
                put(JSONObject().apply {
                    put("nodeId", event.nodeId)
                    put("eventType", event.eventType)
                    put("relevanceScore", event.relevanceScore)
                    put("timestampMs", event.timestampMs)
                    put("payload", JSONObject().apply {
                        event.payload.forEach { (k, v) ->
                            put(k, v)
                        }
                    })
                })
            }
        }
    }

    private fun resolveGlobalTelemetryIngestUrl(): URL? {
        val normalizedBase = apiBaseUrl.trim().trimEnd('/')
        if (normalizedBase.isEmpty()) {
            return null
        }
        val normalizedApiRoot = when {
            normalizedBase.endsWith("/api/mobile") -> {
                normalizedBase.removeSuffix("/mobile")
            }
            normalizedBase.endsWith("/mobile") -> {
                normalizedBase.removeSuffix("/mobile")
            }
            else -> {
                return null
            }
        }
        return runCatching {
            URL("$normalizedApiRoot/telemetry/ingest")
        }.getOrNull()
    }

    private fun sendJsonRequest(
        url: URL,
        body: String,
        reason: String,
        sequence: Long,
        taskId: String,
        endpointTag: String
    ) {
        val connection = (url.openConnection() as HttpURLConnection).apply {
            requestMethod = "POST"
            connectTimeout = 8_000
            readTimeout = 8_000
            doOutput = true
            setRequestProperty("Content-Type", "application/json")
            setRequestProperty("Accept", "application/json")
        }
        try {
            OutputStreamWriter(connection.outputStream, StandardCharsets.UTF_8).use { writer ->
                writer.write(body)
            }
            val code = connection.responseCode
            val stream = if (code in 200..299) connection.inputStream else connection.errorStream
            val text = stream?.use {
                BufferedReader(InputStreamReader(it, StandardCharsets.UTF_8)).readText()
            }.orEmpty()
            if (code !in 200..299) {
                throw IllegalStateException(
                    "HTTP $code: $text (endpoint=$endpointTag, reason=$reason, seq=$sequence, taskId=$taskId)"
                )
            }
        } finally {
            connection.disconnect()
        }
    }
}

/**
 * JNI 桥接对象。
 */
object LexicalNativeBridge {
    init {
        runCatching {
            System.loadLibrary("lexical_onnx_bridge")
        }
    }

    external fun segmentAt(text: String, cursor: Int): String?

    external fun explainToken(token: String, context: String): String?
}

private fun parseMobileTaskMetaPayload(text: String): MobileTaskMetaPayload {
    val root = JSONObject(if (text.isBlank()) "{}" else text)
    val favoritesObj = root.optJSONObject("favorites") ?: JSONObject()
    val deletedObj = root.optJSONObject("deleted") ?: JSONObject()
    val commentsObj = root.optJSONObject("comments") ?: JSONObject()
    val tokenLikeObj = root.optJSONObject("tokenLike") ?: JSONObject()
    val tokenAnnotationsObj = root.optJSONObject("tokenAnnotations") ?: JSONObject()

    val favorites = LinkedHashMap<String, Boolean>()
    val favoriteIter = favoritesObj.keys()
    while (favoriteIter.hasNext()) {
        val key = favoriteIter.next()
        if (key.isNotBlank() && favoritesObj.optBoolean(key, false)) {
            favorites[key] = true
        }
    }

    val comments = LinkedHashMap<String, List<String>>()
    val commentsIter = commentsObj.keys()
    while (commentsIter.hasNext()) {
        val key = commentsIter.next()
        if (key.isBlank()) {
            continue
        }
        val raw = commentsObj.opt(key)
        val normalized = when (raw) {
            is JSONArray -> {
                buildList {
                    for (i in 0 until raw.length()) {
                        val value = raw.optString(i).trim()
                        if (value.isNotBlank()) {
                            add(value)
                        }
                    }
                }
            }
            is String -> {
                val one = raw.trim()
                if (one.isBlank()) emptyList() else listOf(one)
            }
            else -> emptyList()
        }
        if (normalized.isNotEmpty()) {
            comments[key] = normalized
        }
    }

    val deleted = LinkedHashMap<String, Boolean>()
    val deletedIter = deletedObj.keys()
    while (deletedIter.hasNext()) {
        val key = deletedIter.next()
        if (key.isNotBlank() && deletedObj.optBoolean(key, false)) {
            deleted[key] = true
        }
    }

    val tokenLike = LinkedHashMap<String, Boolean>()
    val tokenLikeIter = tokenLikeObj.keys()
    while (tokenLikeIter.hasNext()) {
        val key = tokenLikeIter.next()
        if (key.isNotBlank() && tokenLikeObj.optBoolean(key, false)) {
            tokenLike[key] = true
        }
    }

    val tokenAnnotations = LinkedHashMap<String, String>()
    val tokenAnnotationIter = tokenAnnotationsObj.keys()
    while (tokenAnnotationIter.hasNext()) {
        val key = tokenAnnotationIter.next()
        if (key.isBlank()) {
            continue
        }
        val value = tokenAnnotationsObj.optString(key).trim()
        if (value.isNotBlank()) {
            tokenAnnotations[key] = value
        }
    }

    return MobileTaskMetaPayload(
        taskId = root.optString("taskId"),
        pathKey = root.optString("pathKey"),
        favorites = favorites,
        deleted = deleted,
        comments = comments,
        tokenLike = tokenLike,
        tokenAnnotations = tokenAnnotations,
        taskTitle = root.optString("taskTitle")
    )
}

/**
 * 读取后端 markdown 接口返回中的个性化节点数组。
 * 兼容字段：
 * - personalizedNodes / nodes
 * - relevance_score / relevanceScore
 * - bridge_text / bridgeText
 * - insight_terms / insights_tags
 */
fun parseSemanticNodesFromPayload(payloadText: String): List<SemanticNode> {
    if (payloadText.isBlank()) {
        return emptyList()
    }
    val root = JSONObject(payloadText)
    val nodes = when {
        root.has("personalizedNodes") -> root.optJSONArray("personalizedNodes")
        root.has("nodes") -> root.optJSONArray("nodes")
        else -> null
    } ?: return emptyList()

    return buildList {
        for (i in 0 until nodes.length()) {
            val item = nodes.optJSONObject(i) ?: continue
            add(parseSemanticNode(item))
        }
    }
}

private fun parseSemanticNode(node: JSONObject): SemanticNode {
    val id = node.optStringByAlias("node_id", "nodeId", "id") ?: ""
    val text = node.optRawStringByAlias(
        "text",
        "raw_markdown",
        "rawMarkdown",
        "markdown",
        "content_markdown",
        "contentMarkdown"
    ) ?: ""
    val type = node.optStringByAlias("type", "node_type", "nodeType") ?: "paragraph"
    val originalMarkdown = node.optRawStringByAlias(
        "original_markdown",
        "originalMarkdown",
        "raw_markdown",
        "rawMarkdown",
        "markdown",
        "content_markdown",
        "contentMarkdown"
    )
    val relevanceScore = node.optFloatByAlias("relevance_score", "relevanceScore") ?: 0f
    val bridgeText = node.optStringByAlias("bridge_text", "bridgeText")
    val reasoning = node.optStringByAlias("reasoning")
    val insightTerms = readJsonArrayStringsByAlias(
        node,
        "insight_terms",
        "insightTerms"
    )
    val insightsTags = readJsonArrayStringsByAlias(
        node,
        "insights_tags",
        "insightsTags",
        "insight_tags",
        "insightTags"
    )

    return SemanticNode(
        id = id,
        text = text,
        type = type,
        originalMarkdown = originalMarkdown,
        relevanceScore = relevanceScore.coerceIn(0f, 1f),
        bridgeText = bridgeText,
        reasoning = reasoning,
        insightTerms = insightTerms,
        insightsTags = insightsTags
    )
}

private fun JSONObject.optStringByAlias(vararg aliases: String): String? {
    aliases.forEach { key ->
        if (!has(key)) {
            return@forEach
        }
        val value = optString(key).trim()
        if (value.isNotBlank()) {
            return value
        }
    }
    return null
}

private fun JSONObject.optRawStringByAlias(vararg aliases: String): String? {
    aliases.forEach { key ->
        if (!has(key)) {
            return@forEach
        }
        val value = optString(key)
        if (value.isNotEmpty()) {
            return value
        }
    }
    return null
}

private fun JSONObject.optFloatByAlias(vararg aliases: String): Float? {
    aliases.forEach { key ->
        if (!has(key)) {
            return@forEach
        }
        val raw = opt(key)
        val parsed = when (raw) {
            is Number -> raw.toFloat()
            is String -> raw.toFloatOrNull()
            else -> null
        }
        if (parsed != null) {
            return parsed
        }
    }
    return null
}

private fun readJsonArrayStringsByAlias(
    node: JSONObject,
    vararg aliases: String
): List<String> {
    aliases.forEach { key ->
        if (!node.has(key)) {
            return@forEach
        }
        val value = node.opt(key)
        when (value) {
            is JSONArray -> {
                val items = buildList {
                    for (i in 0 until value.length()) {
                        val item = value.optString(i).trim()
                        if (item.isNotBlank()) {
                            add(item)
                        }
                    }
                }
                if (items.isNotEmpty()) {
                    return items
                }
            }
            is String -> {
                val one = value.trim()
                if (one.isNotBlank()) {
                    return listOf(one)
                }
            }
        }
    }
    return emptyList()
}
