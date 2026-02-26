package com.hongxu.videoToMarkdownTest2

import android.content.ContentResolver
import android.database.Cursor
import android.net.Uri
import android.provider.OpenableColumns
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InterruptedIOException
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.ConnectException
import java.net.HttpURLConnection
import java.net.SocketTimeoutException
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.UUID

data class MobileTaskListItem(
    val taskId: String,
    val title: String,
    val status: String,
    val progress: Double,
    val statusMessage: String,
    val markdownAvailable: Boolean,
    val createdAt: String,
    val lastOpenedAt: String
)

private data class MobileTaskListPage(
    val tasks: List<MobileTaskListItem>,
    val hasMore: Boolean
)

data class MobileTaskSubmitResult(
    val success: Boolean,
    val taskId: String,
    val status: String,
    val message: String
)

data class MobileTaskRuntimeSnapshot(
    val taskId: String,
    val title: String,
    val status: String,
    val progress: Double,
    val statusMessage: String,
    val createdAt: String,
    val completedAt: String
)

data class MobileTaskCancelResult(
    val success: Boolean,
    val status: String,
    val message: String
)

data class MobileTaskMarkdownPayload(
    val taskId: String,
    val title: String,
    val markdown: String,
    val markdownPath: String,
    val baseDir: String,
    val rawPayload: String
)

class HttpMobileTaskApi(
    private val apiBaseUrl: String
) {
    suspend fun listTasks(
        page: Int = 0,
        pageSize: Int = 40,
        onlyMultiSegment: Boolean = true
    ): List<MobileTaskListItem> {
        return withContext(Dispatchers.IO) {
            val allTasks = mutableListOf<MobileTaskListItem>()
            var currentPage = page.coerceAtLeast(0)
            var hasMore = true
            while (hasMore) {
                val pageResult = listTasksPage(
                    page = currentPage,
                    pageSize = pageSize,
                    onlyMultiSegment = onlyMultiSegment
                )
                allTasks += pageResult.tasks
                hasMore = pageResult.hasMore && pageResult.tasks.isNotEmpty()
                currentPage += 1
            }
            allTasks
        }
    }

    private fun listTasksPage(
        page: Int,
        pageSize: Int,
        onlyMultiSegment: Boolean
    ): MobileTaskListPage {
        val query = "?page=$page&pageSize=$pageSize&onlyMultiSegment=$onlyMultiSegment"
        val url = URL("$apiBaseUrl/tasks$query")
        val connection = (url.openConnection() as HttpURLConnection).apply {
            requestMethod = "GET"
            connectTimeout = 10_000
            readTimeout = 15_000
            setRequestProperty("Accept", "application/json")
        }
        return connection.useJsonPayload { json ->
            val array = json.optJSONArray("tasks")
            if (array == null || array.length() == 0) {
                return@useJsonPayload MobileTaskListPage(
                    tasks = emptyList(),
                    hasMore = false
                )
            }
            val result = mutableListOf<MobileTaskListItem>()
            for (i in 0 until array.length()) {
                val item = array.optJSONObject(i) ?: continue
                val taskId = item.optString("taskId").trim()
                if (taskId.isEmpty()) {
                    continue
                }
                result += MobileTaskListItem(
                    taskId = taskId,
                    title = item.optString("title").ifBlank { taskId },
                    status = item.optString("status"),
                    progress = item.optDouble("progress", 0.0),
                    statusMessage = item.optString("statusMessage"),
                    markdownAvailable = item.optBoolean("markdownAvailable", false),
                    createdAt = item.optString("createdAt"),
                    lastOpenedAt = item.optString("lastOpenedAt")
                )
            }
            MobileTaskListPage(
                tasks = result,
                hasMore = json.optBoolean("hasMore", false)
            )
        }
    }

    suspend fun submitVideoUrl(videoUrl: String): MobileTaskSubmitResult {
        return withContext(Dispatchers.IO) {
            val url = URL("$apiBaseUrl/tasks/submit")
            val connection = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                connectTimeout = 10_000
                readTimeout = 20_000
                doOutput = true
                setRequestProperty("Accept", "application/json")
                setRequestProperty("Content-Type", "application/json; charset=UTF-8")
            }
            val body = JSONObject().apply {
                put("videoUrl", videoUrl)
            }.toString()
            OutputStreamWriter(connection.outputStream, StandardCharsets.UTF_8).use {
                it.write(body)
            }
            connection.useJsonPayload { json ->
                MobileTaskSubmitResult(
                    success = json.optBoolean("success", false),
                    taskId = json.optString("taskId"),
                    status = json.optString("status"),
                    message = json.optString("message")
                )
            }
        }
    }

    suspend fun uploadVideoFile(
        contentResolver: ContentResolver,
        uri: Uri
    ): MobileTaskSubmitResult {
        return withContext(Dispatchers.IO) {
            val fileName = queryDisplayName(contentResolver, uri)
                ?.takeIf { it.isNotBlank() }
                ?: "mobile_upload_${System.currentTimeMillis()}.mp4"
            val mimeType = contentResolver.getType(uri) ?: "application/octet-stream"
            val boundary = "----mobile-upload-${UUID.randomUUID()}"

            val url = URL("$apiBaseUrl/tasks/upload")
            val connection = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                connectTimeout = 10_000
                readTimeout = 120_000
                doOutput = true
                setChunkedStreamingMode(0)
                setRequestProperty("Accept", "application/json")
                setRequestProperty("Content-Type", "multipart/form-data; boundary=$boundary")
            }

            connection.outputStream.use { output ->
                val header = buildString {
                    append("--$boundary\r\n")
                    append("Content-Disposition: form-data; name=\"videoFile\"; filename=\"")
                    append(fileName.replace("\"", "_"))
                    append("\"\r\n")
                    append("Content-Type: $mimeType\r\n\r\n")
                }
                output.write(header.toByteArray(StandardCharsets.UTF_8))
                contentResolver.openInputStream(uri).use { input ->
                    requireNotNull(input) { "Cannot open selected file." }
                    val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                    while (true) {
                        val read = input.read(buffer)
                        if (read <= 0) {
                            break
                        }
                        output.write(buffer, 0, read)
                    }
                }
                output.write("\r\n--$boundary--\r\n".toByteArray(StandardCharsets.UTF_8))
            }

            connection.useJsonPayload { json ->
                MobileTaskSubmitResult(
                    success = json.optBoolean("success", false),
                    taskId = json.optString("taskId"),
                    status = json.optString("status"),
                    message = json.optString("message")
                )
            }
        }
    }

    suspend fun loadTaskMarkdown(taskId: String): MobileTaskMarkdownPayload {
        return withContext(Dispatchers.IO) {
            val encoded = URLEncoder.encode(taskId, StandardCharsets.UTF_8)
            val url = URL("$apiBaseUrl/tasks/$encoded/markdown")
            var backoffMs = 500L
            var lastError: Exception? = null
            for (attempt in 0..1) {
                try {
                    val connection = (url.openConnection() as HttpURLConnection).apply {
                        requestMethod = "GET"
                        connectTimeout = 10_000
                        readTimeout = 60_000
                        setRequestProperty("Accept", "application/json")
                    }
                    val text = connection.useTextPayload()
                    val json = JSONObject(if (text.isBlank()) "{}" else text)
                    return@withContext MobileTaskMarkdownPayload(
                        taskId = json.optString("taskId"),
                        title = json.optString("title"),
                        markdown = json.optString("markdown"),
                        markdownPath = json.optString("markdownPath"),
                        baseDir = json.optString("baseDir"),
                        rawPayload = text
                    )
                } catch (error: Exception) {
                    if (error is CancellationException) {
                        throw error
                    }
                    lastError = error
                    val shouldRetry = attempt == 0 && isRetryableMarkdownLoadError(error)
                    if (!shouldRetry) {
                        throw error
                    }
                    delay(backoffMs)
                    backoffMs *= 2
                }
            }
            throw lastError ?: IllegalStateException("load markdown failed")
        }
    }

    suspend fun markTaskOpened(taskId: String): String {
        return withContext(Dispatchers.IO) {
            val encoded = URLEncoder.encode(taskId, StandardCharsets.UTF_8)
            val url = URL("$apiBaseUrl/tasks/$encoded/opened")
            val connection = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                connectTimeout = 10_000
                readTimeout = 15_000
                setRequestProperty("Accept", "application/json")
            }
            connection.useJsonPayload { json ->
                json.optString("lastOpenedAt")
            }
        }
    }

    suspend fun renameTaskTitle(taskId: String, title: String): String {
        return withContext(Dispatchers.IO) {
            val normalizedTaskId = taskId.trim()
            if (normalizedTaskId.isEmpty()) {
                throw IllegalArgumentException("taskId cannot be empty")
            }
            val normalizedTitle = title.trim()
            if (normalizedTitle.isEmpty()) {
                throw IllegalArgumentException("title cannot be empty")
            }
            val encoded = URLEncoder.encode(normalizedTaskId, StandardCharsets.UTF_8)
            val url = URL("$apiBaseUrl/tasks/$encoded/meta")
            val connection = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "PUT"
                connectTimeout = 10_000
                readTimeout = 20_000
                doOutput = true
                setRequestProperty("Accept", "application/json")
                setRequestProperty("Content-Type", "application/json; charset=UTF-8")
            }
            val body = JSONObject().apply {
                put("taskTitle", normalizedTitle)
            }.toString()
            OutputStreamWriter(connection.outputStream, StandardCharsets.UTF_8).use {
                it.write(body)
            }
            connection.useJsonPayload { json ->
                json.optString("taskTitle").ifBlank { normalizedTitle }
            }
        }
    }

    suspend fun getTaskRuntimeSnapshot(taskId: String): MobileTaskRuntimeSnapshot {
        return withContext(Dispatchers.IO) {
            val encoded = URLEncoder.encode(taskId, StandardCharsets.UTF_8)
            val url = URL("$apiBaseUrl/tasks/$encoded")
            val connection = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "GET"
                connectTimeout = 10_000
                readTimeout = 20_000
                setRequestProperty("Accept", "application/json")
            }
            connection.useJsonPayload { json ->
                MobileTaskRuntimeSnapshot(
                    taskId = json.optString("taskId", taskId),
                    title = json.optString("title"),
                    status = json.optString("status"),
                    progress = json.optDouble("progress", 0.0),
                    statusMessage = json.optString("statusMessage"),
                    createdAt = json.optString("createdAt"),
                    completedAt = json.optString("completedAt")
                )
            }
        }
    }

    suspend fun cancelTask(taskId: String): MobileTaskCancelResult {
        return withContext(Dispatchers.IO) {
            val encoded = URLEncoder.encode(taskId, StandardCharsets.UTF_8)
            val url = URL("$apiBaseUrl/tasks/$encoded")
            val connection = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "DELETE"
                connectTimeout = 10_000
                readTimeout = 20_000
                setRequestProperty("Accept", "application/json")
            }
            connection.useJsonPayload { json ->
                MobileTaskCancelResult(
                    success = json.optBoolean("success", false),
                    status = json.optString("status"),
                    message = json.optString("message")
                )
            }
        }
    }

    private fun HttpURLConnection.useTextPayload(): String {
        try {
            val status = responseCode
            val stream = if (status in 200..299) inputStream else errorStream
            val text = stream?.use {
                BufferedReader(InputStreamReader(it, StandardCharsets.UTF_8)).readText()
            }.orEmpty()
            if (status !in 200..299) {
                val message = runCatching {
                    JSONObject(text).optString("message").ifBlank { "HTTP $status" }
                }.getOrDefault("HTTP $status")
                throw IllegalStateException(message)
            }
            return text
        } finally {
            disconnect()
        }
    }

    private fun queryDisplayName(contentResolver: ContentResolver, uri: Uri): String? {
        val projection = arrayOf(OpenableColumns.DISPLAY_NAME)
        val cursor: Cursor? = contentResolver.query(uri, projection, null, null, null)
        cursor.use {
            if (it == null || !it.moveToFirst()) {
                return null
            }
            val index = it.getColumnIndex(OpenableColumns.DISPLAY_NAME)
            if (index < 0) {
                return null
            }
            return it.getString(index)
        }
    }

    private fun isRetryableMarkdownLoadError(error: Exception): Boolean {
        return when (error) {
            is SocketTimeoutException,
            is ConnectException,
            is InterruptedIOException -> true
            is IllegalStateException -> {
                val message = error.message?.trim().orEmpty().uppercase()
                message.startsWith("HTTP 5") || message == "HTTP 429"
            }
            else -> false
        }
    }
}

private inline fun <T> HttpURLConnection.useJsonPayload(parse: (JSONObject) -> T): T {
    try {
        val status = responseCode
        val stream = if (status in 200..299) inputStream else errorStream
        val text = stream?.use {
            BufferedReader(InputStreamReader(it, StandardCharsets.UTF_8)).readText()
        }.orEmpty()
        if (status !in 200..299) {
            val message = runCatching {
                JSONObject(text).optString("message").ifBlank { "HTTP $status" }
            }.getOrDefault("HTTP $status")
            throw IllegalStateException(message)
        }
        val json = if (text.isBlank()) JSONObject() else JSONObject(text)
        return parse(json)
    } finally {
        disconnect()
    }
}
