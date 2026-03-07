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
import java.security.MessageDigest
import java.util.LinkedHashMap
import java.util.Locale
import java.util.UUID

data class MobileTaskListItem(
    val taskId: String,
    val title: String,
    val status: String,
    val progress: Double,
    val statusMessage: String,
    val domain: String,
    val mainTopic: String,
    val markdownAvailable: Boolean,
    val createdAt: String,
    val lastOpenedAt: String,
    val taskPath: String = "",
    val collectionPath: String = ""
)

private data class MobileTaskListPage(
    val tasks: List<MobileTaskListItem>,
    val hasMore: Boolean
)

data class MobileTaskSubmitResult(
    val success: Boolean,
    val taskId: String,
    val status: String,
    val message: String,
    val normalizedVideoUrl: String = "",
    val probeOnly: Boolean = false,
    val reused: Boolean = false,
    val fileMd5: String = "",
    val fileExt: String = ""
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
    private val apiBaseUrl: String,
    private val userId: String? = null
) {
    companion object {
        private val MD5_HEX_REGEX = Regex("^[a-f0-9]{32}$")
        private val FILE_EXT_REGEX = Regex("^\\.[a-z0-9]{1,16}$")
    }

    suspend fun listTasks(
        page: Int = 0,
        pageSize: Int = 0,
        onlyMultiSegment: Boolean = true
    ): List<MobileTaskListItem> {
        return withContext(Dispatchers.IO) {
            if (pageSize <= 0) {
                val singlePage = listTasksPage(
                    page = page.coerceAtLeast(0),
                    pageSize = pageSize,
                    onlyMultiSegment = onlyMultiSegment
                )
                return@withContext deduplicateTaskSnapshots(singlePage.tasks)
            }
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
            deduplicateTaskSnapshots(allTasks)
        }
    }

    private fun deduplicateTaskSnapshots(tasks: List<MobileTaskListItem>): List<MobileTaskListItem> {
        if (tasks.size <= 1) {
            return tasks
        }
        val deduplicated = LinkedHashMap<String, MobileTaskListItem>(tasks.size)
        tasks.forEach { task ->
            val canonicalId = canonicalTaskId(task.taskId)
            if (canonicalId.isEmpty()) {
                return@forEach
            }
            val existing = deduplicated[canonicalId]
            deduplicated[canonicalId] = when {
                existing == null -> task
                !existing.markdownAvailable && task.markdownAvailable -> task
                isProcessingTaskStatus(existing.status) && !isProcessingTaskStatus(task.status) -> existing
                !isProcessingTaskStatus(existing.status) && isProcessingTaskStatus(task.status) -> task
                else -> existing
            }
        }
        return deduplicated.values.toList()
    }

    private fun canonicalTaskId(rawTaskId: String): String {
        val normalized = rawTaskId.trim()
        if (normalized.isEmpty()) {
            return ""
        }
        if (normalized.startsWith("storage:")) {
            return normalized.removePrefix("storage:").trim()
        }
        return normalized
    }

    private fun isProcessingTaskStatus(status: String): Boolean {
        val normalized = status.trim().uppercase(Locale.ROOT)
        return normalized == "PROCESSING" ||
            normalized == "RUNNING" ||
            normalized == "QUEUED" ||
            normalized == "PENDING"
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
                    domain = item.optString("domain"),
                    mainTopic = item.optString("mainTopic").ifBlank { item.optString("main_topic") },
                    markdownAvailable = item.optBoolean("markdownAvailable", false),
                    createdAt = item.optString("createdAt"),
                    lastOpenedAt = item.optString("lastOpenedAt"),
                    taskPath = item.optString("taskPath").trim(),
                    collectionPath = item.optString("collectionPath").trim()
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
                val normalizedUserId = normalizeUserId()
                if (normalizedUserId != null) {
                    put("userId", normalizedUserId)
                }
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
        uri: Uri,
        probeOnly: Boolean = false
    ): MobileTaskSubmitResult {
        return withContext(Dispatchers.IO) {
            val fileName = queryDisplayName(contentResolver, uri)
                ?.takeIf { it.isNotBlank() }
                ?: "mobile_upload_${System.currentTimeMillis()}.mp4"
            val mimeType = contentResolver.getType(uri) ?: "application/octet-stream"
            val fileExt = normalizeFileExt(fileName)
            val fileMd5 = computeFileMd5(contentResolver, uri)

            val reuseResult = checkUploadReuse(
                fileName = fileName,
                fileSize = queryFileSize(contentResolver, uri),
                probeOnly = probeOnly,
                fileMd5 = fileMd5,
                fileExt = fileExt
            )
            if (reuseResult != null) {
                return@withContext reuseResult
            }

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
                val normalizedUserId = normalizeUserId()
                if (normalizedUserId != null) {
                    writeTextPart(output, boundary, "userId", normalizedUserId)
                }
                if (probeOnly) {
                    writeTextPart(output, boundary, "probeOnly", "true")
                }
                if (!fileMd5.isNullOrBlank()) {
                    writeTextPart(output, boundary, "fileMd5", fileMd5)
                }
                if (fileExt.isNotBlank()) {
                    writeTextPart(output, boundary, "fileExt", fileExt)
                }
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
                parseSubmitResult(json, fileMd5, fileExt)
            }
        }
    }

    private fun checkUploadReuse(
        fileName: String,
        fileSize: Long?,
        probeOnly: Boolean,
        fileMd5: String?,
        fileExt: String
    ): MobileTaskSubmitResult? {
        val normalizedMd5 = fileMd5?.trim()?.lowercase(Locale.ROOT).orEmpty()
        if (!MD5_HEX_REGEX.matches(normalizedMd5) || fileExt.isBlank()) {
            return null
        }
        val url = URL("$apiBaseUrl/tasks/upload/reuse-check")
        val connection = (url.openConnection() as HttpURLConnection).apply {
            requestMethod = "POST"
            connectTimeout = 10_000
            readTimeout = 20_000
            doOutput = true
            setRequestProperty("Accept", "application/json")
            setRequestProperty("Content-Type", "application/json; charset=UTF-8")
        }
        val body = JSONObject().apply {
            put("autoSubmit", true)
            val normalizedUserId = normalizeUserId()
            if (normalizedUserId != null) {
                put("userId", normalizedUserId)
            }
            put("outputDir", "./output")
            put("probeOnly", probeOnly)
            put("fileName", fileName)
            if (fileSize != null && fileSize >= 0) {
                put("fileSize", fileSize)
            }
            put("fileMd5", normalizedMd5)
            put("fileExt", fileExt)
        }.toString()
        OutputStreamWriter(connection.outputStream, StandardCharsets.UTF_8).use {
            it.write(body)
        }
        return connection.useJsonPayload { json ->
            if (!json.optBoolean("success", false) || !json.optBoolean("reused", false)) {
                return@useJsonPayload null
            }
            parseSubmitResult(json, normalizedMd5, fileExt)
        }
    }

    private fun parseSubmitResult(
        json: JSONObject,
        fallbackMd5: String?,
        fallbackExt: String?
    ): MobileTaskSubmitResult {
        return MobileTaskSubmitResult(
            success = json.optBoolean("success", false),
            taskId = json.optString("taskId"),
            status = json.optString("status"),
            message = json.optString("message"),
            normalizedVideoUrl = extractNormalizedVideoUrl(json),
            probeOnly = json.optBoolean("probeOnly", false),
            reused = json.optBoolean("reused", false),
            fileMd5 = json.optString("fileMd5").ifBlank { fallbackMd5 ?: "" },
            fileExt = json.optString("fileExt").ifBlank { fallbackExt ?: "" }
        )
    }

    private fun extractNormalizedVideoUrl(json: JSONObject): String {
        return json.optString("normalizedVideoUrl").ifBlank {
            json.optString("normalizedVideoInput").ifBlank {
                json.optString("probeInput").ifBlank {
                    json.optString("videoUrl")
                }
            }
        }
    }

    private fun writeTextPart(
        output: java.io.OutputStream,
        boundary: String,
        fieldName: String,
        value: String
    ) {
        val textPart = buildString {
            append("--$boundary\r\n")
            append("Content-Disposition: form-data; name=\"")
            append(fieldName)
            append("\"\r\n\r\n")
            append(value)
            append("\r\n")
        }
        output.write(textPart.toByteArray(StandardCharsets.UTF_8))
    }

    private fun normalizeFileExt(fileName: String): String {
        val normalized = fileName.trim().lowercase(Locale.ROOT)
        val dotAt = normalized.lastIndexOf('.')
        if (dotAt < 0 || dotAt >= normalized.length - 1) {
            return ""
        }
        val ext = normalized.substring(dotAt)
        return if (FILE_EXT_REGEX.matches(ext)) ext else ""
    }

    private fun computeFileMd5(contentResolver: ContentResolver, uri: Uri): String? {
        return try {
            val digest = MessageDigest.getInstance("MD5")
            contentResolver.openInputStream(uri).use { input ->
                if (input == null) {
                    return null
                }
                val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                while (true) {
                    val read = input.read(buffer)
                    if (read <= 0) {
                        break
                    }
                    digest.update(buffer, 0, read)
                }
            }
            digest.digest().joinToString(separator = "") { byte ->
                "%02x".format(byte.toInt() and 0xFF)
            }
        } catch (_error: Exception) {
            null
        }
    }

    private fun queryFileSize(contentResolver: ContentResolver, uri: Uri): Long? {
        val projection = arrayOf(OpenableColumns.SIZE)
        val cursor: Cursor? = contentResolver.query(uri, projection, null, null, null)
        cursor.use {
            if (it == null || !it.moveToFirst()) {
                return null
            }
            val index = it.getColumnIndex(OpenableColumns.SIZE)
            if (index < 0 || it.isNull(index)) {
                return null
            }
            return it.getLong(index)
        }
    }

    private fun normalizeUserId(): String? {
        val normalized = userId?.trim().orEmpty()
        return if (normalized.isEmpty()) null else normalized
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
