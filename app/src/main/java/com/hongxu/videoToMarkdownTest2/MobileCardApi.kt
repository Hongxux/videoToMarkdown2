package com.hongxu.videoToMarkdownTest2

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

data class Phase2bStructuredMarkdownResult(
    val markdown: String,
    val rawPayload: String
)

interface MobileConceptCardApi {
    suspend fun fetchCardByTerm(term: String): TokenInsightCard?
    suspend fun generatePhase2bStructuredMarkdown(
        taskId: String,
        anchorId: String,
        pathHint: String?,
        markdownBody: String
    ): Phase2bStructuredMarkdownResult
}

class HttpMobileConceptCardApi(
    private val apiBaseUrl: String
) : MobileConceptCardApi {

    override suspend fun fetchCardByTerm(term: String): TokenInsightCard? {
        return withContext(Dispatchers.IO) {
            val normalizedTerm = term.trim()
            if (normalizedTerm.isEmpty()) {
                return@withContext null
            }
            val encodedTerm = encodePathSegment(normalizedTerm)
            val encodedQuery = URLEncoder.encode(normalizedTerm, StandardCharsets.UTF_8)
            val endpoints = listOf(
                URL("$apiBaseUrl/cards?term=$encodedQuery"),
                URL("$apiBaseUrl/cards/$encodedTerm")
            )
            var lastFailure: Throwable? = null
            for ((index, url) in endpoints.withIndex()) {
                val connection = (url.openConnection() as HttpURLConnection).apply {
                    requestMethod = "GET"
                    connectTimeout = 10_000
                    readTimeout = 15_000
                    setRequestProperty("Accept", "application/json")
                }
                val result = runCatching {
                    connection.useCardPayload(normalizedTerm)
                }
                if (result.isSuccess) {
                    val card = result.getOrNull()
                    if (card != null || index == endpoints.lastIndex) {
                        return@withContext card
                    }
                    continue
                }
                lastFailure = result.exceptionOrNull()
                if (index == endpoints.lastIndex) {
                    throw (lastFailure ?: IllegalStateException("Card loading failed"))
                }
            }
            throw (lastFailure ?: IllegalStateException("Card loading failed"))
        }
    }

    override suspend fun generatePhase2bStructuredMarkdown(
        taskId: String,
        anchorId: String,
        pathHint: String?,
        markdownBody: String
    ): Phase2bStructuredMarkdownResult {
        return withContext(Dispatchers.IO) {
            val url = URL("$apiBaseUrl/cards/phase2b/structured-markdown")
            val connection = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                connectTimeout = 10_000
                readTimeout = 30_000
                doOutput = true
                setRequestProperty("Accept", "application/json")
                setRequestProperty("Content-Type", "application/json; charset=UTF-8")
            }
            val body = JSONObject().apply {
                put("bodyText", markdownBody)
                put("sourceText", markdownBody)
                put("taskId", taskId)
                put("anchorId", anchorId)
                put("pathHint", pathHint ?: "")
                put("source", "android_anchor_phase2b_sheet")
                put("progressChannel", "")
                put("requestId", "")
                put("linkUrls", org.json.JSONArray())
            }.toString()
            OutputStreamWriter(connection.outputStream, StandardCharsets.UTF_8).use { writer ->
                writer.write(body)
            }
            try {
                val code = connection.responseCode
                val stream = if (code in 200..299) connection.inputStream else connection.errorStream
                val raw = stream?.use {
                    BufferedReader(InputStreamReader(it, StandardCharsets.UTF_8)).readText()
                }.orEmpty()
                if (code !in 200..299) {
                    val message = runCatching {
                        JSONObject(raw).optString("message").ifBlank { "HTTP $code" }
                    }.getOrDefault("HTTP $code")
                    throw IllegalStateException(message)
                }
                val root = if (raw.isBlank()) JSONObject() else JSONObject(raw)
                val candidates = listOf(
                    root.optString("markdown"),
                    root.optString("structuredMarkdown"),
                    root.optString("result"),
                    root.optString("output"),
                    root.optString("content"),
                    root.optString("text"),
                    root.optJSONObject("data")?.optString("markdown").orEmpty(),
                    root.optJSONObject("payload")?.optString("markdown").orEmpty()
                )
                val markdown = candidates.firstOrNull { it.isNotBlank() }.orEmpty()
                if (markdown.isBlank()) {
                    throw IllegalStateException("Phase2B returned empty markdown")
                }
                Phase2bStructuredMarkdownResult(markdown = markdown, rawPayload = raw)
            } finally {
                connection.disconnect()
            }
        }
    }

    private fun HttpURLConnection.useCardPayload(requestedTerm: String): TokenInsightCard? {
        try {
            val code = responseCode
            val stream = if (code in 200..299) inputStream else errorStream
            val text = stream?.use {
                BufferedReader(InputStreamReader(it, StandardCharsets.UTF_8)).readText()
            }.orEmpty()
            if (code == 404) {
                return null
            }
            if (code !in 200..299) {
                val message = runCatching {
                    JSONObject(text).optString("message").ifBlank { "HTTP $code" }
                }.getOrDefault("HTTP $code")
                throw IllegalStateException(message)
            }

            val json = if (text.isBlank()) JSONObject() else JSONObject(text)
            val title = json.optString("title").trim().ifBlank { requestedTerm }
            val markdown = json.optString("markdown")
            if (markdown.isBlank()) {
                return null
            }
            return TokenInsightCard(
                token = requestedTerm,
                title = title,
                markdown = markdown
            )
        } finally {
            disconnect()
        }
    }
}

private fun encodePathSegment(value: String): String {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20")
}
