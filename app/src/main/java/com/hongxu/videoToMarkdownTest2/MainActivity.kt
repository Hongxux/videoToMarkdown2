package com.hongxu.videoToMarkdownTest2

import android.content.Context
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import com.hongxu.videoToMarkdownTest2.ui.theme.VideoToMarkdownTest2Theme
import io.noties.markwon.AbstractMarkwonPlugin
import io.noties.markwon.Markwon
import io.noties.markwon.SoftBreakAddsNewLinePlugin
import io.noties.markwon.core.MarkwonTheme
import io.noties.markwon.ext.latex.JLatexMathPlugin
import io.noties.markwon.ext.strikethrough.StrikethroughPlugin
import io.noties.markwon.ext.tables.TablePlugin
import io.noties.markwon.html.HtmlPlugin
import io.noties.markwon.image.coil.CoilImagesPlugin
import io.noties.markwon.inlineparser.MarkwonInlineParserPlugin
import io.noties.markwon.linkify.LinkifyPlugin
import kotlinx.coroutines.launch

private data class TaskReaderSession(
    val taskId: String,
    val title: String,
    val pathHint: String?,
    val nodes: List<SemanticNode>
)

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent {
            VideoToMarkdownTest2Theme {
                MobileTaskApp()
            }
        }
    }
}

@Composable
private fun MobileTaskApp() {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    val apiBaseUrl = BuildConfig.MOBILE_API_BASE_URL

    val taskApi = remember(apiBaseUrl) { HttpMobileTaskApi(apiBaseUrl) }
    val metaApi = remember(apiBaseUrl) { HttpMobileMarkdownMetaApi(apiBaseUrl) }
    val telemetryApi = remember(apiBaseUrl) { HttpMobileMarkdownTelemetryApi(apiBaseUrl) }
    val markwon = remember(context) { buildReaderMarkwon(context) }

    var tasks by remember { mutableStateOf<List<MobileTaskListItem>>(emptyList()) }
    var preparedSessions by remember { mutableStateOf<Map<String, TaskReaderSession>>(emptyMap()) }
    var listLoading by remember { mutableStateOf(false) }
    var actionLoading by remember { mutableStateOf(false) }
    var actionMessage by remember { mutableStateOf("") }
    var videoUrlInput by remember { mutableStateOf("") }
    var readerSession by remember { mutableStateOf<TaskReaderSession?>(null) }

    suspend fun refreshTasks() {
        listLoading = true
        actionMessage = "Refreshing tasks..."
        runCatching {
            taskApi.listTasks(onlyMultiSegment = true)
        }.onSuccess { loaded ->
            val ids = loaded.map { it.taskId }.toSet()
            preparedSessions = preparedSessions.filterKeys { it in ids }
            tasks = loaded
            actionMessage = if (loaded.isEmpty()) {
                "No multi-segment text tasks found."
            } else {
                "Loaded ${loaded.size} multi-segment tasks."
            }
        }.onFailure { error ->
            actionMessage = "Failed to load tasks: ${error.message ?: "unknown"}"
        }
        listLoading = false
    }

    val pickVideoLauncher = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.GetContent()
    ) { uri ->
        if (uri == null) {
            return@rememberLauncherForActivityResult
        }
        scope.launch {
            actionLoading = true
            actionMessage = "Uploading file..."
            runCatching {
                taskApi.uploadVideoFile(context.contentResolver, uri)
            }.onSuccess { result ->
                actionMessage = result.message.ifBlank {
                    if (result.success) "Upload submitted." else "Upload failed."
                }
                refreshTasks()
            }.onFailure { error ->
                actionMessage = "Upload failed: ${error.message ?: "unknown"}"
            }
            actionLoading = false
        }
    }

    LaunchedEffect(Unit) {
        refreshTasks()
    }

    if (readerSession != null) {
        val session = readerSession ?: return
        Surface(modifier = Modifier.fillMaxSize()) {
            Column(modifier = Modifier.fillMaxSize()) {
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(horizontal = 8.dp, vertical = 8.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    TextButton(onClick = { readerSession = null }) {
                        Text("Back")
                    }
                    Text(
                        text = session.title.ifBlank { session.taskId },
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                        fontWeight = FontWeight.SemiBold,
                        modifier = Modifier
                            .padding(start = 8.dp)
                            .weight(1f)
                    )
                }
                SemanticTopographyReader(
                    nodes = session.nodes,
                    markwon = markwon,
                    modifier = Modifier.fillMaxSize(),
                    taskId = session.taskId,
                    pathHint = session.pathHint,
                    metaApi = metaApi,
                    telemetryApi = telemetryApi
                )
            }
        }
        return
    }

    Surface(modifier = Modifier.fillMaxSize()) {
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(horizontal = 16.dp, vertical = 12.dp),
            verticalArrangement = Arrangement.spacedBy(10.dp)
        ) {
            Text(
                text = "Task Library",
                fontWeight = FontWeight.SemiBold
            )

            OutlinedTextField(
                value = videoUrlInput,
                onValueChange = { videoUrlInput = it },
                modifier = Modifier.fillMaxWidth(),
                singleLine = true,
                label = { Text("Video URL") },
                placeholder = { Text("Paste URL and submit") },
                enabled = !actionLoading
            )

            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(10.dp)
            ) {
                Button(
                    onClick = {
                        scope.launch {
                            val normalized = videoUrlInput.trim()
                            if (normalized.isEmpty()) {
                                actionMessage = "Please input a video URL."
                                return@launch
                            }
                            actionLoading = true
                            actionMessage = "Submitting URL..."
                            runCatching {
                                taskApi.submitVideoUrl(normalized)
                            }.onSuccess { result ->
                                actionMessage = result.message.ifBlank {
                                    if (result.success) "Task submitted." else "Submit failed."
                                }
                                if (result.success) {
                                    videoUrlInput = ""
                                }
                                refreshTasks()
                            }.onFailure { error ->
                                actionMessage = "Submit failed: ${error.message ?: "unknown"}"
                            }
                            actionLoading = false
                        }
                    },
                    enabled = !actionLoading
                ) {
                    Text("Submit URL")
                }

                Button(
                    onClick = { pickVideoLauncher.launch("video/*") },
                    enabled = !actionLoading
                ) {
                    Text("Upload Video")
                }

                TextButton(
                    onClick = { scope.launch { refreshTasks() } },
                    enabled = !actionLoading
                ) {
                    Text("Refresh")
                }
            }

            if (actionLoading || listLoading) {
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    modifier = Modifier.fillMaxWidth()
                ) {
                    CircularProgressIndicator(modifier = Modifier.size(18.dp), strokeWidth = 2.dp)
                    Text(if (listLoading) "Loading tasks..." else "Processing...")
                }
            }

            if (actionMessage.isNotBlank()) {
                Text(
                    text = actionMessage,
                    color = Color(0xFF475467),
                    modifier = Modifier.fillMaxWidth()
                )
            }

            LazyColumn(
                modifier = Modifier.fillMaxSize(),
                verticalArrangement = Arrangement.spacedBy(10.dp)
            ) {
                if (!listLoading && tasks.isEmpty()) {
                    item {
                        Text(
                            text = "No multi-segment text tasks yet.",
                            color = Color(0xFF667085),
                            modifier = Modifier.padding(top = 8.dp)
                        )
                    }
                }

                items(tasks, key = { it.taskId }) { task ->
                    Card(
                        modifier = Modifier
                            .fillMaxWidth()
                            .clickable(enabled = !actionLoading) {
                                scope.launch {
                                    actionLoading = true
                                    actionMessage = "Opening ${compactTaskId(task.taskId)}..."
                                    val cached = preparedSessions[task.taskId]
                                    if (cached != null) {
                                        readerSession = cached
                                        actionMessage = ""
                                        actionLoading = false
                                        return@launch
                                    }

                                    runCatching {
                                        taskApi.loadTaskMarkdown(task.taskId)
                                    }.onSuccess { payload ->
                                        val session = buildReaderSessionFromPayload(task, payload)
                                        if (session == null) {
                                            actionMessage = "This task has no multi-segment text."
                                            return@onSuccess
                                        }
                                        preparedSessions = preparedSessions + (task.taskId to session)
                                        readerSession = session
                                        actionMessage = ""
                                    }.onFailure { error ->
                                        actionMessage = "Open failed: ${error.message ?: "unknown"}"
                                    }
                                    actionLoading = false
                                }
                            }
                    ) {
                        Column(
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(horizontal = 12.dp, vertical = 10.dp),
                            verticalArrangement = Arrangement.spacedBy(6.dp)
                        ) {
                            Text(
                                text = task.title.ifBlank { task.taskId },
                                fontWeight = FontWeight.SemiBold,
                                maxLines = 1,
                                overflow = TextOverflow.Ellipsis
                            )
                            Text(
                                text = compactTaskId(task.taskId),
                                color = Color(0xFF667085)
                            )
                            Row(
                                modifier = Modifier.fillMaxWidth(),
                                horizontalArrangement = Arrangement.SpaceBetween,
                                verticalAlignment = Alignment.CenterVertically
                            ) {
                                Text(
                                    text = normalizeStatus(task.status),
                                    color = statusColor(task.status)
                                )
                                Text(
                                    text = "Multi-segment ready",
                                    color = Color(0xFF667085)
                                )
                            }
                            val progressValue = normalizeProgress(task.progress)
                            if (progressValue != null) {
                                LinearProgressIndicator(
                                    progress = progressValue,
                                    modifier = Modifier
                                        .fillMaxWidth()
                                        .height(4.dp)
                                )
                            }
                            if (task.statusMessage.isNotBlank()) {
                                Text(
                                    text = task.statusMessage,
                                    color = Color(0xFF475467),
                                    maxLines = 2,
                                    overflow = TextOverflow.Ellipsis
                                )
                            }
                        }
                    }
                }
            }
        }
    }
}

private fun resolvePathHint(markdownPath: String, baseDir: String): String? {
    val path = markdownPath.trim()
    val base = baseDir.trim()
    if (path.isEmpty() || base.isEmpty()) {
        return null
    }
    val normalizedPath = path.replace('\\', '/')
    val normalizedBase = base.replace('\\', '/').trimEnd('/')
    val prefix = "$normalizedBase/"
    if (!normalizedPath.startsWith(prefix)) {
        return null
    }
    return normalizedPath.removePrefix(prefix)
}

private fun buildReaderSessionFromPayload(
    task: MobileTaskListItem,
    payload: MobileTaskMarkdownPayload
): TaskReaderSession? {
    val parsedNodes = runCatching {
        parseSemanticNodesFromPayload(payload.rawPayload)
    }.getOrDefault(emptyList())

    val nodes = if (parsedNodes.size >= 2) {
        parsedNodes
    } else {
        val markdownParts = payload.markdown
            .split(Regex("\\n\\s*\\n+"))
            .map { it.trim() }
            .filter { it.isNotBlank() }
        if (markdownParts.size < 2) {
            return null
        }
        markdownParts.mapIndexed { index, text ->
            SemanticNode(
                id = "md_${index + 1}",
                text = text,
                relevanceScore = (1f - index * 0.02f).coerceAtLeast(0.45f)
            )
        }
    }

    val sanitizedNodes = sanitizeNodesForReader(nodes)

    return TaskReaderSession(
        taskId = payload.taskId.ifBlank { task.taskId },
        title = payload.title.ifBlank { task.title },
        pathHint = resolvePathHint(payload.markdownPath, payload.baseDir),
        nodes = sanitizedNodes
    )
}

private fun sanitizeNodesForReader(nodes: List<SemanticNode>): List<SemanticNode> {
    if (nodes.isEmpty()) {
        return nodes
    }
    val seen = HashSet<String>(nodes.size * 2)
    return nodes.mapIndexed { index, node ->
        val raw = node.id.trim()
        val base = if (raw.isNotEmpty()) raw else "node_${index + 1}"
        var candidate = base
        var seq = 1
        while (!seen.add(candidate)) {
            candidate = "${base}_$seq"
            seq += 1
        }
        if (candidate == node.id) {
            node
        } else {
            node.copy(id = candidate)
        }
    }
}

private fun compactTaskId(taskId: String): String {
    val text = taskId.trim()
    if (text.length <= 30) {
        return text
    }
    return text.take(12) + "..." + text.takeLast(12)
}

private fun normalizeStatus(status: String): String {
    val upper = status.trim().uppercase()
    return when (upper) {
        "COMPLETED", "SUCCESS" -> "Completed"
        "FAILED", "ERROR" -> "Failed"
        "PROCESSING", "RUNNING" -> "Running"
        "QUEUED", "PENDING" -> "Queued"
        else -> if (upper.isBlank()) "Unknown" else upper
    }
}

private fun statusColor(status: String): Color {
    return when (status.trim().uppercase()) {
        "COMPLETED", "SUCCESS" -> Color(0xFF067647)
        "FAILED", "ERROR" -> Color(0xFFB42318)
        "PROCESSING", "RUNNING" -> Color(0xFF175CD3)
        "QUEUED", "PENDING" -> Color(0xFFB54708)
        else -> Color(0xFF667085)
    }
}

private fun normalizeProgress(progress: Double): Float? {
    if (progress <= 0.0) {
        return null
    }
    val normalized = when {
        progress <= 1.0 -> progress
        progress <= 100.0 -> progress / 100.0
        else -> 1.0
    }
    return normalized.coerceIn(0.0, 1.0).toFloat()
}

private fun buildReaderMarkwon(context: Context): Markwon {
    return Markwon.builder(context)
        .usePlugin(MarkwonInlineParserPlugin.create())
        .usePlugin(SoftBreakAddsNewLinePlugin.create())
        .usePlugin(HtmlPlugin.create())
        .usePlugin(LinkifyPlugin.create())
        .usePlugin(StrikethroughPlugin.create())
        .usePlugin(TablePlugin.create(context))
        .usePlugin(CoilImagesPlugin.create(context))
        .usePlugin(JLatexMathPlugin.create(48f))
        .usePlugin(
            object : AbstractMarkwonPlugin() {
                override fun configureTheme(builder: MarkwonTheme.Builder) {
                    builder
                        .codeTextColor(0xFFDDE7FF.toInt())
                        .codeBackgroundColor(0xFF0F172A.toInt())
                        .blockQuoteColor(0xFF93C5FD.toInt())
                        .blockQuoteWidth(10)
                        .linkColor(0xFF4F46E5.toInt())
                }
            }
        )
        .build()
}
