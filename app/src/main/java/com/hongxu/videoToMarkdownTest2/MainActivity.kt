package com.hongxu.videoToMarkdownTest2

import android.Manifest
import android.app.Application
import android.content.ClipboardManager
import android.content.Context
import android.content.ContextWrapper
import android.content.pm.PackageManager
import android.graphics.Typeface
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.provider.OpenableColumns
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInVertically
import androidx.compose.animation.slideOutVertically
import androidx.compose.animation.core.Spring
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.spring
import androidx.compose.animation.core.tween
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.gestures.detectTapGestures
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.statusBarsPadding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.FloatingActionButton
import androidx.compose.material3.Icon
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.draw.clip
import androidx.compose.ui.Alignment
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.focus.onFocusChanged
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.hapticfeedback.HapticFeedbackType
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.platform.LocalHapticFeedback
import androidx.compose.ui.platform.LocalSoftwareKeyboardController
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.TextRange
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.input.TextFieldValue
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.IntOffset
import androidx.core.view.WindowCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.WindowInsetsControllerCompat
import androidx.core.content.ContextCompat
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Add
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.compose.LocalLifecycleOwner
import androidx.lifecycle.viewmodel.compose.viewModel
import com.hongxu.videoToMarkdownTest2.ui.theme.VideoToMarkdownTest2Theme
import io.noties.markwon.AbstractMarkwonPlugin
import io.noties.markwon.Markwon
import io.noties.markwon.MarkwonConfiguration
import io.noties.markwon.MarkwonPlugin
import io.noties.markwon.MarkwonSpansFactory
import io.noties.markwon.MarkwonVisitor
import io.noties.markwon.RenderProps
import io.noties.markwon.core.CorePlugin
import io.noties.markwon.core.MarkwonTheme
import io.noties.markwon.SoftBreakAddsNewLinePlugin
import io.noties.markwon.ext.latex.JLatexMathPlugin
import io.noties.markwon.ext.strikethrough.StrikethroughPlugin
import io.noties.markwon.ext.tables.TablePlugin
import io.noties.markwon.ext.tables.TableTheme
import io.noties.markwon.html.HtmlPlugin
import io.noties.markwon.image.coil.CoilImagesPlugin

import io.noties.markwon.linkify.LinkifyPlugin
import io.noties.markwon.syntax.Prism4jThemeDarkula
import io.noties.markwon.syntax.SyntaxHighlightPlugin
import io.noties.prism4j.Prism4j
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.UUID
import kotlin.math.roundToInt

private data class TaskReaderSession(
    val taskId: String,
    val title: String,
    val pathHint: String?,
    val nodes: List<SemanticNode>
)

internal enum class TaskComposerMode {
    MENU,
    URL
}

internal enum class TaskSortField {
    LAST_OPENED,
    CREATED_AT,
    TASK_NAME
}

internal enum class SortOrder {
    DESC,
    ASC
}

internal enum class HomeSection {
    TASKS,
    COLLECTIONS
}

internal data class TaskRouteUiState(
    val taskSearchQuery: String = "",
    val taskSortField: TaskSortField = TaskSortField.LAST_OPENED,
    val taskSortOrder: SortOrder = SortOrder.DESC,
    val videoUrlInput: String = "",
    val composerExpanded: Boolean = false,
    val composerMode: TaskComposerMode = TaskComposerMode.MENU,
    val dispatchCenterExpanded: Boolean = false,
    val homeSection: HomeSection = HomeSection.TASKS
)

class TaskRouteViewModel : ViewModel() {
    private val _uiState = MutableStateFlow(TaskRouteUiState())
    internal val uiState = _uiState.asStateFlow()

    fun setTaskSearchQuery(value: String) {
        _uiState.update { state -> state.copy(taskSearchQuery = value) }
    }

    fun cycleTaskSortField() {
        _uiState.update { state ->
            state.copy(taskSortField = nextTaskSortField(state.taskSortField))
        }
    }

    fun toggleTaskSortOrder() {
        _uiState.update { state ->
            state.copy(
                taskSortOrder = if (state.taskSortOrder == SortOrder.DESC) {
                    SortOrder.ASC
                } else {
                    SortOrder.DESC
                }
            )
        }
    }

    fun setVideoUrlInput(value: String) {
        _uiState.update { state -> state.copy(videoUrlInput = value) }
    }

    fun setComposerExpanded(expanded: Boolean) {
        _uiState.update { state -> state.copy(composerExpanded = expanded) }
    }

    internal fun setComposerMode(mode: TaskComposerMode) {
        _uiState.update { state -> state.copy(composerMode = mode) }
    }

    fun setDispatchCenterExpanded(expanded: Boolean) {
        _uiState.update { state -> state.copy(dispatchCenterExpanded = expanded) }
    }

    fun toggleDispatchCenterExpanded() {
        _uiState.update { state ->
            state.copy(dispatchCenterExpanded = !state.dispatchCenterExpanded)
        }
    }

    internal fun setHomeSection(section: HomeSection) {
        _uiState.update { state -> state.copy(homeSection = section) }
    }
}

private const val BACKGROUND_TASK_REFRESH_INTERVAL_MS = 15_000L
private const val ACTIVE_TASK_REFRESH_INTERVAL_MS = 5_000L
private const val CLIPBOARD_TASK_PROMPT_PREFS = "clipboard_task_prompt"
private const val CLIPBOARD_TASK_PROMPT_KEY = "last_prompted_url"
private const val TASK_FLASH_DURATION_MS = 10_000L
private const val SUBMISSION_MODE_URL = "url"
private const val SUBMISSION_MODE_UPLOAD = "upload"
private val TASK_TIME_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
private val CLIPBOARD_URL_PATTERN = Regex("""https?://[^\s]+""", RegexOption.IGNORE_CASE)
private val CLIPBOARD_SUPPORTED_HOST_SUFFIXES = setOf(
    "douyin.com",
    "iesdouyin.com",
    "bilibili.com",
    "b23.tv",
    "bili2233.cn"
)

private data class ClipboardTaskCandidate(
    val normalizedUrl: String,
    val displayUrl: String
)

data class ActiveSubmissionHint(
    val workId: String,
    val taskId: String?,
    val title: String,
    val phaseText: String,
    val progressPercent: Int?,
    val running: Boolean,
    val failed: Boolean,
    val failedMessage: String
)

private data class CompletionBannerState(
    val taskId: String,
    val title: String
)

private class ClipboardPromptHistory(
    context: Context
) {
    private val preferences = context.getSharedPreferences(
        CLIPBOARD_TASK_PROMPT_PREFS,
        Context.MODE_PRIVATE
    )

    fun lastPromptedUrl(): String? {
        return preferences.getString(CLIPBOARD_TASK_PROMPT_KEY, null)
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
    }

    fun markPrompted(url: String) {
        val normalized = normalizeClipboardUrl(url) ?: return
        preferences.edit().putString(CLIPBOARD_TASK_PROMPT_KEY, normalized).apply()
    }
}

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
    val lifecycleOwner = LocalLifecycleOwner.current
    val scope = rememberCoroutineScope()
    val haptic = LocalHapticFeedback.current
    val apiBaseUrl = BuildConfig.MOBILE_API_BASE_URL
    val application = remember(context) { context.applicationContext as Application }
    val taskRouteViewModel: TaskRouteViewModel = viewModel(
        factory = remember {
            object : ViewModelProvider.Factory {
                override fun <T : ViewModel> create(modelClass: Class<T>): T {
                    if (modelClass.isAssignableFrom(TaskRouteViewModel::class.java)) {
                        @Suppress("UNCHECKED_CAST")
                        return TaskRouteViewModel() as T
                    }
                    throw IllegalArgumentException("Unknown ViewModel class: ${modelClass.name}")
                }
            }
        }
    )
    val collectionViewModel: CollectionFeatureViewModel = viewModel(
        factory = remember(apiBaseUrl, application) {
            CollectionFeatureViewModelFactory(
                application = application,
                apiBaseUrl = apiBaseUrl
            )
        }
    )

    val renderConfig = remember(context) { MarkdownReaderRenderConfigLoader.load(context) }
    val activeSubmissionHintMap by TaskSubmissionRegistry.activeHints.collectAsState(initial = emptyMap())
    val activeSubmissionHints = remember(activeSubmissionHintMap) {
        activeSubmissionHintMap.values.sortedByDescending { it.running }
    }
    val taskApi = remember(apiBaseUrl) { HttpMobileTaskApi(apiBaseUrl) }
    val metaApi = remember(apiBaseUrl) { HttpMobileMarkdownMetaApi(apiBaseUrl) }
    val telemetryApi = remember(apiBaseUrl) { HttpMobileMarkdownTelemetryApi(apiBaseUrl) }
    val cardApi = remember(apiBaseUrl) { HttpMobileConceptCardApi(apiBaseUrl) }
    val markwon = remember(context, renderConfig) { buildReaderMarkwon(context, renderConfig) }
    val taskCompletionNotifier = remember(context) {
        TaskCompletionNotifier(context.applicationContext)
    }
    val snackbarHostState = remember { SnackbarHostState() }
    val clipboardPromptHistory = remember(context) {
        ClipboardPromptHistory(context.applicationContext)
    }
    val refreshMutex = remember { Mutex() }
    val probeState by collectionViewModel.probeState.collectAsState()
    val selectedProbeEpisodeNos by collectionViewModel.selectedEpisodeNos.collectAsState()
    val collectionCards by collectionViewModel.collections.collectAsState()
    val detailCollection by collectionViewModel.detailCollection.collectAsState()
    val detailEpisodes by collectionViewModel.detailEpisodes.collectAsState()
    val probeSubmitInProgress by collectionViewModel.submitInProgress.collectAsState()
    val taskRouteUiState by taskRouteViewModel.uiState.collectAsState()

    var tasks by remember { mutableStateOf<List<MobileTaskListItem>>(emptyList()) }
    var preparedSessions by remember { mutableStateOf<Map<String, TaskReaderSession>>(emptyMap()) }
    var taskStatusSnapshot by remember { mutableStateOf<Map<String, String>>(emptyMap()) }
    var listLoading by remember { mutableStateOf(false) }
    var actionLoading by remember { mutableStateOf(false) }
    var actionMessage by remember { mutableStateOf("") }
    var editingTaskId by remember { mutableStateOf<String?>(null) }
    var editingTaskTitleValue by remember { mutableStateOf(TextFieldValue("")) }
    var revealedTaskId by remember { mutableStateOf<String?>(null) }
    var renameSavingTaskId by remember { mutableStateOf<String?>(null) }
    var readerSession by remember { mutableStateOf<TaskReaderSession?>(null) }
    var readerChromeVisible by remember { mutableStateOf(true) }
    var clipboardCandidate by remember { mutableStateOf<ClipboardTaskCandidate?>(null) }
    var completionBanner by remember { mutableStateOf<CompletionBannerState?>(null) }
    var flashingTaskDeadlines by remember { mutableStateOf<Map<String, Long>>(emptyMap()) }
    var uiClockMs by remember { mutableStateOf(System.currentTimeMillis()) }
    val taskSearchQuery = taskRouteUiState.taskSearchQuery
    val taskSortField = taskRouteUiState.taskSortField
    val taskSortOrder = taskRouteUiState.taskSortOrder
    val videoUrlInput = taskRouteUiState.videoUrlInput
    val composerExpanded = taskRouteUiState.composerExpanded
    val composerMode = taskRouteUiState.composerMode
    val dispatchCenterExpanded = taskRouteUiState.dispatchCenterExpanded
    val homeSection = taskRouteUiState.homeSection
    val hostActivity = remember(context) { context.findHostActivity() }
    val hasRunningWork = remember(activeSubmissionHints) {
        activeSubmissionHints.any { it.running }
    }
    val hasBackendProcessing = remember(tasks) {
        tasks.any { isProcessingStatus(it.status) }
    }
    val preferActiveRefresh = hasRunningWork || hasBackendProcessing
    val fabRotation by animateFloatAsState(
        targetValue = if (composerExpanded) 45f else 0f,
        animationSpec = spring(
            dampingRatio = Spring.DampingRatioLowBouncy,
            stiffness = Spring.StiffnessMediumLow
        ),
        label = "fab-plus-rotation"
    )

    LaunchedEffect(readerSession?.taskId) {
        readerChromeVisible = true
    }

    LaunchedEffect(hostActivity, readerSession != null, readerChromeVisible) {
        val activity = hostActivity ?: return@LaunchedEffect
        val controller = WindowCompat.getInsetsController(activity.window, activity.window.decorView)
        controller.systemBarsBehavior = WindowInsetsControllerCompat.BEHAVIOR_SHOW_TRANSIENT_BARS_BY_SWIPE
        if (readerSession != null && !readerChromeVisible) {
            controller.hide(WindowInsetsCompat.Type.statusBars())
        } else {
            controller.show(WindowInsetsCompat.Type.statusBars())
        }
    }

    DisposableEffect(hostActivity) {
        onDispose {
            val activity = hostActivity ?: return@onDispose
            val controller = WindowCompat.getInsetsController(activity.window, activity.window.decorView)
            controller.show(WindowInsetsCompat.Type.statusBars())
        }
    }

    suspend fun refreshTasks(showLoading: Boolean = true) {
        if (!refreshMutex.tryLock()) {
            return
        }
        if (showLoading) {
            listLoading = true
            actionMessage = "Refreshing tasks..."
        }
        try {
            runCatching {
                taskApi.listTasks(onlyMultiSegment = true)
            }.onSuccess { loaded ->
                val ids = loaded.map { it.taskId }.toSet()
                preparedSessions = preparedSessions.filterKeys { it in ids }
                tasks = loaded
                if (editingTaskId != null && editingTaskId !in ids) {
                    editingTaskId = null
                    editingTaskTitleValue = TextFieldValue("")
                }
                if (revealedTaskId != null && revealedTaskId !in ids) {
                    revealedTaskId = null
                }

                val previousStatus = taskStatusSnapshot
                loaded.forEach { task ->
                    if (isTaskNewlyCompleted(task.status, previousStatus[task.taskId])) {
                        taskCompletionNotifier.notifyTaskCompleted(
                            taskId = task.taskId,
                            taskTitle = task.title.ifBlank { task.taskId }
                        )
                        completionBanner = CompletionBannerState(
                            taskId = task.taskId,
                            title = task.title.ifBlank { task.taskId }
                        )
                        flashingTaskDeadlines = flashingTaskDeadlines + (
                            task.taskId to (System.currentTimeMillis() + TASK_FLASH_DURATION_MS)
                            )
                    }
                }
                taskStatusSnapshot = loaded.associate { it.taskId to it.status }

                if (showLoading) {
                    actionMessage = ""
                }
            }.onFailure { error ->
                if (showLoading) {
                    actionMessage = "Failed to load tasks: ${error.message ?: "unknown"}"
                }
            }
        } finally {
            if (showLoading) {
                listLoading = false
            }
            refreshMutex.unlock()
        }
    }

    fun updateLastOpenedAt(taskId: String, lastOpenedAt: String) {
        val normalizedTaskId = taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            return
        }
        val normalizedTimestamp = lastOpenedAt.trim().ifEmpty { Instant.now().toString() }
        tasks = tasks.map { task ->
            if (task.taskId == normalizedTaskId) {
                task.copy(lastOpenedAt = normalizedTimestamp)
            } else {
                task
            }
        }
    }

    fun recordTaskOpened(taskId: String) {
        val normalizedTaskId = taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            return
        }
        val fallbackOpenedAt = Instant.now().toString()
        updateLastOpenedAt(normalizedTaskId, fallbackOpenedAt)
        scope.launch {
            runCatching {
                taskApi.markTaskOpened(normalizedTaskId)
            }.onSuccess { serverTimestamp ->
                val normalizedServerTimestamp = serverTimestamp.trim().ifEmpty { fallbackOpenedAt }
                updateLastOpenedAt(normalizedTaskId, normalizedServerTimestamp)
            }
        }
    }

    suspend fun openTask(task: MobileTaskListItem) {
        actionLoading = true
        actionMessage = "Opening ${compactTaskId(task.taskId)}..."
        val cached = preparedSessions[task.taskId]
        if (cached != null) {
            readerSession = cached
            taskRouteViewModel.setComposerExpanded(false)
            taskRouteViewModel.setComposerMode(TaskComposerMode.MENU)
            recordTaskOpened(task.taskId)
            actionMessage = ""
            actionLoading = false
            return
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
            taskRouteViewModel.setComposerExpanded(false)
            taskRouteViewModel.setComposerMode(TaskComposerMode.MENU)
            recordTaskOpened(task.taskId)
            actionMessage = ""
        }.onFailure { error ->
            actionMessage = "Open failed: ${error.message ?: "unknown"}"
        }
        actionLoading = false
    }

    suspend fun openTaskById(taskId: String, fallbackTitle: String) {
        val normalizedTaskId = taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            return
        }
        val existing = tasks.firstOrNull { task -> task.taskId == normalizedTaskId }
        if (existing != null) {
            openTask(existing)
            return
        }
        val fallback = MobileTaskListItem(
            taskId = normalizedTaskId,
            title = fallbackTitle.ifBlank { normalizedTaskId },
            status = "COMPLETED",
            progress = 1.0,
            statusMessage = "",
            markdownAvailable = true,
            createdAt = Instant.now().toString(),
            lastOpenedAt = ""
        )
        openTask(fallback)
    }

    fun syncTaskTitleLocally(taskId: String, title: String) {
        val normalizedTaskId = taskId.trim()
        val normalizedTitle = title.trim()
        if (normalizedTaskId.isEmpty() || normalizedTitle.isEmpty()) {
            return
        }
        tasks = tasks.map { task ->
            if (task.taskId == normalizedTaskId) {
                task.copy(title = normalizedTitle)
            } else {
                task
            }
        }
        val cached = preparedSessions[normalizedTaskId]
        if (cached != null) {
            preparedSessions = preparedSessions + (normalizedTaskId to cached.copy(title = normalizedTitle))
        }
        val session = readerSession
        if (session != null && session.taskId == normalizedTaskId) {
            readerSession = session.copy(title = normalizedTitle)
        }
    }

    fun startInlineRename(task: MobileTaskListItem) {
        val displayName = resolveTaskDisplayName(task)
        editingTaskId = task.taskId
        editingTaskTitleValue = TextFieldValue(
            text = displayName,
            selection = TextRange(0, displayName.length)
        )
        revealedTaskId = null
    }

    suspend fun commitInlineRename(trigger: String) {
        val targetTaskId = editingTaskId?.trim().orEmpty()
        if (targetTaskId.isEmpty()) {
            return
        }
        val targetTask = tasks.firstOrNull { it.taskId == targetTaskId } ?: run {
            editingTaskId = null
            editingTaskTitleValue = TextFieldValue("")
            return
        }
        val draftTitle = editingTaskTitleValue.text.trim()
        editingTaskId = null
        editingTaskTitleValue = TextFieldValue("")
        revealedTaskId = null
        if (draftTitle.isEmpty()) {
            actionMessage = "Title cannot be empty."
            return
        }
        val currentTitle = resolveTaskDisplayName(targetTask).trim()
        if (draftTitle == currentTitle) {
            if (trigger != "focus_lost") {
                actionMessage = ""
            }
            return
        }

        renameSavingTaskId = targetTaskId
        actionMessage = "Saving title..."
        runCatching {
            taskApi.renameTaskTitle(taskId = targetTaskId, title = draftTitle)
        }.onSuccess { serverTitle ->
            val finalTitle = serverTitle.trim().ifEmpty { draftTitle }
            syncTaskTitleLocally(targetTaskId, finalTitle)
            actionMessage = ""
        }.onFailure { error ->
            actionMessage = "Rename failed: ${error.message ?: "unknown"}"
        }
        renameSavingTaskId = null
    }

    val filteredAndSortedTasks = remember(tasks, taskSearchQuery, taskSortField, taskSortOrder) {
        val query = taskSearchQuery.trim().lowercase(Locale.ROOT)
        val filtered = if (query.isEmpty()) {
            tasks
        } else {
            tasks.filter { task ->
                resolveTaskDisplayName(task).lowercase(Locale.ROOT).contains(query) ||
                    task.taskId.lowercase(Locale.ROOT).contains(query)
            }
        }
        sortTasks(filtered, taskSortField, taskSortOrder)
    }

    fun inspectClipboardForTaskCandidate() {
        if (readerSession != null || clipboardCandidate != null) {
            return
        }
        val candidate = detectClipboardTaskCandidate(
            context = context,
            lastPromptedUrl = clipboardPromptHistory.lastPromptedUrl()
        )
        if (candidate != null) {
            clipboardCandidate = candidate
            taskRouteViewModel.setComposerExpanded(true)
            taskRouteViewModel.setComposerMode(TaskComposerMode.URL)
        }
    }

    fun enqueueSubmissionWork(
        mode: String,
        preferredTitle: String,
        videoUrl: String? = null,
        uploadUri: Uri? = null
    ) {
        val submissionId = UUID.randomUUID().toString()
        when (mode) {
            SUBMISSION_MODE_UPLOAD -> {
                val uri = uploadUri ?: return
                TaskSubmissionForegroundService.startUploadSubmission(
                    context = context.applicationContext,
                    submissionId = submissionId,
                    title = preferredTitle,
                    uploadUri = uri
                )
            }

            else -> {
                val normalizedUrl = videoUrl.orEmpty().trim()
                if (normalizedUrl.isBlank()) {
                    return
                }
                TaskSubmissionForegroundService.startUrlSubmission(
                    context = context.applicationContext,
                    submissionId = submissionId,
                    title = preferredTitle,
                    videoUrl = normalizedUrl
                )
            }
        }
    }

    fun cancelTaskFromUi(taskId: String) {
        val normalizedTaskId = taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            return
        }
        scope.launch {
            actionLoading = true
            actionMessage = "Cancelling ${compactTaskId(normalizedTaskId)}..."
            runCatching {
                taskApi.cancelTask(normalizedTaskId)
            }.onSuccess { result ->
                actionMessage = result.message.ifBlank {
                    if (result.success) "Task cancelled." else "Task cannot be cancelled."
                }
                refreshTasks(showLoading = false)
            }.onFailure { error ->
                actionMessage = "Cancel failed: ${error.message ?: "unknown"}"
            }
            actionLoading = false
        }
    }

    val notificationPermissionLauncher = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.RequestPermission()
    ) { }

    val pickVideoLauncher = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.OpenDocument()
    ) { uri ->
        if (uri == null) {
            return@rememberLauncherForActivityResult
        }
        runCatching {
            context.contentResolver.takePersistableUriPermission(
                uri,
                android.content.Intent.FLAG_GRANT_READ_URI_PERMISSION
            )
        }
        val preferredTitle = resolveSubmissionTitleFromUri(
            contentResolver = context.contentResolver,
            uri = uri
        )
        enqueueSubmissionWork(
            mode = SUBMISSION_MODE_UPLOAD,
            preferredTitle = preferredTitle,
            uploadUri = uri
        )
        actionMessage = "任务已开始，已转入后台队列。你可以继续浏览其他页面。"
        taskRouteViewModel.setComposerExpanded(false)
        taskRouteViewModel.setComposerMode(TaskComposerMode.MENU)
        scope.launch {
            refreshTasks(showLoading = false)
        }
    }

    LaunchedEffect(Unit) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            val granted = ContextCompat.checkSelfPermission(
                context,
                Manifest.permission.POST_NOTIFICATIONS
            ) == PackageManager.PERMISSION_GRANTED
            if (!granted) {
                notificationPermissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
            }
        }
        refreshTasks(showLoading = true)
        inspectClipboardForTaskCandidate()
    }

    LaunchedEffect(preferActiveRefresh) {
        while (true) {
            delay(
                if (preferActiveRefresh) {
                    ACTIVE_TASK_REFRESH_INTERVAL_MS
                } else {
                    BACKGROUND_TASK_REFRESH_INTERVAL_MS
                }
            )
            refreshTasks(showLoading = false)
        }
    }

    LaunchedEffect(flashingTaskDeadlines, hasRunningWork, hasBackendProcessing) {
        while (flashingTaskDeadlines.isNotEmpty() || hasRunningWork || hasBackendProcessing) {
            uiClockMs = System.currentTimeMillis()
            val now = uiClockMs
            flashingTaskDeadlines = flashingTaskDeadlines.filterValues { deadline -> deadline > now }
            delay(450)
        }
    }

    LaunchedEffect(Unit) {
        TaskSubmissionRegistry.events.collect { event ->
            when (event.type) {
                SubmissionEventType.STARTED -> {
                    actionMessage = "任务已提交，后台开始处理。"
                }

                SubmissionEventType.SUCCEEDED -> {
                    actionMessage = "任务已完成，点击卡片可立即查看。"
                }

                SubmissionEventType.FAILED -> {
                    actionMessage = "后台任务失败: ${event.message}"
                }

                SubmissionEventType.CANCELLED -> {
                    actionMessage = if (event.taskId.isNullOrBlank()) {
                        "后台任务已取消。"
                    } else {
                        "任务 ${compactTaskId(event.taskId)} 已取消。"
                    }
                }
            }
            refreshTasks(showLoading = false)
        }
    }

    fun triggerVideoProbe() {
        val normalized = videoUrlInput.trim()
        if (normalized.isEmpty()) {
            actionMessage = "请先输入链接"
            return
        }
        collectionViewModel.probeVideoInput(normalized)
    }

    LaunchedEffect(Unit) {
        collectionViewModel.events.collect { event ->
            when (event) {
                is CollectionUiEvent.Snackbar -> {
                    snackbarHostState.showSnackbar(event.message)
                }

                is CollectionUiEvent.SingleTaskSubmitted -> {
                    actionMessage = event.message
                    scope.launch { refreshTasks(showLoading = false) }
                    snackbarHostState.showSnackbar("视频任务已提交：${compactTaskId(event.taskId)}")
                }

                is CollectionUiEvent.BatchSubmitted -> {
                    actionMessage = event.message
                    scope.launch { refreshTasks(showLoading = false) }
                    collectionViewModel.refreshCollections()
                    snackbarHostState.showSnackbar(
                        "合集《${event.collectionTitle}》已提交，${event.submittedCount} 个视频正在排队处理。"
                    )
                    taskRouteViewModel.setHomeSection(HomeSection.TASKS)
                }
            }
        }
    }

    LaunchedEffect(probeState) {
        if (probeState is ProbeUiState.Success) {
            taskRouteViewModel.setComposerExpanded(false)
            taskRouteViewModel.setComposerMode(TaskComposerMode.MENU)
        }
    }

    LaunchedEffect(completionBanner?.taskId) {
        val banner = completionBanner ?: return@LaunchedEffect
        delay(8_000)
        if (completionBanner?.taskId == banner.taskId) {
            completionBanner = null
        }
    }

    DisposableEffect(lifecycleOwner, readerSession) {
        val observer = LifecycleEventObserver { _, event ->
            if (event == Lifecycle.Event.ON_RESUME) {
                inspectClipboardForTaskCandidate()
                scope.launch {
                    refreshTasks(showLoading = false)
                }
            }
        }
        lifecycleOwner.lifecycle.addObserver(observer)
        onDispose {
            lifecycleOwner.lifecycle.removeObserver(observer)
        }
    }

    if (readerSession != null) {
        val session = readerSession ?: return
        Surface(modifier = Modifier.fillMaxSize()) {
            Box(modifier = Modifier.fillMaxSize()) {
                Column(modifier = Modifier.fillMaxSize()) {
                    AnimatedVisibility(
                        visible = readerChromeVisible,
                        enter = fadeIn(animationSpec = tween(durationMillis = 200)) +
                            slideInVertically(
                                animationSpec = tween(durationMillis = 220),
                                initialOffsetY = { -it / 2 }
                            ),
                        exit = fadeOut(animationSpec = tween(durationMillis = 160)) +
                            slideOutVertically(
                                animationSpec = tween(durationMillis = 180),
                                targetOffsetY = { -it }
                            )
                    ) {
                        Row(
                            modifier = Modifier
                                .fillMaxWidth()
                                .statusBarsPadding()
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
                    }

                    SemanticTopographyReader(
                        nodes = session.nodes,
                        markwon = markwon,
                        renderConfig = renderConfig,
                        modifier = Modifier.fillMaxSize(),
                        taskId = session.taskId,
                        pathHint = session.pathHint,
                        metaApi = metaApi,
                        telemetryApi = telemetryApi,
                        cardApi = cardApi,
                        onScrollDown = {
                            if (readerChromeVisible) {
                                readerChromeVisible = false
                            }
                        },
                        onScrollUp = {
                            if (!readerChromeVisible) {
                                readerChromeVisible = true
                            }
                        }
                    )
                }

                if (!readerChromeVisible) {
                    Box(
                        modifier = Modifier
                            .fillMaxWidth()
                            .height(40.dp)
                            .align(Alignment.TopCenter)
                            .pointerInput(session.taskId) {
                                detectTapGestures {
                                    readerChromeVisible = true
                                }
                            }
                    )
                }

                val readerActiveTaskCount = tasks.count { isProcessingStatus(it.status) } + activeSubmissionHints.size
                AnimatedVisibility(
                    visible = readerActiveTaskCount > 0,
                    modifier = Modifier
                        .align(Alignment.BottomEnd)
                        .padding(end = 16.dp, bottom = 26.dp),
                    enter = fadeIn(animationSpec = tween(durationMillis = 150)),
                    exit = fadeOut(animationSpec = tween(durationMillis = 120))
                ) {
                    Card(
                        modifier = Modifier.clickable {
                            readerSession = null
                            taskRouteViewModel.setDispatchCenterExpanded(true)
                        },
                        colors = androidx.compose.material3.CardDefaults.cardColors(
                            containerColor = Color(0xFF175CD3)
                        )
                    ) {
                        Row(
                            modifier = Modifier.padding(horizontal = 10.dp, vertical = 8.dp),
                            verticalAlignment = Alignment.CenterVertically,
                            horizontalArrangement = Arrangement.spacedBy(6.dp)
                        ) {
                            CircularProgressIndicator(
                                modifier = Modifier.size(10.dp),
                                strokeWidth = 2.dp,
                                color = Color.White
                            )
                            Text(
                                text = "鍚庡彴浠诲姟 $readerActiveTaskCount",
                                color = Color.White
                            )
                        }
                    }
                }
            }
        }
        return
    }

    if (homeSection == HomeSection.COLLECTIONS) {
        CollectionsRoute(
            collections = collectionCards,
            detailCollection = detailCollection,
            detailEpisodes = detailEpisodes,
            onBackToTasks = {
                collectionViewModel.closeCollectionDetail()
                taskRouteViewModel.setHomeSection(HomeSection.TASKS)
            },
            onRefresh = { collectionViewModel.refreshCollections() },
            onOpenDetail = { collectionId ->
                collectionViewModel.openCollectionDetail(collectionId)
            },
            onCloseDetail = { collectionViewModel.closeCollectionDetail() },
            onOpenTask = { taskId, title ->
                scope.launch { openTaskById(taskId, title) }
            },
            onRetryEpisode = { collectionId, episode ->
                collectionViewModel.retryEpisode(collectionId, episode)
            }
        )
        return
    }

    TaskRoute {
        Surface(modifier = Modifier.fillMaxSize()) {
            Box(modifier = Modifier.fillMaxSize()) {
                LazyColumn(
                modifier = Modifier
                    .fillMaxSize()
                    .statusBarsPadding()
                    .padding(horizontal = 16.dp, vertical = 12.dp),
                verticalArrangement = Arrangement.spacedBy(10.dp)
            ) {
                item {
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.SpaceBetween
                    ) {
                        Text(
                            text = "Task Library",
                            fontWeight = FontWeight.SemiBold
                        )
                        TextButton(
                            onClick = {
                                taskRouteViewModel.setHomeSection(HomeSection.COLLECTIONS)
                                collectionViewModel.refreshCollections()
                            },
                            enabled = !actionLoading
                        ) {
                            Text("查看合集")
                        }
                    }
                }

                item {
                    OutlinedTextField(
                        value = taskSearchQuery,
                        onValueChange = { taskRouteViewModel.setTaskSearchQuery(it) },
                        modifier = Modifier.fillMaxWidth(),
                        singleLine = true,
                        label = { Text("Search task") },
                        placeholder = { Text("Search by title or task ID") },
                        enabled = !actionLoading
                    )
                }

                item {
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.spacedBy(8.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        TextButton(
                            onClick = {
                                taskRouteViewModel.cycleTaskSortField()
                            },
                            enabled = !actionLoading
                        ) {
                            Text("Sort: ${taskSortFieldLabel(taskSortField)}")
                        }
                        TextButton(
                            onClick = {
                                taskRouteViewModel.toggleTaskSortOrder()
                            },
                            enabled = !actionLoading
                        ) {
                            Text("Order: ${if (taskSortOrder == SortOrder.DESC) "Desc" else "Asc"}")
                        }
                    }
                }

                item {
                    Text(
                        text = "Showing ${filteredAndSortedTasks.size} / ${tasks.size}",
                        color = Color(0xFF667085)
                    )
                }

                if (activeSubmissionHints.isNotEmpty()) {
                    items(activeSubmissionHints, key = { "submission-${it.workId}" }) { hint ->
                        BackgroundSubmissionSkeletonCard(
                            hint = hint,
                            onCancel = {
                                if (hint.taskId.isNullOrBlank()) {
                                    TaskSubmissionForegroundService.cancelSubmission(
                                        context.applicationContext,
                                        hint.workId
                                    )
                                } else {
                                    TaskSubmissionForegroundService.cancelSubmission(
                                        context.applicationContext,
                                        hint.workId
                                    )
                                }
                            },
                            enabled = !actionLoading
                        )
                    }
                }

                if (!listLoading && filteredAndSortedTasks.isEmpty() && activeSubmissionHints.isEmpty()) {
                    item {
                        Text(
                            text = if (taskSearchQuery.isBlank()) {
                                "No multi-segment text tasks yet."
                            } else {
                                "No task matched your search."
                            },
                            color = Color(0xFF667085),
                            modifier = Modifier.padding(top = 8.dp)
                        )
                    }
                }

                items(filteredAndSortedTasks, key = { it.taskId }) { task ->
                    val flashDeadline = flashingTaskDeadlines[task.taskId] ?: 0L
                    val shouldFlash = flashDeadline > uiClockMs
                    val flashTransition = androidx.compose.animation.core.rememberInfiniteTransition(
                        label = "task-flash-${task.taskId}"
                    )
                    val flashPulse by flashTransition.animateFloat(
                        initialValue = 0f,
                        targetValue = 1f,
                        animationSpec = androidx.compose.animation.core.infiniteRepeatable(
                            animation = tween(durationMillis = 600),
                            repeatMode = androidx.compose.animation.core.RepeatMode.Reverse
                        ),
                        label = "task-flash-pulse-${task.taskId}"
                    )
                    val isEditingCurrent = editingTaskId == task.taskId
                    SwipeRenameTaskListItem(
                        task = task,
                        shouldFlash = shouldFlash,
                        flashPulse = flashPulse,
                        isEditing = isEditingCurrent,
                        editValue = editingTaskTitleValue,
                        isRenameSaving = renameSavingTaskId == task.taskId,
                        isMenuRevealed = revealedTaskId == task.taskId,
                        canInteract = !actionLoading && (editingTaskId == null || isEditingCurrent),
                        dimOthers = editingTaskId != null && !isEditingCurrent,
                        onEditValueChange = { editingTaskTitleValue = it },
                        onOpen = {
                            if (revealedTaskId == task.taskId) {
                                revealedTaskId = null
                                return@SwipeRenameTaskListItem
                            }
                            if (editingTaskId != null && editingTaskId != task.taskId) {
                                scope.launch {
                                    commitInlineRename("switch_task")
                                }
                                return@SwipeRenameTaskListItem
                            }
                            scope.launch {
                                openTask(task)
                            }
                        },
                        onRevealMenu = { shouldReveal ->
                            revealedTaskId = if (shouldReveal) task.taskId else null
                        },
                        onStartRename = {
                            startInlineRename(task)
                        },
                        onCommitRename = { trigger ->
                            scope.launch {
                                commitInlineRename(trigger)
                            }
                        },
                        onCancelTask = {
                            cancelTaskFromUi(task.taskId)
                        }
                    )
                }
            }

            AnimatedVisibility(
                visible = composerExpanded,
                modifier = Modifier
                    .align(Alignment.BottomCenter)
                    .padding(horizontal = 16.dp, vertical = 96.dp),
                enter = fadeIn(animationSpec = tween(durationMillis = 180)) +
                    slideInVertically(
                        animationSpec = tween(durationMillis = 220),
                        initialOffsetY = { it / 2 }
                    ),
                exit = fadeOut(animationSpec = tween(durationMillis = 140)) +
                    slideOutVertically(
                        animationSpec = tween(durationMillis = 180),
                        targetOffsetY = { it / 2 }
                    )
            ) {
                Card(
                    modifier = Modifier.fillMaxWidth()
                ) {
                    Column(
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(horizontal = 14.dp, vertical = 12.dp),
                        verticalArrangement = Arrangement.spacedBy(10.dp)
                    ) {
                        if (composerMode == TaskComposerMode.URL) {
                            val probeLoading = probeState is ProbeUiState.Loading
                            val probeError = (probeState as? ProbeUiState.Error)?.message
                            val pendingCandidate = clipboardCandidate
                            if (pendingCandidate != null) {
                                ClipboardPasteBubble(
                                    candidateUrl = pendingCandidate.displayUrl,
                                    enabled = !actionLoading && !probeLoading,
                                    onPaste = {
                                        taskRouteViewModel.setVideoUrlInput(pendingCandidate.normalizedUrl)
                                        clipboardPromptHistory.markPrompted(pendingCandidate.normalizedUrl)
                                        clipboardCandidate = null
                                    },
                                    onDismiss = {
                                        clipboardPromptHistory.markPrompted(pendingCandidate.normalizedUrl)
                                        clipboardCandidate = null
                                    }
                                )
                            }
                            OutlinedTextField(
                                value = videoUrlInput,
                                onValueChange = { taskRouteViewModel.setVideoUrlInput(it) },
                                modifier = Modifier.fillMaxWidth(),
                                singleLine = true,
                                label = { Text("Video URL") },
                                placeholder = { Text("粘贴链接后点击解析，或按回车") },
                                enabled = !actionLoading && !probeLoading,
                                keyboardOptions = KeyboardOptions(imeAction = ImeAction.Search),
                                keyboardActions = KeyboardActions(
                                    onSearch = {
                                        triggerVideoProbe()
                                    },
                                    onDone = {
                                        triggerVideoProbe()
                                    }
                                )
                            )
                            AnimatedVisibility(visible = probeLoading) {
                                ProbeDetectingSkeleton()
                            }
                            if (!probeError.isNullOrBlank()) {
                                Text(
                                    text = probeError,
                                    color = Color(0xFFB42318)
                                )
                            }
                            Row(
                                modifier = Modifier.fillMaxWidth(),
                                horizontalArrangement = Arrangement.End
                            ) {
                                TextButton(
                                    onClick = { taskRouteViewModel.setComposerMode(TaskComposerMode.MENU) },
                                    enabled = !actionLoading
                                ) {
                                    Text("Back")
                                }
                                Button(
                                    onClick = {
                                        triggerVideoProbe()
                                    },
                                    enabled = !actionLoading && !probeLoading
                                ) {
                                    if (probeLoading) {
                                        CircularProgressIndicator(
                                            modifier = Modifier.size(16.dp),
                                            strokeWidth = 2.dp,
                                            color = Color.White
                                        )
                                        Spacer(modifier = Modifier.width(8.dp))
                                    }
                                    Text("解析")
                                }
                            }
                        } else {
                            Text(
                                text = "Create Task",
                                fontWeight = FontWeight.SemiBold
                            )
                            Text(
                                text = "Paste a video URL or upload a file when needed.",
                                color = Color(0xFF667085)
                            )
                            Row(
                                modifier = Modifier.fillMaxWidth(),
                                horizontalArrangement = Arrangement.spacedBy(10.dp)
                            ) {
                                TextButton(
                                    onClick = { taskRouteViewModel.setComposerMode(TaskComposerMode.URL) },
                                    enabled = !actionLoading
                                ) {
                                    Text("Paste URL")
                                }
                                Button(
                                    onClick = {
                                        taskRouteViewModel.setComposerExpanded(false)
                                        taskRouteViewModel.setComposerMode(TaskComposerMode.MENU)
                                        pickVideoLauncher.launch(arrayOf("video/*"))
                                    },
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
                        }
                    }
                }
            }

            AnimatedVisibility(
                visible = completionBanner != null,
                modifier = Modifier
                    .align(Alignment.TopCenter)
                    .statusBarsPadding()
                    .padding(horizontal = 16.dp, vertical = 8.dp),
                enter = fadeIn(animationSpec = tween(durationMillis = 180)) +
                    slideInVertically(
                        animationSpec = tween(durationMillis = 200),
                        initialOffsetY = { -it / 2 }
                    ),
                exit = fadeOut(animationSpec = tween(durationMillis = 160)) +
                    slideOutVertically(
                        animationSpec = tween(durationMillis = 160),
                        targetOffsetY = { -it }
                    )
            ) {
                val banner = completionBanner
                if (banner != null) {
                    Card(
                        modifier = Modifier
                            .fillMaxWidth()
                            .clickable(enabled = !actionLoading) {
                                completionBanner = null
                                val target = tasks.firstOrNull { it.taskId == banner.taskId }
                                if (target != null) {
                                    scope.launch { openTask(target) }
                                }
                            },
                        colors = androidx.compose.material3.CardDefaults.cardColors(
                            containerColor = Color(0xFFE8F5E9)
                        )
                    ) {
                        Text(
                            text = "🎉 您的《${banner.title}》Insight卡片已生成完毕，点击立即查看。",
                            modifier = Modifier.padding(horizontal = 12.dp, vertical = 10.dp),
                            color = Color(0xFF0A6847),
                            maxLines = 2,
                            overflow = TextOverflow.Ellipsis
                        )
                    }
                }
            }

            val activeTaskCount = tasks.count { isProcessingStatus(it.status) } + activeSubmissionHints.size
            AnimatedVisibility(
                visible = activeTaskCount > 0,
                modifier = Modifier
                    .align(Alignment.BottomEnd)
                    .padding(end = 20.dp, bottom = 92.dp),
                enter = fadeIn(animationSpec = tween(durationMillis = 150)),
                exit = fadeOut(animationSpec = tween(durationMillis = 120))
            ) {
                Card(
                    modifier = Modifier.clickable { taskRouteViewModel.toggleDispatchCenterExpanded() },
                    colors = androidx.compose.material3.CardDefaults.cardColors(
                        containerColor = Color(0xFF175CD3)
                    )
                ) {
                    Row(
                        modifier = Modifier.padding(horizontal = 12.dp, vertical = 8.dp),
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        CircularProgressIndicator(
                            modifier = Modifier.size(12.dp),
                            strokeWidth = 2.dp,
                            color = Color.White
                        )
                        Text(
                            text = "浠诲姟璋冨害涓績 $activeTaskCount",
                            color = Color.White,
                            fontWeight = FontWeight.Medium
                        )
                    }
                }
            }

            AnimatedVisibility(
                visible = dispatchCenterExpanded && activeTaskCount > 0,
                modifier = Modifier
                    .align(Alignment.BottomCenter)
                    .padding(horizontal = 16.dp, vertical = 158.dp),
                enter = fadeIn(animationSpec = tween(durationMillis = 180)) +
                    slideInVertically(
                        animationSpec = tween(durationMillis = 220),
                        initialOffsetY = { it / 2 }
                    ),
                exit = fadeOut(animationSpec = tween(durationMillis = 120)) +
                    slideOutVertically(
                        animationSpec = tween(durationMillis = 160),
                        targetOffsetY = { it / 2 }
                    )
            ) {
                Card(modifier = Modifier.fillMaxWidth()) {
                    Column(
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(horizontal = 14.dp, vertical = 12.dp),
                        verticalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        Text("浠诲姟璋冨害涓績", fontWeight = FontWeight.SemiBold)
                        activeSubmissionHints.forEach { hint ->
                            Row(
                                modifier = Modifier.fillMaxWidth(),
                                verticalAlignment = Alignment.CenterVertically,
                                horizontalArrangement = Arrangement.spacedBy(8.dp)
                            ) {
                                CircularProgressIndicator(
                                    modifier = Modifier.size(12.dp),
                                    strokeWidth = 2.dp
                                )
                                Column(modifier = Modifier.weight(1f)) {
                                    Text(
                                        text = hint.title,
                                        maxLines = 1,
                                        overflow = TextOverflow.Ellipsis,
                                        fontWeight = FontWeight.Medium
                                    )
                                    Text(
                                        text = hint.phaseText,
                                        color = Color(0xFF475467),
                                        maxLines = 1,
                                        overflow = TextOverflow.Ellipsis
                                    )
                                }
                                TextButton(
                                    enabled = !actionLoading,
                                    onClick = {
                                        if (hint.taskId.isNullOrBlank()) {
                                            TaskSubmissionForegroundService.cancelSubmission(
                                                context.applicationContext,
                                                hint.workId
                                            )
                                        } else {
                                            TaskSubmissionForegroundService.cancelSubmission(
                                                context.applicationContext,
                                                hint.workId
                                            )
                                        }
                                    }
                                ) {
                                    Text("取消")
                                }
                            }
                        }
                        tasks.filter { isProcessingStatus(it.status) }.take(6).forEach { runningTask ->
                            Row(
                                modifier = Modifier.fillMaxWidth(),
                                verticalAlignment = Alignment.CenterVertically,
                                horizontalArrangement = Arrangement.spacedBy(8.dp)
                            ) {
                                CircularProgressIndicator(
                                    modifier = Modifier.size(12.dp),
                                    strokeWidth = 2.dp
                                )
                                Column(modifier = Modifier.weight(1f)) {
                                    Text(
                                        text = resolveTaskDisplayName(runningTask),
                                        maxLines = 1,
                                        overflow = TextOverflow.Ellipsis,
                                        fontWeight = FontWeight.Medium
                                    )
                                    Text(
                                        text = resolveTaskPhaseText(
                                            status = runningTask.status,
                                            statusMessage = runningTask.statusMessage,
                                            progress = runningTask.progress
                                        ),
                                        color = Color(0xFF475467),
                                        maxLines = 1,
                                        overflow = TextOverflow.Ellipsis
                                    )
                                }
                                TextButton(
                                    enabled = !actionLoading,
                                    onClick = { cancelTaskFromUi(runningTask.taskId) }
                                ) {
                                    Text("取消")
                                }
                            }
                        }
                    }
                }
            }

            AnimatedVisibility(
                visible = actionLoading || listLoading || actionMessage.isNotBlank(),
                modifier = Modifier
                    .align(Alignment.BottomCenter)
                    .padding(horizontal = 16.dp, vertical = 28.dp),
                enter = fadeIn(animationSpec = tween(durationMillis = 150)),
                exit = fadeOut(animationSpec = tween(durationMillis = 150))
            ) {
                Card(modifier = Modifier.fillMaxWidth()) {
                    Row(
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(horizontal = 12.dp, vertical = 10.dp),
                        horizontalArrangement = Arrangement.spacedBy(8.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        if (actionLoading || listLoading) {
                            CircularProgressIndicator(
                                modifier = Modifier.size(16.dp),
                                strokeWidth = 2.dp
                            )
                        }
                        Text(
                            text = if (actionLoading || listLoading) {
                                if (listLoading) "正在刷新任务列表..." else "正在与后端同步任务状态..."
                            } else {
                                actionMessage
                            },
                            color = Color(0xFF475467),
                            maxLines = 2,
                            overflow = TextOverflow.Ellipsis
                        )
                    }
                }
            }

            FloatingActionButton(
                onClick = {
                    haptic.performHapticFeedback(HapticFeedbackType.LongPress)
                    if (composerExpanded) {
                        taskRouteViewModel.setComposerExpanded(false)
                        taskRouteViewModel.setComposerMode(TaskComposerMode.MENU)
                    } else {
                        taskRouteViewModel.setComposerExpanded(true)
                        taskRouteViewModel.setComposerMode(TaskComposerMode.MENU)
                    }
                },
                modifier = Modifier
                    .align(Alignment.BottomEnd)
                    .padding(horizontal = 20.dp, vertical = 20.dp)
            ) {
                Icon(
                    imageVector = Icons.Filled.Add,
                    contentDescription = if (composerExpanded) "Close composer" else "Open composer",
                    modifier = Modifier.graphicsLayer {
                        rotationZ = fabRotation
                    }
                )
            }

            SnackbarHost(
                hostState = snackbarHostState,
                modifier = Modifier
                    .align(Alignment.BottomCenter)
                    .padding(horizontal = 16.dp, vertical = 86.dp)
            )

            val probeSuccess = probeState as? ProbeUiState.Success
            if (probeSuccess != null) {
                ProbeResultBottomSheet(
                    probeResult = probeSuccess.result,
                    selectedEpisodeNos = selectedProbeEpisodeNos,
                    submitting = probeSubmitInProgress,
                    onDismiss = { collectionViewModel.clearProbeResult() },
                    onSubmitSingle = { collectionViewModel.submitDetectedSingleVideo() },
                    onSubmitCollection = { collectionViewModel.submitDetectedCollectionBatch() },
                    onSelectAll = { collectionViewModel.selectAllEpisodes() },
                    onInvertSelection = { collectionViewModel.invertEpisodeSelection() },
                    onToggleEpisode = { episodeNo ->
                        collectionViewModel.toggleEpisodeSelection(episodeNo)
                    }
                )
            }

            }
        }
    }
}

@Composable
private fun TaskRoute(content: @Composable () -> Unit) {
    content()
}

@Composable
private fun CollectionsRoute(
    collections: List<CollectionCardUi>,
    detailCollection: CollectionCardUi?,
    detailEpisodes: List<CollectionEpisodeUi>,
    onBackToTasks: () -> Unit,
    onRefresh: () -> Unit,
    onOpenDetail: (String) -> Unit,
    onCloseDetail: () -> Unit,
    onOpenTask: (String, String) -> Unit,
    onRetryEpisode: (String, CollectionEpisodeUi) -> Unit
) {
    CollectionHubScreen(
        collections = collections,
        detailCollection = detailCollection,
        detailEpisodes = detailEpisodes,
        onBackToTasks = onBackToTasks,
        onRefresh = onRefresh,
        onOpenDetail = onOpenDetail,
        onCloseDetail = onCloseDetail,
        onOpenTask = onOpenTask,
        onRetryEpisode = onRetryEpisode
    )
}

@Composable
private fun BackgroundSubmissionSkeletonCard(
    hint: ActiveSubmissionHint,
    onCancel: () -> Unit,
    enabled: Boolean
) {
    val shimmerTransition = androidx.compose.animation.core.rememberInfiniteTransition(
        label = "submission-skeleton-${hint.workId}"
    )
    val shimmerAlpha by shimmerTransition.animateFloat(
        initialValue = 0.30f,
        targetValue = 0.74f,
        animationSpec = androidx.compose.animation.core.infiniteRepeatable(
            animation = tween(durationMillis = 800),
            repeatMode = androidx.compose.animation.core.RepeatMode.Reverse
        ),
        label = "submission-skeleton-alpha-${hint.workId}"
    )
    Card(
        modifier = Modifier.fillMaxWidth(),
        colors = androidx.compose.material3.CardDefaults.cardColors(
            containerColor = Color(0xFFF5F7FB)
        )
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 12.dp, vertical = 10.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Box(
                modifier = Modifier
                    .width(180.dp)
                    .height(12.dp)
                    .background(
                        color = Color(0xFFCBD5E1).copy(alpha = shimmerAlpha),
                        shape = RoundedCornerShape(8.dp)
                    )
            )
            Text(
                text = hint.phaseText,
                color = Color(0xFF175CD3),
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
            Box(
                modifier = Modifier
                    .fillMaxWidth()
                    .height(8.dp)
                    .background(
                        color = Color(0xFFD1D9E6).copy(alpha = shimmerAlpha * 0.86f),
                        shape = RoundedCornerShape(8.dp)
                    )
            )
            if (hint.progressPercent != null) {
                LinearProgressIndicator(
                    progress = hint.progressPercent / 100f,
                    modifier = Modifier.fillMaxWidth()
                )
            } else {
                LinearProgressIndicator(modifier = Modifier.fillMaxWidth())
            }
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    text = hint.title,
                    color = Color(0xFF475467),
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                    modifier = Modifier.weight(1f)
                )
                TextButton(
                    onClick = onCancel,
                    enabled = enabled
                ) {
                    Text("取消")
                }
            }
        }
    }
}

private fun resolveSubmissionTitleFromUri(
    contentResolver: android.content.ContentResolver,
    uri: Uri
): String {
    val cursor = contentResolver.query(
        uri,
        arrayOf(OpenableColumns.DISPLAY_NAME),
        null,
        null,
        null
    )
    cursor.use {
        if (it == null || !it.moveToFirst()) {
            return "本地视频任务"
        }
        val index = it.getColumnIndex(OpenableColumns.DISPLAY_NAME)
        if (index < 0) {
            return "本地视频任务"
        }
        return it.getString(index)?.trim().takeUnless { it.isNullOrBlank() } ?: "本地视频任务"
    }
}

private fun deriveSubmissionTitleFromUrl(videoUrl: String): String {
    val uri = runCatching { Uri.parse(videoUrl) }.getOrNull() ?: return "链接视频任务"
    val lastSegment = uri.lastPathSegment?.trim().orEmpty()
    if (lastSegment.isNotEmpty()) {
        return lastSegment
    }
    val host = uri.host?.trim().orEmpty()
    if (host.isNotEmpty()) {
        return "$host 任务"
    }
    return "链接视频任务"
}

private fun resolveTaskPhaseText(
    status: String,
    statusMessage: String,
    progress: Double
): String {
    val normalizedStatus = status.trim().uppercase(Locale.ROOT)
    val normalizedMessage = statusMessage.trim()
    if (normalizedMessage.isNotBlank() && !isGenericTaskStatusMessage(normalizedMessage)) {
        return normalizedMessage
    }
    if (isCompletedStatus(normalizedStatus)) {
        return "处理完成"
    }
    if (isFailedStatus(normalizedStatus)) {
        return "处理失败"
    }
    if (normalizedStatus == "CANCELLED" || normalizedStatus == "CANCELED") {
        return "任务已取消"
    }
    if (isQueuedStatus(normalizedStatus)) {
        return "正在排队等待调度..."
    }
    return when {
        progress < 0.20 -> "正在提取音频片段..."
        progress < 0.75 -> "AI正在深度思考（预计1分钟）..."
        else -> "正在进行Markdown排版..."
    }
}

private fun isGenericTaskStatusMessage(message: String): Boolean {
    val normalized = message.trim().lowercase(Locale.ROOT)
    return normalized == "processing" ||
        normalized == "running" ||
        normalized == "queued" ||
        normalized == "pending" ||
        message.trim() == "处理中" ||
        message.trim() == "排队中"
}

@Composable
private fun SwipeRenameTaskListItem(
    task: MobileTaskListItem,
    shouldFlash: Boolean,
    flashPulse: Float,
    isEditing: Boolean,
    editValue: TextFieldValue,
    isRenameSaving: Boolean,
    isMenuRevealed: Boolean,
    canInteract: Boolean,
    dimOthers: Boolean,
    onEditValueChange: (TextFieldValue) -> Unit,
    onOpen: () -> Unit,
    onRevealMenu: (Boolean) -> Unit,
    onStartRename: () -> Unit,
    onCommitRename: (String) -> Unit,
    onCancelTask: () -> Unit
) {
    val menuWidth = 120.dp
    val menuWidthPx = with(LocalDensity.current) { menuWidth.toPx() }
    var dragOffsetPx by remember(task.taskId) { mutableFloatStateOf(0f) }
    val targetOffset = if (isMenuRevealed && !isEditing && canInteract) -menuWidthPx else 0f
    LaunchedEffect(targetOffset, task.taskId) {
        dragOffsetPx = targetOffset
    }
    val displayOffsetPx by animateFloatAsState(
        targetValue = dragOffsetPx,
        animationSpec = tween(durationMillis = 160),
        label = "task-item-swipe-offset-${task.taskId}"
    )

    Box(
        modifier = Modifier
            .fillMaxWidth()
            .graphicsLayer {
                alpha = if (dimOthers) 0.5f else 1f
            }
    ) {
        Box(
            modifier = Modifier
                .matchParentSize()
                .clip(RoundedCornerShape(12.dp))
                .background(Color(0xFF1D2939))
                .padding(horizontal = 12.dp)
        ) {
            Row(
                modifier = Modifier
                    .fillMaxHeight()
                    .width(menuWidth)
                    .align(Alignment.CenterEnd),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.Center
            ) {
                TextButton(
                    onClick = {
                        onStartRename()
                    },
                    enabled = canInteract && !isRenameSaving
                ) {
                    Text(
                        text = "Rename",
                        color = Color.White
                    )
                }
            }
        }

        Card(
            modifier = Modifier
                .fillMaxWidth()
                .offset {
                    IntOffset(displayOffsetPx.roundToInt(), 0)
                }
                .pointerInput(task.taskId, canInteract, isEditing, menuWidthPx) {
                    if (!canInteract || isEditing) {
                        return@pointerInput
                    }
                    detectHorizontalDragGestures(
                        onHorizontalDrag = { change, dragAmount ->
                            change.consume()
                            val nextOffset = (dragOffsetPx + dragAmount).coerceIn(-menuWidthPx, 0f)
                            dragOffsetPx = nextOffset
                        },
                        onDragEnd = {
                            val revealMenu = dragOffsetPx <= -menuWidthPx * 0.45f
                            dragOffsetPx = if (revealMenu) -menuWidthPx else 0f
                            onRevealMenu(revealMenu)
                        },
                        onDragCancel = {
                            dragOffsetPx = if (isMenuRevealed) -menuWidthPx else 0f
                        }
                    )
                }
                .clickable(
                    enabled = canInteract && !isEditing
                ) {
                    onOpen()
                },
            colors = androidx.compose.material3.CardDefaults.cardColors(
                containerColor = if (shouldFlash) {
                    Color(0xFFFFF2CC).copy(alpha = 0.68f + flashPulse * 0.18f)
                } else {
                    Color.White
                }
            )
        ) {
            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 12.dp, vertical = 10.dp),
                verticalArrangement = Arrangement.spacedBy(6.dp)
            ) {
                val isProcessing = isProcessingStatus(task.status)
                val isCompleted = isCompletedStatus(task.status)
                val isFailed = isFailedStatus(task.status)
                val phaseText = resolveTaskPhaseText(
                    status = task.status,
                    statusMessage = task.statusMessage,
                    progress = task.progress
                )
                val keyboardController = LocalSoftwareKeyboardController.current

                if (isEditing) {
                    val focusRequester = remember(task.taskId) { FocusRequester() }
                    var hasFocused by remember(task.taskId) { mutableStateOf(false) }
                    LaunchedEffect(task.taskId, isEditing) {
                        if (isEditing) {
                            focusRequester.requestFocus()
                            keyboardController?.show()
                        }
                    }
                    BasicTextField(
                        value = editValue,
                        onValueChange = onEditValueChange,
                        singleLine = true,
                        textStyle = MaterialTheme.typography.bodyLarge.copy(
                            fontWeight = FontWeight.SemiBold,
                            color = Color(0xFF101828)
                        ),
                        keyboardOptions = KeyboardOptions(imeAction = ImeAction.Done),
                        keyboardActions = KeyboardActions(
                            onDone = {
                                keyboardController?.hide()
                                onCommitRename("ime_done")
                            }
                        ),
                        modifier = Modifier
                            .fillMaxWidth()
                            .focusRequester(focusRequester)
                            .onFocusChanged { focusState ->
                                if (focusState.isFocused) {
                                    hasFocused = true
                                } else if (hasFocused) {
                                    onCommitRename("focus_lost")
                                }
                            }
                    )
                } else {
                    Text(
                        text = resolveTaskDisplayName(task),
                        fontWeight = FontWeight.SemiBold,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis
                    )
                }

                if (isProcessing) {
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.SpaceBetween,
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Row(
                            verticalAlignment = Alignment.CenterVertically,
                            modifier = Modifier.weight(1f)
                        ) {
                            CircularProgressIndicator(
                                modifier = Modifier.size(14.dp),
                                strokeWidth = 2.dp
                            )
                            Text(
                                text = phaseText,
                                color = Color(0xFF175CD3),
                                modifier = Modifier.padding(start = 8.dp),
                                maxLines = 1,
                                overflow = TextOverflow.Ellipsis
                            )
                        }
                        TextButton(
                            onClick = onCancelTask,
                            enabled = canInteract
                        ) {
                            Text("取消")
                        }
                    }
                } else if (isFailed) {
                    Text(
                        text = "Failed",
                        color = statusColor(task.status)
                    )
                }

                val progressValue = normalizeProgress(task.progress)
                if (progressValue != null && progressValue < 0.999f && !isCompleted) {
                    LinearProgressIndicator(
                        progress = progressValue,
                        modifier = Modifier
                            .fillMaxWidth()
                            .height(4.dp)
                    )
                }
                Text(
                    text = "Created: ${formatTaskTimestamp(task.createdAt)}",
                    color = Color(0xFF475467),
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
                Text(
                    text = "Last opened: ${formatTaskTimestamp(task.lastOpenedAt)}",
                    color = Color(0xFF475467),
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
                if (task.statusMessage.isNotBlank()) {
                    Text(
                        text = phaseText,
                        color = Color(0xFF475467),
                        maxLines = 2,
                        overflow = TextOverflow.Ellipsis
                    )
                }
            }
        }

        if (dimOthers) {
            Box(
                modifier = Modifier
                    .matchParentSize()
                    .background(Color.Black.copy(alpha = 0.30f))
            )
        }
    }
}

private fun detectClipboardTaskCandidate(
    context: Context,
    lastPromptedUrl: String?
): ClipboardTaskCandidate? {
    val manager = context.getSystemService(ClipboardManager::class.java) ?: return null
    val clipData = manager.primaryClip ?: return null
    if (clipData.itemCount <= 0) {
        return null
    }
    val text = clipData.getItemAt(0).coerceToText(context)?.toString()?.trim().orEmpty()
    if (text.isEmpty()) {
        return null
    }
    val normalizedLastPrompted = lastPromptedUrl?.let(::normalizeClipboardUrl)
    CLIPBOARD_URL_PATTERN.findAll(text).forEach { match ->
        val cleaned = sanitizeClipboardToken(match.value)
        val normalized = normalizeClipboardUrl(cleaned) ?: return@forEach
        if (normalized == normalizedLastPrompted) {
            return@forEach
        }
        val host = Uri.parse(normalized).host ?: return@forEach
        if (!isClipboardSupportedHost(host)) {
            return@forEach
        }
        return ClipboardTaskCandidate(
            normalizedUrl = normalized,
            displayUrl = cleaned
        )
    }
    return null
}

private fun sanitizeClipboardToken(raw: String): String {
    return raw
        .trim()
        .trimStart('(', '[', '{', '<', '"', '\'')
        .trimEnd('.', ',', ';', ':', '!', '?', ')', ']', '}', '>', '"', '\'')
}

private fun normalizeClipboardUrl(rawUrl: String): String? {
    val sanitized = sanitizeClipboardToken(rawUrl)
    if (sanitized.isEmpty()) {
        return null
    }
    val uri = runCatching { Uri.parse(sanitized) }.getOrNull() ?: return null
    val scheme = uri.scheme?.lowercase() ?: return null
    if (scheme != "http" && scheme != "https") {
        return null
    }
    val host = uri.host?.lowercase() ?: return null
    val path = uri.encodedPath.orEmpty()
    val query = uri.encodedQuery?.let { "?$it" }.orEmpty()
    return "$scheme://$host$path$query"
}

private fun isClipboardSupportedHost(host: String): Boolean {
    val normalized = host.lowercase()
    return CLIPBOARD_SUPPORTED_HOST_SUFFIXES.any { suffix ->
        normalized == suffix || normalized.endsWith(".$suffix")
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

    val markdownParts = splitMarkdownParagraphs(payload.markdown)
    val nodes = when {
        parsedNodes.size >= 2 -> parsedNodes
        markdownParts.size >= 2 -> buildNodesFromMarkdownParts(
            markdownParts = markdownParts,
            templateNodes = parsedNodes
        )
        parsedNodes.isNotEmpty() -> parsedNodes
        markdownParts.isNotEmpty() -> buildNodesFromMarkdownParts(
            markdownParts = markdownParts,
            templateNodes = emptyList()
        )
        else -> return null
    }

    val sanitizedNodes = sanitizeNodesForReader(nodes)

    return TaskReaderSession(
        taskId = payload.taskId.ifBlank { task.taskId },
        title = payload.title.ifBlank { task.title },
        pathHint = resolvePathHint(payload.markdownPath, payload.baseDir),
        nodes = sanitizedNodes
    )
}

private fun splitMarkdownParagraphs(markdown: String): List<String> {
    return markdown
        .split(Regex("\\n\\s*\\n+"))
        .map { it.trimEnd() }
        .filter { it.isNotBlank() }
}

private fun buildNodesFromMarkdownParts(
    markdownParts: List<String>,
    templateNodes: List<SemanticNode>
): List<SemanticNode> {
    val canReuseTemplateByIndex = templateNodes.isNotEmpty() &&
        kotlin.math.abs(templateNodes.size - markdownParts.size) <= 2
    return markdownParts.mapIndexed { index, markdown ->
        val template = if (canReuseTemplateByIndex) templateNodes.getOrNull(index) else null
        if (template != null) {
            template.copy(
                text = markdown,
                originalMarkdown = markdown
            )
        } else {
            SemanticNode(
                id = "md_${index + 1}",
                text = markdown,
                originalMarkdown = markdown,
                relevanceScore = (1f - index * 0.02f).coerceAtLeast(0.45f)
            )
        }
    }
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
    return when {
        isCompletedStatus(status) -> "Completed"
        isFailedStatus(status) -> "Failed"
        isQueuedStatus(status) -> "Queued"
        isProcessingStatus(status) -> "Running"
        upper.isBlank() -> "Unknown"
        else -> upper
    }
}

private fun statusColor(status: String): Color {
    return when {
        isCompletedStatus(status) -> Color(0xFF067647)
        isFailedStatus(status) -> Color(0xFFB42318)
        isQueuedStatus(status) -> Color(0xFFB54708)
        isProcessingStatus(status) -> Color(0xFF175CD3)
        else -> Color(0xFF667085)
    }
}

private fun isTaskNewlyCompleted(currentStatus: String, previousStatus: String?): Boolean {
    if (!isCompletedStatus(currentStatus)) {
        return false
    }
    if (previousStatus == null) {
        return false
    }
    return !isCompletedStatus(previousStatus)
}

private fun isCompletedStatus(status: String): Boolean {
    val upper = status.trim().uppercase()
    return upper == "COMPLETED" || upper == "SUCCESS"
}

private fun isFailedStatus(status: String): Boolean {
    val upper = status.trim().uppercase()
    return upper == "FAILED" || upper == "ERROR"
}

private fun isProcessingStatus(status: String): Boolean {
    val upper = status.trim().uppercase()
    return upper == "PROCESSING" || upper == "RUNNING" || upper == "QUEUED" || upper == "PENDING"
}

private fun isQueuedStatus(status: String): Boolean {
    val upper = status.trim().uppercase()
    return upper == "QUEUED" || upper == "PENDING"
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

private fun resolveTaskDisplayName(task: MobileTaskListItem): String {
    val title = task.title.trim()
    return if (title.isNotEmpty()) title else task.taskId
}

private fun nextTaskSortField(current: TaskSortField): TaskSortField {
    return when (current) {
        TaskSortField.LAST_OPENED -> TaskSortField.CREATED_AT
        TaskSortField.CREATED_AT -> TaskSortField.TASK_NAME
        TaskSortField.TASK_NAME -> TaskSortField.LAST_OPENED
    }
}

private fun taskSortFieldLabel(field: TaskSortField): String {
    return when (field) {
        TaskSortField.LAST_OPENED -> "Last Opened"
        TaskSortField.CREATED_AT -> "Created Time"
        TaskSortField.TASK_NAME -> "Task Name"
    }
}

private fun sortTasks(
    tasks: List<MobileTaskListItem>,
    sortField: TaskSortField,
    sortOrder: SortOrder
): List<MobileTaskListItem> {
    if (tasks.size <= 1) {
        return tasks
    }
    val comparator = when (sortField) {
        TaskSortField.LAST_OPENED -> compareBy<MobileTaskListItem> {
            parseTaskInstant(it.lastOpenedAt)?.toEpochMilli()
                ?: parseTaskInstant(it.createdAt)?.toEpochMilli()
                ?: Long.MIN_VALUE
        }.thenBy { resolveTaskDisplayName(it).lowercase(Locale.ROOT) }

        TaskSortField.CREATED_AT -> compareBy<MobileTaskListItem> {
            parseTaskInstant(it.createdAt)?.toEpochMilli() ?: Long.MIN_VALUE
        }.thenBy { resolveTaskDisplayName(it).lowercase(Locale.ROOT) }

        TaskSortField.TASK_NAME -> compareBy<MobileTaskListItem> {
            resolveTaskDisplayName(it).lowercase(Locale.ROOT)
        }.thenBy {
            parseTaskInstant(it.lastOpenedAt)?.toEpochMilli()
                ?: parseTaskInstant(it.createdAt)?.toEpochMilli()
                ?: Long.MIN_VALUE
        }
    }
    val sorted = tasks.sortedWith(comparator)
    return if (sortOrder == SortOrder.DESC) sorted.reversed() else sorted
}

private fun parseTaskInstant(raw: String): Instant? {
    val value = raw.trim()
    if (value.isEmpty()) {
        return null
    }
    return runCatching { Instant.parse(value) }.getOrNull()
}

private fun formatTaskTimestamp(raw: String): String {
    val instant = parseTaskInstant(raw) ?: return "-"
    return runCatching {
        TASK_TIME_FORMATTER.format(instant.atZone(ZoneId.systemDefault()))
    }.getOrElse { "-" }
}

private fun buildReaderMarkwon(
    context: Context,
    renderConfig: MarkdownReaderRenderConfig
): Markwon {
    val resolvedHeadingTypeface = MarkdownTypefaceResolver.resolveWithWeight(
        context = context,
        fontFamily = renderConfig.headingFontFamily,
        weight = 500
    )
    val tableTheme = TableTheme.buildWithDefaults(context)
        .tableCellPadding(renderConfig.markwonTableCellPadding)
        .tableBorderColor(0xFFD0D7DE.toInt())
        .tableBorderWidth(renderConfig.markwonTableBorderWidth)
        .tableHeaderRowBackgroundColor(0xFFF6F8FA.toInt())
        .tableOddRowBackgroundColor(0xFFFFFFFF.toInt())
        .tableEvenRowBackgroundColor(0xFFF8FAFC.toInt())
        .build()

    val prism4j = Prism4j(MarkdownGrammarLocator())

    return Markwon.builder(context)
        .usePlugin(CorePlugin.create())

        .usePlugin(SoftBreakAddsNewLinePlugin.create())
        .usePlugin(HtmlPlugin.create())
        .usePlugin(LinkifyPlugin.create())
        .usePlugin(StrikethroughPlugin.create())
        .usePlugin(SyntaxHighlightPlugin.create(prism4j, Prism4jThemeDarkula.create()))
        .usePlugin(TablePlugin.create(tableTheme))
        .usePlugin(CoilImagesPlugin.create(context))
        .usePlugin(JLatexMathPlugin.create(renderConfig.markwonLatexTextSizeSp))
        .usePlugin(CustomHeadingPlugin())

        .usePlugin(
            object : AbstractMarkwonPlugin() {
                override fun configureTheme(builder: MarkwonTheme.Builder) {
                    builder
                        .blockMargin(renderConfig.markwonBlockMargin)
                        .listItemColor(0xFF57606A.toInt())
                        .bulletWidth(renderConfig.markwonBulletWidth)
                        .bulletListItemStrokeWidth(renderConfig.markwonBulletStrokeWidth)
                        .codeTextColor(0xFF24292F.toInt())
                        .codeBlockTextColor(0xFF24292F.toInt())
                        .codeBackgroundColor(0xFFF6F8FA.toInt())
                        .codeBlockBackgroundColor(0xFFF6F8FA.toInt())
                        .codeBlockMargin(renderConfig.markwonCodeBlockMargin)
                        .codeTypeface(Typeface.create(renderConfig.monospaceFontFamily, Typeface.NORMAL))
                        .codeBlockTypeface(Typeface.create(renderConfig.monospaceFontFamily, Typeface.NORMAL))
                        .blockQuoteColor(0xFFD0D7DE.toInt())
                        .blockQuoteWidth(renderConfig.markwonBlockquoteWidth)
                        .headingTypeface(resolvedHeadingTypeface)
                        .headingTextSizeMultipliers(renderConfig.markwonHeadingTextSizeMultipliers)
                        .headingBreakHeight(renderConfig.markwonHeadingBreakHeight)
                        .linkColor(0xFF0969DA.toInt())
                        .isLinkUnderlined(false)
                }
            }
        )
        .build()
}

/**
 * 自定义 Markwon 标题行高插件。
 * 目的：压缩标题段落高度，减少阅读页的大段留白，提升信息密度。
 * 策略：
 * 1. 通过 LineHeightSpan 按比例压缩标题高度。
 * 2. 同步调整 ascent/descent，避免文本基线抖动。
 */
private class CustomHeadingPlugin : AbstractMarkwonPlugin() {
    override fun configureSpansFactory(builder: MarkwonSpansFactory.Builder) {
        builder.appendFactory(org.commonmark.node.Heading::class.java) { _, _ ->
            // 标题默认行高偏大，这里统一压缩至 0.85 倍。
            object : android.text.style.LineHeightSpan {
                override fun chooseHeight(
                    text: CharSequence,
                    start: Int,
                    end: Int,
                    spanstartv: Int,
                    v: Int,
                    fm: android.graphics.Paint.FontMetricsInt
                ) {
                    // 基于当前字体度量收缩行高，兼顾上下留白。
                    val compressionRatio = 0.85f // 目标为原始高度的 85%
                    val height = fm.descent - fm.ascent
                    val targetHeight = (height * compressionRatio).toInt()
                    
                    val delta = height - targetHeight
                    val topDelta = delta / 2
                    val bottomDelta = delta - topDelta
                    
                    fm.ascent += topDelta
                    fm.top += topDelta
                    fm.descent -= bottomDelta
                    fm.bottom -= bottomDelta
                }
            }
        }
    }
}

private tailrec fun Context.findHostActivity(): ComponentActivity? {
    return when (this) {
        is ComponentActivity -> this
        is ContextWrapper -> baseContext.findHostActivity()
        else -> null
    }
}
