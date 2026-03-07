package com.hongxu.videoToMarkdownTest2

import android.Manifest
import android.app.Application
import android.content.ClipboardManager
import android.content.Context
import android.content.ContextWrapper
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Typeface
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.provider.OpenableColumns
import androidx.activity.ComponentActivity
import androidx.activity.compose.BackHandler
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInVertically
import androidx.compose.animation.slideOutVertically
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.animateFloatAsState
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
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.navigationBarsPadding
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
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.FloatingActionButton
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.pulltorefresh.PullToRefreshBox
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.rememberModalBottomSheetState
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
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.TextFieldValue
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.IntOffset
import androidx.compose.ui.unit.sp
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
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.LinkedHashMap
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
    COLLECTIONS,
    FOOTPRINTS
}

internal data class TaskRouteUiState(
    val taskSearchQuery: String = "",
    val taskSortField: TaskSortField = TaskSortField.LAST_OPENED,
    val taskSortOrder: SortOrder = SortOrder.DESC,
    val videoUrlInput: String = "",
    val bookPageOffsetInput: String = "",
    val composerExpanded: Boolean = false,
    val composerMode: TaskComposerMode = TaskComposerMode.URL,
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

    fun setBookPageOffsetInput(value: String) {
        _uiState.update { state -> state.copy(bookPageOffsetInput = value) }
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

private const val CLIPBOARD_TASK_PROMPT_PREFS = "clipboard_task_prompt"
private const val CLIPBOARD_TASK_PROMPT_KEY = "last_prompted_url"
private const val READER_SCROLL_POSITION_PREFS = "reader_scroll_position"
private const val READER_SCROLL_SAVE_OFFSET_DELTA_PX = 96
private const val TASK_FLASH_DURATION_MS = 10_000L
private const val SUBMISSION_MODE_URL = "url"
private const val SUBMISSION_MODE_UPLOAD = "upload"
private const val BACKEND_PROCESSING_HINT_WORK_PREFIX = "backend-running-task-"
private val TASK_TIME_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
private val CLIPBOARD_URL_PATTERN = Regex("""https?://[^\s]+""", RegexOption.IGNORE_CASE)
private val DOUYIN_HOST_SUFFIXES = setOf(
    "douyin.com",
    "iesdouyin.com"
)
private val BILIBILI_HOST_SUFFIXES = setOf(
    "bilibili.com",
    "b23.tv",
    "bili2233.cn"
)
private val ZHIHU_HOST_SUFFIXES = setOf(
    "zhihu.com",
    "zhuanlan.zhihu.com"
)
private val JUEJIN_HOST_SUFFIXES = setOf(
    "juejin.cn"
)

private data class ClipboardTaskCandidate(
    val normalizedUrl: String,
    val displayUrl: String
)

private data class ShareIntentPayload(
    val videoUrl: String?,
    val uploadUri: Uri?
)

private data class NotificationTaskOpenRequest(
    val taskId: String,
    val taskTitle: String
)

private enum class ShareDispatchStage {
    RECEIVED,
    PROCESSING,
    QUEUED,
    FAILED
}

private data class ShareDispatchState(
    val stage: ShareDispatchStage,
    val message: String
)

private data class ShareIntentPreview(
    val sourceName: String,
    val sourceBadge: String,
    val sourceBadgeColor: Color,
    val title: String,
    val subtitle: String,
    val rawContent: String
)

private data class TaskReaderScrollPosition(
    val firstVisibleItemIndex: Int,
    val firstVisibleItemScrollOffset: Int
)

private data class ReaderScrollSnapshot(
    val taskId: String,
    val position: TaskReaderScrollPosition
)

private class TaskReaderScrollPositionStore(
    context: Context
) {
    private val preferences = context.getSharedPreferences(
        READER_SCROLL_POSITION_PREFS,
        Context.MODE_PRIVATE
    )
    private val memoryCache = mutableMapOf<String, TaskReaderScrollPosition>()

    fun load(taskId: String): TaskReaderScrollPosition? {
        val normalizedTaskId = taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            return null
        }
        memoryCache[normalizedTaskId]?.let { cached ->
            return cached
        }
        val indexKey = buildIndexKey(normalizedTaskId)
        if (!preferences.contains(indexKey)) {
            return null
        }
        val restored = TaskReaderScrollPosition(
            firstVisibleItemIndex = preferences.getInt(indexKey, 0).coerceAtLeast(0),
            firstVisibleItemScrollOffset = preferences
                .getInt(buildOffsetKey(normalizedTaskId), 0)
                .coerceAtLeast(0)
        )
        memoryCache[normalizedTaskId] = restored
        return restored
    }

    fun save(taskId: String, position: TaskReaderScrollPosition, sync: Boolean = false) {
        val normalizedTaskId = taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            return
        }
        val normalizedPosition = TaskReaderScrollPosition(
            firstVisibleItemIndex = position.firstVisibleItemIndex.coerceAtLeast(0),
            firstVisibleItemScrollOffset = position.firstVisibleItemScrollOffset.coerceAtLeast(0)
        )
        val previous = memoryCache[normalizedTaskId]
        if (previous == normalizedPosition) {
            return
        }
        memoryCache[normalizedTaskId] = normalizedPosition
        val editor = preferences.edit()
            .putInt(buildIndexKey(normalizedTaskId), normalizedPosition.firstVisibleItemIndex)
            .putInt(buildOffsetKey(normalizedTaskId), normalizedPosition.firstVisibleItemScrollOffset)
        if (sync) {
            editor.commit()
        } else {
            editor.apply()
        }
    }

    private fun buildIndexKey(taskId: String): String {
        return "task_${taskId}_index"
    }

    private fun buildOffsetKey(taskId: String): String {
        return "task_${taskId}_offset"
    }
}

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
    private val shareIntentEvents = MutableSharedFlow<ShareIntentPayload>(extraBufferCapacity = 4)
    private val notificationTaskOpenEvents = MutableSharedFlow<NotificationTaskOpenRequest>(extraBufferCapacity = 4)

    override fun onCreate(savedInstanceState: Bundle?) {
        val initialSharePayload = parseShareIntentPayload(intent)
        val initialNotificationTaskOpenRequest = parseNotificationTaskOpenRequest(intent)
        if (initialSharePayload != null) {
            setTheme(R.style.Theme_VideoToMarkdownTest2_ShareOverlay)
        }
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent {
            VideoToMarkdownTest2Theme {
                MobileTaskApp(
                    initialSharePayload = initialSharePayload,
                    shareIntentEvents = shareIntentEvents,
                    initialNotificationTaskOpenRequest = initialNotificationTaskOpenRequest,
                    notificationTaskOpenEvents = notificationTaskOpenEvents
                )
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        parseShareIntentPayload(intent)?.let { payload ->
            shareIntentEvents.tryEmit(payload)
        }
        parseNotificationTaskOpenRequest(intent)?.let { request ->
            notificationTaskOpenEvents.tryEmit(request)
        }
    }
}

private fun parseShareIntentPayload(intent: Intent?): ShareIntentPayload? {
    if (intent == null || intent.action != Intent.ACTION_SEND) {
        return null
    }
    val textPayload = intent.getStringExtra(Intent.EXTRA_TEXT).orEmpty()
    val sharedUrl = CLIPBOARD_URL_PATTERN.find(textPayload)
        ?.value
        ?.let(::sanitizeClipboardToken)
        ?.let(::normalizeClipboardUrl)
    val sharedUri = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
        intent.getParcelableExtra(Intent.EXTRA_STREAM, Uri::class.java)
    } else {
        @Suppress("DEPRECATION")
        intent.getParcelableExtra(Intent.EXTRA_STREAM)
    }
    if (sharedUrl.isNullOrBlank() && sharedUri == null) {
        return null
    }
    return ShareIntentPayload(
        videoUrl = sharedUrl,
        uploadUri = sharedUri
    )
}

private fun parseNotificationTaskOpenRequest(intent: Intent?): NotificationTaskOpenRequest? {
    if (intent == null) {
        return null
    }
    val taskId = intent.getStringExtra("task_id")?.trim().orEmpty()
    if (taskId.isEmpty()) {
        return null
    }
    val taskTitle = intent.getStringExtra("task_title")?.trim().orEmpty()
    return NotificationTaskOpenRequest(
        taskId = taskId,
        taskTitle = taskTitle
    )
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun ShareIntentDispatchRoute(
    payload: ShareIntentPayload,
    state: ShareDispatchState?,
    onClose: () -> Unit
) {
    val sheetState = rememberModalBottomSheetState(skipPartiallyExpanded = false)
    val effectiveState = state ?: ShareDispatchState(
        stage = ShareDispatchStage.RECEIVED,
        message = "已接收分享内容，正在准备投递..."
    )
    val preview = remember(payload.videoUrl, payload.uploadUri) {
        resolveShareIntentPreview(payload)
    }
    val autoCloseProgress by animateFloatAsState(
        targetValue = if (effectiveState.stage == ShareDispatchStage.QUEUED) 1f else 0f,
        animationSpec = tween(durationMillis = 3_000),
        label = "share_auto_close_progress"
    )
    val successScale by animateFloatAsState(
        targetValue = if (effectiveState.stage == ShareDispatchStage.QUEUED) 1f else 0.72f,
        animationSpec = tween(durationMillis = 260),
        label = "share_success_scale"
    )
    val successAlpha by animateFloatAsState(
        targetValue = if (effectiveState.stage == ShareDispatchStage.QUEUED) 1f else 0f,
        animationSpec = tween(durationMillis = 240),
        label = "share_success_alpha"
    )
    LaunchedEffect(effectiveState.stage) {
        if (effectiveState.stage == ShareDispatchStage.QUEUED) {
            delay(3_000)
            onClose()
        }
    }
    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(Color.Black.copy(alpha = 0.26f))
    ) {
        ModalBottomSheet(
            onDismissRequest = {},
            sheetState = sheetState,
            dragHandle = null
        ) {
            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 18.dp, vertical = 18.dp)
                    .navigationBarsPadding(),
                verticalArrangement = Arrangement.spacedBy(14.dp)
            ) {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
                        Text(
                            text = "快速投递",
                            fontWeight = FontWeight.SemiBold
                        )
                        Text(
                            text = "已识别来源并开始分发任务",
                            color = Color(0xFF667085)
                        )
                    }
                    Card(
                        shape = RoundedCornerShape(999.dp),
                        colors = CardDefaults.cardColors(containerColor = preview.sourceBadgeColor.copy(alpha = 0.14f))
                    ) {
                        Text(
                            text = preview.sourceName,
                            modifier = Modifier.padding(horizontal = 12.dp, vertical = 6.dp),
                            color = preview.sourceBadgeColor,
                            fontWeight = FontWeight.SemiBold
                        )
                    }
                }
                ShareIntentPreviewCard(preview = preview)
                Card(
                    modifier = Modifier.fillMaxWidth(),
                    colors = CardDefaults.cardColors(containerColor = Color(0xFFF8FAFC))
                ) {
                    Column(
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(horizontal = 14.dp, vertical = 12.dp),
                        verticalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        Text(
                            text = "投递进度",
                            color = Color(0xFF344054),
                            fontWeight = FontWeight.SemiBold
                        )
                        ShareDispatchStepItem(
                            label = "📥 已接收",
                            status = resolveShareStepStatus(effectiveState.stage, step = 0)
                        )
                        ShareDispatchStepItem(
                            label = "⚙️ 处理中",
                            status = resolveShareStepStatus(effectiveState.stage, step = 1)
                        )
                        ShareDispatchStepItem(
                            label = "📦 已入队",
                            status = resolveShareStepStatus(effectiveState.stage, step = 2)
                        )
                        Text(
                            text = effectiveState.message,
                            color = if (effectiveState.stage == ShareDispatchStage.FAILED) {
                                Color(0xFFB42318)
                            } else {
                                Color(0xFF175CD3)
                            },
                            maxLines = 2,
                            overflow = TextOverflow.Ellipsis
                        )
                        Text(
                            text = resolveShareWaitHint(effectiveState.stage),
                            color = Color(0xFF667085),
                            maxLines = 2,
                            overflow = TextOverflow.Ellipsis
                        )
                    }
                }
                if (effectiveState.stage == ShareDispatchStage.QUEUED) {
                    Card(
                        modifier = Modifier.fillMaxWidth(),
                        colors = CardDefaults.cardColors(containerColor = Color(0xFFEAFBF0))
                    ) {
                        Column(
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(horizontal = 14.dp, vertical = 12.dp),
                            verticalArrangement = Arrangement.spacedBy(8.dp)
                        ) {
                            Row(
                                verticalAlignment = Alignment.CenterVertically,
                                horizontalArrangement = Arrangement.spacedBy(10.dp)
                            ) {
                                Text(
                                    text = "✓",
                                    color = Color(0xFF12B76A),
                                    fontWeight = FontWeight.Bold,
                                    fontSize = 30.sp,
                                    modifier = Modifier.graphicsLayer {
                                        scaleX = successScale
                                        scaleY = successScale
                                        alpha = successAlpha
                                    }
                                )
                                Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
                                    Text(
                                        text = "投递成功，任务已进入后台队列",
                                        color = Color(0xFF067647),
                                        fontWeight = FontWeight.SemiBold
                                    )
                                    Text(
                                        text = "3 秒后自动返回原应用",
                                        color = Color(0xFF067647)
                                    )
                                }
                            }
                            LinearProgressIndicator(
                                progress = autoCloseProgress,
                                modifier = Modifier.fillMaxWidth(),
                                color = Color(0xFF12B76A),
                                trackColor = Color(0xFFB7EACF)
                            )
                        }
                    }
                }
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Text(
                        text = if (effectiveState.stage == ShareDispatchStage.FAILED) {
                            "投递失败，请手动返回并重试"
                        } else {
                            "如需立即返回，可手动关闭"
                        },
                        color = Color(0xFF475467),
                        modifier = Modifier.weight(1f)
                    )
                    TextButton(onClick = onClose) {
                        Text("关闭")
                    }
                }
            }
        }
    }
}

private enum class ShareStepStatus {
    WAITING,
    ACTIVE,
    DONE,
    ERROR
}

private fun resolveShareStepStatus(stage: ShareDispatchStage, step: Int): ShareStepStatus {
    return when (stage) {
        ShareDispatchStage.RECEIVED -> when (step) {
            0 -> ShareStepStatus.ACTIVE
            else -> ShareStepStatus.WAITING
        }

        ShareDispatchStage.PROCESSING -> when (step) {
            0 -> ShareStepStatus.DONE
            1 -> ShareStepStatus.ACTIVE
            else -> ShareStepStatus.WAITING
        }

        ShareDispatchStage.QUEUED -> when (step) {
            0, 1 -> ShareStepStatus.DONE
            else -> ShareStepStatus.ACTIVE
        }

        ShareDispatchStage.FAILED -> when (step) {
            0 -> ShareStepStatus.DONE
            1 -> ShareStepStatus.ERROR
            else -> ShareStepStatus.WAITING
        }
    }
}

private fun resolveShareWaitHint(stage: ShareDispatchStage): String {
    return when (stage) {
        ShareDispatchStage.RECEIVED -> "预计等待 < 1 秒，正在建立投递请求。"
        ShareDispatchStage.PROCESSING -> "预计等待 2-6 秒。当前正在立即处理，繁忙时会自动排队。"
        ShareDispatchStage.QUEUED -> "投递已完成，后台将继续处理，无需停留等待。"
        ShareDispatchStage.FAILED -> "未进入队列，请返回原应用后重新分享。"
    }
}

@Composable
private fun ShareDispatchStepItem(
    label: String,
    status: ShareStepStatus
) {
    val containerColor = when (status) {
        ShareStepStatus.ACTIVE -> Color(0xFFEFF8FF)
        ShareStepStatus.DONE -> Color(0xFFEAFBF0)
        ShareStepStatus.ERROR -> Color(0xFFFEE4E2)
        ShareStepStatus.WAITING -> Color(0xFFF2F4F7)
    }
    val textColor = when (status) {
        ShareStepStatus.ACTIVE -> Color(0xFF175CD3)
        ShareStepStatus.DONE -> Color(0xFF067647)
        ShareStepStatus.ERROR -> Color(0xFFB42318)
        ShareStepStatus.WAITING -> Color(0xFF667085)
    }
    val statusSymbol = when (status) {
        ShareStepStatus.DONE -> "✓"
        ShareStepStatus.ACTIVE -> "•"
        ShareStepStatus.ERROR -> "!"
        ShareStepStatus.WAITING -> "○"
    }
    Card(
        shape = RoundedCornerShape(10.dp),
        colors = CardDefaults.cardColors(containerColor = containerColor)
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 10.dp, vertical = 7.dp),
            horizontalArrangement = Arrangement.spacedBy(6.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = statusSymbol,
                color = textColor,
                fontWeight = FontWeight.Bold
            )
            Text(
                text = label,
                color = textColor,
                fontWeight = FontWeight.Medium
            )
        }
    }
}

@Composable
private fun ShareIntentPreviewCard(preview: ShareIntentPreview) {
    Card(
        modifier = Modifier.fillMaxWidth(),
        colors = CardDefaults.cardColors(containerColor = Color(0xFFEEF4FF))
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 12.dp, vertical = 12.dp),
            horizontalArrangement = Arrangement.spacedBy(10.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Box(
                modifier = Modifier
                    .size(40.dp)
                    .clip(RoundedCornerShape(12.dp))
                    .background(preview.sourceBadgeColor),
                contentAlignment = Alignment.Center
            ) {
                Text(
                    text = preview.sourceBadge,
                    color = Color.White,
                    fontWeight = FontWeight.Bold
                )
            }
            Column(
                modifier = Modifier.weight(1f),
                verticalArrangement = Arrangement.spacedBy(2.dp)
            ) {
                Text(
                    text = preview.title,
                    color = Color(0xFF1D2939),
                    fontWeight = FontWeight.SemiBold,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
                Text(
                    text = preview.subtitle,
                    color = Color(0xFF475467),
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
                Text(
                    text = preview.rawContent,
                    color = Color(0xFF667085),
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
            }
        }
    }
}

private fun resolveShareIntentPreview(payload: ShareIntentPayload): ShareIntentPreview {
    val sharedUrl = payload.videoUrl?.trim().orEmpty()
    if (sharedUrl.isNotEmpty()) {
        val uri = runCatching { Uri.parse(sharedUrl) }.getOrNull()
        val host = uri?.host?.trim().orEmpty()
        val sourceName: String
        val sourceBadge: String
        val sourceColor: Color
        when {
            isBilibiliHost(host) -> {
                sourceName = "B站"
                sourceBadge = "B"
                sourceColor = Color(0xFFFB7299)
            }

            isDouyinHost(host) -> {
                sourceName = "抖音"
                sourceBadge = "抖"
                sourceColor = Color(0xFF111827)
            }

            isZhihuHost(host) -> {
                sourceName = "Zhihu"
                sourceBadge = "Zh"
                sourceColor = Color(0xFF1772F6)
            }

            isJuejinHost(host) -> {
                sourceName = "Juejin"
                sourceBadge = "J"
                sourceColor = Color(0xFF1E80FF)
            }

            else -> {
                sourceName = "网页链接"
                sourceBadge = "链"
                sourceColor = Color(0xFF175CD3)
            }
        }
        return ShareIntentPreview(
            sourceName = sourceName,
            sourceBadge = sourceBadge,
            sourceBadgeColor = sourceColor,
            title = resolveSharePreviewTitleFromUrl(sharedUrl),
            subtitle = host.ifBlank { "外部应用分享" },
            rawContent = sharedUrl
        )
    }
    val uriText = payload.uploadUri?.toString().orEmpty()
    val localName = payload.uploadUri?.lastPathSegment
        ?.substringAfterLast('/')
        ?.takeUnless { it.isBlank() }
        ?: "本地视频文件"
    return ShareIntentPreview(
        sourceName = "本地文件",
        sourceBadge = "本",
        sourceBadgeColor = Color(0xFF16A34A),
        title = localName,
        subtitle = "来自系统分享",
        rawContent = uriText.ifBlank { "content://..." }
    )
}

private fun resolveSharePreviewTitleFromUrl(videoUrl: String): String {
    val uri = runCatching { Uri.parse(videoUrl) }.getOrNull() ?: return "视频链接"
    val queryTitle = runCatching { uri.getQueryParameter("title") }.getOrNull()?.trim().orEmpty()
    if (queryTitle.isNotEmpty()) {
        return queryTitle
    }
    val lastSegment = uri.lastPathSegment?.trim().orEmpty()
    if (lastSegment.isNotEmpty()) {
        return lastSegment
    }
    return uri.host?.trim()?.takeUnless { it.isBlank() } ?: "视频链接"
}

@Composable
private fun ClipboardQuickActionPill(
    candidateUrl: String,
    onGenerateNow: () -> Unit,
    onDismiss: () -> Unit,
    modifier: Modifier = Modifier
) {
    Card(
        modifier = modifier.fillMaxWidth(),
        colors = CardDefaults.cardColors(containerColor = Color(0xFFEAF4FF))
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 12.dp, vertical = 10.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Text(
                text = "检测到刚复制的视频链接",
                fontWeight = FontWeight.SemiBold,
                color = Color(0xFF175CD3)
            )
            Text(
                text = candidateUrl,
                color = Color(0xFF344054),
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Button(onClick = onGenerateNow) {
                    Text("立即生成")
                }
                TextButton(onClick = onDismiss) {
                    Text("稍后")
                }
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun MobileTaskApp(
    initialSharePayload: ShareIntentPayload? = null,
    shareIntentEvents: SharedFlow<ShareIntentPayload>? = null,
    initialNotificationTaskOpenRequest: NotificationTaskOpenRequest? = null,
    notificationTaskOpenEvents: SharedFlow<NotificationTaskOpenRequest>? = null
) {
    val context = LocalContext.current
    val lifecycleOwner = LocalLifecycleOwner.current
    val scope = rememberCoroutineScope()
    val haptic = LocalHapticFeedback.current
    val apiBaseUrl = BuildConfig.MOBILE_API_BASE_URL
    val mobileUserId = remember(context) {
        MobileClientIdentity.resolveUserId(context.applicationContext)
    }
    val autoUpdateEnabled = BuildConfig.MOBILE_AUTO_UPDATE_ENABLED
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
    val taskApi = remember(apiBaseUrl, mobileUserId) { HttpMobileTaskApi(apiBaseUrl, mobileUserId) }
    val metaApi = remember(apiBaseUrl) { HttpMobileMarkdownMetaApi(apiBaseUrl) }
    val telemetryApi = remember(apiBaseUrl) { HttpMobileMarkdownTelemetryApi(apiBaseUrl) }
    val cardApi = remember(apiBaseUrl) { HttpMobileConceptCardApi(apiBaseUrl) }
    val footprintRepo = remember(context) { ReadingFootprintRepository(context.applicationContext) }
    val appUpdateManager = remember(context, apiBaseUrl, autoUpdateEnabled) {
        if (autoUpdateEnabled) {
            MobileAppAutoUpdateManager(
                context = context.applicationContext,
                apiBaseUrl = apiBaseUrl
            )
        } else {
            null
        }
    }
    val markwon = remember(context, renderConfig) { buildReaderMarkwon(context, renderConfig) }
    val taskCompletionNotifier = remember(context) {
        TaskCompletionNotifier(context.applicationContext)
    }
    val snackbarHostState = remember { SnackbarHostState() }
    val clipboardPromptHistory = remember(context) {
        ClipboardPromptHistory(context.applicationContext)
    }
    val readerScrollPositionStore = remember(context) {
        TaskReaderScrollPositionStore(context.applicationContext)
    }
    val refreshMutex = remember { Mutex() }
    val probeState by collectionViewModel.probeState.collectAsState()
    val selectedProbeEpisodeNos by collectionViewModel.selectedEpisodeNos.collectAsState()
    val confirmedProbeStartPage by collectionViewModel.confirmedStartPage.collectAsState()
    val probePreviewDocumentUri by collectionViewModel.probePreviewDocumentUri.collectAsState()
    val collectionCards by collectionViewModel.collections.collectAsState()
    val detailCollection by collectionViewModel.detailCollection.collectAsState()
    val detailEpisodes by collectionViewModel.detailEpisodes.collectAsState()
    val probeSubmitInProgress by collectionViewModel.submitInProgress.collectAsState()
    val taskRouteUiState by taskRouteViewModel.uiState.collectAsState()

    var tasks by remember { mutableStateOf<List<MobileTaskListItem>>(emptyList()) }
    var backendProcessingTasks by remember { mutableStateOf<List<MobileTaskListItem>>(emptyList()) }
    var preparedSessions by remember { mutableStateOf<Map<String, TaskReaderSession>>(emptyMap()) }
    var taskStatusSnapshot by remember { mutableStateOf<Map<String, String>>(emptyMap()) }
    var listLoading by remember { mutableStateOf(false) }
    var actionLoading by remember { mutableStateOf(false) }
    var actionMessage by remember { mutableStateOf("") }
    var forceUpdateRequired by remember { mutableStateOf(false) }
    var forceUpdateVersionCode by remember { mutableStateOf(0) }
    var forceUpdateVersionName by remember { mutableStateOf("") }
    var autoUpdateDownloadInProgress by remember { mutableStateOf(false) }
    var autoUpdateProgressPercent by remember { mutableStateOf<Int?>(null) }
    var autoUpdateStatusText by remember { mutableStateOf("") }
    var autoUpdateReadyToInstall by remember { mutableStateOf(false) }
    var autoUpdateReadyVersionCode by remember { mutableStateOf(0) }
    var autoUpdateReadyVersionName by remember { mutableStateOf("") }
    var editingTaskId by remember { mutableStateOf<String?>(null) }
    var editingTaskTitleValue by remember { mutableStateOf(TextFieldValue("")) }
    var revealedTaskId by remember { mutableStateOf<String?>(null) }
    var renameSavingTaskId by remember { mutableStateOf<String?>(null) }
    var readerSession by remember { mutableStateOf<TaskReaderSession?>(null) }
    var readerScrollSnapshot by remember { mutableStateOf<ReaderScrollSnapshot?>(null) }
    var readerChromeVisible by remember { mutableStateOf(true) }
    var clipboardCandidate by remember { mutableStateOf<ClipboardTaskCandidate?>(null) }
    var shareDispatchState by remember { mutableStateOf<ShareDispatchState?>(null) }
    var currentSharePayload by remember(initialSharePayload) { mutableStateOf(initialSharePayload) }
    var pendingNotificationTaskOpenRequest by remember(initialNotificationTaskOpenRequest) {
        mutableStateOf(initialNotificationTaskOpenRequest)
    }
    var pendingUploadSubmissionId by remember { mutableStateOf<String?>(null) }
    var completionBanner by remember { mutableStateOf<CompletionBannerState?>(null) }
    var flashingTaskDeadlines by remember { mutableStateOf<Map<String, Long>>(emptyMap()) }
    var uiClockMs by remember { mutableStateOf(System.currentTimeMillis()) }
    val processingTasksForSkeleton = remember(tasks, backendProcessingTasks) {
        deduplicateTasksByTaskId(
            buildList {
                addAll(tasks.filter { task -> isProcessingStatus(task.status) })
                addAll(backendProcessingTasks)
            }
        )
    }
    val backendProcessingHints = remember(processingTasksForSkeleton, activeSubmissionHintMap) {
        val trackedTaskIds = activeSubmissionHintMap.values.mapNotNull { hint ->
            hint.taskId?.trim()?.takeIf { it.isNotEmpty() }
        }.toHashSet()
        processingTasksForSkeleton.asSequence()
            .filter { task -> task.taskId !in trackedTaskIds }
            .map { task ->
                val progressPercent = when {
                    task.progress <= 0.0 -> null
                    task.progress <= 1.0 -> (task.progress * 100.0).roundToInt().coerceIn(0, 100)
                    task.progress <= 100.0 -> task.progress.roundToInt().coerceIn(0, 100)
                    else -> 100
                }
                ActiveSubmissionHint(
                    workId = "$BACKEND_PROCESSING_HINT_WORK_PREFIX${task.taskId}",
                    taskId = task.taskId,
                    title = resolveTaskDisplayName(task),
                    phaseText = resolveTaskPhaseText(
                        status = task.status,
                        statusMessage = task.statusMessage,
                        progress = task.progress
                    ),
                    progressPercent = progressPercent,
                    running = true,
                    failed = false,
                    failedMessage = ""
                )
            }
            .toList()
    }
    val activeSubmissionHints = remember(activeSubmissionHintMap, backendProcessingHints) {
        val merged = mutableListOf<ActiveSubmissionHint>()
        val seenTaskIds = hashSetOf<String>()
        activeSubmissionHintMap.values
            .sortedWith(
                compareByDescending<ActiveSubmissionHint> { it.running }
                    .thenBy { it.workId }
            )
            .forEach { hint ->
                merged += hint
                hint.taskId?.trim()?.takeIf { it.isNotEmpty() }?.let(seenTaskIds::add)
            }
        backendProcessingHints.forEach { hint ->
            val taskId = hint.taskId?.trim().orEmpty()
            if (taskId.isNotEmpty() && taskId in seenTaskIds) {
                return@forEach
            }
            merged += hint
            if (taskId.isNotEmpty()) {
                seenTaskIds += taskId
            }
        }
        merged
    }
    val activeSubmissionTaskIds = remember(activeSubmissionHints) {
        activeSubmissionHints.mapNotNull { hint ->
            hint.taskId?.trim()?.takeIf { it.isNotEmpty() }
        }.toSet()
    }
    val runningTaskCount = remember(processingTasksForSkeleton, activeSubmissionHints) {
        val runningTaskIds = processingTasksForSkeleton.asSequence()
            .map { task -> task.taskId }
            .toMutableSet()
        var pendingHintsWithoutTaskId = 0
        activeSubmissionHints.forEach { hint ->
            val taskId = hint.taskId?.trim().orEmpty()
            if (taskId.isNotEmpty()) {
                runningTaskIds += taskId
            } else {
                pendingHintsWithoutTaskId += 1
            }
        }
        runningTaskIds.size + pendingHintsWithoutTaskId
    }
    val dispatchCenterRunningTasks = remember(processingTasksForSkeleton, activeSubmissionTaskIds) {
        processingTasksForSkeleton.asSequence()
            .filter { task -> task.taskId !in activeSubmissionTaskIds }
            .take(6)
            .toList()
    }
    val taskSearchQuery = taskRouteUiState.taskSearchQuery
    val taskSortField = taskRouteUiState.taskSortField
    val taskSortOrder = taskRouteUiState.taskSortOrder
    val videoUrlInput = taskRouteUiState.videoUrlInput
    val bookPageOffsetInput = taskRouteUiState.bookPageOffsetInput
    val composerExpanded = taskRouteUiState.composerExpanded
    val composerMode = taskRouteUiState.composerMode
    val dispatchCenterExpanded = taskRouteUiState.dispatchCenterExpanded
    val homeSection = taskRouteUiState.homeSection
    val hostActivity = remember(context) { context.findHostActivity() }
    val composerBottomSheetState = rememberModalBottomSheetState(skipPartiallyExpanded = false)
    val pendingUploadHint = remember(activeSubmissionHintMap, pendingUploadSubmissionId) {
        pendingUploadSubmissionId?.let { submissionId -> activeSubmissionHintMap[submissionId] }
    }
    val isShareQuickDispatchMode = currentSharePayload != null
    val hasRunningWork = remember(activeSubmissionHints) {
        activeSubmissionHints.any { it.running }
    }
    val hasBackendProcessing = remember(processingTasksForSkeleton) {
        processingTasksForSkeleton.isNotEmpty()
    }

    LaunchedEffect(readerSession?.taskId) {
        readerChromeVisible = true
        if (readerSession == null) {
            readerScrollSnapshot = null
        }
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

    suspend fun warmUpCompletedTaskSession(taskId: String, fallbackTitle: String) {
        val normalizedTaskId = taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            return
        }
        if (preparedSessions.containsKey(normalizedTaskId)) {
            return
        }
        val fallbackTask = tasks.firstOrNull { task -> task.taskId == normalizedTaskId }
            ?: MobileTaskListItem(
                taskId = normalizedTaskId,
                title = fallbackTitle.ifBlank { normalizedTaskId },
                status = "COMPLETED",
                progress = 1.0,
                statusMessage = "",
                domain = "",
                mainTopic = "",
                markdownAvailable = true,
                createdAt = Instant.now().toString(),
                lastOpenedAt = ""
            )
        runCatching {
            taskApi.loadTaskMarkdown(normalizedTaskId)
        }.onSuccess { payload ->
            val session = buildReaderSessionFromPayload(fallbackTask, payload) ?: return@onSuccess
            preparedSessions = preparedSessions + (normalizedTaskId to session)
            if (tasks.none { task -> task.taskId == normalizedTaskId }) {
                tasks = listOf(
                    fallbackTask.copy(
                        title = session.title.ifBlank { fallbackTask.title },
                        markdownAvailable = true
                    )
                ) + tasks
            }
        }
    }

    suspend fun refreshTasks(showLoading: Boolean = true) {
        if (!refreshMutex.tryLock()) {
            return
        }
        if (showLoading) {
            listLoading = true
            actionMessage = "正在刷新..."
        }
        try {
            runCatching {
                val visibleTasks = deduplicateTasksByTaskId(
                    taskApi.listTasks(onlyMultiSegment = true)
                )
                val processingTasks = deduplicateTasksByTaskId(
                    taskApi.listTasks(onlyMultiSegment = false)
                        .filter { task -> isProcessingStatus(task.status) }
                )
                visibleTasks to processingTasks
            }.onSuccess { (loaded, runningLoaded) ->
                val ids = loaded.map { it.taskId }.toSet()
                preparedSessions = preparedSessions.filterKeys { it in ids }
                tasks = loaded
                backendProcessingTasks = runningLoaded
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
                        scope.launch {
                            warmUpCompletedTaskSession(
                                taskId = task.taskId,
                                fallbackTitle = task.title.ifBlank { task.taskId }
                            )
                        }
                    }
                }
                taskStatusSnapshot = loaded.associate { it.taskId to it.status }

                if (showLoading) {
                    actionMessage = ""
                }
            }.onFailure { error ->
                if (showLoading) {
                    actionMessage = "加载失败，请稍后重试"
                }
            }
        } finally {
            if (showLoading) {
                listLoading = false
            }
            refreshMutex.unlock()
        }
    }

    fun applyRealtimeTaskUpdate(update: TaskRealtimeUpdate) {
        val normalizedTaskId = update.taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            return
        }
        val normalizedStatus = update.status.trim().ifEmpty { "PROCESSING" }
        val normalizedMessage = update.message.ifBlank { update.errorMessage }.trim()
        val normalizedProgress = when {
            update.progress <= 0.0 -> 0.0
            update.progress <= 1.0 -> update.progress
            update.progress <= 100.0 -> (update.progress / 100.0)
            else -> 1.0
        }
        val previousStatus = taskStatusSnapshot[normalizedTaskId]

        var foundExisting = false
        tasks = tasks.map { task ->
            if (task.taskId != normalizedTaskId) {
                return@map task
            }
            foundExisting = true
            task.copy(
                status = normalizedStatus,
                progress = normalizedProgress.coerceIn(0.0, 1.0),
                statusMessage = normalizedMessage,
                markdownAvailable = task.markdownAvailable || isCompletedStatus(normalizedStatus)
            )
        }
        if (!foundExisting) {
            backendProcessingTasks = deduplicateTasksByTaskId(
                listOf(
                MobileTaskListItem(
                    taskId = normalizedTaskId,
                    title = normalizedTaskId,
                    status = normalizedStatus,
                    progress = normalizedProgress.coerceIn(0.0, 1.0),
                    statusMessage = normalizedMessage,
                    domain = "",
                    mainTopic = "",
                    markdownAvailable = isCompletedStatus(normalizedStatus),
                    createdAt = Instant.now().toString(),
                    lastOpenedAt = ""
                )
                ) + backendProcessingTasks
            )
        }
        if (
            isCompletedStatus(normalizedStatus) ||
            isFailedStatus(normalizedStatus) ||
            normalizedStatus.equals("CANCELLED", ignoreCase = true) ||
            normalizedStatus.equals("CANCELED", ignoreCase = true)
        ) {
            backendProcessingTasks = backendProcessingTasks.filter { task ->
                task.taskId != normalizedTaskId
            }
        }
        tasks = deduplicateTasksByTaskId(tasks)

        if (isTaskNewlyCompleted(normalizedStatus, previousStatus)) {
            val taskTitle = tasks.firstOrNull { it.taskId == normalizedTaskId }?.title
                ?.ifBlank { normalizedTaskId }
                ?: normalizedTaskId
            taskCompletionNotifier.notifyTaskCompleted(
                taskId = normalizedTaskId,
                taskTitle = taskTitle
            )
            completionBanner = CompletionBannerState(
                taskId = normalizedTaskId,
                title = taskTitle
            )
            flashingTaskDeadlines = flashingTaskDeadlines + (
                normalizedTaskId to (System.currentTimeMillis() + TASK_FLASH_DURATION_MS)
                )
            scope.launch {
                warmUpCompletedTaskSession(
                    taskId = normalizedTaskId,
                    fallbackTitle = taskTitle
                )
            }
        }
        taskStatusSnapshot = taskStatusSnapshot + (normalizedTaskId to normalizedStatus)
    }

    val taskRealtimeClient = remember(apiBaseUrl, mobileUserId) {
        TaskRealtimeClient(
            wsEndpoint = CollectionApiFactory.toWebSocketUrl(apiBaseUrl),
            userId = mobileUserId,
            onTaskUpdate = { update ->
                scope.launch {
                    applyRealtimeTaskUpdate(update)
                }
            }
        )
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
            taskRouteViewModel.setComposerMode(TaskComposerMode.URL)
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
            taskRouteViewModel.setComposerMode(TaskComposerMode.URL)
            recordTaskOpened(task.taskId)
            // 记录阅读足迹：打开文章
            scope.launch {
                runCatching {
                    footprintRepo.recordArticleOpened(
                        taskId = task.taskId,
                        taskTitle = session.title.ifBlank { task.taskId }
                    )
                }
            }
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
            domain = "",
            mainTopic = "",
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
            actionMessage = "标题不能为空"
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
        actionMessage = "正在保存标题..."
        runCatching {
            taskApi.renameTaskTitle(taskId = targetTaskId, title = draftTitle)
        }.onSuccess { serverTitle ->
            val finalTitle = serverTitle.trim().ifEmpty { draftTitle }
            syncTaskTitleLocally(targetTaskId, finalTitle)
            actionMessage = ""
        }.onFailure { error ->
            actionMessage = "重命名失败，请稍后重试"
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
    val visibleTaskCards = remember(filteredAndSortedTasks, activeSubmissionTaskIds) {
        filteredAndSortedTasks.filterNot { task ->
            isProcessingStatus(task.status) && task.taskId in activeSubmissionTaskIds
        }
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
        }
    }

    fun updateForceUpdateTarget(versionCode: Int, versionName: String) {
        if (versionCode > 0) {
            forceUpdateVersionCode = versionCode
        }
        if (versionName.isNotBlank()) {
            forceUpdateVersionName = versionName
        }
    }

    fun updateReadyInstallTarget(versionCode: Int, versionName: String) {
        if (versionCode > 0) {
            autoUpdateReadyVersionCode = versionCode
        }
        if (versionName.isNotBlank()) {
            autoUpdateReadyVersionName = versionName
        }
    }

    fun applyAutoUpdateAction(
        updateAction: MobileAppAutoUpdateManager.AutoUpdateAction,
        trigger: String
    ) {
        when (updateAction) {
            is MobileAppAutoUpdateManager.AutoUpdateAction.DownloadStarted -> {
                autoUpdateReadyToInstall = false
                autoUpdateReadyVersionCode = 0
                autoUpdateReadyVersionName = ""
                autoUpdateDownloadInProgress = true
                autoUpdateProgressPercent = 0
                autoUpdateStatusText = "Downloading ${updateAction.versionName} (0%)"
                forceUpdateRequired = updateAction.forceUpdate
                updateForceUpdateTarget(updateAction.versionCode, updateAction.versionName)
                actionMessage = "Detected new version ${updateAction.versionName}, auto download started."
            }

            is MobileAppAutoUpdateManager.AutoUpdateAction.DownloadInProgress -> {
                autoUpdateReadyToInstall = false
                autoUpdateReadyVersionCode = 0
                autoUpdateReadyVersionName = ""
                val progressText = updateAction.progressPercent?.let { "$it%" } ?: "preparing"
                autoUpdateDownloadInProgress = true
                autoUpdateProgressPercent = updateAction.progressPercent
                autoUpdateStatusText = "Downloading ${updateAction.versionName} ($progressText)"
                forceUpdateRequired = updateAction.forceUpdate
                updateForceUpdateTarget(updateAction.versionCode, updateAction.versionName)
                actionMessage = autoUpdateStatusText
            }

            is MobileAppAutoUpdateManager.AutoUpdateAction.ReadyToInstall -> {
                autoUpdateDownloadInProgress = false
                autoUpdateProgressPercent = 100
                autoUpdateReadyToInstall = true
                updateReadyInstallTarget(updateAction.versionCode, updateAction.versionName)
                autoUpdateStatusText = "Version ${updateAction.versionName} is ready. Tap Install now."
                forceUpdateRequired = updateAction.forceUpdate
                updateForceUpdateTarget(updateAction.versionCode, updateAction.versionName)
                actionMessage = "Version ${updateAction.versionName} downloaded. Tap Install now to continue."
            }

            is MobileAppAutoUpdateManager.AutoUpdateAction.InstallPrompted -> {
                autoUpdateDownloadInProgress = false
                autoUpdateProgressPercent = 100
                autoUpdateReadyToInstall = true
                updateReadyInstallTarget(updateAction.versionCode, updateAction.versionName)
                autoUpdateStatusText = "Installer opened for ${updateAction.versionName}."
                forceUpdateRequired = updateAction.forceUpdate
                updateForceUpdateTarget(updateAction.versionCode, updateAction.versionName)
                actionMessage = "Installer opened for ${updateAction.versionName}."
            }

            is MobileAppAutoUpdateManager.AutoUpdateAction.InstallPermissionRequired -> {
                autoUpdateDownloadInProgress = false
                autoUpdateProgressPercent = 100
                autoUpdateReadyToInstall = true
                updateReadyInstallTarget(updateAction.versionCode, updateAction.versionName)
                autoUpdateStatusText = "Install permission required for ${updateAction.versionName}."
                forceUpdateRequired = updateAction.forceUpdate
                updateForceUpdateTarget(updateAction.versionCode, updateAction.versionName)
                actionMessage = "Allow unknown app installs, then tap Install now again."
            }

            is MobileAppAutoUpdateManager.AutoUpdateAction.Failed -> {
                autoUpdateDownloadInProgress = false
                if (!autoUpdateReadyToInstall) {
                    autoUpdateProgressPercent = null
                }
                if (updateAction.forceUpdate || forceUpdateRequired) {
                    forceUpdateRequired = true
                    updateForceUpdateTarget(
                        updateAction.versionCode ?: forceUpdateVersionCode,
                        updateAction.versionName.ifBlank { forceUpdateVersionName }
                    )
                    autoUpdateStatusText = "Mandatory update failed: ${updateAction.message}"
                    actionMessage = "Update flow failed: ${updateAction.message}"
                } else if (autoUpdateReadyToInstall) {
                    autoUpdateStatusText = "Install failed: ${updateAction.message}. Tap Install now to retry."
                    actionMessage = "Install failed: ${updateAction.message}"
                } else if (trigger == "launch" || trigger == "resume") {
                    autoUpdateStatusText = ""
                    actionMessage = "Auto update check failed: ${updateAction.message}"
                }
            }

            is MobileAppAutoUpdateManager.AutoUpdateAction.NoOp -> {
                forceUpdateRequired = false
                forceUpdateVersionCode = 0
                forceUpdateVersionName = ""
                autoUpdateDownloadInProgress = false
                autoUpdateProgressPercent = null
                autoUpdateStatusText = ""
                autoUpdateReadyToInstall = false
                autoUpdateReadyVersionCode = 0
                autoUpdateReadyVersionName = ""
            }
        }
    }

    suspend fun runAutoUpdateCheck(trigger: String) {
        if (!autoUpdateEnabled) {
            applyAutoUpdateAction(MobileAppAutoUpdateManager.AutoUpdateAction.NoOp, trigger)
            return
        }
        val manager = appUpdateManager ?: return
        applyAutoUpdateAction(manager.checkAndAutoUpdate(), trigger)
    }

    suspend fun runAutoUpdateInstall(trigger: String) {
        if (!autoUpdateEnabled) {
            applyAutoUpdateAction(MobileAppAutoUpdateManager.AutoUpdateAction.NoOp, trigger)
            return
        }
        val manager = appUpdateManager ?: return
        applyAutoUpdateAction(manager.promptInstallReadyUpdate(), trigger)
    }

    fun enqueueSubmissionWork(
        mode: String,
        preferredTitle: String,
        videoUrl: String? = null,
        uploadUri: Uri? = null
    ): String? {
        val submissionId = UUID.randomUUID().toString()
        when (mode) {
            SUBMISSION_MODE_UPLOAD -> {
                val uri = uploadUri ?: return null
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
                    return null
                }
                TaskSubmissionForegroundService.startUrlSubmission(
                    context = context.applicationContext,
                    submissionId = submissionId,
                    title = preferredTitle,
                    videoUrl = normalizedUrl
                )
            }
        }
        return submissionId
    }

    fun dispatchSharePayload(payload: ShareIntentPayload): Boolean {
        val sharedUrl = payload.videoUrl?.trim().orEmpty()
        if (sharedUrl.isNotEmpty()) {
            val submissionId = enqueueSubmissionWork(
                mode = SUBMISSION_MODE_URL,
                preferredTitle = deriveSubmissionTitleFromUrl(sharedUrl),
                videoUrl = sharedUrl
            ) ?: return false
            actionMessage = "链接任务已投递到后台队列。"
            clipboardCandidate = null
            pendingUploadSubmissionId = null
            shareDispatchState = ShareDispatchState(
                stage = ShareDispatchStage.PROCESSING,
                message = "正在投递任务，马上进入后台队列。"
            )
            taskRouteViewModel.setComposerExpanded(false)
            taskRouteViewModel.setComposerMode(TaskComposerMode.URL)
            clipboardPromptHistory.markPrompted(sharedUrl)
            return submissionId.isNotBlank()
        }
        val sharedUri = payload.uploadUri ?: return false
        runCatching {
            context.contentResolver.takePersistableUriPermission(
                sharedUri,
                Intent.FLAG_GRANT_READ_URI_PERMISSION
            )
        }
        val preferredTitle = resolveSubmissionTitleFromUri(
            contentResolver = context.contentResolver,
            uri = sharedUri
        )
        val submissionId = enqueueSubmissionWork(
            mode = SUBMISSION_MODE_UPLOAD,
            preferredTitle = preferredTitle,
            uploadUri = sharedUri
        ) ?: return false
        pendingUploadSubmissionId = submissionId
        actionMessage = "文件任务已投递到后台队列。"
        shareDispatchState = ShareDispatchState(
            stage = ShareDispatchStage.PROCESSING,
            message = "正在投递任务，马上进入后台队列。"
        )
        return true
    }

    fun cancelTaskFromUi(taskId: String) {
        val normalizedTaskId = taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            return
        }
        scope.launch {
            actionLoading = true
            actionMessage = "正在取消..."
            runCatching {
                taskApi.cancelTask(normalizedTaskId)
            }.onSuccess { result ->
                actionMessage = result.message.ifBlank {
                    if (result.success) "任务已取消" else "该任务无法取消"
                }
                refreshTasks(showLoading = false)
            }.onFailure { error ->
                actionMessage = "取消失败，请稍后重试"
            }
            actionLoading = false
        }
    }

    val notificationPermissionLauncher = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.RequestPermission()
    ) { }

    val pickDocumentLauncher = rememberLauncherForActivityResult(
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
        scope.launch {
            val normalizedOffsetText = bookPageOffsetInput.trim()
            val pageOffset = if (normalizedOffsetText.isEmpty()) {
                null
            } else {
                normalizedOffsetText.toIntOrNull()
            }
            if (normalizedOffsetText.isNotEmpty() && pageOffset == null) {
                actionMessage = "页码偏移必须是整数，例如 12 或 -12"
                return@launch
            }
            actionLoading = true
            actionMessage = "正在上传书籍并探测目录..."
            runCatching {
                taskApi.uploadVideoFile(
                    contentResolver = context.contentResolver,
                    uri = uri,
                    probeOnly = true
                )
            }.onSuccess { uploadResult ->
                if (!uploadResult.success) {
                    actionMessage = uploadResult.message.ifBlank { "上传失败，请重试" }
                    return@onSuccess
                }
                val probeInput = uploadResult.normalizedVideoUrl.trim()
                if (probeInput.isBlank()) {
                    actionMessage = "上传成功，但未返回可探测的文件路径"
                    return@onSuccess
                }
                pendingUploadSubmissionId = null
                taskRouteViewModel.setVideoUrlInput(probeInput)
                taskRouteViewModel.setComposerExpanded(true)
                taskRouteViewModel.setComposerMode(TaskComposerMode.URL)
                collectionViewModel.setProbePreviewDocumentUri(uri.toString())
                collectionViewModel.probeVideoInput(probeInput, pageOffset)
                actionMessage = "已完成探测，请确认实际起始页与章节后再提交。"
            }.onFailure { error ->
                actionMessage = "上传探测失败: ${error.message ?: "unknown"}"
            }
            actionLoading = false
        }
    }

    fun cancelSubmissionHintFromUi(hint: ActiveSubmissionHint) {
        val taskId = hint.taskId?.trim().orEmpty()
        if (hint.workId.startsWith(BACKEND_PROCESSING_HINT_WORK_PREFIX) && taskId.isNotEmpty()) {
            cancelTaskFromUi(taskId)
            return
        }
        TaskSubmissionForegroundService.cancelSubmission(
            context.applicationContext,
            hint.workId
        )
    }

    LaunchedEffect(shareIntentEvents) {
        shareIntentEvents?.collect { payload ->
            currentSharePayload = payload
            shareDispatchState = null
        }
    }

    LaunchedEffect(notificationTaskOpenEvents) {
        notificationTaskOpenEvents?.collect { request ->
            pendingNotificationTaskOpenRequest = request
        }
    }

    LaunchedEffect(pendingNotificationTaskOpenRequest?.taskId) {
        val request = pendingNotificationTaskOpenRequest ?: return@LaunchedEffect
        pendingNotificationTaskOpenRequest = null
        val normalizedTaskId = request.taskId.trim()
        if (normalizedTaskId.isEmpty()) {
            return@LaunchedEffect
        }
        taskRouteViewModel.setHomeSection(HomeSection.TASKS)
        taskRouteViewModel.setComposerExpanded(false)
        taskRouteViewModel.setComposerMode(TaskComposerMode.URL)
        refreshTasks(showLoading = false)
        warmUpCompletedTaskSession(
            taskId = normalizedTaskId,
            fallbackTitle = request.taskTitle.ifBlank { normalizedTaskId }
        )
        openTaskById(
            taskId = normalizedTaskId,
            fallbackTitle = request.taskTitle.ifBlank { normalizedTaskId }
        )
    }

    LaunchedEffect(
        isShareQuickDispatchMode,
        currentSharePayload?.videoUrl,
        currentSharePayload?.uploadUri,
        shareDispatchState
    ) {
        if (!isShareQuickDispatchMode) {
            return@LaunchedEffect
        }
        val payload = currentSharePayload ?: return@LaunchedEffect
        if (shareDispatchState != null) {
            return@LaunchedEffect
        }
        val dispatched = dispatchSharePayload(payload)
        if (!dispatched) {
            shareDispatchState = ShareDispatchState(
                stage = ShareDispatchStage.FAILED,
                message = "任务投递失败，请返回后重试。"
            )
            return@LaunchedEffect
        }
        scope.launch { refreshTasks(showLoading = false) }
        delay(520)
        shareDispatchState = ShareDispatchState(
            stage = ShareDispatchStage.QUEUED,
            message = "任务已进入后台队列。"
        )
    }

    LaunchedEffect(Unit) {
        if (isShareQuickDispatchMode) {
            return@LaunchedEffect
        }
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
        runAutoUpdateCheck(trigger = "launch")
        inspectClipboardForTaskCandidate()
    }

    LaunchedEffect(autoUpdateDownloadInProgress) {
        if (!autoUpdateEnabled || !autoUpdateDownloadInProgress) {
            return@LaunchedEffect
        }
        while (autoUpdateDownloadInProgress) {
            delay(1_000)
            runAutoUpdateCheck(trigger = "monitor")
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
                    // 记录阅读足迹：视频任务创建
                    if (!event.taskId.isNullOrBlank()) {
                        scope.launch {
                            runCatching {
                                footprintRepo.recordVideoTaskCreated(
                                    taskId = event.taskId,
                                    taskTitle = event.title.orEmpty().ifBlank { event.taskId }
                                )
                            }
                        }
                    }
                }

                SubmissionEventType.SUCCEEDED -> {
                    actionMessage = "任务已完成，点击卡片可立即查看。"
                    if (!event.taskId.isNullOrBlank()) {
                        scope.launch {
                            warmUpCompletedTaskSession(
                                taskId = event.taskId,
                                fallbackTitle = event.title.orEmpty().ifBlank { event.taskId }
                            )
                        }
                    }
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

    fun openComposer() {
        val pendingCandidate = clipboardCandidate
        if (pendingCandidate != null && videoUrlInput.isBlank()) {
            taskRouteViewModel.setVideoUrlInput(pendingCandidate.normalizedUrl)
            clipboardPromptHistory.markPrompted(pendingCandidate.normalizedUrl)
        }
        taskRouteViewModel.setComposerMode(TaskComposerMode.URL)
        taskRouteViewModel.setComposerExpanded(true)
    }

    fun triggerVideoProbe() {
        val normalized = videoUrlInput.trim()
        if (normalized.isEmpty()) {
            actionMessage = "请先输入链接"
            return
        }
        val normalizedOffsetText = bookPageOffsetInput.trim()
        val pageOffset = if (normalizedOffsetText.isEmpty()) {
            null
        } else {
            normalizedOffsetText.toIntOrNull()
        }
        if (normalizedOffsetText.isNotEmpty() && pageOffset == null) {
            actionMessage = "页码偏移必须是整数，例如：8 或 -12"
            return
        }
        collectionViewModel.setProbePreviewDocumentUri(null)
        collectionViewModel.probeVideoInput(normalized, pageOffset)
    }

    LaunchedEffect(Unit) {
        collectionViewModel.events.collect { event ->
            when (event) {
                is CollectionUiEvent.Snackbar -> {
                    snackbarHostState.showSnackbar(event.message)
                }

                is CollectionUiEvent.SingleTaskSubmitted -> {
                    actionMessage = event.message
                    // 将已提交的 taskId 注册到后台服务监控
                    // 使 Probe 路径也能获得骨架卡片、进度通知和完成通知
                    val trackingId = java.util.UUID.randomUUID().toString()
                    TaskSubmissionForegroundService.startTaskTracking(
                        context = context.applicationContext,
                        submissionId = trackingId,
                        taskId = event.taskId,
                        title = event.title
                    )
                    scope.launch { refreshTasks(showLoading = false) }
                    snackbarHostState.showSnackbar(
                        "《${event.title.take(20)}》已提交，后台处理中"
                    )
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

    LaunchedEffect(completionBanner?.taskId) {
        val banner = completionBanner ?: return@LaunchedEffect
        delay(8_000)
        if (completionBanner?.taskId == banner.taskId) {
            completionBanner = null
        }
    }

    DisposableEffect(lifecycleOwner, readerSession, taskRealtimeClient, isShareQuickDispatchMode) {
        if (isShareQuickDispatchMode) {
            onDispose {}
        } else {
            val observer = LifecycleEventObserver { _, event ->
                when (event) {
                    Lifecycle.Event.ON_START -> {
                        taskRealtimeClient.connect()
                    }

                    Lifecycle.Event.ON_RESUME -> {
                        inspectClipboardForTaskCandidate()
                        scope.launch {
                            refreshTasks(showLoading = false)
                            runAutoUpdateCheck(trigger = "resume")
                        }
                    }

                    Lifecycle.Event.ON_STOP -> {
                        val activeSession = readerSession
                        if (activeSession != null) {
                            val snapshot = readerScrollSnapshot
                            if (snapshot != null && snapshot.taskId == activeSession.taskId) {
                                readerScrollPositionStore.save(
                                    taskId = activeSession.taskId,
                                    position = snapshot.position,
                                    sync = true
                                )
                            }
                        }
                        taskRealtimeClient.disconnect()
                    }

                    else -> {}
                }
            }
            lifecycleOwner.lifecycle.addObserver(observer)
            taskRealtimeClient.connect()
            onDispose {
                taskRealtimeClient.disconnect()
                lifecycleOwner.lifecycle.removeObserver(observer)
            }
        }
    }

    if (autoUpdateEnabled && forceUpdateRequired) {
        ForceUpdateBlockingRoute(
            versionName = forceUpdateVersionName,
            versionCode = forceUpdateVersionCode,
            statusText = autoUpdateStatusText,
            downloading = autoUpdateDownloadInProgress,
            progressPercent = autoUpdateProgressPercent,
            installReady = autoUpdateReadyToInstall,
            onInstallNow = {
                scope.launch {
                    runAutoUpdateInstall(trigger = "force_install_now")
                }
            },
            onRetryUpdate = {
                scope.launch {
                    runAutoUpdateCheck(trigger = "force_manual")
                }
            }
        )
        return
    }

    if (isShareQuickDispatchMode) {
        val payload = currentSharePayload ?: return
        ShareIntentDispatchRoute(
            payload = payload,
            state = shareDispatchState,
            onClose = {
                currentSharePayload = null
                hostActivity?.finish()
            }
        )
        return
    }

    if (readerSession != null) {
        val session = readerSession ?: return
        BackHandler {
            readerSession = null
        }
        val savedReaderScrollPosition = remember(session.taskId) {
            readerScrollPositionStore.load(session.taskId)
        }
        val initialReaderScrollPosition = remember(
            session.taskId,
            session.nodes.size,
            savedReaderScrollPosition
        ) {
            val maxIndex = (session.nodes.size - 1).coerceAtLeast(0)
            TaskReaderScrollPosition(
                firstVisibleItemIndex = (savedReaderScrollPosition?.firstVisibleItemIndex ?: 0)
                    .coerceIn(0, maxIndex),
                firstVisibleItemScrollOffset = (savedReaderScrollPosition?.firstVisibleItemScrollOffset ?: 0)
                    .coerceAtLeast(0)
            )
        }
        var latestReaderScrollPosition by remember(session.taskId) {
            mutableStateOf(initialReaderScrollPosition)
        }
        var checkpointReaderScrollPosition by remember(session.taskId) {
            mutableStateOf(initialReaderScrollPosition)
        }
        LaunchedEffect(session.taskId, initialReaderScrollPosition) {
            readerScrollSnapshot = ReaderScrollSnapshot(
                taskId = session.taskId,
                position = initialReaderScrollPosition
            )
        }

        DisposableEffect(session.taskId) {
            onDispose {
                readerScrollPositionStore.save(
                    taskId = session.taskId,
                    position = latestReaderScrollPosition,
                    sync = true
                )
                if (readerScrollSnapshot?.taskId == session.taskId) {
                    readerScrollSnapshot = null
                }
            }
        }

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
                            TextButton(
                                onClick = {
                                    readerScrollPositionStore.save(
                                        taskId = session.taskId,
                                        position = latestReaderScrollPosition,
                                        sync = true
                                    )
                                    readerSession = null
                                }
                            ) {
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
                        apiBaseUrl = apiBaseUrl,
                        pathHint = session.pathHint,
                        metaApi = metaApi,
                        telemetryApi = telemetryApi,
                        cardApi = cardApi,
                        initialFirstVisibleItemIndex = initialReaderScrollPosition.firstVisibleItemIndex,
                        initialFirstVisibleItemScrollOffset = initialReaderScrollPosition.firstVisibleItemScrollOffset,
                        onScrollDown = {
                            if (readerChromeVisible) {
                                readerChromeVisible = false
                            }
                        },
                        onScrollUp = {},
                        onBlankTap = {
                            if (!readerChromeVisible) {
                                readerChromeVisible = true
                            }
                        },
                        onReadingPositionChanged = { index, offset ->
                            val maxIndex = (session.nodes.size - 1).coerceAtLeast(0)
                            val normalizedPosition = TaskReaderScrollPosition(
                                firstVisibleItemIndex = index.coerceIn(0, maxIndex),
                                firstVisibleItemScrollOffset = offset.coerceAtLeast(0)
                            )
                            latestReaderScrollPosition = normalizedPosition
                            readerScrollSnapshot = ReaderScrollSnapshot(
                                taskId = session.taskId,
                                position = normalizedPosition
                            )
                            val indexChanged = normalizedPosition.firstVisibleItemIndex !=
                                checkpointReaderScrollPosition.firstVisibleItemIndex
                            val offsetDelta = kotlin.math.abs(
                                normalizedPosition.firstVisibleItemScrollOffset -
                                    checkpointReaderScrollPosition.firstVisibleItemScrollOffset
                            )
                            if (indexChanged || offsetDelta >= READER_SCROLL_SAVE_OFFSET_DELTA_PX) {
                                readerScrollPositionStore.save(session.taskId, normalizedPosition)
                                checkpointReaderScrollPosition = normalizedPosition
                            }
                        },
                        onTelemetry = { telemetryEvent ->
                            // 根据 telemetry 事件类型记录阅读足迹
                            val scrollIdx = latestReaderScrollPosition.firstVisibleItemIndex
                            val title = session.title.ifBlank { session.taskId }
                            when (telemetryEvent.eventType) {
                                "resonance_toggle", "swipe_right_favorite" -> {
                                    scope.launch {
                                        runCatching {
                                            footprintRepo.recordParagraphBold(
                                                taskId = session.taskId,
                                                taskTitle = title,
                                                blockId = telemetryEvent.nodeId,
                                                snippetText = telemetryEvent.payload["text"].orEmpty().take(200),
                                                scrollIndex = scrollIdx
                                            )
                                        }
                                    }
                                }
                                "token_segment", "token_double_tap" -> {
                                    scope.launch {
                                        runCatching {
                                            footprintRepo.recordTokenDoubleClick(
                                                taskId = session.taskId,
                                                taskTitle = title,
                                                blockId = telemetryEvent.nodeId,
                                                token = telemetryEvent.payload["token"].orEmpty()
                                                    .ifBlank { telemetryEvent.payload["text"].orEmpty() },
                                                tokenStart = telemetryEvent.payload["start"]?.toIntOrNull() ?: -1,
                                                tokenEnd = telemetryEvent.payload["end"]?.toIntOrNull() ?: -1,
                                                scrollIndex = scrollIdx
                                            )
                                        }
                                    }
                                }
                                "comment_panel_opened" -> {
                                    scope.launch {
                                        runCatching {
                                            footprintRepo.recordAnnotationAdded(
                                                taskId = session.taskId,
                                                taskTitle = title,
                                                blockId = telemetryEvent.nodeId,
                                                annotationText = telemetryEvent.payload["source"].orEmpty(),
                                                scrollIndex = scrollIdx
                                            )
                                        }
                                    }
                                }
                                "insight_term_tapped" -> {
                                    scope.launch {
                                        runCatching {
                                            footprintRepo.recordInsightCardViewed(
                                                taskId = session.taskId,
                                                taskTitle = title,
                                                blockId = telemetryEvent.nodeId,
                                                insightTag = telemetryEvent.payload["term"].orEmpty()
                                                    .ifBlank { telemetryEvent.payload["token"].orEmpty() },
                                                scrollIndex = scrollIdx
                                            )
                                        }
                                    }
                                }
                                "anchor_created" -> {
                                    scope.launch {
                                        runCatching {
                                            footprintRepo.recordTokenDoubleClick(
                                                taskId = session.taskId,
                                                taskTitle = title,
                                                blockId = telemetryEvent.nodeId,
                                                token = telemetryEvent.payload["quote"].orEmpty()
                                                    .ifBlank { telemetryEvent.payload["token"].orEmpty() },
                                                tokenStart = telemetryEvent.payload["start"]?.toIntOrNull() ?: -1,
                                                tokenEnd = telemetryEvent.payload["end"]?.toIntOrNull() ?: -1,
                                                scrollIndex = scrollIdx
                                            )
                                        }
                                    }
                                }
                                "mounted_note_opened" -> {
                                    scope.launch {
                                        runCatching {
                                            footprintRepo.recordAnnotationAdded(
                                                taskId = session.taskId,
                                                taskTitle = title,
                                                blockId = telemetryEvent.nodeId,
                                                annotationText = telemetryEvent.payload["quote"].orEmpty()
                                                    .ifBlank { telemetryEvent.payload["anchorId"].orEmpty() },
                                                scrollIndex = scrollIdx
                                            )
                                        }
                                    }
                                }
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

                val readerActiveTaskCount = runningTaskCount
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
                            readerScrollPositionStore.save(
                                taskId = session.taskId,
                                position = latestReaderScrollPosition,
                                sync = true
                            )
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
                                text = "后台任务 $readerActiveTaskCount",
                                color = Color.White
                            )
                        }
                    }
                }
            }
        }
        return
    }

    if (homeSection == HomeSection.FOOTPRINTS) {
        ReadingFootprintTimeline(
            footprintsFlow = footprintRepo.observeAllFootprints(),
            onBack = {
                taskRouteViewModel.setHomeSection(HomeSection.TASKS)
            },
            onNavigateToArticle = { target ->
                scope.launch {
                    openTaskById(target.taskId, target.taskTitle)
                }
            }
        )
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
                PullToRefreshBox(
                    isRefreshing = listLoading,
                    onRefresh = { scope.launch { refreshTasks(showLoading = true) } },
                    modifier = Modifier.fillMaxSize()
                ) {
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
                            text = "我的任务",
                            fontWeight = FontWeight.SemiBold
                        )
                        Row(horizontalArrangement = Arrangement.spacedBy(4.dp)) {
                            TextButton(
                                onClick = {
                                    taskRouteViewModel.setHomeSection(HomeSection.FOOTPRINTS)
                                },
                                enabled = !actionLoading
                            ) {
                                Text("阅读足迹")
                            }
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
                }

                item {
                    OutlinedTextField(
                        value = taskSearchQuery,
                        onValueChange = { taskRouteViewModel.setTaskSearchQuery(it) },
                        modifier = Modifier.fillMaxWidth(),
                        singleLine = true,
                        label = { Text("搜索任务") },
                        placeholder = { Text("按标题搜索") },
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
                            Text("排序：${taskSortFieldLabel(taskSortField)}")
                        }
                        TextButton(
                            onClick = {
                                taskRouteViewModel.toggleTaskSortOrder()
                            },
                            enabled = !actionLoading
                        ) {
                            Text("顺序：${if (taskSortOrder == SortOrder.DESC) "最新" else "最旧"}")
                        }
                    }
                }

                item {
                    Text(
                        text = "显示 ${visibleTaskCards.size} / ${tasks.size} 条",
                        color = Color(0xFF667085)
                    )
                }

                if (activeSubmissionHints.isNotEmpty()) {
                    items(activeSubmissionHints, key = { "submission-${it.workId}" }) { hint ->
                        BackgroundSubmissionSkeletonCard(
                            hint = hint,
                            onCancel = { cancelSubmissionHintFromUi(hint) },
                            enabled = !actionLoading
                        )
                    }
                }

                if (!listLoading && visibleTaskCards.isEmpty() && activeSubmissionHints.isEmpty()) {
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

                items(visibleTaskCards, key = { it.taskId }) { task ->
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
                }

                if (composerExpanded) {
                    ModalBottomSheet(
                        onDismissRequest = {
                            taskRouteViewModel.setComposerExpanded(false)
                            taskRouteViewModel.setComposerMode(TaskComposerMode.URL)
                        },
                        sheetState = composerBottomSheetState
                    ) {
                        val probeLoading = probeState is ProbeUiState.Loading
                        val probeError = (probeState as? ProbeUiState.Error)?.message
                        val pendingCandidate = clipboardCandidate
                        val uploadHint = pendingUploadHint
                        Column(
                            modifier = Modifier
                                .fillMaxWidth()
                                .heightIn(min = 320.dp)
                                .padding(horizontal = 16.dp, vertical = 12.dp)
                                .navigationBarsPadding(),
                            verticalArrangement = Arrangement.spacedBy(12.dp)
                        ) {
                            Text(
                                text = "添加新内容",
                                fontWeight = FontWeight.SemiBold
                            )
                            if (pendingCandidate != null && videoUrlInput == pendingCandidate.normalizedUrl) {
                                Text(
                                    text = "已自动读取剪贴板链接",
                                    color = Color(0xFF175CD3),
                                    style = MaterialTheme.typography.bodySmall
                                )
                            }
                            Row(
                                modifier = Modifier.fillMaxWidth(),
                                verticalAlignment = Alignment.Bottom
                            ) {
                                OutlinedTextField(
                                    value = videoUrlInput,
                                    onValueChange = { taskRouteViewModel.setVideoUrlInput(it) },
                                    modifier = Modifier.weight(1f),
                                    singleLine = true,
                                    label = { Text("B站 / 抖音 / 书籍链接") },
                                    placeholder = { Text("粘贴链接，或点击右侧上传文件") },
                                    enabled = !actionLoading && !probeLoading,
                                    keyboardOptions = KeyboardOptions(imeAction = ImeAction.Search),
                                    keyboardActions = KeyboardActions(
                                        onSearch = { triggerVideoProbe() },
                                        onDone = { triggerVideoProbe() }
                                    )
                                )
                                Spacer(modifier = Modifier.width(8.dp))
                                IconButton(
                                    onClick = {
                                        pickDocumentLauncher.launch(
                                            arrayOf(
                                                "application/pdf",
                                                "application/epub+zip",
                                                "application/octet-stream"
                                            )
                                        )
                                    },
                                    enabled = !actionLoading
                                ) {
                                    Icon(
                                        imageVector = Icons.Filled.Add,
                                        contentDescription = "上传文件"
                                    )
                                }
                            }
                            OutlinedTextField(
                                value = bookPageOffsetInput,
                                onValueChange = { taskRouteViewModel.setBookPageOffsetInput(it) },
                                modifier = Modifier.fillMaxWidth(),
                                singleLine = true,
                                label = { Text("兼ebook页码偏移（可选）") },
                                placeholder = { Text("书页号 + 偏移 = PDF页码，如：8 或 -12") },
                                enabled = !actionLoading && !probeLoading,
                                keyboardOptions = KeyboardOptions(
                                    imeAction = ImeAction.Done,
                                    keyboardType = KeyboardType.Text
                                )
                            )
                            if (uploadHint != null) {
                                Card(
                                    modifier = Modifier.fillMaxWidth(),
                                    colors = CardDefaults.cardColors(containerColor = Color(0xFFFFF9DB))
                                ) {
                                    Column(
                                        modifier = Modifier
                                            .fillMaxWidth()
                                            .padding(horizontal = 12.dp, vertical = 10.dp),
                                        verticalArrangement = Arrangement.spacedBy(8.dp)
                                    ) {
                                        Text(
                                            text = uploadHint.title,
                                            fontWeight = FontWeight.Medium,
                                            maxLines = 1,
                                            overflow = TextOverflow.Ellipsis
                                        )
                                        Text(
                                            text = uploadHint.phaseText,
                                            color = Color(0xFF475467),
                                            maxLines = 2,
                                            overflow = TextOverflow.Ellipsis
                                        )
                                        val percent = uploadHint.progressPercent?.coerceIn(0, 100)
                                        if (percent != null) {
                                            LinearProgressIndicator(
                                                progress = { percent / 100f },
                                                modifier = Modifier.fillMaxWidth()
                                            )
                                            Text(
                                                text = "进度 $percent%",
                                                color = Color(0xFF667085)
                                            )
                                        } else {
                                            LinearProgressIndicator(modifier = Modifier.fillMaxWidth())
                                        }
                                    }
                                }
                            }
                            AnimatedVisibility(visible = probeLoading) {
                                ProbeDetectingSkeleton()
                            }
                            if (!probeError.isNullOrBlank()) {
                                Text(
                                    text = probeError,
                                    color = Color(0xFFB42318)
                                )
                            }
                            Button(
                                onClick = { triggerVideoProbe() },
                                enabled = !actionLoading && !probeLoading,
                                modifier = Modifier.fillMaxWidth()
                            ) {
                                if (probeLoading) {
                                    CircularProgressIndicator(
                                        modifier = Modifier.size(16.dp),
                                        strokeWidth = 2.dp,
                                        color = Color.White
                                    )
                                    Spacer(modifier = Modifier.width(8.dp))
                                }
                                Text("解析并开始")
                            }
                        }
                    }
                }

            AnimatedVisibility(
                visible = clipboardCandidate != null && !composerExpanded,
                modifier = Modifier
                    .align(Alignment.TopCenter)
                    .statusBarsPadding()
                    .padding(horizontal = 16.dp, vertical = 8.dp),
                enter = fadeIn(animationSpec = tween(durationMillis = 180)) +
                    slideInVertically(
                        animationSpec = tween(durationMillis = 200),
                        initialOffsetY = { -it / 2 }
                    ),
                exit = fadeOut(animationSpec = tween(durationMillis = 140)) +
                    slideOutVertically(
                        animationSpec = tween(durationMillis = 160),
                        targetOffsetY = { -it }
                    )
            ) {
                val candidate = clipboardCandidate
                if (candidate != null) {
                    ClipboardQuickActionPill(
                        candidateUrl = candidate.displayUrl,
                        onGenerateNow = {
                            val submissionId = enqueueSubmissionWork(
                                mode = SUBMISSION_MODE_URL,
                                preferredTitle = deriveSubmissionTitleFromUrl(candidate.normalizedUrl),
                                videoUrl = candidate.normalizedUrl
                            )
                            if (submissionId == null) {
                                actionMessage = "无法提交链接任务，请稍后重试。"
                                return@ClipboardQuickActionPill
                            }
                            clipboardPromptHistory.markPrompted(candidate.normalizedUrl)
                            clipboardCandidate = null
                            actionMessage = "链接任务已进入后台队列。"
                            scope.launch { refreshTasks(showLoading = false) }
                        },
                        onDismiss = {
                            clipboardPromptHistory.markPrompted(candidate.normalizedUrl)
                            clipboardCandidate = null
                        }
                    )
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
                                scope.launch {
                                    openTaskById(
                                        taskId = banner.taskId,
                                        fallbackTitle = banner.title
                                    )
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

            val activeTaskCount = runningTaskCount
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
                            text = "任务调度中心 $activeTaskCount",
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
                        Text("任务调度中心", fontWeight = FontWeight.SemiBold)
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
                                    onClick = { cancelSubmissionHintFromUi(hint) }
                                ) {
                                    Text("取消")
                                }
                            }
                        }
                        dispatchCenterRunningTasks.forEach { runningTask ->
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
                visible = actionLoading ||
                    listLoading ||
                    actionMessage.isNotBlank() ||
                    autoUpdateDownloadInProgress ||
                    (!forceUpdateRequired && autoUpdateReadyToInstall) ||
                    (!forceUpdateRequired && autoUpdateStatusText.isNotBlank()),
                modifier = Modifier
                    .align(Alignment.BottomCenter)
                    .padding(horizontal = 16.dp, vertical = 28.dp),
                enter = fadeIn(animationSpec = tween(durationMillis = 150)),
                exit = fadeOut(animationSpec = tween(durationMillis = 150))
            ) {
                Card(modifier = Modifier.fillMaxWidth()) {
                    Column(
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(horizontal = 12.dp, vertical = 10.dp),
                        verticalArrangement = Arrangement.spacedBy(8.dp)
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
                                actionMessage.ifBlank { autoUpdateStatusText }
                            },
                            color = Color(0xFF475467),
                            maxLines = 2,
                            overflow = TextOverflow.Ellipsis
                        )
                        if (autoUpdateDownloadInProgress) {
                            val normalizedProgress = autoUpdateProgressPercent?.coerceIn(0, 100)
                            val progressText = normalizedProgress?.let { "$it%" } ?: "preparing..."
                            if (normalizedProgress != null) {
                                LinearProgressIndicator(
                                    progress = { normalizedProgress / 100f },
                                    modifier = Modifier.fillMaxWidth()
                                )
                            } else {
                                LinearProgressIndicator(modifier = Modifier.fillMaxWidth())
                            }
                            Text(
                                text = "Update download progress: $progressText",
                                color = Color(0xFF475467)
                            )
                        }
                        if (!forceUpdateRequired && autoUpdateReadyToInstall) {
                            val readyVersionText = when {
                                autoUpdateReadyVersionName.isNotBlank() && autoUpdateReadyVersionCode > 0 -> {
                                    "${autoUpdateReadyVersionName} (${autoUpdateReadyVersionCode})"
                                }
                                autoUpdateReadyVersionName.isNotBlank() -> autoUpdateReadyVersionName
                                autoUpdateReadyVersionCode > 0 -> "version ${autoUpdateReadyVersionCode}"
                                else -> "latest version"
                            }
                            Button(
                                onClick = {
                                    scope.launch {
                                        runAutoUpdateInstall(trigger = "soft_install_now")
                                    }
                                },
                                enabled = !actionLoading
                            ) {
                                Text("Install $readyVersionText Now")
                            }
                        }
                    }
                }
            }

            FloatingActionButton(
                onClick = {
                    haptic.performHapticFeedback(HapticFeedbackType.LongPress)
                    if (composerExpanded) {
                        taskRouteViewModel.setComposerExpanded(false)
                        taskRouteViewModel.setComposerMode(TaskComposerMode.URL)
                    } else {
                        openComposer()
                    }
                },
                modifier = Modifier
                    .align(Alignment.BottomEnd)
                    .padding(horizontal = 20.dp, vertical = 20.dp)
            ) {
                Icon(
                    imageVector = Icons.Filled.Add,
                    contentDescription = if (composerExpanded) "关闭任务抽屉" else "打开任务抽屉"
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
                    confirmedStartPage = confirmedProbeStartPage,
                    previewDocumentUri = probePreviewDocumentUri,
                    submitting = probeSubmitInProgress,
                    onDismiss = { collectionViewModel.clearProbeResult() },
                    onSubmitSingle = { collectionViewModel.submitDetectedSingleVideo() },
                    onSubmitCollection = { collectionViewModel.submitDetectedCollectionBatch() },
                    onSelectAll = { collectionViewModel.selectAllEpisodes() },
                    onInvertSelection = { collectionViewModel.invertEpisodeSelection() },
                    onToggleEpisode = { episodeNo ->
                        collectionViewModel.toggleEpisodeSelection(episodeNo)
                    },
                    onConfirmedStartPageChange = { startPage ->
                        collectionViewModel.updateConfirmedStartPage(startPage)
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
private fun ForceUpdateBlockingRoute(
    versionName: String,
    versionCode: Int,
    statusText: String,
    downloading: Boolean,
    progressPercent: Int?,
    installReady: Boolean,
    onInstallNow: () -> Unit,
    onRetryUpdate: () -> Unit
) {
    val normalizedVersionName = versionName.trim().ifEmpty { "latest" }
    val versionLabel = if (versionCode > 0) {
        "$normalizedVersionName ($versionCode)"
    } else {
        normalizedVersionName
    }
    val normalizedStatusText = when {
        statusText.isNotBlank() -> statusText
        downloading -> "Downloading mandatory update..."
        else -> "This app version is no longer supported. Update is required to continue."
    }
    val normalizedProgress = progressPercent?.coerceIn(0, 100)
    Surface(modifier = Modifier.fillMaxSize()) {
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(horizontal = 20.dp, vertical = 28.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp, Alignment.CenterVertically),
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            Text(
                text = "Mandatory Update Required",
                style = MaterialTheme.typography.headlineSmall,
                fontWeight = FontWeight.SemiBold
            )
            Text(
                text = "Install version $versionLabel before continuing to use this app.",
                color = Color(0xFF475467)
            )
            Card(modifier = Modifier.fillMaxWidth()) {
                Column(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(horizontal = 14.dp, vertical = 14.dp),
                    verticalArrangement = Arrangement.spacedBy(10.dp)
                ) {
                    Text(
                        text = normalizedStatusText,
                        color = Color(0xFF344054)
                    )
                    if (normalizedProgress != null) {
                        LinearProgressIndicator(
                            progress = { normalizedProgress / 100f },
                            modifier = Modifier.fillMaxWidth()
                        )
                        Text(
                            text = "Download progress: $normalizedProgress%",
                            color = Color(0xFF667085)
                        )
                    } else if (downloading) {
                        LinearProgressIndicator(modifier = Modifier.fillMaxWidth())
                    }
                }
            }
            if (installReady) {
                Button(
                    onClick = onInstallNow
                ) {
                    Text("Install Now")
                }
                TextButton(
                    onClick = onRetryUpdate,
                    enabled = !downloading
                ) {
                    Text(
                        text = if (downloading) {
                            "Downloading..."
                        } else {
                            "Recheck Update"
                        }
                    )
                }
            } else {
                Button(
                    onClick = onRetryUpdate,
                    enabled = !downloading
                ) {
                    Text(
                        text = if (downloading) {
                            "Downloading..."
                        } else {
                            "Retry Update"
                        }
                    )
                }
            }
        }
    }
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
                if (task.collectionPath.isNotBlank()) {
                    Text(
                        text = "Path: ${task.collectionPath.trim()}",
                        color = Color(0xFF475467),
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
                if (task.domain.isNotBlank()) {
                    Text(
                        text = "Domain: ${task.domain.trim()}",
                        color = Color(0xFF344054),
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis
                    )
                }
                if (task.mainTopic.isNotBlank()) {
                    Text(
                        text = "Topic: ${task.mainTopic.trim()}",
                        color = Color(0xFF344054),
                        maxLines = 2,
                        overflow = TextOverflow.Ellipsis
                    )
                }
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
    return isBilibiliHost(host) || isDouyinHost(host) || isZhihuHost(host) || isJuejinHost(host)
}

private fun isBilibiliHost(host: String): Boolean {
    val normalized = host.lowercase()
    return BILIBILI_HOST_SUFFIXES.any { suffix ->
        normalized == suffix || normalized.endsWith(".$suffix")
    }
}

private fun isDouyinHost(host: String): Boolean {
    val normalized = host.lowercase()
    return DOUYIN_HOST_SUFFIXES.any { suffix ->
        normalized == suffix || normalized.endsWith(".$suffix")
    }
}

private fun isZhihuHost(host: String): Boolean {
    val normalized = host.lowercase()
    return ZHIHU_HOST_SUFFIXES.any { suffix ->
        normalized == suffix || normalized.endsWith(".$suffix")
    }
}

private fun isJuejinHost(host: String): Boolean {
    val normalized = host.lowercase()
    return JUEJIN_HOST_SUFFIXES.any { suffix ->
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

    val nodes = buildSingleNodeForReader(
        markdown = payload.markdown,
        parsedNodes = parsedNodes
    ) ?: return null

    val sanitizedNodes = sanitizeNodesForReader(nodes)

    return TaskReaderSession(
        taskId = payload.taskId.ifBlank { task.taskId },
        title = payload.title.ifBlank { task.title },
        pathHint = resolvePathHint(payload.markdownPath, payload.baseDir),
        nodes = sanitizedNodes
    )
}

private fun buildSingleNodeForReader(
    markdown: String,
    parsedNodes: List<SemanticNode>
): List<SemanticNode>? {
    val normalizedMarkdown = markdown
        .replace("\r\n", "\n")
    val normalizedMarkdownHasContent = normalizedMarkdown.any { ch -> !ch.isWhitespace() }
    val fallbackMarkdown = parsedNodes
        .mapNotNull { node ->
            val body = (node.originalMarkdown ?: node.text)
                .replace("\r\n", "\n")
            if (body.any { ch -> !ch.isWhitespace() }) body else null
        }
        .joinToString("\n\n")
    val mergedMarkdown = if (normalizedMarkdownHasContent) {
        normalizedMarkdown
    } else {
        fallbackMarkdown
    }
    if (mergedMarkdown.isBlank()) {
        return null
    }

    val mergedInsightTerms = parsedNodes
        .flatMap { node -> node.resolvedInsightTerms() }
        .map { token -> token.trim() }
        .filter { token -> token.isNotBlank() }
        .distinct()
    val mergedReasoning = parsedNodes
        .firstNotNullOfOrNull { node -> node.reasoning?.takeIf { it.isNotBlank() } }

    val singleNode = SemanticNode(
        id = "md_root",
        text = mergedMarkdown,
        type = "paragraph",
        originalMarkdown = mergedMarkdown,
        relevanceScore = 1f,
        reasoning = mergedReasoning,
        insightTerms = mergedInsightTerms,
        insightsTags = emptyList()
    )
    return listOf(singleNode)
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

private fun deduplicateTasksByTaskId(tasks: List<MobileTaskListItem>): List<MobileTaskListItem> {
    if (tasks.size <= 1) {
        return tasks
    }
    val deduplicated = LinkedHashMap<String, MobileTaskListItem>(tasks.size)
    tasks.forEach { task ->
        val taskId = task.taskId.trim()
        if (taskId.isEmpty()) {
            return@forEach
        }
        val existing = deduplicated[taskId]
        deduplicated[taskId] = if (existing == null) {
            task
        } else {
            choosePreferredTaskSnapshot(existing, task)
        }
    }
    return deduplicated.values.toList()
}

private fun choosePreferredTaskSnapshot(
    existing: MobileTaskListItem,
    candidate: MobileTaskListItem
): MobileTaskListItem {
    val existingProcessing = isProcessingStatus(existing.status)
    val candidateProcessing = isProcessingStatus(candidate.status)
    if (existingProcessing != candidateProcessing) {
        return if (existingProcessing) existing else candidate
    }
    if (existing.markdownAvailable != candidate.markdownAvailable) {
        return if (existing.markdownAvailable) existing else candidate
    }
    val existingTimestamp = parseTaskInstant(existing.lastOpenedAt)?.toEpochMilli()
        ?: parseTaskInstant(existing.createdAt)?.toEpochMilli()
        ?: Long.MIN_VALUE
    val candidateTimestamp = parseTaskInstant(candidate.lastOpenedAt)?.toEpochMilli()
        ?: parseTaskInstant(candidate.createdAt)?.toEpochMilli()
        ?: Long.MIN_VALUE
    return if (candidateTimestamp > existingTimestamp) candidate else existing
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
