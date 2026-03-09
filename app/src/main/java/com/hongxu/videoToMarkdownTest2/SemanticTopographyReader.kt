package com.hongxu.videoToMarkdownTest2
import android.annotation.SuppressLint
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.graphics.Typeface
import android.graphics.RectF
import android.net.Uri
import android.os.Build
import android.os.SystemClock
import java.nio.charset.StandardCharsets
import android.text.Layout
import android.text.Selection
import android.text.method.LinkMovementMethod
import android.view.HapticFeedbackConstants
import android.util.LruCache
import android.text.Spannable
import android.text.SpannableStringBuilder
import android.text.Spanned
import android.text.TextPaint
import android.text.style.ClickableSpan
import android.text.style.CharacterStyle
import android.text.style.ForegroundColorSpan
import android.text.style.LeadingMarginSpan
import android.text.style.MetricAffectingSpan
import android.text.style.StyleSpan
import android.text.style.URLSpan
import android.text.style.UpdateAppearance
import android.util.Log
import android.view.GestureDetector
import android.view.MotionEvent
import android.view.View
import android.view.ViewConfiguration
import android.view.ViewGroup
import android.widget.MediaController
import android.widget.Toast
import android.widget.TextView
import android.widget.VideoView
import androidx.activity.compose.BackHandler
import androidx.core.widget.TextViewCompat
import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.expandVertically
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.scaleIn
import androidx.compose.animation.scaleOut
import androidx.compose.animation.shrinkVertically
import androidx.compose.animation.core.AnimationSpec
import androidx.compose.animation.core.Animatable
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.Spring
import androidx.compose.animation.core.animate
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.animation.core.spring
import androidx.compose.animation.core.tween
import androidx.compose.foundation.background
import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.clickable
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.gestures.detectVerticalDragGestures
import androidx.compose.foundation.gestures.detectTransformGestures
import androidx.compose.foundation.gestures.detectTapGestures
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyListState
import androidx.compose.foundation.lazy.itemsIndexed
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Send
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.FloatingActionButton
import androidx.compose.material3.IconButton
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.ScrollableTabRow
import androidx.compose.material3.Surface
import androidx.compose.material3.Tab
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.derivedStateOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.runtime.snapshotFlow
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.draw.drawBehind
import androidx.compose.ui.draw.drawWithContent
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.rotate
import androidx.compose.ui.draw.scale
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.graphics.StrokeCap
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.graphics.StrokeJoin
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.hapticfeedback.HapticFeedbackType
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.boundsInWindow
import androidx.compose.ui.layout.SubcomposeLayout
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.platform.LocalConfiguration
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.platform.LocalHapticFeedback
import androidx.compose.ui.platform.LocalLifecycleOwner
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalSoftwareKeyboardController
import androidx.compose.ui.platform.LocalView
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.font.Font
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.text.TextRange
import androidx.compose.ui.text.input.TextFieldValue
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.Constraints
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.IntOffset
import androidx.compose.ui.unit.IntSize
import androidx.compose.ui.unit.sp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.graphics.vector.path
import androidx.compose.ui.window.Popup
import androidx.compose.ui.window.PopupProperties
import androidx.compose.ui.window.Dialog
import androidx.compose.ui.window.DialogProperties
import coil.compose.AsyncImage
import coil.request.ImageRequest
import io.noties.markwon.Markwon
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.distinctUntilChanged
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.roundToInt

/**
 * 语义地形阅读器主入口。
 * 职责：
 * 1. 渲染语义块列表与选中态。
 * 2. 同步收藏、删除标记与批注到后端 meta API。
 * 3. 上报阅读交互事件到 telemetry API。
 */
@Composable
@OptIn(ExperimentalMaterial3Api::class)
fun SemanticTopographyReader(
    nodes: List<SemanticNode>,
    markwon: Markwon,
    renderConfig: MarkdownReaderRenderConfig,
    modifier: Modifier = Modifier,
    taskId: String? = null,
    apiBaseUrl: String = BuildConfig.MOBILE_API_BASE_URL,
    pathHint: String? = null,
    metaApi: MobileMarkdownMetaApi? = null,
    telemetryApi: MobileMarkdownTelemetryApi? = null,
    cardApi: MobileConceptCardApi? = null,
    initialFirstVisibleItemIndex: Int = 0,
    initialFirstVisibleItemScrollOffset: Int = 0,
    onMarkDeleted: (String) -> Unit = {},
    onResonance: (String) -> Unit = {},
    onScrollDown: () -> Unit = {},
    onScrollUp: () -> Unit = {},
    onBlankTap: () -> Unit = {},
    onReadingPositionChanged: (Int, Int) -> Unit = { _, _ -> },
    onGestureEvent: (ParagraphGestureEvent) -> Unit = {},
    onTelemetry: (ReaderTelemetryEvent) -> Unit = {},
    externalMetaRefreshVersion: Int = 0
) {
    val listState = rememberLazyListState(
        initialFirstVisibleItemIndex = initialFirstVisibleItemIndex.coerceAtLeast(0),
        initialFirstVisibleItemScrollOffset = initialFirstVisibleItemScrollOffset.coerceAtLeast(0)
    )
    val scope = rememberCoroutineScope()
    val lifecycleOwner = LocalLifecycleOwner.current
    val haptic = LocalHapticFeedback.current

    val favoritesState = remember {
        mutableStateMapOf<String, Boolean>()
    }
    val commentsState = remember {
        mutableStateMapOf<String, List<String>>()
    }
    val deletedState = remember {
        mutableStateMapOf<String, Boolean>()
    }
    val tokenAnnotationsState = remember {
        mutableStateMapOf<String, String>()
    }
    val anchorsState = remember {
        mutableStateMapOf<String, MobileAnchorData>()
    }
    var mountedAnchorPreviewState by remember {
        mutableStateOf<MountedAnchorPreviewState?>(null)
    }
    var mountedAnchorPreviewRequestVersion by remember {
        mutableIntStateOf(0)
    }
    var anchorNoteEditorState by remember {
        mutableStateOf<AnchorNoteEditorState?>(null)
    }
    var anchorNoteEditorRequestVersion by remember {
        mutableIntStateOf(0)
    }
    var phase2bFloatingCardState by remember {
        mutableStateOf(Phase2bFloatingCardState(false, false, "", null))
    }

    fun emitTelemetry(event: ReaderTelemetryEvent) {
        onTelemetry(event)
        if (taskId.isNullOrBlank() || telemetryApi == null) {
            return
        }
        scope.launch {
            runCatching {
                telemetryApi.ingestTaskTelemetry(
                    taskId = taskId,
                    pathHint = pathHint,
                    events = listOf(
                        MobileTelemetryEvent(
                            nodeId = event.nodeId,
                            eventType = event.eventType,
                            relevanceScore = event.relevanceScore,
                            timestampMs = event.timestampMs,
                            payload = event.payload
                        )
                    )
                )
            }
        }
    }

    DisposableEffect(lifecycleOwner, telemetryApi, taskId, pathHint) {
        val flushable = telemetryApi as? FlushableMobileMarkdownTelemetryApi
        val observer = LifecycleEventObserver { _, event ->
            if (event == Lifecycle.Event.ON_STOP) {
                flushable?.flushAsync(reason = "screen_locked_or_backgrounded")
            }
        }
        lifecycleOwner.lifecycle.addObserver(observer)
        onDispose {
            lifecycleOwner.lifecycle.removeObserver(observer)
            flushable?.flushAsync(reason = "article_exit")
        }
    }

    DisposableEffect(lifecycleOwner, listState, onReadingPositionChanged) {
        val observer = LifecycleEventObserver { _, event ->
            if (event == Lifecycle.Event.ON_STOP) {
                onReadingPositionChanged(
                    listState.firstVisibleItemIndex,
                    listState.firstVisibleItemScrollOffset
                )
            }
        }
        lifecycleOwner.lifecycle.addObserver(observer)
        onDispose {
            lifecycleOwner.lifecycle.removeObserver(observer)
            onReadingPositionChanged(
                listState.firstVisibleItemIndex,
                listState.firstVisibleItemScrollOffset
            )
        }
    }

    fun scheduleMetaSync(reason: String) {
        if (taskId.isNullOrBlank() || metaApi == null) {
            return
        }
        val favoriteSnapshot = favoritesState
            .filterValues { it }
            .toMap()
        val deletedSnapshot = deletedState
            .filterValues { it }
            .toMap()
        val commentsSnapshot = commentsState.toMap()
        val tokenAnnotationsSnapshot = tokenAnnotationsState
            .mapValues { it.value.trim() }
            .filterValues { it.isNotBlank() }
            .toMap()
        val anchorsSnapshot = anchorsState
            .mapValues { (_, value) ->
                value.copy(
                    blockId = value.blockId.trim(),
                    quote = value.quote.trim(),
                    contextQuote = value.contextQuote.trim(),
                    anchorHint = value.anchorHint.trim(),
                    status = value.status.trim(),
                    mountedPath = value.mountedPath.trim(),
                    mountedRevisionId = value.mountedRevisionId.trim(),
                    updatedAt = value.updatedAt.trim(),
                    revisions = value.revisions.map { revision ->
                        revision.copy(
                            revisionId = revision.revisionId.trim(),
                            createdAt = revision.createdAt.trim(),
                            relativeDir = revision.relativeDir.trim(),
                            notePath = revision.notePath.trim(),
                            files = revision.files
                                .map { path -> path.trim() }
                                .filter { path -> path.isNotBlank() }
                        )
                    }
                )
            }
            .filterValues { anchor ->
                anchor.blockId.isNotBlank() &&
                    anchor.startIndex >= 0 &&
                    anchor.endIndex > anchor.startIndex
            }
            .toMap()
        scope.launch {
            runCatching {
                metaApi.updateTaskMeta(
                    taskId = taskId,
                    request = MobileTaskMetaUpdateRequest(
                        path = pathHint,
                        taskTitle = null,
                        favorites = favoriteSnapshot,
                        deleted = deletedSnapshot,
                        comments = commentsSnapshot,
                        tokenLike = emptyMap(),
                        tokenAnnotations = tokenAnnotationsSnapshot,
                        anchors = anchorsSnapshot
                    )
                )
            }.onSuccess {
                emitTelemetry(
                    ReaderTelemetryEvent(
                        nodeId = "global",
                        eventType = "meta_sync_success",
                        relevanceScore = 0f,
                        payload = mapOf(
                            "reason" to reason,
                            "favoritesCount" to favoriteSnapshot.size.toString(),
                            "deletedCount" to deletedSnapshot.size.toString(),
                            "commentsCount" to commentsSnapshot.size.toString(),
                            "tokenAnnotationCount" to tokenAnnotationsSnapshot.size.toString(),
                            "anchorCount" to anchorsSnapshot.size.toString()
                        )
                    )
                )
            }.onFailure { error ->
                emitTelemetry(
                    ReaderTelemetryEvent(
                        nodeId = "global",
                        eventType = "meta_sync_failed",
                        relevanceScore = 0f,
                        payload = mapOf(
                            "reason" to reason,
                            "error" to (error.message ?: "unknown")
                        )
                    )
                )
            }
        }
    }

    LaunchedEffect(taskId, pathHint, metaApi, externalMetaRefreshVersion) {
        if (taskId.isNullOrBlank() || metaApi == null) {
            favoritesState.clear()
            commentsState.clear()
            deletedState.clear()
            tokenAnnotationsState.clear()
            anchorsState.clear()
            return@LaunchedEffect
        }
        runCatching {
            metaApi.fetchTaskMeta(taskId = taskId, pathHint = pathHint)
        }.onSuccess { payload ->
            favoritesState.clear()
            favoritesState.putAll(payload.favorites)
            commentsState.clear()
            commentsState.putAll(payload.comments)
            deletedState.clear()
            deletedState.putAll(payload.deleted)
            tokenAnnotationsState.clear()
            tokenAnnotationsState.putAll(payload.tokenAnnotations)
            anchorsState.clear()
            anchorsState.putAll(payload.anchors)
            emitTelemetry(
                ReaderTelemetryEvent(
                    nodeId = "global",
                    eventType = "meta_loaded",
                    relevanceScore = 0f,
                    payload = mapOf(
                        "taskId" to payload.taskId,
                        "pathKey" to payload.pathKey,
                        "favoritesCount" to payload.favorites.size.toString(),
                        "deletedCount" to payload.deleted.size.toString(),
                        "commentsCount" to payload.comments.size.toString(),
                        "tokenAnnotationCount" to payload.tokenAnnotations.size.toString(),
                        "anchorCount" to payload.anchors.size.toString()
                    )
                )
            )
        }.onFailure { error ->
            emitTelemetry(
                ReaderTelemetryEvent(
                    nodeId = "global",
                    eventType = "meta_load_failed",
                    relevanceScore = 0f,
                    payload = mapOf(
                        "error" to (error.message ?: "unknown")
                    )
                )
            )
        }
    }

    LaunchedEffect(listState) {
        var previousAbsoluteOffset: Long? = null
        snapshotFlow {
            listState.firstVisibleItemIndex to listState.firstVisibleItemScrollOffset
        }.collect { (index, offset) ->
            onReadingPositionChanged(index, offset)
            val absoluteOffset = index.toLong() * 100_000L + offset.toLong()
            val previous = previousAbsoluteOffset
            if (previous != null) {
                val delta = absoluteOffset - previous
                when {
                    delta >= 10L -> onScrollDown()
                    delta <= -10L -> onScrollUp()
                }
            }
            previousAbsoluteOffset = absoluteOffset
        }
    }
    val tokenSelections = remember {
        mutableStateMapOf<String, TokenSelection>()
    }
    var floatingBubbleState by remember {
        mutableStateOf<FloatingCardBubbleState?>(null)
    }
    var floatingCard by remember {
        mutableStateOf<TokenInsightCard?>(null)
    }
    var floatingCardLoading by remember {
        mutableStateOf(false)
    }
    var floatingCardError by remember {
        mutableStateOf<String?>(null)
    }
    var floatingCardRequestVersion by remember {
        mutableIntStateOf(0)
    }
    var overlayRootWindowOffset by remember {
        mutableStateOf(Offset.Zero)
    }
    var overlayViewportSize by remember {
        mutableStateOf(IntSize.Zero)
    }
    var activeCommentBlockId by remember {
        mutableStateOf<String?>(null)
    }
    var tokenAnnotationEditorState by remember {
        mutableStateOf<TokenAnnotationEditorState?>(null)
    }
    var tokenAnnotationBubbleState by remember {
        mutableStateOf<TokenAnnotationBubbleState?>(null)
    }
    val paragraphOverlayBoundsState = remember {
        mutableStateMapOf<String, ParagraphOverlayBounds>()
    }
    val tokenAnnotationsByBlock by remember {
        derivedStateOf {
            groupTokenAnnotationsByBlock(tokenAnnotationsState)
        }
    }
    val anchorsByBlock by remember {
        derivedStateOf {
            groupAnchorsByBlock(anchorsState)
        }
    }
    val activeParagraphBoundsBlockId = tokenAnnotationEditorState?.blockId ?: tokenAnnotationBubbleState?.blockId

    fun updateMountedPreviewState(transform: (MountedAnchorPreviewState) -> MountedAnchorPreviewState) {
        mountedAnchorPreviewState = mountedAnchorPreviewState?.let(transform)
    }

    fun updateAnchorNoteEditorState(transform: (AnchorNoteEditorState) -> AnchorNoteEditorState) {
        anchorNoteEditorState = anchorNoteEditorState?.let(transform)
    }

    fun currentAnchorEditorNote(state: AnchorNoteEditorState): AnchorEditorNoteTab? {
        return state.notes.firstOrNull { note -> note.localId == state.activeNoteId } ?: state.notes.firstOrNull()
    }

    fun deriveEditorTitleFromMarkdown(markdown: String, fallback: String): String {
        val heading = markdown.lineSequence()
            .map { line -> line.trim() }
            .firstOrNull { line -> line.startsWith("#") }
            ?.trimStart('#')
            ?.trim()
            .orEmpty()
        if (heading.isNotBlank()) {
            return heading
        }
        val firstLine = markdown.lineSequence()
            .map { line -> line.trim() }
            .firstOrNull { line -> line.isNotBlank() }
            .orEmpty()
        return firstLine.ifBlank { fallback }.take(64)
    }

    fun buildEditorNotePath(title: String, index: Int): String {
        val slug = title.lowercase()
            .replace(Regex("[^a-z0-9\u4e00-\u9fa5]+"), "_")
            .trim('_')
            .ifBlank { "note_${index + 1}" }
        return normalizeMountedNotePath("cards/$slug.md")
    }

    fun createEditorNote(title: String, markdown: String, notePath: String, index: Int): AnchorEditorNoteTab {
        val effectiveTitle = title.ifBlank { deriveMountedNoteDisplayTitle(notePath).ifBlank { "Note ${index + 1}" } }
        val normalizedPath = normalizeMountedNotePath(notePath).ifBlank { buildEditorNotePath(effectiveTitle, index) }
        val effectiveMarkdown = markdown.ifBlank { "# $effectiveTitle\n\n" }
        return AnchorEditorNoteTab(
            localId = "note_${SystemClock.elapsedRealtime()}_${index}",
            title = effectiveTitle,
            notePath = normalizedPath,
            markdown = effectiveMarkdown,
            updatedAtMs = System.currentTimeMillis()
        )
    }

    fun rewriteMarkdownTitle(markdown: String, title: String): String {
        val normalizedTitle = title.trim().ifBlank { "Untitled" }
        val markdownLines = markdown.lines().toMutableList()
        val headingIndex = markdownLines.indexOfFirst { line -> line.trim().startsWith("#") }
        if (headingIndex >= 0) {
            markdownLines[headingIndex] = "# $normalizedTitle"
            return markdownLines.joinToString("\n")
        }
        return "# $normalizedTitle\n\n${markdown.trim()}".trim()
    }

    fun updateAnchorEditorCurrentNote(transform: (AnchorEditorNoteTab, Int) -> AnchorEditorNoteTab) {
        val current = anchorNoteEditorState ?: return
        val currentIndex = current.notes.indexOfFirst { note -> note.localId == current.activeNoteId }
        if (currentIndex < 0) {
            return
        }
        val updatedNotes = current.notes.toMutableList()
        updatedNotes[currentIndex] = transform(updatedNotes[currentIndex], currentIndex)
        anchorNoteEditorState = current.copy(notes = updatedNotes)
    }

    fun applyMarkdownWrap(value: TextFieldValue, prefix: String, suffix: String = prefix): TextFieldValue {
        val selection = value.selection
        val text = value.text
        val start = selection.start.coerceAtLeast(0)
        val end = selection.end.coerceAtLeast(start)
        val selected = text.substring(start, end)
        val replacement = prefix + selected + suffix
        val nextText = text.replaceRange(start, end, replacement)
        val cursor = start + replacement.length
        return TextFieldValue(nextText, TextRange(cursor, cursor))
    }

    fun applyMarkdownHeading(value: TextFieldValue, level: Int): TextFieldValue {
        val headingPrefix = "#".repeat(level.coerceIn(1, 6)) + " "
        val selection = value.selection
        val text = value.text
        val lineStart = text.lastIndexOf('\n', (selection.start - 1).coerceAtLeast(0)).let { if (it < 0) 0 else it + 1 }
        val lineEnd = text.indexOf('\n', selection.end).let { if (it < 0) text.length else it }
        val currentLine = text.substring(lineStart, lineEnd).trimStart()
        val normalizedLine = currentLine.removePrefix("# ")
            .removePrefix("## ")
            .removePrefix("### ")
            .removePrefix("#### ")
            .removePrefix("##### ")
            .removePrefix("###### ")
        val replacement = headingPrefix + normalizedLine
        val nextText = text.replaceRange(lineStart, lineEnd, replacement)
        val cursor = lineStart + replacement.length
        return TextFieldValue(nextText, TextRange(cursor, cursor))
    }

    suspend fun openAnchorNoteEditor(
        anchorId: String,
        blockId: String,
        quote: String,
        anchorHint: String,
        anchorData: MobileAnchorData
    ) {
        val requestVersion = anchorNoteEditorRequestVersion + 1
        anchorNoteEditorRequestVersion = requestVersion
        val fallbackPath = anchorData.mountedPath.ifBlank { buildEditorNotePath(quote.ifBlank { "Note 1" }, 0) }
        val fallbackNote = createEditorNote(
            title = deriveMountedNoteDisplayTitle(fallbackPath),
            markdown = "# ${deriveMountedNoteDisplayTitle(fallbackPath)}\n\n",
            notePath = fallbackPath,
            index = 0
        )
        anchorNoteEditorState = AnchorNoteEditorState(
            anchorId = anchorId,
            blockId = blockId,
            quote = quote,
            anchorHint = anchorHint,
            notes = listOf(fallbackNote),
            activeNoteId = fallbackNote.localId,
            shadowByPath = emptyMap(),
            pathHint = pathHint.orEmpty(),
            mode = AnchorEditorMode.EDIT,
            isSaving = false,
            errorMessage = null
        )
        if (taskId.isNullOrBlank() || metaApi == null || !anchorData.status.equals("mounted", ignoreCase = true)) {
            return
        }
        val initialPayload = runCatching {
            metaApi.fetchMountedAnchorNote(taskId = taskId, anchorId = anchorId, pathHint = pathHint, notePath = null)
        }.getOrNull() ?: return
        if (anchorNoteEditorRequestVersion != requestVersion) {
            return
        }
        val markdownPaths = mergeMountedMarkdownPaths(emptyList(), initialPayload)
        val loadedNotes = buildList<AnchorEditorNoteTab> {
            markdownPaths.forEachIndexed { index, candidatePath ->
                val payload = runCatching {
                    metaApi.fetchMountedAnchorNote(taskId = taskId, anchorId = anchorId, pathHint = pathHint, notePath = candidatePath)
                }.getOrNull() ?: return@forEachIndexed
                add(
                    createEditorNote(
                        title = deriveEditorTitleFromMarkdown(payload.rawMarkdown, deriveMountedNoteDisplayTitle(candidatePath)),
                        markdown = payload.rawMarkdown.ifBlank { payload.markdown },
                        notePath = candidatePath,
                        index = index
                    )
                )
            }
        }.ifEmpty { listOf(fallbackNote) }
        anchorNoteEditorState = anchorNoteEditorState?.copy(
            notes = loadedNotes,
            activeNoteId = loadedNotes.first().localId,
            shadowByPath = loadedNotes.associate { note -> note.notePath to note.markdown },
            errorMessage = null
        )
    }

    suspend fun saveAnchorNoteEditor() {
        val state = anchorNoteEditorState ?: return
        if (taskId.isNullOrBlank() || metaApi == null) {
            updateAnchorNoteEditorState { current ->
                current.copy(errorMessage = "Anchor note sync unavailable.")
            }
            return
        }
        updateAnchorNoteEditorState { current -> current.copy(isSaving = true, errorMessage = null) }
        val notes = state.notes.ifEmpty {
            listOf(createEditorNote(title = "Note 1", markdown = "# Note 1\n\n", notePath = "cards/note_1.md", index = 0))
        }
        val currentMap = notes.associate { note -> note.notePath to note.markdown }
        val activeNote = currentAnchorEditorNote(state) ?: notes.first()
        val result = runCatching {
            if (state.shadowByPath.isEmpty()) {
                metaApi.mountAnchorNotes(
                    taskId = taskId,
                    anchorId = state.anchorId,
                    request = MobileAnchorMountRequest(
                        pathHint = state.pathHint,
                        mainNotePath = activeNote.notePath,
                        files = notes.mapIndexed { index, note ->
                            MobileAnchorMountFile(
                                relativePath = note.notePath,
                                fileName = note.notePath.substringAfterLast('/').ifBlank { "note_${index + 1}.md" },
                                bytes = note.markdown.toByteArray(StandardCharsets.UTF_8)
                            )
                        }
                    )
                )
            } else {
                val deleteOps = state.shadowByPath.keys
                    .filter { path -> path !in currentMap.keys }
                    .map { path -> MobileAnchorSyncOperation(op = "delete", relativePath = path) }
                val upsertOps = currentMap.entries.mapNotNull { (notePath, markdown) ->
                    val previous = state.shadowByPath[notePath]
                    when {
                        previous == null -> MobileAnchorSyncOperation(op = "add", relativePath = notePath, content = markdown)
                        previous != markdown -> MobileAnchorSyncOperation(op = "replace", relativePath = notePath, content = markdown)
                        else -> null
                    }
                }
                metaApi.syncAnchorNotes(
                    taskId = taskId,
                    anchorId = state.anchorId,
                    request = MobileAnchorSyncRequest(
                        pathHint = state.pathHint,
                        mainNotePath = activeNote.notePath,
                        operations = deleteOps + upsertOps
                    )
                )
            }
        }
        result.onSuccess { syncResult ->
            val syncedAnchor = syncResult.anchor.copy(anchorHint = state.anchorHint.trim())
            anchorsState[state.anchorId] = syncedAnchor
            scheduleMetaSync(reason = "anchor_editor_save")
            updateAnchorNoteEditorState { current ->
                current.copy(
                    notes = notes,
                    shadowByPath = currentMap,
                    isSaving = false,
                    errorMessage = null,
                    anchorHint = syncedAnchor.anchorHint
                )
            }
        }.onFailure { error ->
            updateAnchorNoteEditorState { current ->
                current.copy(isSaving = false, errorMessage = error.message ?: "save failed")
            }
        }
    }

    fun openPhase2bFloatingCard() {
        phase2bFloatingCardState = phase2bFloatingCardState.copy(visible = true, loading = true, errorMessage = null)
        val state = anchorNoteEditorState ?: run {
            phase2bFloatingCardState = phase2bFloatingCardState.copy(loading = false, errorMessage = "Anchor editor unavailable")
            return
        }
        val activeNote = currentAnchorEditorNote(state) ?: run {
            phase2bFloatingCardState = phase2bFloatingCardState.copy(loading = false, errorMessage = "No active note")
            return
        }
        val api = cardApi ?: run {
            phase2bFloatingCardState = phase2bFloatingCardState.copy(loading = false, errorMessage = "Phase2B service unavailable")
            return
        }
        if (taskId.isNullOrBlank()) {
            phase2bFloatingCardState = phase2bFloatingCardState.copy(loading = false, errorMessage = "Task context missing")
            return
        }
        scope.launch {
            runCatching {
                api.generatePhase2bStructuredMarkdown(
                    taskId = taskId,
                    anchorId = state.anchorId,
                    pathHint = state.pathHint,
                    markdownBody = activeNote.markdown
                )
            }.onSuccess { result ->
                phase2bFloatingCardState = phase2bFloatingCardState.copy(
                    visible = true,
                    loading = false,
                    resultMarkdown = result.markdown,
                    errorMessage = null
                )
            }.onFailure { error ->
                phase2bFloatingCardState = phase2bFloatingCardState.copy(
                    visible = true,
                    loading = false,
                    errorMessage = error.message ?: "Phase2B failed"
                )
            }
        }
    }

    fun mergeMountedPreviewFromPayload(

        current: MountedAnchorPreviewState,
        payload: MobileMountedAnchorPayload,
        displayTitle: String,
        appendToStack: Boolean
    ): MountedAnchorPreviewState {
        val normalizedEntry = normalizeMountedNotePath(
            payload.entryNotePath.ifBlank { current.entryNotePath }
        )
        val resolvedNotePath = normalizeMountedNotePath(
            payload.notePath.ifBlank {
                payload.entryNotePath.ifBlank {
                    current.stack.lastOrNull()?.notePath.orEmpty()
                }
            }
        )
        val resolvedTitle = displayTitle.ifBlank {
            deriveMountedNoteDisplayTitle(
                resolvedNotePath.ifBlank { normalizedEntry }
            )
        }
        val document = MountedAnchorDocument(
            notePath = resolvedNotePath,
            displayTitle = resolvedTitle,
            markdown = payload.markdown.ifBlank { payload.rawMarkdown },
            rawMarkdown = payload.rawMarkdown,
            isGhost = false,
            ghostInputPath = ""
        )
        val nextStack = if (!appendToStack || current.stack.isEmpty()) {
            listOf(document)
        } else {
            val existingIndex = current.stack.indexOfFirst { item ->
                normalizeMountedNotePath(item.notePath)
                    .equals(document.notePath, ignoreCase = true)
            }
            if (existingIndex >= 0) {
                current.stack.take(existingIndex + 1)
            } else {
                current.stack + document
            }
        }
        return current.copy(
            entryNotePath = normalizedEntry.ifBlank { current.entryNotePath },
            markdownPaths = mergeMountedMarkdownPaths(current.markdownPaths, payload),
            stack = nextStack,
            isLoading = false,
            errorMessage = null
        )
    }

    fun fetchMountedAnchorDocument(
        anchorId: String,
        requestedNotePath: String?,
        displayTitle: String,
        appendToStack: Boolean,
        fallbackAsGhost: Boolean
    ) {
        if (taskId.isNullOrBlank() || metaApi == null) {
            return
        }
        mountedAnchorPreviewRequestVersion += 1
        val requestVersion = mountedAnchorPreviewRequestVersion
        updateMountedPreviewState { current ->
            current.copy(
                isLoading = true,
                errorMessage = null
            )
        }
        scope.launch {
            runCatching {
                metaApi.fetchMountedAnchorNote(
                    taskId = taskId,
                    anchorId = anchorId,
                    pathHint = pathHint,
                    notePath = requestedNotePath
                )
            }.onSuccess { payload ->
                if (requestVersion != mountedAnchorPreviewRequestVersion) {
                    return@onSuccess
                }
                mountedAnchorPreviewState = mountedAnchorPreviewState?.let { current ->
                    mergeMountedPreviewFromPayload(
                        current = current,
                        payload = payload,
                        displayTitle = displayTitle,
                        appendToStack = appendToStack
                    )
                }
            }.onFailure { error ->
                if (requestVersion != mountedAnchorPreviewRequestVersion) {
                    return@onFailure
                }
                val message = error.message ?: "加载失败"
                mountedAnchorPreviewState = mountedAnchorPreviewState?.let { current ->
                    if (fallbackAsGhost && requestedNotePath != null && isMountedNoteMissingError(message)) {
                        val ghostPath = normalizeMountedNotePath(requestedNotePath)
                        val ghostNode = MountedAnchorDocument(
                            notePath = ghostPath,
                            displayTitle = displayTitle.ifBlank {
                                deriveMountedNoteDisplayTitle(ghostPath)
                            },
                            markdown = "",
                            rawMarkdown = "",
                            isGhost = true,
                            ghostInputPath = ghostPath
                        )
                        val nextStack = if (appendToStack && current.stack.isNotEmpty()) {
                            current.stack + ghostNode
                        } else {
                            listOf(ghostNode)
                        }
                        current.copy(
                            stack = nextStack,
                            isLoading = false,
                            errorMessage = null
                        )
                    } else {
                        current.copy(
                            isLoading = false,
                            errorMessage = message
                        )
                    }
                }
            }
        }
    }

    fun openMountedAnchorPreview(
        blockId: String,
        selection: TokenSelection,
        anchorData: MobileAnchorData
    ) {
        if (taskId.isNullOrBlank() || metaApi == null) {
            return
        }
        val anchorId = buildTokenMetaKey(
            blockId = blockId,
            start = selection.start,
            end = selection.end
        )
        val entryPath = normalizeMountedNotePath(anchorData.mountedPath)
        mountedAnchorPreviewState = MountedAnchorPreviewState(
            anchorId = anchorId,
            blockId = blockId,
            quote = selection.token.trim(),
            entryNotePath = entryPath,
            markdownPaths = if (entryPath.isNotBlank()) listOf(entryPath) else emptyList(),
            stack = emptyList(),
            isLoading = true,
            errorMessage = null,
            isFullscreen = false
        )
        emitTelemetry(
            ReaderTelemetryEvent(
                nodeId = blockId,
                eventType = "mounted_note_opened",
                relevanceScore = 0f,
                payload = mapOf(
                    "anchorId" to anchorId,
                    "start" to selection.start.toString(),
                    "end" to selection.end.toString(),
                    "quote" to selection.token.trim(),
                    "source" to "anchor"
                )
            )
        )
        fetchMountedAnchorDocument(
            anchorId = anchorId,
            requestedNotePath = entryPath.ifBlank { null },
            displayTitle = deriveMountedNoteDisplayTitle(entryPath),
            appendToStack = false,
            fallbackAsGhost = false
        )
    }

    fun openMountedWikilink(link: MountedWikilinkTap) {
        val snapshot = mountedAnchorPreviewState ?: return
        val normalizedTarget = normalizeMountedNotePath(link.targetNotePath)
        if (normalizedTarget.isBlank()) {
            return
        }
        haptic.performHapticFeedback(HapticFeedbackType.TextHandleMove)
        val existingIndex = snapshot.stack.indexOfFirst { item ->
            normalizeMountedNotePath(item.notePath)
                .equals(normalizedTarget, ignoreCase = true)
        }
        if (existingIndex >= 0) {
            mountedAnchorPreviewState = snapshot.copy(
                stack = snapshot.stack.take(existingIndex + 1),
                errorMessage = null
            )
            return
        }
        if (link.isGhost) {
            val ghostNode = MountedAnchorDocument(
                notePath = normalizedTarget,
                displayTitle = link.displayText.ifBlank {
                    deriveMountedNoteDisplayTitle(normalizedTarget)
                },
                markdown = "",
                rawMarkdown = "",
                isGhost = true,
                ghostInputPath = normalizedTarget
            )
            mountedAnchorPreviewState = snapshot.copy(
                stack = snapshot.stack + ghostNode,
                isLoading = false,
                errorMessage = null
            )
            return
        }
        emitTelemetry(
            ReaderTelemetryEvent(
                nodeId = snapshot.blockId,
                eventType = "mounted_note_opened",
                relevanceScore = 0f,
                payload = mapOf(
                    "anchorId" to snapshot.anchorId,
                    "quote" to snapshot.quote,
                    "source" to "wikilink",
                    "targetNotePath" to normalizedTarget,
                    "wikilinkTitle" to link.displayText
                )
            )
        )
        fetchMountedAnchorDocument(
            anchorId = snapshot.anchorId,
            requestedNotePath = normalizedTarget,
            displayTitle = link.displayText,
            appendToStack = true,
            fallbackAsGhost = true
        )
    }

    fun bindGhostMountedNote(rawPath: String) {
        val snapshot = mountedAnchorPreviewState ?: return
        val normalizedTarget = normalizeMountedNotePath(rawPath)
        if (normalizedTarget.isBlank()) {
            updateMountedPreviewState { current ->
                current.copy(errorMessage = "请输入可绑定的 Markdown 路径")
            }
            return
        }
        mountedAnchorPreviewState = snapshot.copy(
            stack = if (snapshot.stack.isNotEmpty()) snapshot.stack.dropLast(1) else snapshot.stack,
            errorMessage = null
        )
        fetchMountedAnchorDocument(
            anchorId = snapshot.anchorId,
            requestedNotePath = normalizedTarget,
            displayTitle = deriveMountedNoteDisplayTitle(normalizedTarget),
            appendToStack = true,
            fallbackAsGhost = true
        )
    }

    fun popMountedPreviewTo(index: Int) {
        updateMountedPreviewState { current ->
            if (index < 0 || index >= current.stack.size) {
                current
            } else {
                current.copy(
                    stack = current.stack.take(index + 1),
                    errorMessage = null
                )
            }
        }
    }

    fun popMountedPreviewOneLevel() {
        updateMountedPreviewState { current ->
            when {
                current.stack.size > 1 -> current.copy(
                    stack = current.stack.dropLast(1),
                    errorMessage = null
                )
                current.isFullscreen -> current.copy(isFullscreen = false)
                else -> current
            }
        }
    }

    fun backToMountedPreviewRoot() {
        updateMountedPreviewState { current ->
            if (current.stack.size <= 1) {
                current
            } else {
                current.copy(
                    stack = listOf(current.stack.first()),
                    errorMessage = null
                )
            }
        }
    }

    LaunchedEffect(activeParagraphBoundsBlockId) {
        if (activeParagraphBoundsBlockId == null) {
            if (paragraphOverlayBoundsState.isNotEmpty()) {
                paragraphOverlayBoundsState.clear()
            }
            return@LaunchedEffect
        }
        val staleBlockIds = paragraphOverlayBoundsState.keys
            .filter { blockId -> blockId != activeParagraphBoundsBlockId }
        staleBlockIds.forEach { blockId ->
            paragraphOverlayBoundsState.remove(blockId)
        }
    }

    fun persistTokenAnnotationEditor(reason: String) {
        val editor = tokenAnnotationEditorState ?: return
        val metaKey = buildTokenMetaKey(
            blockId = editor.blockId,
            start = editor.selection.start,
            end = editor.selection.end
        )
        val normalized = editor.draft.trim()
        val changed = if (normalized.isBlank()) {
            tokenAnnotationsState.remove(metaKey) != null
        } else {
            val previous = tokenAnnotationsState[metaKey]
            tokenAnnotationsState[metaKey] = normalized
            previous != normalized
        }
        tokenAnnotationEditorState = null
        if (changed) {
            scheduleMetaSync(reason = "token_annotation_autosave_$reason")
        }
        if (normalized.isNotBlank()) {
            tokenAnnotationBubbleState = TokenAnnotationBubbleState(
                blockId = editor.blockId,
                selection = editor.selection,
                text = normalized,
                anchor = editor.anchor
            )
        } else {
            tokenAnnotationBubbleState = null
        }
    }

    fun dismissFloatingBubble(clearSelection: Boolean) {
        floatingBubbleState = null
        floatingCard = null
        floatingCardLoading = false
        floatingCardError = null
        if (clearSelection) {
            tokenSelections.clear()
        }
    }

    fun openFloatingBubble(nodeId: String, tap: InsightTermTapPayload) {
        val selectedToken = tap.range.term.trim()
        if (selectedToken.isBlank()) {
            return
        }
        tokenSelections.clear()
        floatingBubbleState = FloatingCardBubbleState(
            nodeId = nodeId,
            token = selectedToken,
            anchor = tap.anchor
        )
        floatingCard = null
        floatingCardError = null
        floatingCardLoading = true
        val requestVersion = floatingCardRequestVersion + 1
        floatingCardRequestVersion = requestVersion
        scope.launch {
            val api = cardApi
            if (api == null) {
                if (floatingCardRequestVersion != requestVersion) {
                    return@launch
                }
                floatingCardLoading = false
                floatingCardError = "Card service unavailable"
                return@launch
            }
            val result = runCatching {
                api.fetchCardByTerm(selectedToken)
            }
            if (floatingCardRequestVersion != requestVersion) {
                return@launch
            }
            floatingCardLoading = false
            val cardResult = result.getOrNull()
            if (cardResult != null) {
                floatingCard = cardResult
                floatingCardError = null
            } else {
                floatingCard = null
                floatingCardError = if (result.isFailure) {
                    "Card loading failed, tap again."
                } else {
                    "Card not found"
                }
            }
        }
    }
    fun openTokenAnnotationEditor(
        blockId: String,
        selection: TokenSelection,
        anchor: InsightTermAnchor?
    ) {
        persistTokenAnnotationEditor(reason = "switch_editor")
        val metaKey = buildTokenMetaKey(blockId, selection.start, selection.end)
        tokenAnnotationBubbleState = null
        tokenAnnotationEditorState = TokenAnnotationEditorState(
            blockId = blockId,
            selection = selection,
            draft = tokenAnnotationsState[metaKey].orEmpty(),
            anchor = anchor
        )
    }
    fun openTokenAnnotationBubble(
        blockId: String,
        selection: TokenSelection,
        anchor: InsightTermAnchor?
    ) {
        if (anchor == null) {
            return
        }
        persistTokenAnnotationEditor(reason = "open_bubble")
        val metaKey = buildTokenMetaKey(blockId, selection.start, selection.end)
        val text = tokenAnnotationsState[metaKey].orEmpty().trim()
        if (text.isBlank()) {
            tokenAnnotationBubbleState = null
            return
        }
        tokenAnnotationBubbleState = TokenAnnotationBubbleState(
            blockId = blockId,
            selection = selection,
            text = text,
            anchor = anchor
        )
    }

    LaunchedEffect(listState) {
        snapshotFlow { listState.isScrollInProgress }
            .distinctUntilChanged()
            .collect { isScrolling ->
                if (isScrolling) {
                    persistTokenAnnotationEditor(reason = "scroll")
                    tokenAnnotationBubbleState = null
                    tokenSelections.clear()
                    dismissFloatingBubble(clearSelection = true)
                }
            }
    }

    Box(
        modifier = modifier
            .fillMaxSize()
            .background(Color(0xFFFCFCFC))
            .pointerInput(activeCommentBlockId) {
                detectTapGestures(
                    onTap = {
                        persistTokenAnnotationEditor(reason = "blank_tap")
                        tokenAnnotationBubbleState = null
                        tokenSelections.clear()
                        dismissFloatingBubble(clearSelection = true)
                        onBlankTap()
                        if (activeCommentBlockId != null) {
                            activeCommentBlockId = null
                            emitTelemetry(
                                ReaderTelemetryEvent(
                                    nodeId = "global",
                                    eventType = "comment_panel_closed_by_blank_tap",
                                    relevanceScore = 0f
                                )
                            )
                        }
                    }
                )
            }
            .onGloballyPositioned { coordinates ->
                overlayRootWindowOffset = coordinates.localToWindow(Offset.Zero)
            }
            .onSizeChanged { size ->
                overlayViewportSize = size
            }
    ) {
        // 将 SemanticNode 按 Markdown 语义块拆分为 SemanticBlock
        val blocks = remember(nodes) { buildSingleMarkdownReaderBlock(nodes) }
        val blockInteractionEnabled = false

        LazyColumn(
            state = listState,
            modifier = Modifier
                .fillMaxSize()
                .alpha(if (floatingBubbleState != null) 0.4f else 1f),
            contentPadding = PaddingValues(top = 32.dp, bottom = 80.dp)
        ) {
            itemsIndexed(
                items = blocks,
                key = { index, block ->
                    val base = block.blockId.trim()
                    if (base.isNotEmpty()) {
                        "$base#$index"
                    } else {
                        "block#$index"
                    }
                }
            ) { index, block ->
                val blockTokenAnnotations = tokenAnnotationsByBlock[block.blockId].orEmpty()
                val blockAnchors = anchorsByBlock[block.blockId].orEmpty()
                val shouldTrackParagraphBounds = activeParagraphBoundsBlockId == block.blockId
                TopographyParagraph(
                    block = block,
                    index = index,
                    listState = listState,
                    markwon = markwon,
                    renderConfig = renderConfig,
                    taskId = taskId,
                    apiBaseUrl = apiBaseUrl,
                    selection = tokenSelections[block.blockId],
                    overlayRootWindowOffset = overlayRootWindowOffset,
                    isFavorited = false,
                    isMarkedDeleted = false,
                    existingComments = emptyList(),
                    likedTokenKeys = emptySet(),
                    tokenAnnotations = blockTokenAnnotations,
                    anchors = blockAnchors,
                    isCommentPanelExpanded = false,
                    shouldTrackParagraphBounds = shouldTrackParagraphBounds,
                    enableBlockInteractions = blockInteractionEnabled,
                    onParagraphBoundsChanged = { bounds ->
                        if (bounds == null) {
                            paragraphOverlayBoundsState.remove(block.blockId)
                        } else {
                            val previous = paragraphOverlayBoundsState[block.blockId]
                            if (previous != bounds) {
                                paragraphOverlayBoundsState[block.blockId] = bounds
                            }
                        }
                    },
                    onSelectionChanged = { selected ->
                        val shouldKeepEditor = selected == null &&
                            tokenAnnotationEditorState?.blockId == block.blockId
                        if (!shouldKeepEditor) {
                            persistTokenAnnotationEditor(reason = "selection_changed")
                            tokenAnnotationBubbleState = null
                        }
                        if (selected == null) {
                            tokenSelections.remove(block.blockId)
                        } else {
                            tokenSelections.clear()
                            tokenSelections[block.blockId] = selected
                        }
                        if (floatingBubbleState != null) {
                            dismissFloatingBubble(clearSelection = false)
                        }
                    },
                    onInsightTermTapped = { tap ->
                        openFloatingBubble(block.blockId, tap)
                    },
                    onToggleTokenLike = { _, _ ->
                        // token 喜欢功能已下线：保留参数位，避免旧分支调用导致崩溃。
                    },
                    onUpsertTokenAnnotation = { tokenSelection, annotationText ->
                        val metaKey = buildTokenMetaKey(
                            blockId = block.blockId,
                            start = tokenSelection.start,
                            end = tokenSelection.end
                        )
                        val normalized = annotationText.trim()
                        if (normalized.isBlank()) {
                            tokenAnnotationsState.remove(metaKey)
                        } else {
                            tokenAnnotationsState[metaKey] = normalized
                        }
                        scheduleMetaSync(reason = "token_annotation_upsert")
                    },
                    onUpsertAnchor = { tokenSelection, anchorHint ->
                        val metaKey = buildTokenMetaKey(
                            blockId = block.blockId,
                            start = tokenSelection.start,
                            end = tokenSelection.end
                        )
                        val existing = anchorsState[metaKey]
                        val contextQuote = buildAnchorContextQuoteSnapshot(
                            blockText = block.plainText,
                            start = tokenSelection.start,
                            end = tokenSelection.end
                        )
                        val nextAnchorHint = if (anchorHint == null) {
                            existing?.anchorHint.orEmpty().trim()
                        } else {
                            anchorHint.trim()
                        }
                        val nextStatus = when {
                            existing?.status.equals("mounted", ignoreCase = true) -> "mounted"
                            existing?.status.equals("files_uploaded", ignoreCase = true) -> "files_uploaded"
                            else -> "pending"
                        }
                        anchorsState[metaKey] = MobileAnchorData(
                            blockId = block.blockId,
                            startIndex = tokenSelection.start,
                            endIndex = tokenSelection.end,
                            quote = tokenSelection.token.trim(),
                            contextQuote = contextQuote.ifBlank { existing?.contextQuote.orEmpty() },
                            anchorHint = nextAnchorHint,
                            status = nextStatus,
                            mountedPath = existing?.mountedPath.orEmpty(),
                            mountedRevisionId = existing?.mountedRevisionId.orEmpty(),
                            updatedAt = java.time.Instant.now().toString(),
                            revisions = existing?.revisions.orEmpty()
                        )
                        scheduleMetaSync(reason = "anchor_marked")
                    },
                    onRemoveAnchor = { tokenSelection ->
                        val metaKey = buildTokenMetaKey(
                            blockId = block.blockId,
                            start = tokenSelection.start,
                            end = tokenSelection.end
                        )
                        val removed = anchorsState.remove(metaKey)
                        if (removed != null) {
                            scheduleMetaSync(reason = "anchor_removed")
                        }
                    },
                    onRequestOpenMountedAnchor = { selection, anchorData ->
                        scope.launch {
                            openAnchorNoteEditor(
                                anchorId = buildTokenMetaKey(
                                    blockId = block.blockId,
                                    start = selection.start,
                                    end = selection.end
                                ),
                                blockId = block.blockId,
                                quote = selection.token.trim(),
                                anchorHint = anchorData.anchorHint,
                                anchorData = anchorData
                            )
                        }
                    },
                    onRequestOpenTokenAnnotationEditor = { tokenSelection, anchor ->
                        openTokenAnnotationEditor(
                            blockId = block.blockId,
                            selection = tokenSelection,
                            anchor = anchor
                        )
                    },
                    onRequestOpenTokenAnnotationBubble = { tokenSelection, anchor ->
                        openTokenAnnotationBubble(
                            blockId = block.blockId,
                            selection = tokenSelection,
                            anchor = anchor
                        )
                    },
                    onMarkDeleted = {},
                    onRestoreDeleted = {},
                    onResonance = {},
                    onCommentCommitted = { _ -> },
                    onRequestOpenCommentPanel = { _ -> },
                    onRequestCloseCommentPanel = { _ -> },
                    onGestureEvent = onGestureEvent,
                    onTelemetry = ::emitTelemetry
                )
            }
        }

        FloatingCardBubbleOverlay(
            state = floatingBubbleState,
            card = floatingCard,
            isLoading = floatingCardLoading,
            errorMessage = floatingCardError,
            markwon = markwon,
            onDismiss = {
                dismissFloatingBubble(clearSelection = false)
            }
        )
        TokenAnnotationEditorOverlay(
            state = tokenAnnotationEditorState,
            viewportSize = overlayViewportSize,
            paragraphBoundsByBlockId = paragraphOverlayBoundsState,
            onDraftChange = { next ->
                tokenAnnotationEditorState = tokenAnnotationEditorState?.copy(draft = next)
            },
            onCommit = {
                persistTokenAnnotationEditor(reason = "editor_commit")
            },
            onDismiss = {
                persistTokenAnnotationEditor(reason = "editor_dismiss")
            }
        )
        TokenAnnotationBubbleOverlay(
            state = tokenAnnotationBubbleState,
            viewportSize = overlayViewportSize,
            paragraphBoundsByBlockId = paragraphOverlayBoundsState,
            onDismiss = {
                tokenAnnotationBubbleState = null
            }
        )
        val anchorEditorState = anchorNoteEditorState
        if (anchorEditorState != null) {
            val activeNote = currentAnchorEditorNote(anchorEditorState)
            ModalBottomSheet(
                onDismissRequest = {
                    anchorNoteEditorState = null
                    anchorNoteEditorRequestVersion += 1
                }
            ) {
                Column(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(horizontal = 16.dp, vertical = 12.dp),
                    verticalArrangement = Arrangement.spacedBy(10.dp)
                ) {
                    Text(
                        text = "Anchor Notes",
                        fontWeight = FontWeight.SemiBold,
                        color = Color(0xFF0F172A)
                    )
                    if (anchorEditorState.quote.isNotBlank()) {
                        Text(
                            text = "?${anchorEditorState.quote}?",
                            color = Color(0xFF475467),
                            maxLines = 2,
                            overflow = TextOverflow.Ellipsis
                        )
                    }
                    ScrollableTabRow(selectedTabIndex = anchorEditorState.notes.indexOfFirst { it.localId == anchorEditorState.activeNoteId }.coerceAtLeast(0)) {
                        anchorEditorState.notes.forEachIndexed { index, note ->
                            Tab(
                                selected = note.localId == anchorEditorState.activeNoteId,
                                onClick = {
                                    updateAnchorNoteEditorState { current ->
                                        current.copy(activeNoteId = note.localId)
                                    }
                                },
                                text = { Text(note.title.take(12).ifBlank { "Note ${index + 1}" }) }
                            )
                        }
                    }
                    OutlinedTextField(
                        value = anchorEditorState.anchorHint,
                        onValueChange = { next ->
                            updateAnchorNoteEditorState { current ->
                                current.copy(anchorHint = next.take(120))
                            }
                        },
                        modifier = Modifier.fillMaxWidth(),
                        singleLine = true,
                        label = { Text("Anchor Hint") }
                    )
                    if (activeNote != null) {
                        var editorValue by remember(activeNote.localId, activeNote.markdown) {
                            mutableStateOf(TextFieldValue(activeNote.markdown, TextRange(activeNote.markdown.length)))
                        }
                        OutlinedTextField(
                            value = activeNote.title,
                            onValueChange = { next ->
                                updateAnchorEditorCurrentNote { note, index ->
                                    val normalizedTitle = next.trim().ifBlank { "Note ${index + 1}" }
                                    val nextMarkdown = rewriteMarkdownTitle(editorValue.text, normalizedTitle)
                                    editorValue = editorValue.copy(text = nextMarkdown)
                                    note.copy(
                                        title = normalizedTitle,
                                        notePath = buildEditorNotePath(normalizedTitle, index),
                                        markdown = nextMarkdown,
                                        updatedAtMs = System.currentTimeMillis()
                                    )
                                }
                            },
                            modifier = Modifier.fillMaxWidth(),
                            singleLine = true,
                            label = { Text("Note Title") }
                        )
                        Row(
                            modifier = Modifier.fillMaxWidth(),
                            horizontalArrangement = Arrangement.spacedBy(8.dp)
                        ) {
                            FloatingActionButton(
                                onClick = {
                                    updateAnchorEditorCurrentNote { note, _ ->
                                        val nextValue = applyMarkdownHeading(editorValue, 1)
                                        editorValue = nextValue
                                        note.copy(markdown = nextValue.text, updatedAtMs = System.currentTimeMillis())
                                    }
                                },
                                modifier = Modifier.weight(1f)
                            ) { Text("H1") }
                            FloatingActionButton(
                                onClick = {
                                    updateAnchorEditorCurrentNote { note, _ ->
                                        val nextValue = applyMarkdownHeading(editorValue, 2)
                                        editorValue = nextValue
                                        note.copy(markdown = nextValue.text, updatedAtMs = System.currentTimeMillis())
                                    }
                                },
                                modifier = Modifier.weight(1f)
                            ) { Text("H2") }
                            FloatingActionButton(
                                onClick = {
                                    updateAnchorEditorCurrentNote { note, _ ->
                                        val nextValue = applyMarkdownWrap(editorValue, "**")
                                        editorValue = nextValue
                                        note.copy(markdown = nextValue.text, updatedAtMs = System.currentTimeMillis())
                                    }
                                },
                                modifier = Modifier.weight(1f)
                            ) { Text("Bold") }
                            FloatingActionButton(
                                onClick = {
                                    updateAnchorEditorCurrentNote { note, _ ->
                                        val nextValue = applyMarkdownWrap(editorValue, "*")
                                        editorValue = nextValue
                                        note.copy(markdown = nextValue.text, updatedAtMs = System.currentTimeMillis())
                                    }
                                },
                                modifier = Modifier.weight(1f)
                            ) { Text("Italic") }
                        }
                        if (!editorValue.selection.collapsed) {
                            Row(
                                modifier = Modifier.fillMaxWidth(),
                                horizontalArrangement = Arrangement.spacedBy(8.dp)
                            ) {
                                TextButton(
                                    onClick = {
                                        updateAnchorEditorCurrentNote { note, _ ->
                                            val nextValue = applyMarkdownWrap(editorValue, "**")
                                            editorValue = nextValue
                                            note.copy(markdown = nextValue.text, updatedAtMs = System.currentTimeMillis())
                                        }
                                    }
                                ) { Text("Bold Selection") }
                                TextButton(
                                    onClick = {
                                        updateAnchorEditorCurrentNote { note, _ ->
                                            val nextValue = applyMarkdownWrap(editorValue, "*")
                                            editorValue = nextValue
                                            note.copy(markdown = nextValue.text, updatedAtMs = System.currentTimeMillis())
                                        }
                                    }
                                ) { Text("Italic Selection") }
                                TextButton(
                                    onClick = {
                                        updateAnchorEditorCurrentNote { note, _ ->
                                            val nextValue = applyMarkdownHeading(editorValue, 3)
                                            editorValue = nextValue
                                            note.copy(markdown = nextValue.text, updatedAtMs = System.currentTimeMillis())
                                        }
                                    }
                                ) { Text("H3 Selection") }
                            }
                        }
                        Row(
                            modifier = Modifier.fillMaxWidth(),
                            horizontalArrangement = Arrangement.spacedBy(8.dp)
                        ) {
                            FloatingActionButton(
                                onClick = {
                                    updateAnchorNoteEditorState { current ->
                                        current.copy(mode = if (current.mode == AnchorEditorMode.EDIT) AnchorEditorMode.PREVIEW else AnchorEditorMode.EDIT)
                                    }
                                },
                                modifier = Modifier.weight(1f)
                            ) {
                                Text(if (anchorEditorState.mode == AnchorEditorMode.EDIT) "Preview" else "Edit")
                            }
                            FloatingActionButton(
                                onClick = {
                                    updateAnchorNoteEditorState { current ->
                                        val nextIndex = current.notes.size
                                        val note = createEditorNote(
                                            title = "Note ${nextIndex + 1}",
                                            markdown = "# Note ${nextIndex + 1}\n\n",
                                            notePath = buildEditorNotePath("Note ${nextIndex + 1}", nextIndex),
                                            index = nextIndex
                                        )
                                        current.copy(
                                            notes = current.notes + note,
                                            activeNoteId = note.localId,
                                            mode = AnchorEditorMode.EDIT
                                        )
                                    }
                                },
                                modifier = Modifier.weight(1f)
                            ) {
                                Text("Add")
                            }
                            FloatingActionButton(
                                onClick = {
                                    openPhase2bFloatingCard()
                                },
                                modifier = Modifier.weight(1f)
                            ) {
                                Text("Phase2B")
                            }
                            FloatingActionButton(
                                onClick = {
                                    scope.launch { saveAnchorNoteEditor() }
                                },
                                modifier = Modifier.weight(1f)
                            ) {
                                Text(if (anchorEditorState.isSaving) "Saving" else "Save")
                            }
                        }
                        if (anchorEditorState.notes.size > 1) {
                            TextButton(
                                onClick = {
                                    updateAnchorNoteEditorState { current ->
                                        val nextNotes = current.notes.filterNot { it.localId == activeNote.localId }
                                        current.copy(
                                            notes = nextNotes,
                                            activeNoteId = nextNotes.firstOrNull()?.localId.orEmpty()
                                        )
                                    }
                                }
                            ) {
                                Text("Delete Current Note")
                            }
                        }
                        OutlinedTextField(
                            value = editorValue,
                            onValueChange = { next ->
                                editorValue = next
                                updateAnchorEditorCurrentNote { note, _ ->
                                    note.copy(markdown = next.text, updatedAtMs = System.currentTimeMillis())
                                }
                            },
                            modifier = Modifier
                                .fillMaxWidth()
                                .heightIn(min = 180.dp),
                            label = { Text("Markdown") }
                        )
                        AndroidView(
                            modifier = Modifier
                                .fillMaxWidth()
                                .heightIn(min = 180.dp)
                                .background(Color(0xFFF8FAFC), RoundedCornerShape(14.dp))
                                .padding(horizontal = 12.dp, vertical = 10.dp),
                            factory = {
                                TextView(it).apply {
                                    setTextIsSelectable(false)
                                    textSize = 15f
                                    setLineSpacing(0f, 1.3f)
                                }
                            },
                            update = { textView ->
                                renderMountedAnchorDocument(
                                    textView = textView,
                                    markwon = markwon,
                                    markdown = editorValue.text,
                                    currentNotePath = activeNote.notePath,
                                    markdownPaths = anchorEditorState.notes.map { note -> note.notePath },
                                    onWikilinkTap = { link ->
                                        updateAnchorNoteEditorState { current ->
                                            current.copy(errorMessage = "WikiLink open is not supported in editor: ${link.displayText}")
                                        }
                                    }
                                )
                            }
                        )
                        if (phase2bFloatingCardState.visible) {
                            Card(
                                modifier = Modifier.fillMaxWidth(),
                                colors = CardDefaults.cardColors(containerColor = Color(0xFFF8FAFC))
                            ) {
                                Column(
                                    modifier = Modifier
                                        .fillMaxWidth()
                                        .padding(horizontal = 12.dp, vertical = 10.dp),
                                    verticalArrangement = Arrangement.spacedBy(8.dp)
                                ) {
                                    Row(
                                        modifier = Modifier.fillMaxWidth(),
                                        horizontalArrangement = Arrangement.SpaceBetween,
                                        verticalAlignment = Alignment.CenterVertically
                                    ) {
                                        Text(
                                            text = "Phase2B Card",
                                            fontWeight = FontWeight.SemiBold,
                                            color = Color(0xFF0F172A)
                                        )
                                        TextButton(onClick = {
                                            phase2bFloatingCardState = phase2bFloatingCardState.copy(visible = false)
                                        }) {
                                            Text("Close")
                                        }
                                    }
                                    if (phase2bFloatingCardState.loading) {
                                        CircularProgressIndicator(modifier = Modifier.size(18.dp), strokeWidth = 2.dp)
                                    } else if (!phase2bFloatingCardState.errorMessage.isNullOrBlank()) {
                                        val phase2bErrorText = phase2bFloatingCardState.errorMessage.orEmpty()
                                        Text(
                                            text = phase2bErrorText,
                                            color = Color(0xFFB42318)
                                        )
                                    } else {
                                        AndroidView(
                                            modifier = Modifier
                                                .fillMaxWidth()
                                                .heightIn(min = 120.dp)
                                                .background(Color.White, RoundedCornerShape(12.dp))
                                                .padding(horizontal = 12.dp, vertical = 10.dp),
                                            factory = {
                                                TextView(it).apply {
                                                    setTextIsSelectable(false)
                                                    textSize = 14f
                                                    setLineSpacing(0f, 1.3f)
                                                }
                                            },
                                            update = { textView ->
                                                renderMountedAnchorDocument(
                                                    textView = textView,
                                                    markwon = markwon,
                                                    markdown = phase2bFloatingCardState.resultMarkdown,
                                                    currentNotePath = activeNote.notePath,
                                                    markdownPaths = anchorEditorState.notes.map { note -> note.notePath },
                                                    onWikilinkTap = { }
                                                )
                                            }
                                        )
                                    }
                                }
                            }
                        }
                    }
                    if (!anchorEditorState.errorMessage.isNullOrBlank()) {
                        Text(
                            text = anchorEditorState.errorMessage,
                            color = Color(0xFFB42318)
                        )
                    }
                }
            }
        }
        val mountedPreviewState = mountedAnchorPreviewState
        if (mountedPreviewState != null) {
            ModalBottomSheet(
                onDismissRequest = {
                    mountedAnchorPreviewState = null
                    mountedAnchorPreviewRequestVersion += 1
                }
            ) {
                MountedAnchorPreviewContent(
                    state = mountedPreviewState,
                    markwon = markwon,
                    isFullscreen = false,
                    onWikilinkTap = { link ->
                        openMountedWikilink(link)
                    },
                    onJumpToBreadcrumb = { index ->
                        popMountedPreviewTo(index)
                    },
                    onPopOneLevel = {
                        popMountedPreviewOneLevel()
                    },
                    onBackToRoot = {
                        backToMountedPreviewRoot()
                    },
                    onSetFullscreen = { enabled ->
                        updateMountedPreviewState { current ->
                            current.copy(isFullscreen = enabled)
                        }
                    },
                    onBindGhostPath = { path ->
                        bindGhostMountedNote(path)
                    }
                )
            }
            if (mountedPreviewState.isFullscreen) {
                BackHandler(enabled = true) {
                    popMountedPreviewOneLevel()
                }
                Dialog(
                    onDismissRequest = {
                        updateMountedPreviewState { current ->
                            current.copy(isFullscreen = false)
                        }
                    },
                    properties = DialogProperties(
                        usePlatformDefaultWidth = false,
                        decorFitsSystemWindows = false
                    )
                ) {
                    Surface(
                        modifier = Modifier.fillMaxSize(),
                        color = Color(0xFFF8FAFC)
                    ) {
                        MountedAnchorPreviewContent(
                            state = mountedPreviewState,
                            markwon = markwon,
                            isFullscreen = true,
                            onWikilinkTap = { link ->
                                openMountedWikilink(link)
                            },
                            onJumpToBreadcrumb = { index ->
                                popMountedPreviewTo(index)
                            },
                            onPopOneLevel = {
                                popMountedPreviewOneLevel()
                            },
                            onBackToRoot = {
                                backToMountedPreviewRoot()
                            },
                            onSetFullscreen = { enabled ->
                                updateMountedPreviewState { current ->
                                    current.copy(isFullscreen = enabled)
                                }
                            },
                            onBindGhostPath = { path ->
                                bindGhostMountedNote(path)
                            }
                        )
                    }
                }
            }
        }
    }
}

/**
 * 单段语义块渲染器，负责：
 * 1. 处理段落级交互（选中、收藏、批注、删除）。
 * 2. 触发段落级手势和埋点上报。
 * 3. 在当前段内渲染 markdown 与强调样式。
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun TopographyParagraph(
    block: SemanticBlock,
    index: Int,
    listState: LazyListState,
    markwon: Markwon,
    renderConfig: MarkdownReaderRenderConfig,
    taskId: String?,
    apiBaseUrl: String,
    selection: TokenSelection?,
    overlayRootWindowOffset: Offset,
    isFavorited: Boolean,
    isMarkedDeleted: Boolean,
    existingComments: List<String>,
    likedTokenKeys: Set<String>,
    tokenAnnotations: Map<String, String>,
    anchors: Map<String, MobileAnchorData>,
    isCommentPanelExpanded: Boolean,
    shouldTrackParagraphBounds: Boolean,
    enableBlockInteractions: Boolean,
    onParagraphBoundsChanged: (ParagraphOverlayBounds?) -> Unit,
    onSelectionChanged: (TokenSelection?) -> Unit,
    onInsightTermTapped: (InsightTermTapPayload) -> Unit,
    onToggleTokenLike: (TokenSelection, Boolean) -> Unit,
    onUpsertTokenAnnotation: (TokenSelection, String) -> Unit,
    onUpsertAnchor: (TokenSelection, String?) -> Unit,
    onRemoveAnchor: (TokenSelection) -> Unit,
    onRequestOpenMountedAnchor: (TokenSelection, MobileAnchorData) -> Unit,
    onRequestOpenTokenAnnotationEditor: (TokenSelection, InsightTermAnchor?) -> Unit,
    onRequestOpenTokenAnnotationBubble: (TokenSelection, InsightTermAnchor?) -> Unit,
    onMarkDeleted: () -> Unit,
    onRestoreDeleted: () -> Unit,
    onResonance: () -> Unit,
    onCommentCommitted: (String) -> Unit,
    onRequestOpenCommentPanel: (String) -> Unit,
    onRequestCloseCommentPanel: (String) -> Unit,
    onGestureEvent: (ParagraphGestureEvent) -> Unit,
    onTelemetry: (ReaderTelemetryEvent) -> Unit
) {
    val scope = rememberCoroutineScope()
    val context = LocalContext.current
    val keyboardController = LocalSoftwareKeyboardController.current
    val haptic = LocalHapticFeedback.current
    val density = LocalDensity.current
    val view = LocalView.current
    var offsetX by remember(block.blockId) {
        mutableFloatStateOf(0f)
    }
    val rippleProgress = remember(block.blockId) {
        Animatable(1f)
    }
    var rippleCenter by remember(block.blockId) {
        mutableStateOf<Offset?>(null)
    }

    var paragraphWidthPx by remember(block.blockId) {
        mutableIntStateOf(1)
    }
    var emphasizedSelections by remember(block.blockId) {
        mutableStateOf<List<TokenSelection>>(emptyList())
    }
    var textRenderRefreshVersion by remember(block.blockId) {
        mutableIntStateOf(0)
    }
    var isNoiseExpanded by remember(block.blockId) {
        mutableStateOf(false)
    }
    var noteDraft by remember(block.blockId) {
        mutableStateOf("")
    }
    var pendingAnchorHintSelection by remember(block.blockId) {
        mutableStateOf<TokenSelection?>(null)
    }
    var pendingAnchorHintDraft by remember(block.blockId) {
        mutableStateOf("")
    }
    var activePreviewImage by remember(block.blockId) {
        mutableStateOf<InlineImageItem?>(null)
    }
    val paragraphBoundsForOverlayRef = remember(block.blockId) {
        ParagraphBoundsRef()
    }
    fun rangeKey(selection: TokenSelection): String {
        return "${selection.start}:${selection.end}"
    }
    fun isSelectionEmphasized(target: TokenSelection): Boolean {
        return emphasizedSelections.any { item ->
            item.start == target.start &&
                item.end == target.end &&
                item.token == target.token
        }
    }

    fun toggleSelectionEmphasis(target: TokenSelection): Boolean {
        val exists = isSelectionEmphasized(target)
        emphasizedSelections = if (exists) {
            emphasizedSelections.filterNot { item ->
                item.start == target.start &&
                    item.end == target.end &&
                    item.token == target.token
            }
        } else {
            emphasizedSelections + target
        }
        return !exists
    }
    suspend fun animateOffsetXTo(
        targetValue: Float,
        animationSpec: AnimationSpec<Float>
    ) {
        val initial = offsetX
        if (initial == targetValue) {
            return
        }
        animate(
            initialValue = initial,
            targetValue = targetValue,
            animationSpec = animationSpec
        ) { value, _ ->
            offsetX = value
        }
    }

    var hasHapticPlayedForSwipeCommit by remember(block.blockId) {
        mutableStateOf(false)
    }
    var swipeArmed by remember(block.blockId) {
        mutableStateOf(false)
    }
    var deleteConfirmArmedAtMs by remember(block.blockId) {
        mutableStateOf<Long?>(null)
    }
    var accumulatedDragX by remember(block.blockId) {
        mutableFloatStateOf(0f)
    }
    var accumulatedDragY by remember(block.blockId) {
        mutableFloatStateOf(0f)
    }
    var isNativeSelectionModeActive by remember(block.blockId) {
        mutableStateOf(false)
    }

    val readerText = remember(block.plainText, block.type) {
        compactReaderParagraphContent(
            raw = block.plainText,
            nodeType = block.type
        )
    }
    val rewrittenMedia = remember(block.markdown, taskId, apiBaseUrl) {
        rewriteReaderMarkdownMedia(
            markdown = block.markdown,
            taskId = taskId,
            apiBaseUrl = apiBaseUrl
        )
    }
    val readerMarkdown = rewrittenMedia.markdown
    val normalizedReaderMarkdown = remember(
        readerMarkdown,
        renderConfig.listIndentInputUnitSpaces,
        renderConfig.listIndentOutputUnitSpaces,
        renderConfig.listIndentMaxDepth,
        renderConfig.listIndentTabSpaces
    ) {
        readerMarkdown
    }
    val inlineVideos = rewrittenMedia.videos
    fun resolveSelectionByRangeKey(rangeKey: String): TokenSelection? {
        val parts = rangeKey.split(':')
        if (parts.size != 2) {
            return null
        }
        val start = parts[0].toIntOrNull() ?: return null
        val end = parts[1].toIntOrNull() ?: return null
        if (start < 0 || end <= start || end > readerText.length) {
            return null
        }
        val token = readerText.substring(start, end)
        if (token.isBlank()) {
            return null
        }
        return TokenSelection(
            token = token,
            start = start,
            end = end
        )
    }
    val likedSelections = emptyList<TokenSelection>()
    val annotatedSelections = tokenAnnotations
        .keys
        .mapNotNull(::resolveSelectionByRangeKey)
        .sortedBy { it.start }
    val resolvedAnchorSelections = anchors
        .mapNotNull { (rangeKey, anchorData) ->
            val resolved = resolveAnchorSelectionForDisplay(
                rangeKey = rangeKey,
                anchorData = anchorData,
                source = readerText
            ) ?: return@mapNotNull null
            ResolvedAnchorSelection(
                selection = resolved,
                anchorData = anchorData
            )
        }
        .sortedBy { it.selection.start }
    val anchorsByDisplayRange = remember(resolvedAnchorSelections) {
        linkedMapOf<String, MobileAnchorData>().apply {
            resolvedAnchorSelections.forEach { item ->
                putIfAbsent(rangeKey(item.selection), item.anchorData)
            }
        }
    }
    val pendingAnchorSelections = resolvedAnchorSelections
        .filter { !it.anchorData.status.equals("mounted", ignoreCase = true) }
        .map { it.selection }
    val mountedAnchorSelections = resolvedAnchorSelections
        .filter { it.anchorData.status.equals("mounted", ignoreCase = true) }
        .map { it.selection }
    val tokenAnnotationItems = tokenAnnotations
        .mapNotNull { (rangeKey, note) ->
            val selection = resolveSelectionByRangeKey(rangeKey) ?: return@mapNotNull null
            TokenAnnotationItem(
                token = selection.token.trim(),
                note = note
            )
        }
        .filter { it.note.isNotBlank() }

    val normalizedScore = block.relevanceScore.coerceIn(0f, 1f)
    val hasReasoning = !block.reasoning.isNullOrBlank()
    val isAbsoluteFocus = normalizedScore >= 0.85f
    val isNoise = !DISABLE_TEXT_IS_NOISE_JUDGMENT && normalizedScore < 0.3f
    val focusGuideColor = Color(0xFF4F46E5)
    val focusBridgeColor = focusGuideColor.copy(alpha = 0.7f)
    val textSize = renderConfig.textSizeDefaultSp.sp
    val isStandaloneHeading = remember(readerMarkdown) {
        val content = readerMarkdown.trim()
        content.startsWith("#") && content.lines().count { it.isNotBlank() } == 1
    }
    val lineSpacingMultiplier = renderConfig.lineSpacingDefault
    val textColor = when {
        isNoise -> Color(0xFF5B6169)
        else -> Color(0xFF050505)
    }
    val fontWeight = FontWeight.Normal
    val focusShrinkRatio = ((normalizedScore - 0.85f) / 0.15f).coerceIn(0f, 1f)
    val horizontalContentPadding = if (isAbsoluteFocus) {
        (14f - 6f * focusShrinkRatio).dp
    } else {
        14.dp
    }
    val finalFontWeight = if (isStandaloneHeading) FontWeight.Medium else FontWeight.Normal
    val isBlockquote = remember(readerMarkdown) {
        readerMarkdown.trimStart().startsWith(">")
    }

    // 同一父 node 内的子块之间用更紧凑的间距
    val isFirstBlock = block.blockIndex == 0
    val isLastBlock = block.blockIndex == block.blockCount - 1
    val isMultiBlock = block.blockCount > 1

    val outerPaddingTop = when {
        isMultiBlock && !isFirstBlock -> renderConfig.spacingSubblockTopDp.dp
        isBlockquote -> renderConfig.spacingBlockquoteTopDp.dp
        else -> renderConfig.spacingDefaultTopDp.dp
    }

    val headingInnerTopPadding = if (isStandaloneHeading) renderConfig.spacingHeadingTopDp.dp else 0.dp

    val outerPaddingBottom = when {
        isMultiBlock && !isLastBlock -> renderConfig.spacingSubblockBottomDp.dp
        isStandaloneHeading -> renderConfig.spacingHeadingBottomDp.dp
        else -> renderConfig.spacingDefaultBottomDp.dp
    }

    val paragraphInnerVerticalPadding = 0.dp

    val deleteRevealLimit = paragraphWidthPx * 0.32f
    val annotateRevealLimit = paragraphWidthPx * 0.28f
    val deleteRevealProgress = if (deleteRevealLimit <= 0f) {
        0f
    } else {
        ((-offsetX) / deleteRevealLimit).coerceIn(0f, 1f)
    }
    val annotateRevealProgress = if (annotateRevealLimit <= 0f) {
        0f
    } else {
        (offsetX / annotateRevealLimit).coerceIn(0f, 1f)
    }
    val deleteCommitThreshold = (deleteRevealLimit * 0.92f)
        .coerceAtLeast(with(density) { 96.dp.toPx() })
        .coerceAtMost(deleteRevealLimit)
    val annotateOpenThreshold = (annotateRevealLimit * 0.74f)
        .coerceAtLeast(with(density) { 68.dp.toPx() })
        .coerceAtMost(annotateRevealLimit)
    val deleteConfirmTimeoutMs = 5_000L
    val swipeArmThresholdPx = with(density) { 44.dp.toPx() }
    val swipeIntentDominanceRatio = 1.85f
    val latestIsMarkedDeleted = androidx.compose.runtime.rememberUpdatedState(isMarkedDeleted)
    val breathingAlpha = rememberNoiseBreathingAlpha(enabled = isNoise && !hasReasoning)
    val resolvedInsightTerms = remember(block.blockId, block.insightTerms, block.insightsTags) {
        block.resolvedInsightTerms()
    }
    val useNoiseCapsule = isNoise && hasReasoning
    val noiseGuideExpandProgress by animateFloatAsState(
        targetValue = if (useNoiseCapsule && isNoiseExpanded) 1f else 0f,
        animationSpec = spring(dampingRatio = 0.8f, stiffness = 400f),
        label = "noise-guide-expand-progress"
    )

    val safeBlockIndentLevel = block.indentLevel
        .coerceIn(0, renderConfig.listIndentMaxDepth.coerceAtLeast(0))
    val indentPaddingStart = (safeBlockIndentLevel * renderConfig.spacingIndentLevelDp).dp
    LaunchedEffect(block.blockId, block.indentLevel, safeBlockIndentLevel) {
        if (safeBlockIndentLevel != block.indentLevel) {
            Log.w(
                READER_LAYOUT_LOG_TAG,
                "block-indent-clamped blockId=${block.blockId} rawIndent=${block.indentLevel} safeIndent=$safeBlockIndentLevel"
            )
        }
    }

    DisposableEffect(block.blockId) {
        onDispose {
            paragraphBoundsForOverlayRef.value = null
            onParagraphBoundsChanged(null)
        }
    }

    SubcomposeAnchorLayout(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 18.dp)
            .padding(start = indentPaddingStart)
            .padding(top = outerPaddingTop, bottom = outerPaddingBottom)
            // 限制段落最大可读宽度，避免超宽屏导致行长过长。
            .widthIn(max = 720.dp)
            .onSizeChanged { paragraphWidthPx = max(1, it.width) },
        background = {
            if (enableBlockInteractions) {
                ParagraphSwipeBackdrop(
                    deleteRevealProgress = deleteRevealProgress,
                    annotateRevealProgress = annotateRevealProgress,
                    hasComments = existingComments.isNotEmpty(),
                    isMarkedDeleted = isMarkedDeleted,
                    isCommentPanelExpanded = isCommentPanelExpanded
                )
            }
        },
        foregroundOffsetX = offsetX.roundToInt(),
        foreground = {
            Column(
                modifier = Modifier
                    .fillMaxWidth()
            ) {
                Surface(
                    color = Color(0xFFFCFCFC),
                    modifier = Modifier
                        .fillMaxWidth()
                        .onGloballyPositioned { coordinates ->
                            val topLeftInWindow = coordinates.localToWindow(Offset.Zero)
                            val left = topLeftInWindow.x - overlayRootWindowOffset.x
                            val top = topLeftInWindow.y - overlayRootWindowOffset.y
                            val width = coordinates.size.width.toFloat()
                            val height = coordinates.size.height.toFloat()
                            val paragraphBounds = ParagraphOverlayBounds(
                                left = left,
                                top = top,
                                right = left + width,
                                bottom = top + height
                            )
                            paragraphBoundsForOverlayRef.value = paragraphBounds
                            if (shouldTrackParagraphBounds) {
                                onParagraphBoundsChanged(paragraphBounds)
                            }
                        }
                        .clip(RoundedCornerShape(14.dp))
                        .drawBehind {
                            if (isAbsoluteFocus) {
                                val x = 6.dp.toPx()
                                val topY = size.height * 0.06f
                                val bottomY = size.height * 0.94f
                                drawLine(
                                    color = focusGuideColor.copy(alpha = 0.16f),
                                    start = Offset(x = x, y = topY),
                                    end = Offset(x = x, y = bottomY),
                                    strokeWidth = 12.dp.toPx(),
                                    cap = StrokeCap.Round
                                )
                                drawLine(
                                    color = focusGuideColor.copy(alpha = 0.42f),
                                    start = Offset(x = x, y = topY),
                                    end = Offset(x = x, y = bottomY),
                                    strokeWidth = 6.dp.toPx(),
                                    cap = StrokeCap.Round
                                )
                                drawLine(
                                    color = focusGuideColor.copy(alpha = 0.98f),
                                    start = Offset(x = x, y = topY),
                                    end = Offset(x = x, y = bottomY),
                                    strokeWidth = 3.dp.toPx(),
                                    cap = StrokeCap.Round
                                )
                            } else if (isNoise && !hasReasoning) {
                                val x = 2.dp.toPx()
                                drawLine(
                                    color = Color(0xFF8DA4B5).copy(alpha = 0.28f + 0.2f * breathingAlpha),
                                    start = Offset(x = x, y = size.height * 0.1f),
                                    end = Offset(x = x, y = size.height * 0.9f),
                                    strokeWidth = 1.6.dp.toPx(),
                                    cap = StrokeCap.Round
                                )
                            }
                        }
                        .alpha(if (isMarkedDeleted) 0.6f else 1f)
                        .pointerInput(
                            enableBlockInteractions,
                            block.blockId,
                            deleteRevealLimit,
                            annotateRevealLimit,
                            isMarkedDeleted
                        ) {
                            if (!enableBlockInteractions) {
                                return@pointerInput
                            }
                            detectHorizontalDragGestures(
                                onDragStart = {
                                    hasHapticPlayedForSwipeCommit = false
                                    swipeArmed = false
                                    accumulatedDragX = 0f
                                    accumulatedDragY = 0f
                                },
                                onDragCancel = {
                                    scope.launch {
                                        accumulatedDragX = 0f
                                        accumulatedDragY = 0f
                                        if (!swipeArmed && abs(offsetX) < 1f) {
                                            return@launch
                                        }
                                        animateOffsetXTo(
                                            targetValue = 0f,
                                            animationSpec = spring(
                                                dampingRatio = Spring.DampingRatioNoBouncy,
                                                stiffness = Spring.StiffnessMedium
                                            )
                                        )
                                        onGestureEvent(
                                            ParagraphGestureEvent.Settle(
                                                nodeId = block.blockId,
                                                finalOffsetX = 0f
                                            )
                                        )
                                    }
                                },
                                onDragEnd = {
                                    scope.launch {
                                        accumulatedDragX = 0f
                                        accumulatedDragY = 0f
                                        if (!swipeArmed) {
                                            if (abs(offsetX) > 1f) {
                                                animateOffsetXTo(
                                                    targetValue = 0f,
                                                    animationSpec = spring(
                                                        dampingRatio = Spring.DampingRatioNoBouncy,
                                                        stiffness = Spring.StiffnessMedium
                                                    )
                                                )
                                            }
                                            return@launch
                                        }
                                        if (isNativeSelectionModeActive) {
                                            animateOffsetXTo(
                                                targetValue = 0f,
                                                animationSpec = spring(
                                                    dampingRatio = Spring.DampingRatioNoBouncy,
                                                    stiffness = Spring.StiffnessMedium
                                                )
                                            )
                                            swipeArmed = false
                                            return@launch
                                        }
                                        val endOffset = offsetX
                                        if (endOffset <= -deleteCommitThreshold) {
                                            onGestureEvent(
                                                ParagraphGestureEvent.SwipeLeft(
                                                    nodeId = block.blockId,
                                                    offsetX = endOffset,
                                                    threshold = deleteCommitThreshold
                                                )
                                            )
                                            if (latestIsMarkedDeleted.value) {
                                                deleteConfirmArmedAtMs = null
                                                onRestoreDeleted()
                                                onTelemetry(
                                                    ReaderTelemetryEvent(
                                                        nodeId = block.blockId,
                                                        eventType = "paragraph_restore_deleted_by_swipe",
                                                        relevanceScore = block.relevanceScore,
                                                        payload = mapOf(
                                                            "offsetX" to endOffset.toString(),
                                                            "threshold" to deleteCommitThreshold.toString()
                                                        )
                                                    )
                                                )
                                            } else {
                                                val now = System.currentTimeMillis()
                                                val armedAt = deleteConfirmArmedAtMs
                                                val armed = armedAt != null && (now - armedAt) <= deleteConfirmTimeoutMs
                                                if (!armed) {
                                                    deleteConfirmArmedAtMs = now
                                                    Toast.makeText(context, "再次左滑确认删除", Toast.LENGTH_SHORT).show()
                                                    onTelemetry(
                                                        ReaderTelemetryEvent(
                                                            nodeId = block.blockId,
                                                            eventType = "paragraph_mark_deleted_armed",
                                                            relevanceScore = block.relevanceScore,
                                                            payload = mapOf(
                                                                "offsetX" to endOffset.toString(),
                                                                "threshold" to deleteCommitThreshold.toString()
                                                            )
                                                        )
                                                    )
                                                } else {
                                                    deleteConfirmArmedAtMs = null
                                                    onMarkDeleted()
                                                    onTelemetry(
                                                        ReaderTelemetryEvent(
                                                            nodeId = block.blockId,
                                                            eventType = "paragraph_mark_deleted_by_swipe_confirmed",
                                                            relevanceScore = block.relevanceScore,
                                                            payload = mapOf(
                                                                "offsetX" to endOffset.toString(),
                                                                "threshold" to deleteCommitThreshold.toString()
                                                            )
                                                        )
                                                    )
                                                }
                                            }
                                        } else if (endOffset >= annotateOpenThreshold) {
                                            deleteConfirmArmedAtMs = null
                                            onGestureEvent(
                                                ParagraphGestureEvent.SwipeRight(
                                                    nodeId = block.blockId,
                                                    offsetX = endOffset,
                                                    threshold = annotateOpenThreshold
                                                )
                                            )
                                            onRequestOpenCommentPanel("swipe_right")
                                            onTelemetry(
                                                ReaderTelemetryEvent(
                                                    nodeId = block.blockId,
                                                    eventType = "paragraph_comment_affordance_triggered",
                                                    relevanceScore = block.relevanceScore,
                                                    payload = mapOf(
                                                        "offsetX" to endOffset.toString(),
                                                        "threshold" to annotateOpenThreshold.toString()
                                                    )
                                                )
                                            )
                                        }
                                        if (endOffset > -deleteCommitThreshold) {
                                            deleteConfirmArmedAtMs = null
                                        }
                                        animateOffsetXTo(
                                            targetValue = 0f,
                                            animationSpec = spring(
                                                dampingRatio = Spring.DampingRatioNoBouncy,
                                                stiffness = Spring.StiffnessMedium
                                            )
                                        )
                                        onGestureEvent(
                                            ParagraphGestureEvent.Settle(
                                                nodeId = block.blockId,
                                                finalOffsetX = offsetX
                                            )
                                        )
                                        swipeArmed = false
                                    }
                                },
                                onHorizontalDrag = { change, dragAmount ->
                                    if (isNativeSelectionModeActive) {
                                        return@detectHorizontalDragGestures
                                    }
                                    if (!swipeArmed) {
                                        if (listState.isScrollInProgress) {
                                            return@detectHorizontalDragGestures
                                        }
                                        val deltaY = change.position.y - change.previousPosition.y
                                        accumulatedDragX += dragAmount
                                        accumulatedDragY += deltaY
                                        val horizontalDominant = abs(accumulatedDragX) >= abs(accumulatedDragY) * swipeIntentDominanceRatio
                                        val reachedArmDistance = abs(accumulatedDragX) >= swipeArmThresholdPx
                                        if (!(horizontalDominant && reachedArmDistance)) {
                                            return@detectHorizontalDragGestures
                                        }
                                        swipeArmed = true
                                        onTelemetry(
                                            ReaderTelemetryEvent(
                                                nodeId = block.blockId,
                                                eventType = "paragraph_swipe_start",
                                                relevanceScore = block.relevanceScore,
                                                payload = mapOf(
                                                    "offsetX" to offsetX.toString()
                                                )
                                            )
                                        )
                                    }
                                    change.consume()
                                    val next = offsetX + dragAmount
                                    val clamped = next.coerceIn(
                                        minimumValue = -deleteRevealLimit,
                                        maximumValue = annotateRevealLimit
                                    )
                                    val crossedDeleteThreshold = clamped <= -deleteCommitThreshold
                                    val crossedAnnotateThreshold = clamped >= annotateOpenThreshold
                                    val crossedAnyThreshold = crossedDeleteThreshold || crossedAnnotateThreshold
                                    if (crossedAnyThreshold && !hasHapticPlayedForSwipeCommit) {
                                        haptic.performHapticFeedback(HapticFeedbackType.TextHandleMove)
                                        hasHapticPlayedForSwipeCommit = true
                                    } else if (!crossedAnyThreshold && hasHapticPlayedForSwipeCommit) {
                                        hasHapticPlayedForSwipeCommit = false
                                    }
                                    offsetX = clamped
                                }
                            )
                        }
                ) {
                Column(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(horizontal = horizontalContentPadding, vertical = paragraphInnerVerticalPadding)
                ) {
                    if (isAbsoluteFocus && hasReasoning) {
                        FocusBridgeLead(
                            text = block.reasoning.orEmpty(),
                            color = focusBridgeColor,
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(bottom = 5.dp)
                        )
                    }
                    Column(modifier = Modifier.padding(top = headingInnerTopPadding)) {
                        val showNoiseDetails = !useNoiseCapsule || isNoiseExpanded
                    androidx.compose.animation.AnimatedVisibility(
                        visible = useNoiseCapsule && !isNoiseExpanded,
                        enter = fadeIn(animationSpec = tween(durationMillis = 150)) + expandVertically(
                            expandFrom = Alignment.Top,
                            animationSpec = spring(dampingRatio = 0.8f, stiffness = 400f)
                        ),
                        exit = fadeOut(animationSpec = tween(durationMillis = 150)) + shrinkVertically(
                            shrinkTowards = Alignment.Top,
                            animationSpec = tween(durationMillis = 150)
                        )
                    ) {
                        NoiseBridgeCapsule(
                            text = block.reasoning.orEmpty(),
                            expanded = isNoiseExpanded,
                            chevronColor = Color(renderConfig.noiseChevronColorArgb),
                            chevronRotateDurationMs = renderConfig.noiseChevronRotateDurationMs,
                            modifier = Modifier.testTag("noise_capsule_${block.blockId}"),
                            onTap = {
                                isNoiseExpanded = true
                                haptic.performHapticFeedback(HapticFeedbackType.TextHandleMove)
                                onTelemetry(
                                    ReaderTelemetryEvent(
                                        nodeId = block.blockId,
                                        eventType = "noise_capsule_expanded",
                                        relevanceScore = block.relevanceScore
                                    )
                                )
                            }
                        )
                    }
                    androidx.compose.animation.AnimatedVisibility(
                        visible = showNoiseDetails,
                        enter = fadeIn(animationSpec = tween(durationMillis = 150)) + expandVertically(
                            expandFrom = Alignment.Top,
                            animationSpec = spring(dampingRatio = 0.8f, stiffness = 400f)
                        ),
                        exit = fadeOut(animationSpec = tween(durationMillis = 150)) + shrinkVertically(
                            shrinkTowards = Alignment.Top,
                            animationSpec = tween(durationMillis = 150)
                        )
                    ) {
                        Column(
                            modifier = Modifier
                                .fillMaxWidth()
                                .testTag("noise_expanded_${block.blockId}")
                                .drawBehind {
                                    if (useNoiseCapsule) {
                                        val x = 1.dp.toPx()
                                        val stroke = (2f + 0.8f * noiseGuideExpandProgress).dp.toPx()
                                        val topY = size.height * (0.18f - 0.1f * noiseGuideExpandProgress)
                                        val bottomY = size.height * (0.82f + 0.14f * noiseGuideExpandProgress)
                                        drawLine(
                                            color = Color(0xFF8DA4B5).copy(alpha = 0.3f),
                                            start = Offset(x = x, y = topY),
                                            end = Offset(x = x, y = bottomY),
                                            strokeWidth = stroke,
                                            cap = StrokeCap.Round
                                        )
                                    }
                                }
                                .padding(start = if (useNoiseCapsule) 4.dp else 0.dp)
                        ) {
                            Row(
                                verticalAlignment = Alignment.Top,
                                modifier = Modifier
                                    .fillMaxWidth()
                            ) {
                                Box(
                                    modifier = Modifier
                                        .weight(1f)
                                        .alpha(if (isCommentPanelExpanded) 0.98f else 1f)
                                        .drawBehind {
                                            val center = rippleCenter
                                            if (center != null) {
                                                val progress = rippleProgress.value.coerceIn(0f, 1f)
                                                if (progress > 0f && progress < 1f) {
                                                    val maxRippleRadius = max(size.width, size.height) * 0.9f
                                                    val baseRadius = 12.dp.toPx()
                                                    
                                                    // 使用 3 圈涟漪做双击共振反馈，兼顾性能与可感知度。
                                                    val rings = 3
                                                    for (i in 0 until rings) {
                                                        // 每一圈引入延迟，形成层叠扩散效果。
                                                        val ringDelay = i * 0.12f
                                                        if (progress > ringDelay) {
                                                            // 将当前圈的动画进度归一化到 [0, 1]。
                                                            val ringProgress = ((progress - ringDelay) / (1f - ringDelay)).coerceIn(0f, 1f)
                                                            
                                                            // 用缓出曲线控制半径扩张，让收尾更自然。
                                                            val expansion = 1f - (1f - ringProgress).toDouble().pow(3.0).toFloat()
                                                            
                                                            val radius = baseRadius + maxRippleRadius * expansion
                                                            
                                                            // 绘制外环：先扩散后衰减，增强反馈可感知性。
                                                            
                                                            val baseAlpha = if (ringProgress < 0.1f) {
                                                                ringProgress / 0.1f
                                                                } else {
                                                                1f - ringProgress // 先亮后暗，保持波纹自然收尾。
                                                            }
                                                            
                                                            // 叠加描边与填充，提升动效层次感。
                                                            
                                                            val ringColor = Color(0xFFFBBF24)
                                                            val strokeAlpha = baseAlpha * (0.6f - i * 0.15f).coerceAtLeast(0.1f)
                                                            val fillAlpha = baseAlpha * (0.2f - i * 0.05f).coerceAtLeast(0.05f)
                                                            
                                                            // 绘制外圈描边，强调选区边界。
                                                            drawCircle(
                                                                color = ringColor.copy(alpha = strokeAlpha),
                                                                radius = radius,
                                                                center = center,
                                                                style = androidx.compose.ui.graphics.drawscope.Stroke(
                                                                    width = (6.dp.toPx() * (1f - ringProgress)).coerceAtLeast(1.dp.toPx())
                                                                )
                                                            )
                                                            
                                                            // 绘制内圈填充，避免视觉断层。
                                                            
                                                            drawCircle(
                                                                color = ringColor.copy(alpha = fillAlpha),
                                                                radius = radius * 0.98f,
                                                                center = center
                                                            )
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                ) {
                                    MarkdownParagraph(
                                        markdown = normalizedReaderMarkdown,
                                        plainText = readerText,
                                        markwon = markwon,
                                        renderConfig = renderConfig,
                                        renderRefreshVersion = textRenderRefreshVersion,
                                        textSizeSp = textSize.value,
                                        lineSpacingMultiplier = lineSpacingMultiplier,
                                        textColor = textColor,
                                        fontWeight = if (useNoiseCapsule) FontWeight.Normal else finalFontWeight,
                                        blockId = block.blockId,
                                        blockType = block.type,
                                        blockIndentLevel = safeBlockIndentLevel,
                                        selection = selection,
                                        isFavorited = isFavorited,
                                        overlayRootWindowOffset = overlayRootWindowOffset,
                                        insightTerms = resolvedInsightTerms,
                                        emphasizedSelections = emphasizedSelections,
                                        likedSelections = likedSelections,
                                        annotatedSelections = annotatedSelections,
                                        pendingAnchorSelections = pendingAnchorSelections,
                                        mountedAnchorSelections = mountedAnchorSelections,
                                        modifier = Modifier.fillMaxWidth(),
                                        onSelectionAction = { action, selected, anchor ->
                                            if (action == SelectionContextAction.ToggleBold) {
                                                onSelectionChanged(null)
                                            } else {
                                                onSelectionChanged(selected)
                                            }
                                            when (action) {
                                                SelectionContextAction.Copy -> {
                                                    val tokenText = selected.token.trim()
                                                    val copied = copyTextToClipboard(context, tokenText)
                                                    val hint = if (copied) {
                                                        "已复制：$tokenText"
                                                    } else {
                                                        "复制失败"
                                                    }
                                                    Toast.makeText(context, hint, Toast.LENGTH_SHORT).show()
                                                    onTelemetry(
                                                        ReaderTelemetryEvent(
                                                            nodeId = block.blockId,
                                                            eventType = "selection_action_copy",
                                                            relevanceScore = block.relevanceScore,
                                                            payload = mapOf(
                                                                "token" to tokenText,
                                                                "success" to copied.toString()
                                                            )
                                                        )
                                                    )
                                                }
                                                SelectionContextAction.ToggleLike -> {
                                                    val key = rangeKey(selected)
                                                    val isLiked = likedTokenKeys.contains(key)
                                                    onToggleTokenLike(selected, !isLiked)
                                                    Toast.makeText(
                                                        context,
                                                        if (isLiked) "已取消喜欢" else "已喜欢",
                                                        Toast.LENGTH_SHORT
                                                    ).show()
                                                    onTelemetry(
                                                        ReaderTelemetryEvent(
                                                            nodeId = block.blockId,
                                                            eventType = if (isLiked) "selection_action_unlike" else "selection_action_like",
                                                            relevanceScore = block.relevanceScore,
                                                            payload = mapOf(
                                                                "token" to selected.token
                                                            )
                                                        )
                                                    )
                                                }
                                                SelectionContextAction.ToggleBold -> {
                                                    val emphasizedNow = toggleSelectionEmphasis(selected)
                                                    textRenderRefreshVersion += 1
                                                    onSelectionChanged(null)
                                                    Toast.makeText(
                                                        context,
                                                        if (emphasizedNow) "已加粗" else "已取消加粗",
                                                        Toast.LENGTH_SHORT
                                                    ).show()
                                                    onTelemetry(
                                                        ReaderTelemetryEvent(
                                                            nodeId = block.blockId,
                                                            eventType = if (emphasizedNow) "selection_action_bold" else "selection_action_unbold",
                                                            relevanceScore = block.relevanceScore,
                                                            payload = mapOf(
                                                                "token" to selected.token
                                                            )
                                                        )
                                                    )
                                                }
                                                SelectionContextAction.Annotate -> {
                                                    val annotationAnchor = anchor
                                                        ?: paragraphBoundsForOverlayRef.value?.toOverlayAnchor()
                                                    onRequestOpenTokenAnnotationEditor(selected, annotationAnchor)
                                                    onTelemetry(
                                                        ReaderTelemetryEvent(
                                                            nodeId = block.blockId,
                                                            eventType = "selection_action_annotate",
                                                            relevanceScore = block.relevanceScore,
                                                            payload = mapOf(
                                                                "token" to selected.token
                                                            )
                                                        )
                                                    )
                                                }
                                                SelectionContextAction.SearchCard -> {
                                                    val tokenText = selected.token.trim()
                                                    if (tokenText.isNotEmpty() && anchor != null) {
                                                        onInsightTermTapped(
                                                            InsightTermTapPayload(
                                                                range = InsightTermRange(
                                                                    term = tokenText,
                                                                    start = selected.start,
                                                                    end = selected.end
                                                                ),
                                                                anchor = anchor
                                                            )
                                                        )
                                                    }
                                                    onTelemetry(
                                                        ReaderTelemetryEvent(
                                                            nodeId = block.blockId,
                                                            eventType = "selection_action_search_card",
                                                            relevanceScore = block.relevanceScore,
                                                            payload = mapOf(
                                                                "token" to selected.token
                                                            )
                                                        )
                                                    )
                                                }
                                                SelectionContextAction.MarkAnchor -> {
                                                    val anchorId = buildTokenMetaKey(
                                                        blockId = block.blockId,
                                                        start = selected.start,
                                                        end = selected.end
                                                    )
                                                    val rangeKey = rangeKey(selected)
                                                    val existingAnchor = anchors[rangeKey]
                                                    if (existingAnchor != null) {
                                                        onRemoveAnchor(selected)
                                                        pendingAnchorHintSelection = null
                                                        pendingAnchorHintDraft = ""
                                                        haptic.performHapticFeedback(HapticFeedbackType.LongPress)
                                                        Toast.makeText(
                                                            context,
                                                            "锚点已移除",
                                                            Toast.LENGTH_SHORT
                                                        ).show()
                                                        onTelemetry(
                                                            ReaderTelemetryEvent(
                                                                nodeId = block.blockId,
                                                                eventType = "anchor_removed",
                                                                relevanceScore = block.relevanceScore,
                                                                payload = mapOf(
                                                                    "anchorId" to anchorId,
                                                                    "blockId" to block.blockId,
                                                                    "start" to selected.start.toString(),
                                                                    "end" to selected.end.toString(),
                                                                    "quote" to selected.token.trim(),
                                                                    "status_before_remove" to existingAnchor.status.trim()
                                                                )
                                                            )
                                                        )
                                                    } else {
                                                        val contextQuote = buildAnchorContextQuoteSnapshot(
                                                            blockText = readerText,
                                                            start = selected.start,
                                                            end = selected.end
                                                        )
                                                        onUpsertAnchor(selected, null)
                                                        pendingAnchorHintDraft = ""
                                                        pendingAnchorHintSelection = null
                                                        onRequestOpenMountedAnchor(
                                                            selected,
                                                            MobileAnchorData(
                                                                blockId = block.blockId,
                                                                startIndex = selected.start,
                                                                endIndex = selected.end,
                                                                quote = selected.token.trim(),
                                                                contextQuote = contextQuote,
                                                                anchorHint = "",
                                                                status = "pending",
                                                                mountedPath = "",
                                                                mountedRevisionId = "",
                                                                updatedAt = "",
                                                                revisions = emptyList()
                                                            )
                                                        )
                                                        haptic.performHapticFeedback(HapticFeedbackType.LongPress)
                                                        Toast.makeText(
                                                            context,
                                                            "锚点已标记，可补充一句备忘",
                                                            Toast.LENGTH_SHORT
                                                        ).show()
                                                        onTelemetry(
                                                            ReaderTelemetryEvent(
                                                                nodeId = block.blockId,
                                                                eventType = "anchor_created",
                                                                relevanceScore = block.relevanceScore,
                                                                payload = mapOf(
                                                                    "anchorId" to anchorId,
                                                                    "blockId" to block.blockId,
                                                                    "start" to selected.start.toString(),
                                                                    "end" to selected.end.toString(),
                                                                    "quote" to selected.token.trim(),
                                                                    "context_quote" to contextQuote,
                                                                    "anchor_hint" to pendingAnchorHintDraft.trim()
                                                                )
                                                            )
                                                        )
                                                    }
                                                }
                                            }
                                        },
                                        onTokenSingleTap = { selection, anchor ->
                                            if (selection != null) {
                                                val anchorKey = rangeKey(selection)
                                                val mountedAnchor = anchorsByDisplayRange[anchorKey]
                                                    ?: anchors[anchorKey]
                                                if (mountedAnchor != null) {
                                                    onSelectionChanged(null)
                                                    haptic.performHapticFeedback(HapticFeedbackType.TextHandleMove)
                                                    onRequestOpenMountedAnchor(selection, mountedAnchor)
                                                    return@MarkdownParagraph
                                                }
                                                val note = tokenAnnotations[rangeKey(selection)].orEmpty().trim()
                                                if (note.isNotBlank()) {
                                                    val annotationAnchor = anchor
                                                        ?: paragraphBoundsForOverlayRef.value?.toOverlayAnchor()
                                                    onRequestOpenTokenAnnotationBubble(selection, annotationAnchor)
                                                }
                                                onSelectionChanged(null)
                                                haptic.performHapticFeedback(HapticFeedbackType.TextHandleMove)
                                                onTelemetry(
                                                    ReaderTelemetryEvent(
                                                        nodeId = block.blockId,
                                                        eventType = "lexical_token_selected",
                                                        relevanceScore = block.relevanceScore,
                                                        payload = mapOf(
                                                            "token" to selection.token,
                                                            "start" to selection.start.toString(),
                                                            "end" to selection.end.toString()
                                                        )
                                                    )
                                                )
                                            } else if (enableBlockInteractions && existingComments.isNotEmpty()) {
                                                onRequestOpenCommentPanel("paragraph_tap_with_comment")
                                            }
                                        },
                                        onInsightTermTap = { tap ->
                                            val selectedToken = tap.range.term.trim()
                                            onSelectionChanged(null)
                                            haptic.performHapticFeedback(HapticFeedbackType.LongPress)
                                            onTelemetry(
                                                ReaderTelemetryEvent(
                                                    nodeId = block.blockId,
                                                    eventType = "insight_term_tapped",
                                                    relevanceScore = block.relevanceScore,
                                                    payload = mapOf(
                                                        "token" to selectedToken,
                                                        "source" to "insight_terms"
                                                    )
                                                )
                                            )
                                            onInsightTermTapped(tap)
                                            scope.launch {
                                                autoCenterItem(
                                                    listState = listState,
                                                    itemIndex = index,
                                                    centerRatio = 0.45f
                                                )
                                            }
                                        },
                                        onInlineImageTap = { image ->
                                            activePreviewImage = image
                                            onTelemetry(
                                                ReaderTelemetryEvent(
                                                    nodeId = block.blockId,
                                                    eventType = "inline_image_preview_opened",
                                                    relevanceScore = block.relevanceScore,
                                                    payload = mapOf("url" to image.url)
                                                )
                                            )
                                        },
                                        onParagraphDoubleTap = { tapOffset ->
                                            if (!enableBlockInteractions) {
                                                return@MarkdownParagraph
                                            }
                                            rippleCenter = tapOffset
                                            scope.launch {
                                                rippleProgress.snapTo(0f)
                                                rippleProgress.animateTo(
                                                    targetValue = 1f,
                                                    animationSpec = tween(durationMillis = 620, easing = androidx.compose.animation.core.FastOutSlowInEasing)
                                                )
                                                // 动画结束后清空中心点，避免残留波纹状态。
                                                rippleCenter = null
                                                }
                                            scope.launch {
                                                view.performHapticFeedback(HapticFeedbackConstants.KEYBOARD_TAP)
                                                delay(90)
                                                view.performHapticFeedback(HapticFeedbackConstants.KEYBOARD_TAP)
                                            }
                                            onGestureEvent(
                                                ParagraphGestureEvent.DoubleTap(nodeId = block.blockId)
                                            )
                                            onResonance()
                                            onTelemetry(
                                                ReaderTelemetryEvent(
                                                    nodeId = block.blockId,
                                                    eventType = "paragraph_resonance_double_tap",
                                                    relevanceScore = block.relevanceScore,
                                                    payload = mapOf(
                                                        "x" to tapOffset.x.toString(),
                                                        "y" to tapOffset.y.toString()
                                                    )
                                                )
                                            )
                                        },
                                        onSelectionModeChanged = { isActive ->
                                            isNativeSelectionModeActive = isActive
                                            if (!isActive) {
                                                onSelectionChanged(null)
                                            }
                                        },
                                    )
                                }
                                if (inlineVideos.isNotEmpty()) {
                                    InlineVideoList(
                                        videos = inlineVideos,
                                        modifier = Modifier
                                            .fillMaxWidth()
                                            .padding(top = 12.dp),
                                        onTelemetry = { eventType, url ->
                                            onTelemetry(
                                                ReaderTelemetryEvent(
                                                    nodeId = block.blockId,
                                                    eventType = eventType,
                                                    relevanceScore = block.relevanceScore,
                                                    payload = mapOf("url" to url)
                                                )
                                            )
                                        }
                                    )
                                }
                            }
                            if (useNoiseCapsule && isNoiseExpanded) {
                                Row(
                                    modifier = Modifier
                                        .fillMaxWidth()
                                        .testTag("noise_collapse_affordance_${block.blockId}")
                                        .padding(top = 8.dp)
                                        .pointerInput(block.blockId) {
                                            detectTapGestures(
                                                onTap = {
                                                    isNoiseExpanded = false
                                                    scope.launch {
                                                        listState.animateScrollToItem(index)
                                                    }
                                                    onTelemetry(
                                                        ReaderTelemetryEvent(
                                                            nodeId = block.blockId,
                                                            eventType = "noise_capsule_collapsed_by_affordance",
                                                            relevanceScore = block.relevanceScore
                                                        )
                                                    )
                                                }
                                            )
                                        },
                                    horizontalArrangement = Arrangement.End
                                ) {
                                    NoiseChevronIndicator(
                                        expanded = isNoiseExpanded,
                                        modifier = Modifier.size(16.dp),
                                        tint = Color(renderConfig.noiseChevronColorArgb).copy(alpha = 0.55f),
                                        rotateDurationMs = renderConfig.noiseChevronRotateDurationMs
                                    )
                                }
                            }
                        }
                    }
                    val previewImage = activePreviewImage
                    if (previewImage != null) {
                        ReaderImageLightbox(
                            image = previewImage,
                            onDismiss = {
                                activePreviewImage = null
                                onTelemetry(
                                    ReaderTelemetryEvent(
                                        nodeId = block.blockId,
                                        eventType = "inline_image_preview_closed",
                                        relevanceScore = block.relevanceScore
                                    )
                                )
                            }
                        )
                    }
                    if (enableBlockInteractions && isMarkedDeleted) {
                        Row(
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(top = 10.dp),
                            horizontalArrangement = Arrangement.spacedBy(8.dp),
                            verticalAlignment = Alignment.CenterVertically
                        ) {
                            if (isMarkedDeleted) {
                                Surface(
                                    color = Color(0xFFFEF2F2),
                                    shape = RoundedCornerShape(4.dp)
                                ) {
                                    Text(
                                        text = "Deleted",
                                        fontSize = 11.sp,
                                        color = Color(0xFFDC2626),
                                        fontWeight = FontWeight.Medium,
                                        modifier = Modifier.padding(horizontal = 6.dp, vertical = 2.dp)
                                    )
                                }
                            }
                        }
                    }

                    if (enableBlockInteractions && existingComments.isNotEmpty()) {
                        CommentUnderlineAffordance(
                            commentsCount = existingComments.size,
                            isExpanded = isCommentPanelExpanded,
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(top = 8.dp),
                            onTap = {
                                onRequestOpenCommentPanel("underline_tap")
                            }
                        )
                    }
                }
            }
            androidx.compose.animation.AnimatedVisibility(
                visible = enableBlockInteractions && isCommentPanelExpanded,
                enter = fadeIn(animationSpec = tween(durationMillis = 110)) + expandVertically(
                    expandFrom = Alignment.Top,
                    animationSpec = spring(dampingRatio = 0.82f, stiffness = 420f)
                ),
                exit = fadeOut(animationSpec = tween(durationMillis = 80)) + shrinkVertically(
                    shrinkTowards = Alignment.Top,
                    animationSpec = tween(durationMillis = 95)
                )
            ) {
                InlineCommentPanel(
                    blockId = block.blockId,
                    existingComments = existingComments,
                    tokenAnnotations = tokenAnnotationItems,
                    draft = noteDraft,
                    onDraftChange = { noteDraft = it },
                    onCommit = {
                        val normalized = noteDraft.trim()
                        if (normalized.isNotEmpty()) {
                            onCommentCommitted(normalized)
                            noteDraft = ""
                            onTelemetry(
                                ReaderTelemetryEvent(
                                    nodeId = block.blockId,
                                    eventType = "note_saved",
                                    relevanceScore = block.relevanceScore,
                                    payload = mapOf("length" to normalized.length.toString())
                                )
                            )
                        }
                    },
                    onCloseByTap = {
                        onRequestCloseCommentPanel("panel_tap_close")
                    },
                    onCloseByPinch = {
                        onRequestCloseCommentPanel("pinch_in")
                    }
                )
            }
        }
        }
        }
    )
    val hintSelection = pendingAnchorHintSelection
    if (hintSelection != null) {
        ModalBottomSheet(
            onDismissRequest = {
                pendingAnchorHintSelection = null
                pendingAnchorHintDraft = ""
                keyboardController?.hide()
            }
        ) {
            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 16.dp, vertical = 8.dp),
                verticalArrangement = Arrangement.spacedBy(10.dp)
            ) {
                Text(
                    text = "正在为 \"${hintSelection.token.trim()}\" 设立锚点…",
                    fontSize = 15.sp,
                    color = Color(0xFF0F172A),
                    fontWeight = FontWeight.SemiBold
                )
                OutlinedTextField(
                    value = pendingAnchorHintDraft,
                    onValueChange = { next ->
                        if (next.contains('\n')) {
                            val normalized = next.replace('\n', ' ').trim()
                            onUpsertAnchor(hintSelection, normalized)
                            onTelemetry(
                                ReaderTelemetryEvent(
                                    nodeId = block.blockId,
                                    eventType = "anchor_hint_updated",
                                    relevanceScore = block.relevanceScore,
                                    payload = mapOf(
                                        "anchorId" to buildTokenMetaKey(
                                            blockId = block.blockId,
                                            start = hintSelection.start,
                                            end = hintSelection.end
                                        ),
                                        "anchor_hint" to normalized
                                    )
                                )
                            )
                            pendingAnchorHintSelection = null
                            pendingAnchorHintDraft = ""
                            keyboardController?.hide()
                            Toast.makeText(context, "锚点备忘已保存", Toast.LENGTH_SHORT).show()
                        } else {
                            pendingAnchorHintDraft = next.take(120)
                        }
                    },
                    placeholder = {
                        Text("一句话记录你此刻的灵感（选填，回车直接保存）")
                    },
                    modifier = Modifier.fillMaxWidth()
                )
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.End
                ) {
                    TextButton(
                        onClick = {
                            pendingAnchorHintSelection = null
                            pendingAnchorHintDraft = ""
                            keyboardController?.hide()
                        }
                    ) {
                        Text("跳过")
                    }
                    TextButton(
                        onClick = {
                            val normalized = pendingAnchorHintDraft.trim()
                            onUpsertAnchor(hintSelection, normalized)
                            onTelemetry(
                                ReaderTelemetryEvent(
                                    nodeId = block.blockId,
                                    eventType = "anchor_hint_updated",
                                    relevanceScore = block.relevanceScore,
                                    payload = mapOf(
                                        "anchorId" to buildTokenMetaKey(
                                            blockId = block.blockId,
                                            start = hintSelection.start,
                                            end = hintSelection.end
                                        ),
                                        "anchor_hint" to normalized
                                    )
                                )
                            )
                            pendingAnchorHintSelection = null
                            pendingAnchorHintDraft = ""
                            keyboardController?.hide()
                            if (normalized.isNotBlank()) {
                                Toast.makeText(context, "锚点备忘已保存", Toast.LENGTH_SHORT).show()
                            }
                        }
                    ) {
                        Text("保存")
                    }
                }
            }
        }
    }
}

private fun copyTextToClipboard(context: Context, text: String): Boolean {
    val content = text.trim()
    if (content.isEmpty()) {
        return false
    }
    val manager = context.getSystemService(Context.CLIPBOARD_SERVICE) as? ClipboardManager
        ?: return false
    manager.setPrimaryClip(ClipData.newPlainText("selected_token", content))
    return true
}

/**
 * 焦点桥样式字体族：统一“桥接提示”在正文中的视觉语言。
 */
private val FocusBridgeSerifFontFamily = FontFamily(
    Font(R.font.noto_serif_regular, weight = FontWeight.Normal),
    Font(R.font.noto_serif_medium, weight = FontWeight.Medium),
    Font(R.font.noto_serif_bold, weight = FontWeight.Bold)
)

@Composable
private fun FocusBridgeLead(
    text: String,
    color: Color,
    modifier: Modifier = Modifier
) {
    val bridgeLine = text.trim()
    if (bridgeLine.isBlank()) {
        return
    }
    Row(
        modifier = modifier,
        verticalAlignment = Alignment.Top
    ) {
        Text(
            text = "\u2726",
            color = color,
            fontSize = 10.sp,
            lineHeight = 14.sp,
            fontFamily = FocusBridgeSerifFontFamily,
            fontWeight = FontWeight.SemiBold,
            modifier = Modifier.padding(top = 1.dp, end = 5.dp)
        )
        Text(
            text = bridgeLine,
            color = color,
            fontSize = 15.sp,
            lineHeight = 21.sp,
            fontFamily = FocusBridgeSerifFontFamily,
            fontWeight = FontWeight.SemiBold
        )
    }
}

@Composable
private fun NoiseBridgeCapsule(
    text: String,
    expanded: Boolean,
    chevronColor: Color,
    chevronRotateDurationMs: Int,
    onTap: () -> Unit,
    modifier: Modifier = Modifier
) {
    val summary = text.trim().ifEmpty { "..." }
    Surface(
        modifier = modifier
            .fillMaxWidth()
            .pointerInput(summary) {
                detectTapGestures(onTap = { onTap() })
            },
        shape = RoundedCornerShape(12.dp),
        color = Color(0xFFF5F7F9),
        tonalElevation = 1.dp,
        shadowElevation = 1.dp
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .drawBehind {
                    val x = 1.dp.toPx()
                    drawLine(
                        color = Color(0xFF8DA4B5).copy(alpha = 0.3f),
                        start = Offset(x = x, y = size.height * 0.22f),
                        end = Offset(x = x, y = size.height * 0.78f),
                        strokeWidth = 2.dp.toPx(),
                        cap = StrokeCap.Round
                    )
                }
                .padding(start = 12.dp, end = 10.dp, top = 10.dp, bottom = 10.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = summary,
                color = Color(0xFF5B6169),
                fontSize = 14.sp,
                lineHeight = 20.sp,
                fontWeight = FontWeight.Medium,
                maxLines = 2,
                overflow = TextOverflow.Ellipsis,
                modifier = Modifier.weight(1f)
            )
            NoiseChevronIndicator(
                expanded = expanded,
                modifier = Modifier
                    .padding(start = 8.dp)
                    .size(16.dp),
                tint = chevronColor.copy(alpha = 0.52f),
                rotateDurationMs = chevronRotateDurationMs
            )
        }
    }
}

@Composable
private fun NoiseChevronIndicator(
    expanded: Boolean,
    modifier: Modifier = Modifier,
    tint: Color = Color(0xFF8DA4B5).copy(alpha = 0.52f),
    rotateDurationMs: Int = 220
) {
    val rotation by animateFloatAsState(
        targetValue = if (expanded) 180f else 0f,
        animationSpec = tween(durationMillis = rotateDurationMs),
        label = "noise-chevron-rotation"
    )
    Icon(
        imageVector = NoiseChevronDownVector,
        contentDescription = null,
        tint = tint,
        modifier = modifier.rotate(rotation)
    )
}

@Composable
private fun rememberNoiseBreathingAlpha(enabled: Boolean): Float {
    if (!enabled) {
        return 1f
    }
    val alpha by rememberInfiniteTransition(label = "noise-bridge-breathing").animateFloat(
        initialValue = 0.3f,
        targetValue = 1f,
        animationSpec = infiniteRepeatable(
            animation = tween(durationMillis = 2_000),
            repeatMode = RepeatMode.Reverse
        ),
        label = "noise-bridge-breathing-alpha"
    )
    return alpha
}

private val NoiseChevronDownVector: ImageVector by lazy {
    ImageVector.Builder(
        name = "NoiseChevronDownVector",
        defaultWidth = 16.dp,
        defaultHeight = 16.dp,
        viewportWidth = 24f,
        viewportHeight = 24f
    ).apply {
        path(
            fill = SolidColor(Color.Transparent),
            stroke = SolidColor(Color.Black),
            strokeLineWidth = 2.4f,
            strokeLineCap = StrokeCap.Round,
            strokeLineJoin = StrokeJoin.Round
        ) {
            moveTo(7f, 10f)
            lineTo(12f, 15f)
            lineTo(17f, 10f)
        }
    }.build()
}

@Composable
private fun SubcomposeAnchorLayout(
    modifier: Modifier,
    background: @Composable () -> Unit,
    foregroundOffsetX: Int,
    foreground: @Composable () -> Unit
) {
    SubcomposeLayout(modifier = modifier) { constraints ->
        val hasBoundedWidth = constraints.maxWidth != Constraints.Infinity
        val foregroundConstraints = if (hasBoundedWidth) {
            constraints.copy(
                minWidth = constraints.maxWidth,
                maxWidth = constraints.maxWidth
            )
        } else {
            constraints
        }
        val foregroundPlaceables = subcompose("foreground", foreground).map {
            it.measure(foregroundConstraints)
        }

        val layoutWidth = if (hasBoundedWidth) {
            constraints.maxWidth.coerceAtLeast(constraints.minWidth)
        } else {
            (foregroundPlaceables.maxOfOrNull { it.width } ?: 0)
                .coerceAtLeast(constraints.minWidth)
        }
        // 前景层尺寸以内容尺寸为准，保证滑动底板与正文区域严格对齐。
        val layoutHeight = foregroundPlaceables.sumOf { it.height }
            .coerceIn(constraints.minHeight, constraints.maxHeight)
        val backgroundConstraints = Constraints(
            minWidth = layoutWidth,
            maxWidth = layoutWidth,
            minHeight = layoutHeight,
            maxHeight = layoutHeight
        )
        val backgroundPlaceables = subcompose("background", background).map {
            it.measure(backgroundConstraints)
        }
        layout(layoutWidth, layoutHeight) {
            backgroundPlaceables.forEach { placeable ->
                placeable.placeRelative(x = 0, y = 0)
            }
            var foregroundY = 0
            foregroundPlaceables.forEach { placeable ->
                placeable.placeRelative(
                    x = foregroundOffsetX,
                    y = foregroundY
                )
                foregroundY += placeable.height
            }
        }
    }
}

/**
 * 段落滑动操作的背景层。
 * 左滑显示“批注”动作，右滑显示“删除”动作，并根据手势进度做透明度与图标缩放反馈。
 */
@Composable
private fun ParagraphSwipeBackdrop(
    deleteRevealProgress: Float,
    annotateRevealProgress: Float,
    hasComments: Boolean,
    isMarkedDeleted: Boolean,
    isCommentPanelExpanded: Boolean
) {
    val leftProgress = annotateRevealProgress.coerceIn(0f, 1f)
    val rightProgress = deleteRevealProgress.coerceIn(0f, 1f)
    val alpha = if (isCommentPanelExpanded) {
        0f
    } else {
        (0.2f + max(leftProgress, rightProgress) * 0.8f).coerceIn(0f, 1f)
    }
    val annotateIconScale by animateFloatAsState(
        targetValue = if (leftProgress > 0.58f) 1.15f else 0.85f + 0.15f * leftProgress,
        animationSpec = spring(dampingRatio = 0.5f, stiffness = 400f),
        label = "annotate_swipe_icon_scale"
    )
    val deleteIconScale by animateFloatAsState(
        targetValue = if (rightProgress > 0.58f) 1.15f else 0.85f + 0.15f * rightProgress,
        animationSpec = spring(dampingRatio = 0.5f, stiffness = 400f),
        label = "delete_swipe_icon_scale"
    )

    Box(
        modifier = Modifier
            .fillMaxSize()
            .clip(RoundedCornerShape(14.dp))
            .background(Color(0xFFF4F1EB))
            .alpha(alpha)
            .padding(horizontal = 14.dp, vertical = 8.dp)
    ) {
        Box(
            modifier = Modifier
                .align(Alignment.CenterStart)
                .size(34.dp)
                .clip(CircleShape)
                .background(
                    (if (hasComments) Color(0xFF295D76) else Color(0xFF4B5563))
                        .copy(alpha = 0.22f + leftProgress * 0.56f)
                ),
            contentAlignment = Alignment.Center
        ) {
            Text(
                text = "\uD83D\uDCAC",
                fontSize = 15.sp,
                modifier = Modifier.scale(annotateIconScale)
            )
        }
        Box(
            modifier = Modifier
                .align(Alignment.CenterEnd)
                .size(34.dp)
                .clip(CircleShape)
                .background(
                    (if (isMarkedDeleted) Color(0xFF2E7D32) else Color(0xFFC62828))
                        .copy(alpha = 0.22f + rightProgress * 0.56f)
                ),
            contentAlignment = Alignment.Center
        ) {
            Text(
                text = if (isMarkedDeleted) "↺" else "🗑",
                fontSize = 15.sp,
                modifier = Modifier.scale(deleteIconScale)
            )
        }
    }
}

@Composable
private fun CommentUnderlineAffordance(
    commentsCount: Int,
    isExpanded: Boolean,
    modifier: Modifier = Modifier,
    onTap: () -> Unit
) {
    Box(
        modifier = modifier
            .height(18.dp)
            .pointerInput(commentsCount, isExpanded) {
                detectTapGestures(onTap = { onTap() })
            }
            .drawBehind {
                val y = size.height * 0.72f
                val dashWidth = 8.dp.toPx()
                val gapWidth = 5.dp.toPx()
                val strokeWidth = if (isExpanded) 1.7.dp.toPx() else 1.2.dp.toPx()
                val color = if (isExpanded) {
                    Color(0xFF2B5F7B)
                } else {
                    Color(0xFF6B7280)
                }
                var startX = 0f
                while (startX < size.width) {
                    val endX = min(size.width, startX + dashWidth)
                    drawLine(
                        color = color.copy(alpha = 0.66f),
                        start = Offset(x = startX, y = y),
                        end = Offset(x = endX, y = y),
                        strokeWidth = strokeWidth,
                        cap = StrokeCap.Round
                    )
                    startX += dashWidth + gapWidth
                }
            }
    ) {
        Text(
            text = "\uD83D\uDCAC $commentsCount",
            fontSize = 11.sp,
            color = Color(0xFF4B5563),
            modifier = Modifier
                .align(Alignment.TopEnd)
                .padding(end = 2.dp)
        )
    }
}

@Composable
private fun InlineCommentPanel(
    blockId: String,
    existingComments: List<String>,
    tokenAnnotations: List<TokenAnnotationItem>,
    draft: String,
    onDraftChange: (String) -> Unit,
    onCommit: () -> Unit,
    onCloseByTap: () -> Unit,
    onCloseByPinch: () -> Unit
) {
    val focusRequester = remember { FocusRequester() }
    Surface(
        color = Color(0xFFF6F1E8),
        shape = RoundedCornerShape(14.dp),
        modifier = Modifier
            .fillMaxWidth()
            .padding(top = 8.dp)
            .pointerInput(blockId) {
                var cumulativeZoom = 1f
                detectTransformGestures(panZoomLock = true) { _, _, zoom, _ ->
                    cumulativeZoom *= zoom
                    if (cumulativeZoom <= 0.86f) {
                        cumulativeZoom = 1f
                        onCloseByPinch()
                    }
                }
            }
            .drawBehind {
                val brush = Brush.verticalGradient(
                    colors = listOf(Color.Black.copy(alpha = 0.08f), Color.Transparent),
                    startY = 0f,
                    endY = 16.dp.toPx()
                )
                drawRect(
                    brush = brush,
                    topLeft = Offset.Zero,
                    size = androidx.compose.ui.geometry.Size(size.width, 16.dp.toPx())
                )
            }
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 12.dp, vertical = 10.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    text = "批注",
                    fontWeight = FontWeight.SemiBold,
                    color = Color(0xFF2C3B46),
                    fontSize = 13.sp
                )
                Text(
                    text = "双指捏合可关闭",
                    color = Color(0xFF6B7280),
                    fontSize = 10.sp
                )
            }
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.End
            ) {
                IconButton(
                    onClick = onCloseByTap,
                    modifier = Modifier.size(24.dp)
                ) {
                    Icon(
                        imageVector = NoiseChevronDownVector,
                        contentDescription = "Collapse comments",
                        tint = Color(0xFF4B5563),
                        modifier = Modifier.size(15.dp)
                    )
                }
            }
            if (existingComments.isEmpty()) {
                Text(
                    text = "还没有批注，写下第一条。",
                    color = Color(0xFF66717D),
                    fontSize = 12.sp
                )
            } else {
                Column(
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    existingComments.takeLast(6).forEach { comment ->
                        Text(
                            text = comment,
                            color = Color(0xFF4A5568),
                            fontSize = 12.sp,
                            lineHeight = 18.sp,
                            modifier = Modifier
                                .drawBehind {
                                    drawLine(
                                        color = Color(0xFF2B5F7B).copy(alpha = 0.6f),
                                        start = Offset(x = 0f, y = 2.dp.toPx()),
                                        end = Offset(x = 0f, y = size.height - 2.dp.toPx()),
                                        strokeWidth = 2.dp.toPx(),
                                        cap = StrokeCap.Round
                                    )
                                }
                                .padding(start = 10.dp)
                        )
                    }
                }
            }
            if (tokenAnnotations.isNotEmpty()) {
                Text(
                    text = "词级批注",
                    color = Color(0xFF2B5F7B),
                    fontSize = 11.sp,
                    fontWeight = FontWeight.Medium
                )
                Column(
                    verticalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    tokenAnnotations.takeLast(12).forEach { item ->
                        Text(
                            text = "「${item.token}」 ${item.note}",
                            color = Color(0xFF4A5568),
                            fontSize = 12.sp,
                            lineHeight = 18.sp,
                            modifier = Modifier
                                .drawBehind {
                                    drawLine(
                                        color = Color(0xFFFBBF24).copy(alpha = 0.7f),
                                        start = Offset(x = 0f, y = size.height - 1.dp.toPx()),
                                        end = Offset(x = size.width, y = size.height - 1.dp.toPx()),
                                        strokeWidth = 1.dp.toPx(),
                                        cap = StrokeCap.Round
                                    )
                                }
                        )
                    }
                }
            }
            BasicTextField(
                value = draft,
                onValueChange = onDraftChange,
                textStyle = androidx.compose.ui.text.TextStyle(
                    color = Color(0xFF1A212B),
                    fontSize = 13.sp,
                    lineHeight = 18.sp
                ),
                cursorBrush = SolidColor(Color(0xFF2B5F7B)),
                modifier = Modifier
                    .fillMaxWidth()
                    .focusRequester(focusRequester)
                    .padding(top = 6.dp)
                    .clip(RoundedCornerShape(12.dp))
                    .background(Color.Black.copy(alpha = 0.04f))
                    .padding(horizontal = 12.dp, vertical = 10.dp),
                decorationBox = { innerTextField ->
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Box(
                            modifier = Modifier.weight(1f),
                            contentAlignment = Alignment.CenterStart
                        ) {
                            if (draft.isEmpty()) {
                                Text(
                                    text = "写下对这一段的洞察...",
                                    color = Color(0xFF8B95A1),
                                    fontSize = 13.sp
                                )
                            }
                            innerTextField()
                        }
                        
                        IconButton(
                            onClick = onCommit,
                            modifier = Modifier.size(28.dp)
                        ) {
                            Icon(
                                imageVector = Icons.Default.Send,
                                contentDescription = "发送",
                                tint = if (draft.isNotBlank()) Color(0xFF2B5F7B) else Color(0xFFB0B7C3),
                                modifier = Modifier.size(16.dp)
                            )
                        }
                    }
                }
            )
        }
    }
    LaunchedEffect(Unit) {
        delay(300) // 等展开动画完成后再聚焦，确保键盘弹出
        try {
            focusRequester.requestFocus()
        } catch (_: Exception) {
            // 面板可能在 delay 期间被关闭
        }
    }
}

/**
 * 悬浮卡片气泡布局信息。
 * 用于计算气泡相对锚点的位置与尾巴方向。
 */
private data class FloatingBubblePlacement(
    val leftPx: Float,
    val topPx: Float,
    val tailCenterXPx: Float,
    val isAboveAnchor: Boolean
)

@Composable
private fun FloatingCardBubbleOverlay(
    state: FloatingCardBubbleState?,
    card: TokenInsightCard?,
    isLoading: Boolean,
    errorMessage: String?,
    markwon: Markwon,
    onDismiss: () -> Unit
) {
    if (state == null) {
        return
    }
    var viewportSize by remember {
        mutableStateOf(IntSize.Zero)
    }
    var bubbleSize by remember(state.token) {
        mutableStateOf(IntSize.Zero)
    }
    val density = androidx.compose.ui.platform.LocalDensity.current
    val horizontalMarginPx = with(density) { 16.dp.toPx() }
    val verticalMarginPx = with(density) { 18.dp.toPx() }
    val anchorGapPx = with(density) { 10.dp.toPx() }
    val tailSizePx = with(density) { 12.dp.toPx() }
    Box(
        modifier = Modifier
            .fillMaxSize()
            .onSizeChanged { viewportSize = it }
    ) {
        val placement = remember(state, viewportSize, bubbleSize) {
            resolveFloatingBubblePlacement(
                anchor = state.anchor,
                viewportSize = viewportSize,
                bubbleSize = bubbleSize,
                horizontalMarginPx = horizontalMarginPx,
                verticalMarginPx = verticalMarginPx,
                anchorGapPx = anchorGapPx,
                tailSizePx = tailSizePx
            )
        }
        if (placement == null) {
            return@Box
        }
        Box(
            modifier = Modifier
                .fillMaxSize()
                .pointerInput(state.token) {
                    detectTapGestures(
                        onTap = { onDismiss() }
                    )
                }
        )
        AnimatedVisibility(
            visible = true,
            enter = fadeIn(animationSpec = tween(durationMillis = 140)) + scaleIn(
                animationSpec = spring(
                    dampingRatio = 0.8f,
                    stiffness = Spring.StiffnessLow
                ),
                initialScale = 0.92f
            ),
            exit = fadeOut(animationSpec = tween(durationMillis = 90)) + scaleOut(
                animationSpec = tween(durationMillis = 90),
                targetScale = 0.96f
            )
        ) {
            Box(
                modifier = Modifier
                    .offset {
                        IntOffset(
                            x = placement.leftPx.roundToInt(),
                            y = placement.topPx.roundToInt()
                        )
                    }
                    .fillMaxWidth(0.88f)
                    .widthIn(max = 460.dp)
                    .onSizeChanged { bubbleSize = it }
                    .pointerInput(state.token) {
                        detectTapGestures(
                            onTap = { }
                        )
                    }
            ) {
                val bubbleColor = Color(0xFFF2F8FF).copy(alpha = 0.94f)
                Card(
                    shape = RoundedCornerShape(20.dp),
                    colors = CardDefaults.cardColors(containerColor = bubbleColor),
                    border = BorderStroke(width = 1.dp, color = Color.White.copy(alpha = 0.72f)),
                    elevation = CardDefaults.cardElevation(defaultElevation = 12.dp),
                    modifier = Modifier.fillMaxWidth()
                ) {
                    FloatingBubbleCardContent(
                        state = state,
                        card = card,
                        isLoading = isLoading,
                        errorMessage = errorMessage,
                        markwon = markwon
                    )
                }
                val tailHalfPx = with(density) { 7.dp.roundToPx() }
                val localTailX = (placement.tailCenterXPx - placement.leftPx).roundToInt()
                Box(
                    modifier = Modifier
                        .offset {
                            val tailX = (localTailX - tailHalfPx).coerceAtLeast(8)
                            val tailY = if (placement.isAboveAnchor) {
                                bubbleSize.height - tailHalfPx
                            } else {
                                -tailHalfPx
                            }
                            IntOffset(x = tailX, y = tailY)
                        }
                        .size(14.dp)
                        .rotate(45f)
                        .background(
                            color = bubbleColor,
                            shape = RoundedCornerShape(2.dp)
                        )
                )
            }
        }
    }
}

@Composable
private fun FloatingBubbleCardContent(
    state: FloatingCardBubbleState,
    card: TokenInsightCard?,
    isLoading: Boolean,
    errorMessage: String?,
    markwon: Markwon
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .padding(20.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        Text(
            text = card?.title?.ifBlank { state.token } ?: state.token,
            fontSize = 16.sp,
            color = Color(0xFF163C57),
            fontWeight = FontWeight.Bold
        )
        when {
            isLoading -> FloatingBubbleSkeletonLines()
            card != null -> {
                AndroidView(
                    modifier = Modifier.fillMaxWidth(),
                    factory = { context ->
                        TextView(context).apply {
                            includeFontPadding = false
                            setTextColor(Color(0xFF5D7283).toArgbSafe())
                            setLineSpacing(0f, 1.45f)
                        }
                    },
                    update = { view ->
                        markwon.setMarkdown(view, card.markdown)
                    }
                )
            }
            !errorMessage.isNullOrBlank() -> {
                Text(
                    text = errorMessage,
                    fontSize = 13.sp,
                    lineHeight = 19.sp,
                    color = Color(0xFF8A4E4E)
                )
            }
            else -> {
                Text(
                    text = "Card content is loading...",

                    fontSize = 13.sp,
                    lineHeight = 19.sp,
                    color = Color(0xFF5D7283)
                )
            }
        }
    }
}

@Composable
private fun FloatingBubbleSkeletonLines() {
    val shimmerAlpha by rememberInfiniteTransition(label = "floating-card-skeleton-shimmer").animateFloat(
        initialValue = 0.22f,
        targetValue = 0.5f,
        animationSpec = infiniteRepeatable(
            animation = tween(durationMillis = 960),
            repeatMode = RepeatMode.Reverse
        ),
        label = "floating-card-skeleton-shimmer-alpha"
    )
    val shimmerColor = Color(0xFFAAC2D8).copy(alpha = shimmerAlpha)
    Column(
        modifier = Modifier.fillMaxWidth(),
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        Box(
            modifier = Modifier
                .fillMaxWidth(0.92f)
                .height(12.dp)
                .clip(RoundedCornerShape(4.dp))
                .background(shimmerColor)
        )
        Box(
            modifier = Modifier
                .fillMaxWidth(0.84f)
                .height(12.dp)
                .clip(RoundedCornerShape(4.dp))
                .background(shimmerColor)
        )
        Box(
            modifier = Modifier
                .fillMaxWidth(0.72f)
                .height(12.dp)
                .clip(RoundedCornerShape(4.dp))
                .background(shimmerColor)
        )
    }
}

private fun resolveFloatingBubblePlacement(
    anchor: InsightTermAnchor,
    viewportSize: IntSize,
    bubbleSize: IntSize,
    horizontalMarginPx: Float,
    verticalMarginPx: Float,
    anchorGapPx: Float,
    tailSizePx: Float
): FloatingBubblePlacement? {
    if (viewportSize.width <= 0 || viewportSize.height <= 0) {
        return null
    }
    val viewportWidth = viewportSize.width.toFloat()
    val viewportHeight = viewportSize.height.toFloat()
    val estimatedWidth = if (bubbleSize.width > 0) {
        bubbleSize.width.toFloat()
    } else {
        viewportWidth * 0.88f
    }.coerceAtMost(viewportWidth - horizontalMarginPx * 2f)
    val estimatedHeight = if (bubbleSize.height > 0) {
        bubbleSize.height.toFloat()
    } else {
        220f
    }.coerceAtMost(viewportHeight - verticalMarginPx * 2f)
    val left = (anchor.centerX - estimatedWidth / 2f).coerceIn(
        minimumValue = horizontalMarginPx,
        maximumValue = viewportWidth - estimatedWidth - horizontalMarginPx
    )
    val canPlaceAbove = anchor.topY - estimatedHeight - anchorGapPx - tailSizePx >= verticalMarginPx
    val canPlaceBelow = anchor.bottomY + estimatedHeight + anchorGapPx + tailSizePx <= viewportHeight - verticalMarginPx
    val placeAbove = canPlaceAbove || !canPlaceBelow
    val top = if (placeAbove) {
        (anchor.topY - estimatedHeight - anchorGapPx - tailSizePx).coerceAtLeast(verticalMarginPx)
    } else {
        (anchor.bottomY + anchorGapPx + tailSizePx).coerceAtMost(viewportHeight - estimatedHeight - verticalMarginPx)
    }
    val tailCenterX = anchor.centerX.coerceIn(
        minimumValue = left + 28f,
        maximumValue = left + estimatedWidth - 28f
    )
    return FloatingBubblePlacement(
        leftPx = left,
        topPx = top,
        tailCenterXPx = tailCenterX,
        isAboveAnchor = placeAbove
    )
}

@Composable
private fun TokenAnnotationEditorOverlay(
    state: TokenAnnotationEditorState?,
    viewportSize: IntSize,
    paragraphBoundsByBlockId: Map<String, ParagraphOverlayBounds>,
    onDraftChange: (String) -> Unit,
    onCommit: () -> Unit,
    onDismiss: () -> Unit
) {
    if (state == null) {
        return
    }
    val anchor = state.anchor
    val paragraphBounds = paragraphBoundsByBlockId[state.blockId]
    val keyboard = LocalSoftwareKeyboardController.current
    val focusRequester = remember { FocusRequester() }
    var editorSize by remember(state.blockId, state.selection.start, state.selection.end) {
        mutableStateOf(IntSize(width = 280, height = 52))
    }
    val widthPx = editorSize.width.toFloat().coerceAtLeast(220f)
    val heightPx = editorSize.height.toFloat().coerceAtLeast(52f)
    val marginPx = 12f
    val fallbackCenterX = (viewportSize.width.toFloat() * 0.5f).coerceAtLeast(marginPx + widthPx / 2f)
    val targetCenterX = anchor?.centerX ?: paragraphBounds?.centerX ?: fallbackCenterX
    val left = (targetCenterX - widthPx / 2f).coerceIn(
        minimumValue = marginPx,
        maximumValue = (viewportSize.width.toFloat() - widthPx - marginPx).coerceAtLeast(marginPx)
    )
    val targetBottomY = anchor?.bottomY ?: paragraphBounds?.bottom ?: marginPx
    val top = (targetBottomY + 8f).coerceAtMost(
        (viewportSize.height.toFloat() - heightPx - marginPx).coerceAtLeast(marginPx)
    )
    LaunchedEffect(state.blockId, state.selection.start, state.selection.end) {
        runCatching {
            focusRequester.requestFocus()
            keyboard?.show()
        }
    }
    Popup(
        alignment = Alignment.TopStart,
        offset = IntOffset(left.roundToInt(), top.roundToInt()),
        onDismissRequest = onDismiss,
        properties = PopupProperties(
            focusable = true,
            dismissOnClickOutside = true
        )
    ) {
        androidx.compose.animation.AnimatedVisibility(
            visible = true,
            enter = expandVertically(animationSpec = tween(durationMillis = 120)) + fadeIn(animationSpec = tween(durationMillis = 120)),
            exit = shrinkVertically(animationSpec = tween(durationMillis = 120)) + fadeOut(animationSpec = tween(durationMillis = 120))
        ) {
            Surface(
                color = Color.White,
                shape = RoundedCornerShape(10.dp),
                tonalElevation = 4.dp,
                shadowElevation = 6.dp,
                border = BorderStroke(1.dp, Color(0xFFFBBF24).copy(alpha = 0.65f)),
                modifier = Modifier
                    .widthIn(min = 220.dp, max = 320.dp)
                    .onSizeChanged { measured ->
                        if (measured.width > 0 && measured.height > 0) {
                            editorSize = measured
                        }
                    }
            ) {
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(horizontal = 8.dp, vertical = 6.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    BasicTextField(
                        value = state.draft,
                        onValueChange = onDraftChange,
                        singleLine = true,
                        textStyle = androidx.compose.ui.text.TextStyle(
                            color = Color(0xFF111827),
                            fontSize = 13.sp
                        ),
                        cursorBrush = SolidColor(Color(0xFFFBBF24)),
                        modifier = Modifier
                            .weight(1f)
                            .focusRequester(focusRequester)
                            .padding(horizontal = 4.dp, vertical = 2.dp),
                        decorationBox = { inner ->
                            Box(
                                modifier = Modifier.fillMaxWidth(),
                                contentAlignment = Alignment.CenterStart
                            ) {
                                if (state.draft.isBlank()) {
                                    Text(
                                        text = "输入词级批注",
                                        fontSize = 12.sp,
                                        color = Color(0xFF9CA3AF)
                                    )
                                }
                                inner()
                            }
                        }
                    )
                    TextButton(onClick = onCommit) {
                        Text(text = "保存", color = Color(0xFFB45309), fontSize = 12.sp)
                    }
                }
            }
        }
    }
}

@Composable
private fun TokenAnnotationBubbleOverlay(
    state: TokenAnnotationBubbleState?,
    viewportSize: IntSize,
    paragraphBoundsByBlockId: Map<String, ParagraphOverlayBounds>,
    onDismiss: () -> Unit
) {
    if (state == null) {
        return
    }
    val anchor = state.anchor
    val paragraphBounds = paragraphBoundsByBlockId[state.blockId]
    val widthPx = 260f
    val marginPx = 12f
    val fallbackCenterX = (viewportSize.width.toFloat() * 0.5f).coerceAtLeast(marginPx + widthPx / 2f)
    val targetCenterX = anchor?.centerX ?: paragraphBounds?.centerX ?: fallbackCenterX
    val left = (targetCenterX - widthPx / 2f).coerceIn(
        minimumValue = marginPx,
        maximumValue = (viewportSize.width.toFloat() - widthPx - marginPx).coerceAtLeast(marginPx)
    )
    val targetBottomY = anchor?.bottomY ?: paragraphBounds?.bottom ?: marginPx
    val top = (targetBottomY + 8f).coerceAtMost(
        (viewportSize.height.toFloat() - 64f - marginPx).coerceAtLeast(marginPx)
    )
    Popup(
        alignment = Alignment.TopStart,
        offset = IntOffset(left.roundToInt(), top.roundToInt()),
        onDismissRequest = onDismiss,
        properties = PopupProperties(
            focusable = false,
            dismissOnClickOutside = true
        )
    ) {
        Surface(
            color = Color(0xFFFFFBEB),
            shape = RoundedCornerShape(10.dp),
            border = BorderStroke(1.dp, Color(0xFFFBBF24).copy(alpha = 0.75f)),
            tonalElevation = 3.dp,
            shadowElevation = 4.dp,
            modifier = Modifier.widthIn(min = 180.dp, max = 300.dp)
        ) {
            Text(
                text = state.text,
                color = Color(0xFF4B5563),
                fontSize = 12.sp,
                lineHeight = 18.sp,
                modifier = Modifier.padding(horizontal = 10.dp, vertical = 8.dp)
            )
        }
    }
}

private fun compactReaderParagraphContent(
    raw: String,
    nodeType: String
): String {
    if (raw.isBlank()) {
        return raw
    }
    val normalizedType = nodeType.trim().lowercase()
    if (normalizedType != "paragraph") {
        return raw
    }
    val trimmed = raw.trim()
    if (trimmed.startsWith("#") || trimmed.startsWith(">") || trimmed.contains("```")) {
        return raw
    }
    val hasUnorderedList = READER_UNORDERED_LIST_PATTERN.containsMatchIn(raw)
    if (hasUnorderedList) {
        return raw
    }
    val hasOrderedList = READER_ORDERED_LIST_PATTERN.containsMatchIn(raw)
    if (hasOrderedList) {
        return raw
    }
    return raw
        .lines()
        .map { it.trim() }
        .filter { it.isNotEmpty() }
        .joinToString(separator = " ")
}

private val READER_UNORDERED_LIST_PATTERN = Regex("(?m)^[\\s\\u3000]*(?:[-*+•]\\s+)")
private val READER_ORDERED_LIST_PATTERN = Regex("(?m)^[\\s\\u3000]*\\d+[\\.\\)\\u3001]\\s+")
private val READER_LIST_MARKER_PATTERN = Regex("^(?:[-*+]\\s+|\\d+[\\.)]\\s+)")
private val READER_CODE_FENCE_LINE_PATTERN = Regex("^\\s*(`{3,}|~{3,}).*$")

private fun normalizeReaderListIndentation(
    markdown: String,
    renderConfig: MarkdownReaderRenderConfig
): String {
    if (markdown.isBlank()) {
        return markdown
    }
    val maxDepth = renderConfig.listIndentMaxDepth.coerceAtLeast(0)
    if (maxDepth == 0) {
        return markdown
    }
    val inputUnit = renderConfig.listIndentInputUnitSpaces.coerceAtLeast(1)
    val outputUnit = renderConfig.listIndentOutputUnitSpaces.coerceAtLeast(1)
    val tabSpaces = renderConfig.listIndentTabSpaces.coerceAtLeast(1)
    var changed = false
    var inFence = false
    var inListContext = false
    val normalized = markdown.lines().map { rawLine ->
        val line = expandLeadingTabs(rawLine, tabSpaces)
        if (line != rawLine) {
            changed = true
        }
        if (READER_CODE_FENCE_LINE_PATTERN.matches(line)) {
            inFence = !inFence
            inListContext = false
            return@map line
        }
        if (inFence) {
            return@map line
        }
        val trimmed = line.trimStart()
        if (trimmed.isBlank()) {
            inListContext = false
            return@map line
        }
        val leadingSpaces = line.length - trimmed.length
        val isListLine = READER_LIST_MARKER_PATTERN.containsMatchIn(trimmed)
        val isContinuation = inListContext && leadingSpaces > 0
        if (!(isListLine || isContinuation)) {
            inListContext = false
            return@map line
        }
        inListContext = true
        val depth = (leadingSpaces / inputUnit).coerceIn(0, maxDepth)
        val normalizedLeading = depth * outputUnit
        if (normalizedLeading == leadingSpaces) {
            return@map line
        }
        changed = true
        " ".repeat(normalizedLeading) + trimmed
    }
    return if (changed) normalized.joinToString("\n") else markdown
}

private fun expandLeadingTabs(line: String, tabSpaces: Int): String {
    var index = 0
    while (index < line.length && (line[index] == ' ' || line[index] == '\t')) {
        index++
    }
    if (index == 0 || !line.substring(0, index).contains('\t')) {
        return line
    }
    val builder = StringBuilder(line.length + tabSpaces)
    line.substring(0, index).forEach { ch ->
        if (ch == '\t') {
            builder.append(" ".repeat(tabSpaces))
        } else {
            builder.append(ch)
        }
    }
    builder.append(line.substring(index))
    return builder.toString()
}

private fun applyReaderParagraphLineSpacing(
    textView: TextView,
    lineSpacingMultiplier: Float
) : Int {
    val safeMultiplier = lineSpacingMultiplier.coerceIn(1.0f, 1.4f)
    val targetLineHeightPx = (textView.textSize * safeMultiplier * 1.35f).roundToInt().coerceAtLeast(1)
    textView.setLineSpacing(0f, 1f)
    TextViewCompat.setLineHeight(textView, targetLineHeightPx)
    return targetLineHeightPx
}

private fun resolveReaderWeight(fontWeight: FontWeight): Int {
    return when (fontWeight) {
        FontWeight.Bold,
        FontWeight.SemiBold -> 700
        FontWeight.Medium -> 500
        else -> 400
    }
}

private fun buildReaderTextStyleFingerprint(
    textSizeSp: Float,
    lineSpacingMultiplier: Float,
    letterSpacing: Float,
    textColorArgb: Int,
    fontWeight: Int,
    mediumFontFamily: String,
    bodyFontFamily: String
): Int {
    return arrayOf(
        textSizeSp,
        lineSpacingMultiplier,
        letterSpacing,
        textColorArgb,
        fontWeight,
        mediumFontFamily,
        bodyFontFamily
    ).contentHashCode()
}

private fun resolveSelectionFingerprint(selection: TokenSelection?): Int {
    if (selection == null) {
        return 0
    }
    return arrayOf(
        selection.start,
        selection.end,
        selection.token
    ).contentHashCode()
}

private fun resolveReaderLineHeightPx(
    textSizeSp: Float,
    lineSpacingMultiplier: Float,
    scaledDensity: Float
): Int {
    val safeMultiplier = lineSpacingMultiplier.coerceIn(1.0f, 1.4f)
    val textSizePx = textSizeSp * scaledDensity
    return (textSizePx * safeMultiplier * 1.35f).roundToInt().coerceAtLeast(1)
}

private fun applyReaderParagraphLineHeightSpan(
    textView: TextView,
    lineHeightPx: Int
) {
    val baseText = textView.text as? Spanned ?: return
    if (baseText.isEmpty()) {
        return
    }
    val spannable = if (baseText is SpannableStringBuilder) {
        baseText
    } else {
        SpannableStringBuilder(baseText)
    }
    spannable
        .getSpans(0, spannable.length, ReaderParagraphLineHeightSpan::class.java)
        .forEach(spannable::removeSpan)
    spannable.setSpan(
        ReaderParagraphLineHeightSpan(lineHeightPx),
        0,
        spannable.length,
        Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
    )
    if (textView.text !== spannable) {
        textView.text = spannable
    }
}

private fun mergeAdjacentSelections(
    selections: List<TokenSelection>,
    source: String
): List<TokenSelection> {
    if (selections.isEmpty()) {
        return emptyList()
    }
    val sorted = selections
        .filter { it.start >= 0 && it.end <= source.length && it.start < it.end }
        .sortedBy { it.start }
    if (sorted.isEmpty()) {
        return emptyList()
    }
    val merged = mutableListOf<TokenSelection>()
    var current = sorted.first()
    for (i in 1 until sorted.size) {
        val next = sorted[i]
        val gapStart = current.end
        val gapEnd = next.start
        val canJoin = gapEnd >= gapStart &&
            source.substring(gapStart, gapEnd).all { it.isWhitespace() }
        if (next.start <= current.end || canJoin) {
            val mergedEnd = max(current.end, next.end)
            current = TokenSelection(
                token = source.substring(current.start, mergedEnd),
                start = current.start,
                end = mergedEnd
            )
        } else {
            merged += current
            current = next
        }
    }
    merged += current
    return merged
}

private fun applyReaderTextLayoutPolicy(textView: TextView) {
    textView.includeFontPadding = false
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
        textView.breakStrategy = Layout.BREAK_STRATEGY_SIMPLE
        textView.hyphenationFrequency = Layout.HYPHENATION_FREQUENCY_NONE
    }
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
        textView.isFallbackLineSpacing = true
    }
}

private val MARKDOWN_LIST_LINE_PATTERN = Regex("(?m)^\\s*(?:[-*+]\\s+|\\d+[\\.)]\\s+)")
private val MOUNTED_WIKILINK_PATTERN = Regex("\\[\\[([^\\[\\]]+)\\]\\]")
private val OBSIDIAN_EMBED_PATTERN = Regex("!\\[\\[([^\\]]+)\\]\\]")
private val MARKDOWN_IMAGE_INLINE_PATTERN = Regex("!\\[([^\\]]*)]\\(([^)]*)\\)")
private val HTML_VIDEO_TAG_PATTERN = Regex(
    "<video\\b[^>]*\\bsrc\\s*=\\s*([\"']?)([^\"'\\s>]+)\\1[^>]*>(?:\\s*</video>)?",
    RegexOption.IGNORE_CASE
)
private val MARKDOWN_LINK_GENERIC_PATTERN = Regex("(?<!!)\\[([^\\]]+)]\\(([^)\\s]+)([^)]*)\\)")
private val ABSOLUTE_URL_PATTERN = Regex("^(?:[a-zA-Z][a-zA-Z0-9+.-]*:|#).+")
private val IMAGE_EXTENSIONS = setOf("png", "jpg", "jpeg", "gif", "webp", "bmp", "svg")
private val VIDEO_EXTENSIONS = setOf("mp4", "webm", "mov", "m4v")
private const val DISABLE_TEXT_IS_NOISE_JUDGMENT = true
private const val MARKDOWN_CACHE_MAX_ENTRIES = 180
private const val MARKDOWN_CACHE_MAX_TEXT_LENGTH = 12_000
private const val READER_LAYOUT_LOG_TAG = "ReaderLayoutProbe"
private val READER_MARKDOWN_CACHE = object : LruCache<String, CharSequence>(MARKDOWN_CACHE_MAX_ENTRIES) {}

private fun readCachedReaderMarkdown(markdown: String): CharSequence? {
    if (markdown.length > MARKDOWN_CACHE_MAX_TEXT_LENGTH) {
        return null
    }
    val cached = synchronized(READER_MARKDOWN_CACHE) {
        READER_MARKDOWN_CACHE.get(markdown)
    } ?: return null
    // 返回副本，避免后续 span 叠加污染缓存本体。
    return SpannableStringBuilder.valueOf(cached)
}

private fun writeCachedReaderMarkdown(markdown: String, rendered: CharSequence?) {
    if (markdown.length > MARKDOWN_CACHE_MAX_TEXT_LENGTH) {
        return
    }
    val text = rendered ?: return
    if (text.isEmpty()) {
        return
    }
    synchronized(READER_MARKDOWN_CACHE) {
        READER_MARKDOWN_CACHE.put(markdown, SpannableStringBuilder.valueOf(text))
    }
}

@Composable
private fun MountedAnchorPreviewContent(
    state: MountedAnchorPreviewState,
    markwon: Markwon,
    isFullscreen: Boolean,
    onWikilinkTap: (MountedWikilinkTap) -> Unit,
    onJumpToBreadcrumb: (Int) -> Unit,
    onPopOneLevel: () -> Unit,
    onBackToRoot: () -> Unit,
    onSetFullscreen: (Boolean) -> Unit,
    onBindGhostPath: (String) -> Unit
) {
    val context = LocalContext.current
    val stack = state.stack
    val current = stack.lastOrNull()
    val containerModifier = if (isFullscreen) {
        Modifier
            .fillMaxSize()
            .pointerInput(isFullscreen, stack.size) {
                val edgeThresholdPx = 24.dp.toPx()
                val triggerThresholdPx = 92.dp.toPx()
                var armed = false
                var distance = 0f
                detectHorizontalDragGestures(
                    onDragStart = { offset ->
                        armed = offset.x <= edgeThresholdPx
                        distance = 0f
                    },
                    onHorizontalDrag = { change, dragAmount ->
                        if (!armed) {
                            return@detectHorizontalDragGestures
                        }
                        distance += dragAmount
                        if (distance >= triggerThresholdPx) {
                            onPopOneLevel()
                            armed = false
                            distance = 0f
                        }
                        change.consume()
                    },
                    onDragEnd = {
                        armed = false
                        distance = 0f
                    },
                    onDragCancel = {
                        armed = false
                        distance = 0f
                    }
                )
            }
            .padding(horizontal = 16.dp, vertical = 12.dp)
    } else {
        Modifier
            .fillMaxWidth()
            .padding(horizontal = 16.dp, vertical = 12.dp)
            .padding(bottom = 24.dp)
    }
    var ghostDraftPath by remember(current?.notePath, current?.ghostInputPath) {
        mutableStateOf(current?.ghostInputPath.orEmpty().ifBlank { current?.notePath.orEmpty() })
    }
    Column(
        modifier = containerModifier,
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = if (isFullscreen) "挂载笔记 · 深度阅读" else "锚点挂载笔记",
                fontWeight = FontWeight.SemiBold,
                color = Color(0xFF0F172A),
                fontSize = 18.sp
            )
            Row(
                horizontalArrangement = Arrangement.spacedBy(4.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                TextButton(
                    onClick = {
                        onSetFullscreen(!isFullscreen)
                    }
                ) {
                    Text(if (isFullscreen) "退出全屏" else "全屏阅读")
                }
                if (stack.size > 1) {
                    TextButton(onClick = onPopOneLevel) {
                        Text("返回上层")
                    }
                }
            }
        }
        if (state.quote.isNotBlank()) {
            Text(
                text = "「${state.quote}」",
                color = Color(0xFF1E3A8A),
                fontSize = 14.sp,
                maxLines = 2,
                overflow = TextOverflow.Ellipsis
            )
        }
        if (stack.isNotEmpty()) {
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .horizontalScroll(rememberScrollState()),
                horizontalArrangement = Arrangement.spacedBy(6.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                stack.forEachIndexed { index, item ->
                    val selected = index == stack.lastIndex
                    TextButton(
                        onClick = {
                            if (!selected) {
                                onJumpToBreadcrumb(index)
                            }
                        },
                        enabled = !selected
                    ) {
                        Text(
                            text = item.displayTitle,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis,
                            color = if (selected) Color(0xFF1E3A8A) else Color(0xFF475569),
                            fontSize = 12.sp
                        )
                    }
                    if (index != stack.lastIndex) {
                        Text(
                            text = "›",
                            color = Color(0xFF94A3B8),
                            fontSize = 12.sp
                        )
                    }
                }
            }
        }
        if (!current?.notePath.isNullOrBlank()) {
            Text(
                text = current?.notePath.orEmpty(),
                color = Color(0xFF64748B),
                fontSize = 12.sp,
                maxLines = 2,
                overflow = TextOverflow.Ellipsis
            )
        }
        if (state.isLoading) {
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(vertical = 14.dp),
                horizontalArrangement = Arrangement.spacedBy(10.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                CircularProgressIndicator(
                    modifier = Modifier.size(18.dp),
                    strokeWidth = 2.dp
                )
                Text(
                    text = "正在加载挂载笔记...",
                    color = Color(0xFF334155),
                    fontSize = 14.sp
                )
            }
            return
        }
        if (current == null) {
            Text(
                text = "该锚点暂未发现可展示的 Markdown 内容。",
                color = Color(0xFF64748B),
                fontSize = 14.sp
            )
            return
        }
        if (!state.errorMessage.isNullOrBlank()) {
            Text(
                text = "加载失败：${state.errorMessage}",
                color = Color(0xFFB42318),
                fontSize = 14.sp
            )
        }
        if (current.isGhost) {
            Text(
                text = "该双链尚未绑定现有笔记，可立即指定路径尝试绑定。",
                color = Color(0xFF64748B),
                fontSize = 13.sp
            )
            OutlinedTextField(
                value = ghostDraftPath,
                onValueChange = { next ->
                    ghostDraftPath = next
                },
                modifier = Modifier.fillMaxWidth(),
                label = { Text("绑定路径（相对 revision 目录）") },
                singleLine = true
            )
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(8.dp, Alignment.End),
                verticalAlignment = Alignment.CenterVertically
            ) {
                TextButton(
                    onClick = {
                        onBindGhostPath(ghostDraftPath)
                    }
                ) {
                    Text("立即绑定")
                }
            }
            if (isFullscreen && stack.size > 2) {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.End
                ) {
                    Button(onClick = onBackToRoot) {
                        Text("返回起始节点")
                    }
                }
            }
            return
        }
        val markdownText = current.markdown.ifBlank { current.rawMarkdown }
        if (markdownText.isBlank()) {
            Text(
                text = "该锚点暂未发现可展示的 Markdown 内容。",
                color = Color(0xFF64748B),
                fontSize = 14.sp
            )
            return
        }
        AndroidView(
            modifier = if (isFullscreen) {
                Modifier
                    .fillMaxWidth()
                    .weight(1f)
                    .background(Color(0xFFF8FAFC), RoundedCornerShape(14.dp))
                    .padding(horizontal = 12.dp, vertical = 10.dp)
            } else {
                Modifier
                    .fillMaxWidth()
                    .height(420.dp)
                    .background(Color(0xFFF8FAFC), RoundedCornerShape(14.dp))
                    .padding(horizontal = 12.dp, vertical = 10.dp)
            },
            factory = {
                TextView(context).apply {
                    setTextIsSelectable(false)
                    textSize = 15f
                    setLineSpacing(0f, 1.3f)
                }
            },
            update = { textView ->
                renderMountedAnchorDocument(
                    textView = textView,
                    markwon = markwon,
                    markdown = markdownText,
                    currentNotePath = current.notePath,
                    markdownPaths = state.markdownPaths,
                    onWikilinkTap = onWikilinkTap
                )
            }
        )
        if (isFullscreen && stack.size > 2) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.End
            ) {
                Button(onClick = onBackToRoot) {
                    Text("返回起始节点")
                }
            }
        }
    }
}

private fun renderMountedAnchorDocument(
    textView: TextView,
    markwon: Markwon,
    markdown: String,
    currentNotePath: String,
    markdownPaths: List<String>,
    onWikilinkTap: (MountedWikilinkTap) -> Unit
) {
    val rewritten = rewriteMountedMarkdownWithWikilinks(
        markdown = markdown,
        currentNotePath = currentNotePath,
        markdownPaths = markdownPaths
    )
    markwon.setMarkdown(textView, rewritten.markdown)
    textView.movementMethod = LinkMovementMethod.getInstance()
    textView.highlightColor = android.graphics.Color.TRANSPARENT
    val spanned = textView.text as? Spanned ?: return
    val spannable = SpannableStringBuilder.valueOf(spanned)
    clearReaderOverlaySpans(spannable)
    val urlSpans = spannable.getSpans(0, spannable.length, URLSpan::class.java)
    urlSpans.forEach { span ->
        val url = span.url ?: return@forEach
        val wikilink = rewritten.linksByUrl[url] ?: return@forEach
        val start = spannable.getSpanStart(span)
        val end = spannable.getSpanEnd(span)
        if (start < 0 || end <= start) {
            return@forEach
        }
        spannable.removeSpan(span)
        spannable.setSpan(
            object : ClickableSpan() {
                override fun onClick(widget: View) {
                    onWikilinkTap(wikilink)
                }

                override fun updateDrawState(ds: TextPaint) {
                    ds.isUnderlineText = false
                }
            },
            start,
            end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        if (wikilink.isGhost) {
            spannable.setSpan(
                ForegroundColorSpan(0xFF64748B.toInt()),
                start,
                end,
                Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
            )
            spannable.setSpan(
                ReaderAnchorUnderlineSpan(
                    color = 0xFF94A3B8.toInt(),
                    thicknessPx = 2.2f,
                    dashed = true
                ),
                start,
                end,
                Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
            )
        } else {
            spannable.setSpan(
                ReaderWikilinkChipSpan(backgroundColor = 0xFFDDEDE4.toInt()),
                start,
                end,
                Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
            )
            spannable.setSpan(
                ForegroundColorSpan(0xFF065F46.toInt()),
                start,
                end,
                Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
            )
            spannable.setSpan(
                StyleSpan(Typeface.BOLD),
                start,
                end,
                Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
            )
        }
    }
    textView.text = spannable
}

private fun rewriteMountedMarkdownWithWikilinks(
    markdown: String,
    currentNotePath: String,
    markdownPaths: List<String>
): MountedWikilinkRewrite {
    if (markdown.isBlank()) {
        return MountedWikilinkRewrite(markdown = markdown, linksByUrl = emptyMap())
    }
    val normalizedKnown = markdownPaths
        .map { path -> normalizeMountedNotePath(path) }
        .filter { path -> path.isNotBlank() && isMarkdownNotePath(path) }
        .distinct()
    val knownPathMap = linkedMapOf<String, String>()
    normalizedKnown.forEach { onePath ->
        knownPathMap[onePath.lowercase()] = onePath
    }
    val linksByUrl = LinkedHashMap<String, MountedWikilinkTap>()
    val builder = StringBuilder(markdown.length + 32)
    var cursor = 0
    MOUNTED_WIKILINK_PATTERN.findAll(markdown).forEach { match ->
        builder.append(markdown, cursor, match.range.first)
        val parsed = parseMountedWikilinkBody(match.groupValues[1])
        if (parsed.targetPath.isBlank()) {
            builder.append(match.value)
            cursor = match.range.last + 1
            return@forEach
        }
        val resolved = resolveMountedWikilinkTarget(
            rawTargetPath = parsed.targetPath,
            currentNotePath = currentNotePath,
            knownPathMap = knownPathMap,
            knownPaths = normalizedKnown
        )
        val displayLabel = parsed.displayText.ifBlank {
            deriveMountedNoteDisplayTitle(resolved.targetNotePath)
        }
        val titleWithMarker = if (resolved.isGhost) {
            displayLabel
        } else {
            "📄 $displayLabel"
        }
        val urlKey = "wikilink://mounted/${linksByUrl.size + 1}"
        linksByUrl[urlKey] = MountedWikilinkTap(
            displayText = displayLabel,
            targetNotePath = resolved.targetNotePath,
            isGhost = resolved.isGhost
        )
        builder.append('[')
            .append(escapeMarkdownLinkLabel(titleWithMarker))
            .append("](")
            .append(urlKey)
            .append(')')
        cursor = match.range.last + 1
    }
    builder.append(markdown.substring(cursor))
    return MountedWikilinkRewrite(
        markdown = builder.toString(),
        linksByUrl = linksByUrl
    )
}

private fun parseMountedWikilinkBody(rawBody: String): ParsedMountedWikilink {
    val trimmed = rawBody.trim()
    if (trimmed.isBlank()) {
        return ParsedMountedWikilink(targetPath = "", displayText = "")
    }
    val parts = trimmed.split('|', limit = 2)
    val rawTarget = parts.firstOrNull().orEmpty()
        .substringBefore('#')
        .substringBefore('^')
        .trim()
    val alias = parts.getOrNull(1).orEmpty().trim()
    val inferredDisplay = rawTarget
        .substringAfterLast('/')
        .substringAfterLast('\\')
        .substringBeforeLast('.')
        .ifBlank { rawTarget }
    return ParsedMountedWikilink(
        targetPath = rawTarget,
        displayText = alias.ifBlank { inferredDisplay }
    )
}

private fun resolveMountedWikilinkTarget(
    rawTargetPath: String,
    currentNotePath: String,
    knownPathMap: Map<String, String>,
    knownPaths: List<String>
): ResolvedMountedWikilink {
    val normalizedTarget = normalizeMountedNotePath(rawTargetPath)
    if (normalizedTarget.isBlank()) {
        return ResolvedMountedWikilink(
            targetNotePath = "",
            isGhost = true
        )
    }
    val currentDir = normalizeMountedNotePath(
        currentNotePath.substringBeforeLast('/', "")
    )
    val normalizedCandidates = linkedSetOf<String>()
    val extensionCandidates = if (isMarkdownNotePath(normalizedTarget)) {
        listOf(normalizedTarget)
    } else {
        listOf("$normalizedTarget.md", "$normalizedTarget.markdown")
    }
    extensionCandidates.forEach { candidate ->
        normalizedCandidates += normalizeMountedNotePath(candidate)
        if (currentDir.isNotBlank()) {
            normalizedCandidates += normalizeMountedNotePath("$currentDir/$candidate")
        }
    }
    normalizedCandidates
        .filter { path -> path.isNotBlank() }
        .forEach { candidate ->
            val existing = knownPathMap[candidate.lowercase()]
            if (!existing.isNullOrBlank()) {
                return ResolvedMountedWikilink(
                    targetNotePath = existing,
                    isGhost = false
                )
            }
        }
    val expectedStem = normalizedTarget
        .substringAfterLast('/')
        .substringBeforeLast('.')
        .lowercase()
    val fallbackExisting = knownPaths.firstOrNull { onePath ->
        onePath.substringAfterLast('/')
            .substringBeforeLast('.')
            .lowercase() == expectedStem
    }
    if (!fallbackExisting.isNullOrBlank()) {
        return ResolvedMountedWikilink(
            targetNotePath = fallbackExisting,
            isGhost = false
        )
    }
    val fallbackPath = normalizedCandidates.firstOrNull { path ->
        path.isNotBlank()
    } ?: if (currentDir.isNotBlank()) {
        normalizeMountedNotePath("$currentDir/$normalizedTarget.md")
    } else {
        normalizeMountedNotePath("$normalizedTarget.md")
    }
    return ResolvedMountedWikilink(
        targetNotePath = fallbackPath,
        isGhost = true
    )
}

private fun escapeMarkdownLinkLabel(label: String): String {
    return label
        .replace("\\", "\\\\")
        .replace("[", "\\[")
        .replace("]", "\\]")
}

private data class ParsedMarkdownLinkTarget(
    val destination: String,
    val suffix: String
)

private fun parseMarkdownLinkTarget(rawTarget: String): ParsedMarkdownLinkTarget {
    val trimmed = rawTarget.trim()
    if (trimmed.isEmpty()) {
        return ParsedMarkdownLinkTarget("", "")
    }
    if (trimmed.startsWith("<")) {
        val closeIndex = trimmed.indexOf('>')
        if (closeIndex > 0) {
            val destination = trimmed.substring(1, closeIndex).trim()
            val suffix = trimmed.substring(closeIndex + 1)
            return ParsedMarkdownLinkTarget(destination, suffix)
        }
    }
    val splitIndex = trimmed.indexOfFirst { char -> char.isWhitespace() }
    return if (splitIndex < 0) {
        ParsedMarkdownLinkTarget(trimmed, "")
    } else {
        ParsedMarkdownLinkTarget(
            destination = trimmed.substring(0, splitIndex),
            suffix = trimmed.substring(splitIndex)
        )
    }
}

private fun buildMarkdownLinkTarget(destination: String, suffix: String): String {
    val normalizedDestination = destination.trim()
    if (normalizedDestination.isBlank()) {
        return suffix.trim()
    }
    val needsAngleWrap = normalizedDestination.any { char -> char.isWhitespace() } ||
        normalizedDestination.startsWith("<") ||
        normalizedDestination.endsWith(">")
    val encodedDestination = if (needsAngleWrap) {
        "<$normalizedDestination>"
    } else {
        normalizedDestination
    }
    return encodedDestination + suffix
}

private fun mergeMountedMarkdownPaths(
    existingPaths: List<String>,
    payload: MobileMountedAnchorPayload
): List<String> {
    val merged = LinkedHashSet<String>()
    fun collect(path: String?) {
        val normalized = normalizeMountedNotePath(path.orEmpty())
        if (normalized.isBlank() || !isMarkdownNotePath(normalized)) {
            return
        }
        merged += normalized
    }
    existingPaths.forEach { path -> collect(path) }
    payload.latestRevision?.files?.forEach { path -> collect(path) }
    collect(payload.latestRevision?.notePath)
    collect(payload.entryNotePath)
    collect(payload.notePath)
    return merged.toList()
}

private fun deriveMountedNoteDisplayTitle(notePath: String): String {
    val normalized = normalizeMountedNotePath(notePath)
    if (normalized.isBlank()) {
        return "挂载笔记"
    }
    val fileName = normalized.substringAfterLast('/')
    val title = fileName.substringBeforeLast('.')
    return title.ifBlank { fileName.ifBlank { "挂载笔记" } }
}

private fun isMountedNoteMissingError(message: String): Boolean {
    val normalized = message.lowercase()
    return normalized.contains("http 404") ||
        normalized.contains("not found") ||
        normalized.contains("missing")
}

private fun isMarkdownNotePath(path: String): Boolean {
    val normalized = path.lowercase()
    return normalized.endsWith(".md") || normalized.endsWith(".markdown")
}

private fun normalizeMountedNotePath(rawPath: String): String {
    val replaced = rawPath.trim().replace('\\', '/')
    if (replaced.isBlank()) {
        return ""
    }
    if (replaced.contains(":")) {
        return ""
    }
    val segments = replaced
        .removePrefix("./")
        .removePrefix("/")
        .split('/')
    val stack = mutableListOf<String>()
    segments.forEach { segment ->
        when (segment) {
            "", "." -> Unit
            ".." -> {
                if (stack.isNotEmpty()) {
                    stack.removeLast()
                }
            }
            else -> stack += segment
        }
    }
    return stack.joinToString("/")
}

private enum class ReaderMediaKind {
    IMAGE,
    VIDEO,
    OTHER
}

private data class InlineImageItem(
    val url: String,
    val alt: String
)

private data class InlineVideoItem(
    val url: String,
    val title: String
)

private data class ReaderMediaRewriteResult(
    val markdown: String,
    val images: List<InlineImageItem>,
    val videos: List<InlineVideoItem>
)

private fun rewriteReaderMarkdownMedia(
    markdown: String,
    taskId: String?,
    apiBaseUrl: String
): ReaderMediaRewriteResult {
    if (markdown.isBlank()) {
        return ReaderMediaRewriteResult(
            markdown = markdown,
            images = emptyList(),
            videos = emptyList()
        )
    }

    val images = mutableListOf<InlineImageItem>()
    val videos = mutableListOf<InlineVideoItem>()
    var rewritten = markdown

    rewritten = OBSIDIAN_EMBED_PATTERN.replace(rewritten) { match ->
        val rawBody = match.groupValues.getOrNull(1).orEmpty().trim()
        if (rawBody.isBlank()) {
            return@replace match.value
        }
        val parts = rawBody.split('|', limit = 2)
        val rawPath = parts.firstOrNull().orEmpty().trim()
        val alias = parts.getOrNull(1).orEmpty().trim()
        if (rawPath.isBlank()) {
            return@replace match.value
        }
        val resolvedUrl = resolveReaderMediaUrl(
            rawUrl = rawPath,
            taskId = taskId,
            apiBaseUrl = apiBaseUrl
        ) ?: rawPath
        val label = alias
        when (resolveReaderMediaKind(rawPath)) {
            ReaderMediaKind.IMAGE -> {
                val alt = sanitizeInlineImageLabel(
                    rawLabel = label,
                    rawUrl = rawPath
                )
                "![${escapeMarkdownLinkLabel(alt)}]($resolvedUrl)"
            }

            ReaderMediaKind.VIDEO -> {
                videos += InlineVideoItem(
                    url = resolvedUrl,
                    title = label
                )
                "\n\n"
            }

            ReaderMediaKind.OTHER -> {
                "[${escapeMarkdownLinkLabel(label)}]($resolvedUrl)"
            }
        }
    }

    rewritten = HTML_VIDEO_TAG_PATTERN.replace(rewritten) { match ->
        val rawUrl = match.groupValues.getOrNull(2).orEmpty().trim()
        if (rawUrl.isBlank()) {
            return@replace "\n\n"
        }
        val resolvedUrl = resolveReaderMediaUrl(
            rawUrl = rawUrl,
            taskId = taskId,
            apiBaseUrl = apiBaseUrl
        ) ?: rawUrl
        videos += InlineVideoItem(
            url = resolvedUrl,
            title = deriveReaderMediaLabel(rawUrl)
        )
        "\n\n"
    }

    rewritten = MARKDOWN_IMAGE_INLINE_PATTERN.replace(rewritten) { match ->
        val alt = match.groupValues.getOrNull(1).orEmpty().trim()
        val parsedTarget = parseMarkdownLinkTarget(
            match.groupValues.getOrNull(2).orEmpty()
        )
        val rawUrl = parsedTarget.destination.trim()
        if (rawUrl.isBlank()) {
            return@replace match.value
        }
        val resolvedUrl = resolveReaderMediaUrl(
            rawUrl = rawUrl,
            taskId = taskId,
            apiBaseUrl = apiBaseUrl
        ) ?: rawUrl
        when (resolveReaderMediaKind(rawUrl)) {
            ReaderMediaKind.IMAGE -> {
                val sanitizedAlt = sanitizeInlineImageLabel(
                    rawLabel = alt,
                    rawUrl = rawUrl
                )
                val rebuiltTarget = buildMarkdownLinkTarget(
                    destination = resolvedUrl,
                    suffix = parsedTarget.suffix
                )
                "![${escapeMarkdownLinkLabel(sanitizedAlt)}]($rebuiltTarget)"
            }

            ReaderMediaKind.VIDEO -> {
                videos += InlineVideoItem(
                    url = resolvedUrl,
                    title = alt.ifBlank { deriveReaderMediaLabel(rawUrl) }
                )
                "\n\n"
            }

            ReaderMediaKind.OTHER -> {
                val rebuiltTarget = buildMarkdownLinkTarget(
                    destination = resolvedUrl,
                    suffix = parsedTarget.suffix
                )
                "![${escapeMarkdownLinkLabel(alt)}]($rebuiltTarget)"
            }
        }
    }

    rewritten = MARKDOWN_LINK_GENERIC_PATTERN.replace(rewritten) { match ->
        val label = match.groupValues.getOrNull(1).orEmpty()
        val rawUrl = match.groupValues.getOrNull(2).orEmpty().trim()
        if (rawUrl.isBlank()) {
            return@replace match.value
        }
        val resolvedUrl = resolveReaderMediaUrl(
            rawUrl = rawUrl,
            taskId = taskId,
            apiBaseUrl = apiBaseUrl
        ) ?: rawUrl
        if (resolveReaderMediaKind(rawUrl) == ReaderMediaKind.VIDEO) {
            videos += InlineVideoItem(
                url = resolvedUrl,
                title = label.trim().ifBlank { deriveReaderMediaLabel(rawUrl) }
            )
            "\n\n"
        } else {
            val suffix = match.groupValues.getOrNull(3).orEmpty()
            "[${escapeMarkdownLinkLabel(label)}]($resolvedUrl$suffix)"
        }
    }

    val normalized = rewritten
        .replace("\r\n", "\n")
        .replace(Regex("\\n{3,}"), "\n\n")
        .trimEnd()

    return ReaderMediaRewriteResult(
        markdown = normalized,
        images = images,
        videos = videos
    )
}

private fun resolveReaderMediaKind(rawPath: String): ReaderMediaKind {
    val normalized = rawPath
        .substringBefore('?')
        .substringBefore('#')
        .trim()
        .lowercase()
    if (normalized.isEmpty()) {
        return ReaderMediaKind.OTHER
    }
    val ext = normalized.substringAfterLast('.', "")
    return when {
        ext in IMAGE_EXTENSIONS -> ReaderMediaKind.IMAGE
        ext in VIDEO_EXTENSIONS -> ReaderMediaKind.VIDEO
        else -> ReaderMediaKind.OTHER
    }
}

private fun deriveReaderMediaLabel(rawPath: String): String {
    val normalized = rawPath
        .substringBefore('?')
        .substringBefore('#')
        .replace('\\', '/')
        .trim()
    if (normalized.isBlank()) {
        return "媒体资源"
    }
    val fileName = normalized.substringAfterLast('/')
    return fileName
        .substringBeforeLast('.')
        .ifBlank { fileName.ifBlank { "媒体资源" } }
}

private fun sanitizeInlineImageLabel(rawLabel: String, rawUrl: String): String {
    val trimmed = rawLabel.trim()
    if (trimmed.isBlank()) {
        return ""
    }
    val derived = deriveReaderMediaLabel(rawUrl).trim()
    if (derived.isNotBlank() && trimmed.equals(derived, ignoreCase = true)) {
        return ""
    }
    if (looksLikeRawPathOrUrl(trimmed) || looksLikeGeneratedMediaLabel(trimmed)) {
        return ""
    }
    return trimmed
}

private fun looksLikeRawPathOrUrl(text: String): Boolean {
    val normalized = text.trim()
    if (normalized.isBlank()) {
        return false
    }
    val lower = normalized.lowercase()
    if (ABSOLUTE_URL_PATTERN.matches(normalized) ||
        lower.startsWith("data:") ||
        lower.startsWith("blob:") ||
        lower.startsWith("content:") ||
        normalized.startsWith("/") ||
        normalized.startsWith("./") ||
        normalized.startsWith("../")
    ) {
        return true
    }
    return normalized.contains('\\') ||
        normalized.contains('/') ||
        Regex("^[a-zA-Z]:[/\\\\]").containsMatchIn(normalized)
}

private fun looksLikeGeneratedMediaLabel(text: String): Boolean {
    val normalized = text.trim()
    if (normalized.length !in 4..40) {
        return false
    }
    if (!normalized.any(Char::isDigit)) {
        return false
    }
    if (!(normalized.contains('_') || normalized.contains('-'))) {
        return false
    }
    if (!normalized.all { it.isLetterOrDigit() || it == '_' || it == '-' }) {
        return false
    }
    return true
}

private fun resolveReaderMediaUrl(
    rawUrl: String,
    taskId: String?,
    apiBaseUrl: String
): String? {
    val source = rawUrl.trim()
    if (source.isBlank()) {
        return null
    }
    val lower = source.lowercase()
    if (ABSOLUTE_URL_PATTERN.matches(source) ||
        lower.startsWith("data:") ||
        lower.startsWith("blob:") ||
        lower.startsWith("mailto:") ||
        lower.startsWith("tel:")
    ) {
        return source
    }
    if (lower.startsWith("/api/mobile/tasks/")) {
        val origin = resolveReaderApiOrigin(apiBaseUrl)
        return if (origin.isNotBlank()) origin + source else source
    }
    if (source.startsWith("/")) {
        val origin = resolveReaderApiOrigin(apiBaseUrl)
        return if (origin.isNotBlank()) origin + source else source
    }
    val normalizedTaskId = taskId?.trim().orEmpty()
    if (normalizedTaskId.isEmpty()) {
        return source
    }
    val normalizedPath = normalizeMountedNotePath(source).ifBlank {
        source.removePrefix("./").removePrefix("/").trim()
    }
    if (normalizedPath.isBlank()) {
        return source
    }
    val base = apiBaseUrl.trim().trimEnd('/')
    if (base.isBlank()) {
        return source
    }
    return "$base/tasks/${Uri.encode(normalizedTaskId)}/asset?path=${Uri.encode(normalizedPath)}"
}

private fun resolveReaderApiOrigin(apiBaseUrl: String): String {
    val normalized = apiBaseUrl.trim()
    if (normalized.isBlank()) {
        return ""
    }
    return runCatching {
        val uri = Uri.parse(normalized)
        val scheme = uri.scheme.orEmpty()
        val authority = uri.encodedAuthority.orEmpty()
        if (scheme.isBlank() || authority.isBlank()) {
            ""
        } else {
            "$scheme://$authority"
        }
    }.getOrDefault("")
}

@Composable
private fun InlineImageGallery(
    images: List<InlineImageItem>,
    modifier: Modifier = Modifier,
    onImageTap: (InlineImageItem) -> Unit
) {
    if (images.isEmpty()) {
        return
    }
    val context = LocalContext.current
    val density = LocalDensity.current
    val configuration = LocalConfiguration.current
    val viewportHeightPx = remember(configuration, density) {
        with(density) {
            configuration.screenHeightDp.dp.toPx()
        }
    }
    val viewportWidthPx = remember(configuration, density) {
        with(density) {
            configuration.screenWidthDp.dp.toPx()
        }
    }
    val preloadVerticalPx = remember(viewportHeightPx, density) {
        max(viewportHeightPx, with(density) { 420.dp.toPx() })
    }
    val preloadHorizontalPx = remember(viewportWidthPx, density) {
        max(viewportWidthPx * 0.65f, with(density) { 220.dp.toPx() })
    }
    Column(
        modifier = modifier,
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        images.forEachIndexed { index, image ->
            InlineImagePageCard(
                image = image,
                index = index,
                context = context,
                viewportWidthPx = viewportWidthPx,
                viewportHeightPx = viewportHeightPx,
                preloadHorizontalPx = preloadHorizontalPx,
                preloadVerticalPx = preloadVerticalPx,
                onImageTap = onImageTap
            )
            InlineImageCaption(
                caption = image.alt
            )
        }
    }
}

@Composable
private fun InlineImagePageCard(
    image: InlineImageItem,
    index: Int,
    context: Context,
    viewportWidthPx: Float,
    viewportHeightPx: Float,
    preloadHorizontalPx: Float,
    preloadVerticalPx: Float,
    onImageTap: (InlineImageItem) -> Unit
) {
    var shouldLoad by remember(image.url) {
        mutableStateOf(false)
    }
    var isLoading by remember(image.url) {
        mutableStateOf(false)
    }
    var imageAspectRatio by remember(image.url) {
        mutableFloatStateOf(0f)
    }
    val imageFrameModifier = if (imageAspectRatio > 0f) {
        Modifier
            .fillMaxWidth()
            .heightIn(min = 72.dp)
            .aspectRatio(imageAspectRatio)
    } else {
        Modifier
            .fillMaxWidth()
            .height(72.dp)
    }
    Box(
        modifier = Modifier
            .fillMaxWidth()
            .onGloballyPositioned { coordinates ->
                if (shouldLoad) {
                    return@onGloballyPositioned
                }
                val bounds = coordinates.boundsInWindow()
                val inHorizontalRange = bounds.right >= -preloadHorizontalPx &&
                    bounds.left <= viewportWidthPx + preloadHorizontalPx
                val inVerticalRange = bounds.bottom >= -preloadVerticalPx &&
                    bounds.top <= viewportHeightPx + preloadVerticalPx
                if (inHorizontalRange && inVerticalRange) {
                    shouldLoad = true
                    isLoading = true
                }
            }
            .clickable { onImageTap(image) }
            .clip(RoundedCornerShape(10.dp))
            .background(Color(0xFFF8FAFC))
            .then(imageFrameModifier)
    ) {
        if (shouldLoad) {
            AsyncImage(
                model = ImageRequest.Builder(context)
                    .data(image.url)
                    .crossfade(true)
                    .build(),
                contentDescription = image.alt.ifBlank { "image_${index + 1}" },
                contentScale = ContentScale.Fit,
                modifier = Modifier
                    .fillMaxSize()
                    .background(Color(0xFFF8FAFC))
                    .clip(RoundedCornerShape(10.dp)),
                onLoading = {
                    isLoading = true
                },
                onSuccess = { state ->
                    val intrinsicWidth = state.result.drawable.intrinsicWidth
                    val intrinsicHeight = state.result.drawable.intrinsicHeight
                    if (intrinsicWidth > 0 && intrinsicHeight > 0) {
                        val nextAspectRatio = intrinsicWidth.toFloat() / intrinsicHeight.toFloat()
                        if (abs(nextAspectRatio - imageAspectRatio) > 0.001f) {
                            imageAspectRatio = nextAspectRatio
                        }
                    }
                    isLoading = false
                },
                onError = {
                    isLoading = false
                }
            )
        }
        if (isLoading) {
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .background(Color(0xFFF8FAFC).copy(alpha = 0.72f)),
                contentAlignment = Alignment.Center
            ) {
                CircularProgressIndicator(
                    modifier = Modifier.size(18.dp),
                    strokeWidth = 2.dp,
                    color = Color(0xFF94A3B8)
                )
            }
        }
        if (!shouldLoad) {
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .background(Color(0xFFF8FAFC)),
                contentAlignment = Alignment.Center
            ) {
                if (isLoading) {
                    CircularProgressIndicator(
                        modifier = Modifier.size(18.dp),
                        strokeWidth = 2.dp,
                        color = Color(0xFF94A3B8)
                    )
                }
            }
        }
    }
}

@Composable
private fun InlineImageCaption(
    caption: String,
    modifier: Modifier = Modifier
) {
    val trimmedCaption = caption.trim()
    if (trimmedCaption.isBlank()) {
        return
    }
    Text(
        text = trimmedCaption,
        fontSize = 12.sp,
        color = Color(0xFF475467),
        maxLines = 2,
        overflow = TextOverflow.Ellipsis,
        modifier = modifier.padding(horizontal = 10.dp, vertical = 2.dp)
    )
}

@Composable
private fun InlineVideoList(
    videos: List<InlineVideoItem>,
    modifier: Modifier = Modifier,
    onTelemetry: (eventType: String, url: String) -> Unit
) {
    if (videos.isEmpty()) {
        return
    }
    Column(
        modifier = modifier,
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        videos.forEach { video ->
            if (video.title.isNotBlank()) {
                Text(
                    text = video.title,
                    fontSize = 12.sp,
                    color = Color(0xFF475467),
                    modifier = Modifier.padding(start = 2.dp)
                )
            }
            InlineVideoPlayer(
                video = video,
                onTelemetry = onTelemetry
            )
        }
    }
}

@Composable
private fun InlineVideoPlayer(
    video: InlineVideoItem,
    onTelemetry: (eventType: String, url: String) -> Unit
) {
    var videoViewRef by remember(video.url) {
        mutableStateOf<VideoView?>(null)
    }
    DisposableEffect(video.url) {
        onDispose {
            videoViewRef?.stopPlayback()
            videoViewRef = null
        }
    }
    Card(
        modifier = Modifier.fillMaxWidth(),
        shape = RoundedCornerShape(12.dp),
        colors = CardDefaults.cardColors(
            containerColor = Color(0xFF0B1220)
        )
    ) {
        AndroidView(
            modifier = Modifier
                .fillMaxWidth()
                .heightIn(min = 190.dp),
            factory = { context ->
                VideoView(context).apply {
                    setBackgroundColor(android.graphics.Color.BLACK)
                    val controller = MediaController(context)
                    controller.setAnchorView(this)
                    setMediaController(controller)
                    setOnPreparedListener { player ->
                        player.isLooping = false
                        seekTo(1)
                        onTelemetry("inline_video_prepared", video.url)
                    }
                    setOnCompletionListener {
                        onTelemetry("inline_video_completed", video.url)
                    }
                    setOnErrorListener { _, what, extra ->
                        onTelemetry("inline_video_error", "${video.url}#$what#$extra")
                        false
                    }
                    setOnClickListener {
                        if (isPlaying) {
                            pause()
                            onTelemetry("inline_video_paused", video.url)
                        } else {
                            start()
                            onTelemetry("inline_video_started", video.url)
                        }
                    }
                    tag = video.url
                    setVideoURI(Uri.parse(video.url))
                }.also { view ->
                    videoViewRef = view
                }
            },
            update = { view ->
                videoViewRef = view
                val boundUrl = view.tag as? String
                if (boundUrl != video.url) {
                    view.stopPlayback()
                    view.tag = video.url
                    view.setVideoURI(Uri.parse(video.url))
                }
            }
        )
    }
}

@Composable
private fun ReaderImageLightbox(
    image: InlineImageItem,
    onDismiss: () -> Unit
) {
    var scale by remember(image.url) {
        mutableFloatStateOf(1f)
    }
    var panOffset by remember(image.url) {
        mutableStateOf(Offset.Zero)
    }
    var verticalDragOffset by remember(image.url) {
        mutableFloatStateOf(0f)
    }
    val dismissThreshold = with(LocalDensity.current) { 120.dp.toPx() }
    val context = LocalContext.current

    Dialog(
        onDismissRequest = onDismiss,
        properties = DialogProperties(
            usePlatformDefaultWidth = false,
            decorFitsSystemWindows = false
        )
    ) {
        Box(
            modifier = Modifier
                .fillMaxSize()
                .background(Color.Black.copy(alpha = 0.94f))
                .pointerInput(image.url) {
                    detectTapGestures(onTap = { onDismiss() })
                }
        ) {
            AsyncImage(
                model = ImageRequest.Builder(context)
                    .data(image.url)
                    .crossfade(true)
                    .build(),
                contentDescription = image.alt.ifBlank { "preview_image" },
                contentScale = ContentScale.Fit,
                modifier = Modifier
                    .align(Alignment.Center)
                    .fillMaxWidth()
                    .graphicsLayer {
                        scaleX = scale
                        scaleY = scale
                        translationX = panOffset.x
                        translationY = panOffset.y + verticalDragOffset
                    }
                    .pointerInput(image.url) {
                        detectTransformGestures { _, pan, zoom, _ ->
                            val nextScale = (scale * zoom).coerceIn(1f, 4f)
                            scale = nextScale
                            panOffset = if (nextScale <= 1.02f) {
                                Offset.Zero
                            } else {
                                panOffset + pan
                            }
                        }
                    }
                    .pointerInput(image.url, scale) {
                        detectVerticalDragGestures(
                            onVerticalDrag = { change, dragAmount ->
                                if (scale > 1.02f) {
                                    return@detectVerticalDragGestures
                                }
                                verticalDragOffset += dragAmount
                                change.consume()
                            },
                            onDragCancel = {
                                verticalDragOffset = 0f
                            },
                            onDragEnd = {
                                if (verticalDragOffset <= -dismissThreshold) {
                                    onDismiss()
                                } else {
                                    verticalDragOffset = 0f
                                }
                            }
                        )
                    }
            )
            Text(
                text = "双指缩放，上滑收起",
                color = Color.White.copy(alpha = 0.86f),
                fontSize = 12.sp,
                modifier = Modifier
                    .align(Alignment.BottomCenter)
                    .padding(bottom = 22.dp)
            )
        }
    }
}

@SuppressLint("ClickableViewAccessibility")
@Composable
private fun MarkdownParagraph(
    markdown: String,
    plainText: String,
    markwon: Markwon,
    renderConfig: MarkdownReaderRenderConfig,
    renderRefreshVersion: Int,
    textSizeSp: Float,
    lineSpacingMultiplier: Float,
    textColor: Color,
    fontWeight: FontWeight,
    blockId: String,
    blockType: String,
    blockIndentLevel: Int,
    selection: TokenSelection?,
    isFavorited: Boolean,
    overlayRootWindowOffset: Offset,
    insightTerms: List<String>,
    emphasizedSelections: List<TokenSelection>,
    likedSelections: List<TokenSelection>,
    annotatedSelections: List<TokenSelection>,
    pendingAnchorSelections: List<TokenSelection>,
    mountedAnchorSelections: List<TokenSelection>,
    onSelectionAction: (SelectionContextAction, TokenSelection, InsightTermAnchor?) -> Unit,
    onTokenSingleTap: (selection: TokenSelection?, anchor: InsightTermAnchor?) -> Unit,
    onInsightTermTap: (tap: InsightTermTapPayload) -> Unit,
    onInlineImageTap: (InlineImageItem) -> Unit,
    onParagraphDoubleTap: (offset: Offset) -> Unit,
    onSelectionModeChanged: (Boolean) -> Unit,
    modifier: Modifier = Modifier
) {
    val latestSelectionAction = androidx.compose.runtime.rememberUpdatedState(onSelectionAction)
    val latestOverlayRootOffset = androidx.compose.runtime.rememberUpdatedState(overlayRootWindowOffset)
    val latestSelectionModeChanged = androidx.compose.runtime.rememberUpdatedState(onSelectionModeChanged)
    var layoutWidthPx by remember(blockId) {
        mutableIntStateOf(0)
    }
    AndroidView(
        modifier = modifier.onSizeChanged { size ->
            val measured = size.width
            if (measured > 0 && measured != layoutWidthPx) {
                layoutWidthPx = measured
            }
        },
        factory = { context ->
            val textView = TextView(context).apply {
                applyReaderTextLayoutPolicy(this)
                layoutParams = ViewGroup.LayoutParams(
                    ViewGroup.LayoutParams.MATCH_PARENT,
                    ViewGroup.LayoutParams.WRAP_CONTENT
                )
                setTextIsSelectable(true)
                applyReaderParagraphLineSpacing(this, lineSpacingMultiplier)
                letterSpacing = renderConfig.textLetterSpacing
                setHorizontallyScrolling(false)
                isSingleLine = false
                maxLines = Int.MAX_VALUE
                isLongClickable = true
                gravity = android.view.Gravity.START or android.view.Gravity.TOP
                textAlignment = android.view.View.TEXT_ALIGNMENT_GRAVITY
                layoutDirection = android.view.View.LAYOUT_DIRECTION_LTR
                textDirection = android.view.View.TEXT_DIRECTION_LTR
                // 部分设备缺少 justificationMode，统一由 fallback 分支兜底处理。
            }
            var suppressNativeSelectionUntilMs = 0L

            val detector = GestureDetector(
                context,
                object : GestureDetector.SimpleOnGestureListener() {
                    override fun onDown(e: MotionEvent): Boolean = true

                    override fun onSingleTapConfirmed(e: MotionEvent): Boolean {
                        if (textView.hasSelection()) {
                            return false
                        }
                        val tappedImage = resolveTappedInlineImage(
                            textView = textView,
                            x = e.x,
                            y = e.y
                        )
                        if (tappedImage != null) {
                            clearNativeTextSelection(textView)
                            suppressNativeSelectionUntilMs =
                                SystemClock.uptimeMillis() + ViewConfiguration.getTapTimeout().toLong()
                            onInlineImageTap(tappedImage)
                            return true
                        }
                        val cursor = resolveCursorOffset(
                            textView = textView,
                            x = e.x,
                            y = e.y
                        ) ?: return false
                        val insightTerm = resolveTappedInsightTerm(textView, cursor)
                        if (insightTerm != null) {
                            val anchor = resolveInsightTermAnchor(
                                textView = textView,
                                range = insightTerm,
                                rootWindowOffset = overlayRootWindowOffset
                            ) ?: resolveFallbackAnchor(
                                textView = textView,
                                touchX = e.x,
                                touchY = e.y,
                                rootWindowOffset = overlayRootWindowOffset
                            )
                            onInsightTermTap(
                                InsightTermTapPayload(
                                    range = insightTerm,
                                    anchor = anchor
                                )
                            )
                            return true
                        }
                        val sourceText = textView.text
                            ?.toString()
                            .orEmpty()
                            .ifBlank { plainText }
                        val normalizedCursor = normalizeLexicalCursor(
                            text = sourceText,
                            cursor = cursor
                        )
                        val tokenSelection = if (normalizedCursor == null) {
                            null
                        } else {
                            resolveTokenSelection(
                                text = sourceText,
                                cursor = normalizedCursor,
                                nativePayload = LexicalNativeBridge.segmentAt(
                                    sourceText,
                                    normalizedCursor
                                )
                            )
                        }
                        val tokenAnchor = if (tokenSelection == null) {
                            null
                        } else {
                            resolveInsightTermAnchor(
                                textView = textView,
                                range = InsightTermRange(
                                    term = tokenSelection.token,
                                    start = tokenSelection.start,
                                    end = tokenSelection.end
                                ),
                                rootWindowOffset = latestOverlayRootOffset.value
                            )
                        }
                        val fallbackAnchor = resolveFallbackAnchor(
                            textView = textView,
                            touchX = e.x,
                            touchY = e.y,
                            rootWindowOffset = latestOverlayRootOffset.value
                        )
                        onTokenSingleTap(tokenSelection, tokenAnchor ?: fallbackAnchor)
                        return false
                    }

                    override fun onDoubleTap(e: MotionEvent): Boolean {
                        // 双击应稳定触发整段加粗态，先清理原生选区避免被系统双击选词抢占。
                        clearNativeTextSelection(textView)
                        suppressNativeSelectionUntilMs =
                            SystemClock.uptimeMillis() + ViewConfiguration.getDoubleTapTimeout().toLong()
                        onParagraphDoubleTap(
                            Offset(
                                x = e.x,
                                y = e.y
                            )
                        )
                        return true
                    }
                }
            )

            val actionCopyId = 2001
            val actionBoldId = 2003
            val actionAnnotateId = 2004
            val actionSearchCardId = 2005
            val actionMarkAnchorId = 2006
            val selectionActionModeCallback = object : android.view.ActionMode.Callback {
                override fun onCreateActionMode(mode: android.view.ActionMode?, menu: android.view.Menu?): Boolean {
                    val safeMenu = menu ?: return false
                    latestSelectionModeChanged.value(true)
                    textView.parent?.requestDisallowInterceptTouchEvent(true)
                    safeMenu.clear()
                    safeMenu.add(0, actionCopyId, 0, "复制").setShowAsAction(android.view.MenuItem.SHOW_AS_ACTION_ALWAYS)
                    safeMenu.add(0, actionBoldId, 2, "加粗/取消").setShowAsAction(android.view.MenuItem.SHOW_AS_ACTION_ALWAYS)
                    safeMenu.add(0, actionAnnotateId, 3, "批注").setShowAsAction(android.view.MenuItem.SHOW_AS_ACTION_ALWAYS)
                    safeMenu.add(0, actionSearchCardId, 4, "搜索建卡").setShowAsAction(android.view.MenuItem.SHOW_AS_ACTION_ALWAYS)
                    safeMenu.add(0, actionMarkAnchorId, 5, "标记锚点").setShowAsAction(android.view.MenuItem.SHOW_AS_ACTION_ALWAYS)
                    return true
                }

                override fun onPrepareActionMode(mode: android.view.ActionMode?, menu: android.view.Menu?): Boolean {
                    return false
                }

                override fun onActionItemClicked(mode: android.view.ActionMode?, item: android.view.MenuItem?): Boolean {
                    val selected = resolveCurrentTextSelection(textView) ?: return false
                    val anchor = resolveInsightTermAnchor(
                        textView = textView,
                        range = InsightTermRange(
                            term = selected.token,
                            start = selected.start,
                            end = selected.end
                        ),
                        rootWindowOffset = latestOverlayRootOffset.value
                    ) ?: resolveFallbackAnchor(
                        textView = textView,
                        touchX = textView.width * 0.5f,
                        touchY = textView.height * 0.5f,
                        rootWindowOffset = latestOverlayRootOffset.value
                    )
                    when (item?.itemId) {
                        actionCopyId -> latestSelectionAction.value(SelectionContextAction.Copy, selected, anchor)
                        actionBoldId -> latestSelectionAction.value(SelectionContextAction.ToggleBold, selected, anchor)
                        actionAnnotateId -> latestSelectionAction.value(SelectionContextAction.Annotate, selected, anchor)
                        actionSearchCardId -> latestSelectionAction.value(SelectionContextAction.SearchCard, selected, anchor)
                        actionMarkAnchorId -> latestSelectionAction.value(SelectionContextAction.MarkAnchor, selected, anchor)
                        else -> return false
                    }
                    mode?.finish()
                    return true
                }

                override fun onDestroyActionMode(mode: android.view.ActionMode?) {
                    latestSelectionModeChanged.value(false)
                    textView.parent?.requestDisallowInterceptTouchEvent(false)
                }
            }
            TextViewCompat.setCustomSelectionActionModeCallback(textView, selectionActionModeCallback)
            textView.setOnLongClickListener {
                textView.parent?.requestDisallowInterceptTouchEvent(true)
                false
            }

            textView.setOnTouchListener { _, event ->
                if (textView.hasSelection()) {
                    textView.parent?.requestDisallowInterceptTouchEvent(true)
                } else if (event.actionMasked == MotionEvent.ACTION_UP || event.actionMasked == MotionEvent.ACTION_CANCEL) {
                    textView.parent?.requestDisallowInterceptTouchEvent(false)
                }
                detector.onTouchEvent(event)
                val shouldSuppressNativeSelection =
                    SystemClock.uptimeMillis() <= suppressNativeSelectionUntilMs
                if (!shouldSuppressNativeSelection &&
                    (event.actionMasked == MotionEvent.ACTION_UP || event.actionMasked == MotionEvent.ACTION_CANCEL)
                ) {
                    suppressNativeSelectionUntilMs = 0L
                }
                shouldSuppressNativeSelection
            }

            textView
        },
        update = { textView ->
            applyReaderTextLayoutPolicy(textView)
            if (!textView.isTextSelectable) {
                textView.setTextIsSelectable(true)
            }
            // 宽度未稳定时不进行正文渲染，避免在 0 宽首帧写入错误布局状态。
            val parentWidthNow = (textView.parent as? View)?.width ?: 0
            val stableWidthPx = max(layoutWidthPx, max(parentWidthNow, textView.width))
            if (stableWidthPx <= 0) {
                if (textView.text.isNullOrEmpty()) {
                    // 宽度未就绪时维持一个最小文本高度，避免段落塌陷为 0 高。
                    textView.text = "\u00A0"
                }
                return@AndroidView
            }
            if (textView.minWidth != stableWidthPx) {
                textView.minWidth = stableWidthPx
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.JELLY_BEAN) {
                if (textView.maxWidth != stableWidthPx) {
                    textView.maxWidth = stableWidthPx
                }
            }
            val textColorArgb = textColor.toArgbSafe()
            val weight = resolveReaderWeight(fontWeight)
            val textStyleFingerprint = buildReaderTextStyleFingerprint(
                textSizeSp = textSizeSp,
                lineSpacingMultiplier = lineSpacingMultiplier,
                letterSpacing = renderConfig.textLetterSpacing,
                textColorArgb = textColorArgb,
                fontWeight = weight,
                mediumFontFamily = renderConfig.mediumFontFamily,
                bodyFontFamily = renderConfig.bodyFontFamily
            )
            val targetLineHeightPx = resolveReaderLineHeightPx(
                textSizeSp = textSizeSp,
                lineSpacingMultiplier = lineSpacingMultiplier,
                scaledDensity = textView.resources.displayMetrics.scaledDensity
            )
            val selectionFingerprint = resolveSelectionFingerprint(selection)
            val insightTermsFingerprint = insightTerms.hashCode()
            val emphasisFingerprint = emphasizedSelections.hashCode()
            val favoriteFingerprint = if (isFavorited) 1 else 0
            val likedFingerprint = likedSelections.hashCode()
            val annotatedFingerprint = annotatedSelections.hashCode()
            val pendingAnchorFingerprint = pendingAnchorSelections.hashCode()
            val mountedAnchorFingerprint = mountedAnchorSelections.hashCode()
            val previousContext = textView.tag as? InsightTapContext
            val refreshChanged = previousContext?.renderRefreshVersion != renderRefreshVersion
            val markdownChanged = refreshChanged || previousContext?.renderedMarkdown != markdown || textView.text.isNullOrEmpty()
            val widthChanged = previousContext?.appliedWidthPx != stableWidthPx
            val canFastSkip = previousContext != null &&
                !markdownChanged &&
                previousContext.textStyleFingerprint == textStyleFingerprint &&
                previousContext.lineHeightPx == targetLineHeightPx &&
                previousContext.selectionFingerprint == selectionFingerprint &&
                previousContext.insightTermsFingerprint == insightTermsFingerprint &&
                previousContext.emphasisFingerprint == emphasisFingerprint &&
                previousContext.favoriteFingerprint == favoriteFingerprint &&
                previousContext.likedFingerprint == likedFingerprint &&
                previousContext.annotatedFingerprint == annotatedFingerprint &&
                previousContext.pendingAnchorFingerprint == pendingAnchorFingerprint &&
                previousContext.mountedAnchorFingerprint == mountedAnchorFingerprint &&
                !widthChanged
            if (canFastSkip) {
                return@AndroidView
            }

            val textStyleChanged = previousContext?.textStyleFingerprint != textStyleFingerprint
            val lineHeightChanged = previousContext?.lineHeightPx != targetLineHeightPx
            if (textStyleChanged) {
                textView.textSize = textSizeSp
                textView.letterSpacing = renderConfig.textLetterSpacing
                textView.gravity = android.view.Gravity.START
                textView.textAlignment = android.view.View.TEXT_ALIGNMENT_VIEW_START
                textView.layoutDirection = android.view.View.LAYOUT_DIRECTION_LTR
                textView.textDirection = android.view.View.TEXT_DIRECTION_LTR
                textView.setTextColor(textColorArgb)
                textView.typeface = when (weight) {
                    700 -> MarkdownTypefaceResolver.resolveWithWeight(
                        context = textView.context,
                        fontFamily = renderConfig.mediumFontFamily,
                        weight = 700
                    )
                    500 -> MarkdownTypefaceResolver.resolveWithWeight(
                        context = textView.context,
                        fontFamily = renderConfig.mediumFontFamily,
                        weight = 500
                    )
                    else -> MarkdownTypefaceResolver.resolveWithWeight(
                        context = textView.context,
                        fontFamily = renderConfig.bodyFontFamily,
                        weight = 400
                    )
                }
                applyReaderParagraphLineSpacing(textView, lineSpacingMultiplier)
            }
            if (markdownChanged) {
                val cachedMarkdown = readCachedReaderMarkdown(markdown)
                if (cachedMarkdown != null) {
                    textView.text = cachedMarkdown
                } else {
                    markwon.setMarkdown(textView, markdown)
                    writeCachedReaderMarkdown(
                        markdown = markdown,
                        rendered = textView.text
                    )
                }
            }

            val currentSpanned = textView.text as? Spanned
            val preserveListLayout = MARKDOWN_LIST_LINE_PATTERN.containsMatchIn(markdown) ||
                (currentSpanned?.let(::containsListLayoutSpan) == true)

            if (markdownChanged && !preserveListLayout) {
                applyHeadingSpacing(textView)
            }
            if (!preserveListLayout && (markdownChanged || lineHeightChanged)) {
                applyReaderParagraphLineHeightSpan(
                    textView = textView,
                    lineHeightPx = targetLineHeightPx
                )
            }
            val hasMediaBlocks = if (preserveListLayout) {
                false
            } else if (markdownChanged) {
                applyMediaLayout(textView)
            } else {
                previousContext?.hasMediaBlocks ?: applyMediaLayout(textView)
            }
            val source = textView.text
                ?.toString()
                .orEmpty()
                .ifBlank { plainText }
            val sourceChanged = previousContext?.sourceText != source
            val termsChanged = previousContext?.insightTermsFingerprint != insightTermsFingerprint
            val selectionChanged = previousContext?.selectionFingerprint != selectionFingerprint
            val emphasisChanged = previousContext?.emphasisFingerprint != emphasisFingerprint
            val favoriteChanged = previousContext?.favoriteFingerprint != favoriteFingerprint
            val likedChanged = previousContext?.likedFingerprint != likedFingerprint
            val annotationChanged = previousContext?.annotatedFingerprint != annotatedFingerprint
            val pendingAnchorChanged = previousContext?.pendingAnchorFingerprint != pendingAnchorFingerprint
            val mountedAnchorChanged = previousContext?.mountedAnchorFingerprint != mountedAnchorFingerprint
            val insightRanges = if (!sourceChanged && !termsChanged) {
                previousContext.ranges
            } else {
                resolveInsightTermRanges(
                    source = source,
                    terms = insightTerms
                )
            }
            if (markdownChanged || sourceChanged || termsChanged || selectionChanged || emphasisChanged || favoriteChanged || likedChanged || annotationChanged || pendingAnchorChanged || mountedAnchorChanged) {
                applySelectionStyle(
                    textView = textView,
                    selection = selection,
                    isFavorited = isFavorited,
                    insightRanges = insightRanges,
                    emphasizedSelections = emphasizedSelections,
                    likedSelections = likedSelections,
                    annotatedSelections = annotatedSelections,
                    pendingAnchorSelections = pendingAnchorSelections,
                    mountedAnchorSelections = mountedAnchorSelections
                )
            }
            if (markdownChanged || widthChanged || previousContext?.hasMediaBlocks != hasMediaBlocks) {
                applyMediaContainerInset(
                    textView = textView,
                    hasMediaBlocks = hasMediaBlocks,
                    preferredWidthPx = stableWidthPx
                )
            }
            textView.tag = InsightTapContext(
                ranges = insightRanges,
                renderedMarkdown = markdown,
                sourceText = source,
                renderRefreshVersion = renderRefreshVersion,
                insightTermsFingerprint = insightTermsFingerprint,
                selectionFingerprint = selectionFingerprint,
                emphasisFingerprint = emphasisFingerprint,
                favoriteFingerprint = favoriteFingerprint,
                likedFingerprint = likedFingerprint,
                annotatedFingerprint = annotatedFingerprint,
                pendingAnchorFingerprint = pendingAnchorFingerprint,
                mountedAnchorFingerprint = mountedAnchorFingerprint,
                textStyleFingerprint = textStyleFingerprint,
                lineHeightPx = targetLineHeightPx,
                hasMediaBlocks = hasMediaBlocks,
                appliedWidthPx = stableWidthPx
            )
        }
    )
}

private enum class SelectionContextAction {
    Copy,
    ToggleLike,
    ToggleBold,
    Annotate,
    SearchCard,
    MarkAnchor
}

/**
 * 将当前选中态与洞察词范围渲染到 TextView。
 * 说明：若段内包含媒体 span，则跳过覆盖样式，避免破坏媒体排版。
 */
private fun applySelectionStyle(
    textView: TextView,
    selection: TokenSelection?,
    isFavorited: Boolean,
    insightRanges: List<InsightTermRange>,
    emphasizedSelections: List<TokenSelection>,
    likedSelections: List<TokenSelection>,
    annotatedSelections: List<TokenSelection>,
    pendingAnchorSelections: List<TokenSelection>,
    mountedAnchorSelections: List<TokenSelection>
) {
    val baseText = textView.text
        ?.takeIf { it.isNotEmpty() }
        ?: return
    val source = baseText.toString()
    val hasSelectionOverlay = selection != null &&
        selection.start >= 0 &&
        selection.end <= source.length &&
        selection.start < selection.end
    val hasManualEmphasisOverlay = emphasizedSelections.any { item ->
        item.start >= 0 &&
            item.end <= source.length &&
            item.start < item.end
    }
    val hasLikedOverlay = likedSelections.any { item ->
        item.start >= 0 &&
            item.end <= source.length &&
            item.start < item.end
    }
    val hasFavoriteOverlay = isFavorited && source.isNotBlank()
    val hasAnnotationOverlay = annotatedSelections.any { item ->
        item.start >= 0 &&
            item.end <= source.length &&
            item.start < item.end
    }
    val hasPendingAnchorOverlay = pendingAnchorSelections.any { item ->
        item.start >= 0 &&
            item.end <= source.length &&
            item.start < item.end
    }
    val hasMountedAnchorOverlay = mountedAnchorSelections.any { item ->
        item.start >= 0 &&
            item.end <= source.length &&
            item.start < item.end
    }
    val hasOverlay = insightRanges.isNotEmpty()
        || hasSelectionOverlay
        || hasManualEmphasisOverlay
        || hasFavoriteOverlay
        || hasLikedOverlay
        || hasAnnotationOverlay
        || hasPendingAnchorOverlay
        || hasMountedAnchorOverlay
    val hasMediaSpan = (baseText as? Spanned)?.let(::containsMediaSpan) == true
    val hasTokenLevelOverlay = hasSelectionOverlay ||
        hasManualEmphasisOverlay ||
        hasLikedOverlay ||
        hasAnnotationOverlay ||
        hasPendingAnchorOverlay ||
        hasMountedAnchorOverlay
    val spannable = if (baseText is SpannableStringBuilder) {
        baseText
    } else {
        SpannableStringBuilder(baseText)
    }
    val hasExistingOverlay = spannable
        .getSpans(0, spannable.length, ReaderOverlaySpan::class.java)
        .isNotEmpty()
    val shouldSkipOverlayForMedia = hasMediaSpan && !hasTokenLevelOverlay
    if (!hasOverlay || shouldSkipOverlayForMedia) {
        if (hasExistingOverlay) {
            clearReaderOverlaySpans(spannable)
            textView.text = SpannableStringBuilder(spannable)
        }
        return
    }
    clearReaderOverlaySpans(spannable)
    val originalStrongRanges = if (hasFavoriteOverlay) {
        resolveOriginalStrongRanges(
            text = spannable,
            sourceLength = source.length
        )
    } else {
        emptyList()
    }

    val safeInsightRanges = if (hasMediaSpan) {
        emptyList()
    } else {
        insightRanges.filter { range ->
            range.start >= 0 &&
                range.end <= source.length &&
                range.start < range.end
        }
    }
    safeInsightRanges.forEach { range ->
        spannable.setSpan(
            ReaderForegroundColorSpan(0xFF1A7FB0.toInt()),
            range.start,
            range.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            ReaderBoldSpan(),
            range.start,
            range.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
    }

    emphasizedSelections.forEach { range ->
        if (range.start < 0 || range.end > source.length || range.start >= range.end) {
            return@forEach
        }
        spannable.setSpan(
            ReaderForegroundColorSpan(0xFF184E77.toInt()),
            range.start,
            range.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            ReaderBoldSpan(),
            range.start,
            range.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            ReaderRelativeSizeSpan(1.04f),
            range.start,
            range.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
    }

    mergeAdjacentSelections(
        selections = likedSelections,
        source = source
    ).forEach { range ->
        spannable.setSpan(
            ReaderLikedUnderlineSpan(
                color = 0xFFFBBF24.toInt(),
                thicknessPx = 3f
            ),
            range.start,
            range.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
    }

    annotatedSelections.forEach { range ->
        if (range.start < 0 || range.end > source.length || range.start >= range.end) {
            return@forEach
        }
        spannable.setSpan(
            ReaderLikedUnderlineSpan(
                color = 0xFF2B5F7B.toInt(),
                thicknessPx = 2f
            ),
            range.start,
            range.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            ReaderAnnotationBubbleIndicatorSpan(
                color = 0xFF2B5F7B.toInt()
            ),
            range.start,
            range.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
    }

    pendingAnchorSelections.forEach { range ->
        if (range.start < 0 || range.end > source.length || range.start >= range.end) {
            return@forEach
        }
        spannable.setSpan(
            ReaderAnchorUnderlineSpan(
                color = 0xFF1D4ED8.toInt(),
                thicknessPx = 2.6f,
                dashed = true
            ),
            range.start,
            range.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
    }

    mountedAnchorSelections.forEach { range ->
        if (range.start < 0 || range.end > source.length || range.start >= range.end) {
            return@forEach
        }
        spannable.setSpan(
            ReaderAnchorUnderlineSpan(
                color = 0xFF155EEF.toInt(),
                thicknessPx = 2.8f,
                dashed = false
            ),
            range.start,
            range.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            ReaderAnchorMountedIndicatorSpan(
                color = 0xFF155EEF.toInt()
            ),
            range.start,
            range.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
    }

    if (hasFavoriteOverlay) {
        spannable.setSpan(
            ReaderForegroundColorSpan(0xFF0E65A0.toInt()),
            0,
            source.length,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            ReaderBoldSpan(),
            0,
            source.length,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            ReaderRelativeSizeSpan(1.08f),
            0,
            source.length,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        originalStrongRanges.forEach { range ->
            spannable.setSpan(
                ReaderRelativeSizeSpan(1.04f),
                range.first,
                range.last + 1,
                Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
            )
        }
    }

    if (hasSelectionOverlay) {
        val safeSelection = requireNotNull(selection)
        spannable.setSpan(
            ReaderForegroundColorSpan(0xFF0E65A0.toInt()),
            safeSelection.start,
            safeSelection.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            ReaderBoldSpan(),
            safeSelection.start,
            safeSelection.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            ReaderRelativeSizeSpan(1.08f),
            safeSelection.start,
            safeSelection.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            ReaderLiftSpan(0.1f),
            safeSelection.start,
            safeSelection.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
    }

    // 保证 insight tags 的下划线在整段加粗等样式后依然可见。
    safeInsightRanges.forEach { range ->
        spannable.setSpan(
            ReaderLikedUnderlineSpan(
                color = 0xFF1A7FB0.toInt(),
                thicknessPx = 2.2f
            ),
            range.start,
            range.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
    }

    textView.text = SpannableStringBuilder(spannable)
}

private fun resolveOriginalStrongRanges(text: Spanned, sourceLength: Int): List<IntRange> {
    if (sourceLength <= 0) {
        return emptyList()
    }
    return text
        .getSpans(0, sourceLength, MetricAffectingSpan::class.java)
        .mapNotNull { span ->
            if (!isOriginalStrongSpan(span)) {
                return@mapNotNull null
            }
            val start = text.getSpanStart(span)
            val end = text.getSpanEnd(span)
            if (start < 0 || end <= start || end > sourceLength) {
                return@mapNotNull null
            }
            IntRange(start, end - 1)
        }
}

private fun isOriginalStrongSpan(span: MetricAffectingSpan): Boolean {
    return when (span) {
        is StyleSpan -> (span.style and Typeface.BOLD) != 0
        else -> {
            val spanName = span.javaClass.simpleName
            spanName.contains("Strong", ignoreCase = true) || spanName.contains("Bold", ignoreCase = true)
        }
    }
}

private fun applyHeadingSpacing(textView: TextView) {
    val baseText = textView.text as? Spanned ?: return
    if (baseText.isEmpty()) {
        return
    }
    val spannable = if (baseText is SpannableStringBuilder) {
        baseText
    } else {
        SpannableStringBuilder(baseText)
    }
    spannable
        .getSpans(0, spannable.length, ReaderHeadingSpacingSpan::class.java)
        .forEach { span ->
            spannable.removeSpan(span)
        }
    val headingSpans = spannable
        .getSpans(0, spannable.length, Any::class.java)
        .filter { span ->
            span.javaClass.name.contains("HeadingSpan", ignoreCase = true)
        }
    if (headingSpans.isEmpty()) {
        if (textView.text !== spannable) {
            textView.text = spannable
        }
        return
    }
    val density = textView.resources.displayMetrics.density
    val marginTopPx = (36f * density).roundToInt()
    val marginBottomPx = (12f * density).roundToInt()
    headingSpans.forEach { span ->
        val start = spannable.getSpanStart(span)
        val end = spannable.getSpanEnd(span)
        if (start < 0 || end <= start) {
            return@forEach
        }
        spannable.setSpan(
            ReaderHeadingSpacingSpan(
                topPx = marginTopPx,
                bottomPx = marginBottomPx
            ),
            start,
            end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
    }
    if (textView.text !== spannable) {
        textView.text = spannable
    }
}

private fun applyMediaLayout(textView: TextView): Boolean {
    val baseText = textView.text as? Spanned ?: return false
    if (baseText.isEmpty()) {
        return false
    }
    val spannable = if (baseText is SpannableStringBuilder) {
        baseText
    } else {
        SpannableStringBuilder(baseText)
    }
    spannable.getSpans(0, spannable.length, ReaderMediaBlockSpacingSpan::class.java).forEach(spannable::removeSpan)
    spannable.getSpans(0, spannable.length, ReaderPostMediaIndentSpan::class.java).forEach(spannable::removeSpan)

    val mediaSpans = spannable
        .getSpans(0, spannable.length, Any::class.java)
        .filter(::isMediaRenderSpan)
        .sortedByDescending { span -> spannable.getSpanStart(span) }
    if (mediaSpans.isEmpty()) {
        if (textView.text !== spannable) {
            textView.text = spannable
        }
        return false
    }

    val density = textView.resources.displayMetrics.density
    val mediaGapPx = (22f * density).roundToInt()
    val postIndentPx = (14f * density).roundToInt()

    mediaSpans.forEach { mediaSpan ->
        var start = spannable.getSpanStart(mediaSpan)
        var end = spannable.getSpanEnd(mediaSpan)
        if (start < 0 || end <= start) {
            return@forEach
        }
        if (start > 0 && spannable[start - 1] != '\n') {
            spannable.insert(start, "\n")
            start = spannable.getSpanStart(mediaSpan)
            end = spannable.getSpanEnd(mediaSpan)
        }
        if (end < spannable.length && spannable[end] != '\n') {
            spannable.insert(end, "\n")
            start = spannable.getSpanStart(mediaSpan)
            end = spannable.getSpanEnd(mediaSpan)
        }
        if (start < 0 || end <= start) {
            return@forEach
        }
        spannable.setSpan(
            ReaderMediaBlockSpacingSpan(
                topPx = mediaGapPx,
                bottomPx = mediaGapPx
            ),
            start,
            end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        applyPostMediaIndent(
            spannable = spannable,
            from = end,
            firstLineIndentPx = postIndentPx
        )
    }
    if (textView.text !== spannable) {
        textView.text = spannable
    }
    return true
}

private fun isMediaRenderSpan(span: Any): Boolean {
    val name = span.javaClass.name
    return name.contains("AsyncDrawable", ignoreCase = true) ||
        name.contains("ImageSpan", ignoreCase = true) ||
        name.contains("Video", ignoreCase = true)
}

private fun applyPostMediaIndent(
    spannable: SpannableStringBuilder,
    from: Int,
    firstLineIndentPx: Int
) {
    if (from >= spannable.length) {
        return
    }
    var index = from
    while (index < spannable.length && spannable[index] == '\n') {
        index++
    }
    if (index >= spannable.length) {
        return
    }
    val leadingChar = spannable[index]
    if (leadingChar == '#' || leadingChar == '>' || leadingChar == '-' || leadingChar == '*' || leadingChar == '+') {
        return
    }
    if (leadingChar.isDigit() && index + 1 < spannable.length && spannable[index + 1] == '.') {
        return
    }
    var lineEnd = index
    while (lineEnd < spannable.length && spannable[lineEnd] != '\n') {
        lineEnd++
    }
    if (lineEnd <= index) {
        return
    }
    spannable.setSpan(
        ReaderPostMediaIndentSpan(firstLineIndentPx),
        index,
        lineEnd,
        Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
    )
}

private fun applyMediaContainerInset(
    textView: TextView,
    hasMediaBlocks: Boolean,
    preferredWidthPx: Int
) {
    if (!hasMediaBlocks) {
        if (textView.paddingStart != 0 || textView.paddingEnd != 0) {
            textView.setPaddingRelative(0, textView.paddingTop, 0, textView.paddingBottom)
        }
        return
    }
    val width = if (textView.width > 0) textView.width else preferredWidthPx
    if (width <= 0) {
        return
    }
    val inset = (width * 0.05f).roundToInt()
    if (textView.paddingStart != inset || textView.paddingEnd != inset) {
        textView.setPaddingRelative(inset, textView.paddingTop, inset, textView.paddingBottom)
    }
}

private fun clearReaderOverlaySpans(spannable: SpannableStringBuilder) {
    spannable
        .getSpans(0, spannable.length, ReaderOverlaySpan::class.java)
        .forEach { span ->
            spannable.removeSpan(span)
        }
}

private fun containsMediaSpan(spanned: Spanned): Boolean {
    return spanned
        .getSpans(0, spanned.length, Any::class.java)
        .any { span ->
            span.javaClass.name.contains("AsyncDrawable", ignoreCase = true) ||
                span.javaClass.name.contains("Image", ignoreCase = true) ||
                span.javaClass.name.contains("Video", ignoreCase = true)
        }
}

private fun containsListLayoutSpan(spanned: Spanned): Boolean {
    return spanned
        .getSpans(0, spanned.length, Any::class.java)
        .any { span ->
            val name = span.javaClass.name
            name.contains("Bullet", ignoreCase = true) ||
                name.contains("ListItem", ignoreCase = true) ||
                name.contains("LeadingMargin", ignoreCase = true)
        }
}

private interface ReaderOverlaySpan

private class ReaderForegroundColorSpan(
    private val color: Int
) : CharacterStyle(), UpdateAppearance, ReaderOverlaySpan {
    override fun updateDrawState(tp: TextPaint) {
        tp.color = color
    }
}

private class ReaderBoldSpan : MetricAffectingSpan(), ReaderOverlaySpan {
    private fun applyBold(tp: TextPaint) {
        tp.typeface = Typeface.create(tp.typeface, Typeface.BOLD)
        tp.isFakeBoldText = true
    }

    override fun updateMeasureState(tp: TextPaint) {
        applyBold(tp)
    }

    override fun updateDrawState(tp: TextPaint) {
        applyBold(tp)
    }
}

private class ReaderRelativeSizeSpan(
    private val proportion: Float
) : MetricAffectingSpan(), ReaderOverlaySpan {
    override fun updateMeasureState(tp: TextPaint) {
        tp.textSize *= proportion
    }

    override fun updateDrawState(tp: TextPaint) {
        tp.textSize *= proportion
    }
}

private class ReaderLiftSpan(
    private val ratio: Float
) : MetricAffectingSpan(), ReaderOverlaySpan {
    override fun updateMeasureState(tp: TextPaint) {
        tp.baselineShift += -(tp.textSize * ratio).roundToInt()
    }

    override fun updateDrawState(tp: TextPaint) {
        tp.baselineShift += -(tp.textSize * ratio).roundToInt()
    }
}

private class ReaderLikedUnderlineSpan(
    private val color: Int,
    private val thicknessPx: Float
) : android.text.style.LineBackgroundSpan, ReaderOverlaySpan {
    override fun drawBackground(
        c: android.graphics.Canvas,
        p: android.graphics.Paint,
        left: Int,
        right: Int,
        top: Int,
        baseline: Int,
        bottom: Int,
        text: CharSequence,
        start: Int,
        end: Int,
        lnum: Int
    ) {
        val spanned = text as? Spanned ?: return
        val spanStart = max(start, spanned.getSpanStart(this))
        val spanEnd = min(end, spanned.getSpanEnd(this))
        if (spanStart < 0 || spanEnd <= spanStart) {
            return
        }
        val prefix = text.subSequence(start, spanStart).toString()
        val segment = text.subSequence(spanStart, spanEnd).toString()
        val startX = left + p.measureText(prefix)
        val endX = startX + p.measureText(segment)
        val oldColor = p.color
        val oldStrokeWidth = p.strokeWidth
        val oldStyle = p.style
        val oldPathEffect = p.pathEffect
        val dashEffect = android.graphics.DashPathEffect(floatArrayOf(6f, 4f), 0f)
        p.color = color
        p.strokeWidth = thicknessPx
        p.style = android.graphics.Paint.Style.STROKE
        p.pathEffect = dashEffect
        val y = baseline + thicknessPx
        c.drawLine(startX, y, endX, y, p)
        p.color = oldColor
        p.strokeWidth = oldStrokeWidth
        p.style = oldStyle
        p.pathEffect = oldPathEffect
    }
}

private class ReaderAnchorUnderlineSpan(
    private val color: Int,
    private val thicknessPx: Float,
    private val dashed: Boolean
) : android.text.style.LineBackgroundSpan, ReaderOverlaySpan {
    override fun drawBackground(
        c: android.graphics.Canvas,
        p: android.graphics.Paint,
        left: Int,
        right: Int,
        top: Int,
        baseline: Int,
        bottom: Int,
        text: CharSequence,
        start: Int,
        end: Int,
        lnum: Int
    ) {
        val spanned = text as? Spanned ?: return
        val spanStart = max(start, spanned.getSpanStart(this))
        val spanEnd = min(end, spanned.getSpanEnd(this))
        if (spanStart < 0 || spanEnd <= spanStart) {
            return
        }
        val prefix = text.subSequence(start, spanStart).toString()
        val segment = text.subSequence(spanStart, spanEnd).toString()
        val startX = left + p.measureText(prefix)
        val endX = startX + p.measureText(segment)
        val oldColor = p.color
        val oldStrokeWidth = p.strokeWidth
        val oldStyle = p.style
        val oldPathEffect = p.pathEffect
        p.color = color
        p.strokeWidth = thicknessPx
        p.style = android.graphics.Paint.Style.STROKE
        p.pathEffect = if (dashed) {
            android.graphics.DashPathEffect(floatArrayOf(7f, 4f), 0f)
        } else {
            null
        }
        val y = baseline + thicknessPx
        c.drawLine(startX, y, endX, y, p)
        p.color = oldColor
        p.strokeWidth = oldStrokeWidth
        p.style = oldStyle
        p.pathEffect = oldPathEffect
    }
}

private class ReaderAnchorMountedIndicatorSpan(
    private val color: Int
) : android.text.style.LineBackgroundSpan, ReaderOverlaySpan {
    override fun drawBackground(
        c: android.graphics.Canvas,
        p: android.graphics.Paint,
        left: Int,
        right: Int,
        top: Int,
        baseline: Int,
        bottom: Int,
        text: CharSequence,
        start: Int,
        end: Int,
        lnum: Int
    ) {
        val spanned = text as? Spanned ?: return
        val spanStart = max(start, spanned.getSpanStart(this))
        val spanEnd = min(end, spanned.getSpanEnd(this))
        if (spanStart < 0 || spanEnd <= spanStart) {
            return
        }
        val prefix = text.subSequence(start, spanStart).toString()
        val segment = text.subSequence(spanStart, spanEnd).toString()
        val startX = left + p.measureText(prefix)
        val endX = startX + p.measureText(segment)
        val centerX = (startX + endX) / 2f
        val radius = 2.4f
        val oldColor = p.color
        val oldStyle = p.style
        p.color = color
        p.style = android.graphics.Paint.Style.FILL
        c.drawCircle(centerX, baseline + 6f, radius, p)
        p.color = oldColor
        p.style = oldStyle
    }
}

private class ReaderWikilinkChipSpan(
    private val backgroundColor: Int
) : android.text.style.LineBackgroundSpan, ReaderOverlaySpan {
    override fun drawBackground(
        c: android.graphics.Canvas,
        p: android.graphics.Paint,
        left: Int,
        right: Int,
        top: Int,
        baseline: Int,
        bottom: Int,
        text: CharSequence,
        start: Int,
        end: Int,
        lnum: Int
    ) {
        val spanned = text as? Spanned ?: return
        val spanStart = max(start, spanned.getSpanStart(this))
        val spanEnd = min(end, spanned.getSpanEnd(this))
        if (spanStart < 0 || spanEnd <= spanStart) {
            return
        }
        val prefix = text.subSequence(start, spanStart).toString()
        val segment = text.subSequence(spanStart, spanEnd).toString()
        val startX = left + p.measureText(prefix)
        val endX = startX + p.measureText(segment)
        val horizontalPadding = 10f
        val verticalInset = 3f
        val rect = RectF(
            startX - horizontalPadding,
            top + verticalInset,
            endX + horizontalPadding,
            bottom - verticalInset
        )
        val oldColor = p.color
        val oldStyle = p.style
        p.color = backgroundColor
        p.style = android.graphics.Paint.Style.FILL
        c.drawRoundRect(rect, 11f, 11f, p)
        p.color = oldColor
        p.style = oldStyle
    }
}

private class ReaderAnnotationBubbleIndicatorSpan(
    private val color: Int
) : android.text.style.LineBackgroundSpan, ReaderOverlaySpan {
    override fun drawBackground(
        c: android.graphics.Canvas,
        p: android.graphics.Paint,
        left: Int,
        right: Int,
        top: Int,
        baseline: Int,
        bottom: Int,
        text: CharSequence,
        start: Int,
        end: Int,
        lnum: Int
    ) {
        val spanned = text as? Spanned ?: return
        val spanStart = max(start, spanned.getSpanStart(this))
        val spanEnd = min(end, spanned.getSpanEnd(this))
        if (spanStart < 0 || spanEnd <= spanStart) {
            return
        }
        val prefix = text.subSequence(start, spanStart).toString()
        val segment = text.subSequence(spanStart, spanEnd).toString()
        val startX = left + p.measureText(prefix)
        val endX = startX + p.measureText(segment)
        val centerX = (startX + endX) / 2f
        val radius = 2.2f
        val oldColor = p.color
        val oldStyle = p.style
        c.drawCircle(centerX, baseline + 6f, radius + 1f, p.apply {
            color = Color.White.toArgbSafe()
            style = android.graphics.Paint.Style.FILL
        })
        c.drawCircle(centerX, baseline + 6f, radius, p.apply {
            color = color
            style = android.graphics.Paint.Style.FILL
        })
        p.color = oldColor
        p.style = oldStyle
    }
}

private class ReaderParagraphLineHeightSpan(
    private val targetHeightPx: Int
) : android.text.style.LineHeightSpan {
    override fun chooseHeight(
        text: CharSequence,
        start: Int,
        end: Int,
        spanstartv: Int,
        v: Int,
        fm: android.graphics.Paint.FontMetricsInt
    ) {
        val originHeight = fm.descent - fm.ascent
        if (originHeight <= 0 || targetHeightPx <= 0) {
            return
        }
        val safeTarget = max(targetHeightPx, originHeight)
        if (safeTarget == originHeight) {
            return
        }
        val extra = safeTarget - originHeight
        val addBottom = extra / 2
        val addTop = extra - addBottom
        fm.descent += addBottom
        fm.ascent -= addTop
        fm.bottom = fm.descent
        fm.top = fm.ascent
    }
}

private class ReaderHeadingSpacingSpan(
    private val topPx: Int,
    private val bottomPx: Int
) : android.text.style.LineHeightSpan {
    override fun chooseHeight(
        text: CharSequence,
        start: Int,
        end: Int,
        spanstartv: Int,
        v: Int,
        fm: android.graphics.Paint.FontMetricsInt
    ) {
        val spanned = text as? Spanned ?: return
        val spanStart = spanned.getSpanStart(this)
        val spanEnd = spanned.getSpanEnd(this)
        if (spanStart < 0 || spanEnd < 0) {
            return
        }
        if (start == spanStart) {
            fm.ascent -= topPx
            fm.top -= topPx
        }
        if (end == spanEnd) {
            fm.descent += bottomPx
            fm.bottom += bottomPx
        }
    }
}

private class ReaderMediaBlockSpacingSpan(
    private val topPx: Int,
    private val bottomPx: Int
) : android.text.style.LineHeightSpan {
    override fun chooseHeight(
        text: CharSequence,
        start: Int,
        end: Int,
        spanstartv: Int,
        v: Int,
        fm: android.graphics.Paint.FontMetricsInt
    ) {
        val spanned = text as? Spanned ?: return
        val spanStart = spanned.getSpanStart(this)
        val spanEnd = spanned.getSpanEnd(this)
        if (spanStart < 0 || spanEnd < 0) {
            return
        }
        if (start == spanStart) {
            fm.ascent -= topPx
            fm.top -= topPx
        }
        if (end == spanEnd) {
            fm.descent += bottomPx
            fm.bottom += bottomPx
        }
    }
}

private class ReaderPostMediaIndentSpan(
    private val firstLineIndentPx: Int
) : android.text.style.LeadingMarginSpan {
    override fun getLeadingMargin(first: Boolean): Int {
        return if (first) firstLineIndentPx else 0
    }

    override fun drawLeadingMargin(
        c: android.graphics.Canvas,
        p: android.graphics.Paint,
        x: Int,
        dir: Int,
        top: Int,
        baseline: Int,
        bottom: Int,
        text: CharSequence,
        start: Int,
        end: Int,
        first: Boolean,
        layout: android.text.Layout
    ) {
    }
}

private data class InsightTermRange(
    val term: String,
    val start: Int,
    val end: Int
)

private data class TokenAnnotationItem(
    val token: String,
    val note: String
)

private data class InsightTapContext(
    val ranges: List<InsightTermRange>,
    val renderedMarkdown: String,
    val sourceText: String,
    val renderRefreshVersion: Int,
    val insightTermsFingerprint: Int,
    val selectionFingerprint: Int,
    val emphasisFingerprint: Int,
    val favoriteFingerprint: Int,
    val likedFingerprint: Int,
    val annotatedFingerprint: Int,
    val pendingAnchorFingerprint: Int,
    val mountedAnchorFingerprint: Int,
    val textStyleFingerprint: Int,
    val lineHeightPx: Int,
    val hasMediaBlocks: Boolean,
    val appliedWidthPx: Int
)

private data class InsightTermAnchor(
    val centerX: Float,
    val topY: Float,
    val bottomY: Float
)

private data class InsightTermTapPayload(
    val range: InsightTermRange,
    val anchor: InsightTermAnchor
)

private data class FloatingCardBubbleState(
    val nodeId: String,
    val token: String,
    val anchor: InsightTermAnchor
)

private data class ParagraphOverlayBounds(
    val left: Float,
    val top: Float,
    val right: Float,
    val bottom: Float
) {
    val centerX: Float
        get() = (left + right) / 2f
}

private fun ParagraphOverlayBounds.toOverlayAnchor(): InsightTermAnchor {
    return InsightTermAnchor(
        centerX = centerX,
        topY = top,
        bottomY = bottom
    )
}

private data class TokenAnnotationEditorState(
    val blockId: String,
    val selection: TokenSelection,
    val draft: String,
    val anchor: InsightTermAnchor?
)

private data class TokenAnnotationBubbleState(
    val blockId: String,
    val selection: TokenSelection,
    val text: String,
    val anchor: InsightTermAnchor?
)

private data class MountedAnchorDocument(
    val notePath: String,
    val displayTitle: String,
    val markdown: String,
    val rawMarkdown: String,
    val isGhost: Boolean,
    val ghostInputPath: String
)

private data class MountedWikilinkTap(
    val displayText: String,
    val targetNotePath: String,
    val isGhost: Boolean
)

private data class MountedWikilinkRewrite(
    val markdown: String,
    val linksByUrl: Map<String, MountedWikilinkTap>
)

private data class ParsedMountedWikilink(
    val targetPath: String,
    val displayText: String
)

private data class ResolvedMountedWikilink(
    val targetNotePath: String,
    val isGhost: Boolean
)

private data class MountedAnchorPreviewState(
    val anchorId: String,
    val blockId: String,
    val quote: String,
    val entryNotePath: String,
    val markdownPaths: List<String>,
    val stack: List<MountedAnchorDocument>,
    val isLoading: Boolean,
    val errorMessage: String?,
    val isFullscreen: Boolean
)

private enum class AnchorEditorMode {
    EDIT,
    PREVIEW
}

private data class AnchorEditorNoteTab(
    val localId: String,
    val title: String,
    val notePath: String,
    val markdown: String,
    val updatedAtMs: Long
)

private data class AnchorNoteEditorState(
    val anchorId: String,
    val blockId: String,
    val quote: String,
    val anchorHint: String,
    val notes: List<AnchorEditorNoteTab>,
    val activeNoteId: String,
    val shadowByPath: Map<String, String>,
    val pathHint: String,
    val mode: AnchorEditorMode,
    val isSaving: Boolean,
    val errorMessage: String?
)

private data class Phase2bFloatingCardState(
    val visible: Boolean,
    val loading: Boolean,
    val resultMarkdown: String,
    val errorMessage: String?
)

private fun resolveTappedInsightTerm(
    textView: TextView,
    cursor: Int
): InsightTermRange? {
    val context = textView.tag as? InsightTapContext ?: return null
    return context.ranges.firstOrNull { cursor in it.start until it.end }
}

private fun resolveInsightTermRanges(
    source: String,
    terms: List<String>
): List<InsightTermRange> {
    if (source.isBlank() || terms.isEmpty()) {
        return emptyList()
    }

    val normalizedTerms = terms
        .map { it.trim() }
        .filter { it.isNotBlank() }
        .distinct()
        .sortedByDescending { it.length }

    val allMatches = mutableListOf<InsightTermRange>()
    normalizedTerms.forEach { term ->
        var cursor = 0
        while (cursor < source.length) {
            val hit = source.indexOf(term, startIndex = cursor, ignoreCase = true)
            if (hit < 0) {
                break
            }
            val end = hit + term.length
            allMatches += InsightTermRange(
                term = source.substring(hit, end),
                start = hit,
                end = end
            )
            cursor = end
        }
    }

    if (allMatches.isEmpty()) {
        return emptyList()
    }

    val selected = mutableListOf<InsightTermRange>()
    allMatches
        .sortedWith(compareBy<InsightTermRange> { it.start }.thenByDescending { it.end - it.start })
        .forEach { candidate ->
            val overlap = selected.any { existing ->
                candidate.start < existing.end && candidate.end > existing.start
            }
            if (!overlap) {
                selected += candidate
            }
        }
    return selected.sortedBy { it.start }
}


/**
 * 计算术语高亮的锚点位置。
 * 当术语跨行时优先取首行可见区域，避免弹层锚点落在不可见位置。
 */
private fun resolveInsightTermAnchor(
    textView: TextView,
    range: InsightTermRange,
    rootWindowOffset: Offset
): InsightTermAnchor? {
    val layout = textView.layout ?: return null
    val textLength = textView.text?.length ?: 0
    if (textLength <= 0) {
        return null
    }
    val safeStart = range.start.coerceIn(0, textLength - 1)
    val safeEndExclusive = range.end.coerceIn(safeStart + 1, textLength)
    val safeEndOffset = (safeEndExclusive - 1).coerceIn(safeStart, textLength - 1)
    val startLine = layout.getLineForOffset(safeStart)
    val endLine = layout.getLineForOffset(safeEndOffset)
    var left = min(
        layout.getPrimaryHorizontal(safeStart),
        layout.getPrimaryHorizontal(safeEndOffset)
    )
    var right = max(
        layout.getPrimaryHorizontal(safeStart),
        layout.getPrimaryHorizontal(safeEndOffset)
    )
    if (startLine != endLine) {
        left = layout.getLineLeft(startLine)
        right = layout.getLineRight(startLine)
    }
    if (right - left < 1f) {
        val measuredWidth = textView.paint.measureText(range.term).coerceAtLeast(8f)
        right = left + measuredWidth
    }
    val centerXInView = ((left + right) / 2f) + textView.totalPaddingLeft - textView.scrollX
    val topYInView = layout.getLineTop(startLine).toFloat() + textView.totalPaddingTop - textView.scrollY
    val bottomYInView = layout.getLineBottom(endLine).toFloat() + textView.totalPaddingTop - textView.scrollY
    val location = IntArray(2)
    textView.getLocationInWindow(location)
    return InsightTermAnchor(
        centerX = location[0] + centerXInView - rootWindowOffset.x,
        topY = location[1] + topYInView - rootWindowOffset.y,
        bottomY = location[1] + bottomYInView - rootWindowOffset.y
    )
}

private fun resolveFallbackAnchor(
    textView: TextView,
    touchX: Float,
    touchY: Float,
    rootWindowOffset: Offset
): InsightTermAnchor {
    val location = IntArray(2)
    textView.getLocationInWindow(location)
    val globalX = location[0] + touchX - rootWindowOffset.x
    val globalY = location[1] + touchY - rootWindowOffset.y
    val halfLine = textView.textSize * 0.9f
    return InsightTermAnchor(
        centerX = globalX,
        topY = globalY - halfLine,
        bottomY = globalY + halfLine
    )
}

private fun resolveCursorOffset(
    textView: TextView,
    x: Float,
    y: Float
): Int? {
    val layout = textView.layout ?: return null
    val adjustedX = x - textView.totalPaddingLeft + textView.scrollX
    val adjustedY = y - textView.totalPaddingTop + textView.scrollY
    val line = layout.getLineForVertical(adjustedY.roundToInt())
    val offset = layout.getOffsetForHorizontal(line, adjustedX)
    return offset.coerceIn(
        minimumValue = 0,
        maximumValue = textView.text.length
    )
}

private fun resolveTappedInlineImage(
    textView: TextView,
    x: Float,
    y: Float
): InlineImageItem? {
    val spanned = textView.text as? Spanned ?: return null
    val cursor = resolveCursorOffset(textView, x, y) ?: return null
    val start = (cursor - 1).coerceAtLeast(0)
    val end = (cursor + 1).coerceAtMost(spanned.length)
    val spans = spanned.getSpans(start, end, Any::class.java)
    val imageUrl = spans.firstNotNullOfOrNull { span ->
        if (!isImageRenderSpan(span)) {
            return@firstNotNullOfOrNull null
        }
        resolveImageUrlFromSpan(span)
    } ?: return null
    return InlineImageItem(
        url = imageUrl,
        alt = deriveReaderMediaLabel(imageUrl)
    )
}

private fun isImageRenderSpan(span: Any): Boolean {
    val name = span.javaClass.name
    return (name.contains("AsyncDrawable", ignoreCase = true) ||
        name.contains("ImageSpan", ignoreCase = true) ||
        name.contains("Image", ignoreCase = true)) &&
        !name.contains("Video", ignoreCase = true)
}

private fun resolveImageUrlFromSpan(span: Any): String? {
    val directDestination = invokeZeroArgStringGetter(span, "getDestination")
    if (!directDestination.isNullOrBlank()) {
        return directDestination
    }
    val drawable = invokeZeroArgGetter(span, "getDrawable") ?: return null
    val drawableDestination = invokeZeroArgStringGetter(drawable, "getDestination")
    return drawableDestination?.takeIf { value -> value.isNotBlank() }
}

private fun invokeZeroArgGetter(target: Any, methodName: String): Any? {
    return runCatching {
        target.javaClass.methods
            .firstOrNull { method ->
                method.name == methodName && method.parameterCount == 0
            }
            ?.invoke(target)
    }.getOrNull()
}

private fun invokeZeroArgStringGetter(target: Any, methodName: String): String? {
    val raw = invokeZeroArgGetter(target, methodName) as? String
    val normalized = raw?.trim().orEmpty()
    return normalized.takeIf { value -> value.isNotBlank() }
}

private fun normalizeLexicalCursor(
    text: String,
    cursor: Int
): Int? {
    if (text.isEmpty()) {
        return null
    }
    return cursor.coerceIn(0, text.length - 1)
}

private fun clearNativeTextSelection(textView: TextView) {
    val text = textView.text as? Spannable ?: return
    Selection.removeSelection(text)
}

private fun resolveCurrentTextSelection(textView: TextView): TokenSelection? {
    val source = textView.text ?: return null
    if (source.isEmpty()) {
        return null
    }
    val start = textView.selectionStart
    val end = textView.selectionEnd
    if (start < 0 || end < 0 || start == end) {
        return null
    }
    val safeStart = min(start, end).coerceIn(0, source.length)
    val safeEnd = max(start, end).coerceIn(0, source.length)
    if (safeStart >= safeEnd) {
        return null
    }
    var normalizedStart = safeStart
    var normalizedEnd = safeEnd
    while (normalizedStart < normalizedEnd && source[normalizedStart].isWhitespace()) {
        normalizedStart++
    }
    while (normalizedEnd > normalizedStart && source[normalizedEnd - 1].isWhitespace()) {
        normalizedEnd--
    }
    if (normalizedStart >= normalizedEnd) {
        return null
    }
    val selectedText = source.subSequence(normalizedStart, normalizedEnd).toString()
    if (selectedText.isBlank()) {
        return null
    }
    return TokenSelection(
        token = selectedText,
        start = normalizedStart,
        end = normalizedEnd
    )
}

private fun buildSingleMarkdownReaderBlock(nodes: List<SemanticNode>): List<SemanticBlock> {
    if (nodes.isEmpty()) {
        return emptyList()
    }
    val mergedMarkdown = nodes
        .mapNotNull { node ->
            val source = (node.originalMarkdown ?: node.text)
                .replace("\r\n", "\n")
            source.takeIf { it.any { ch -> !ch.isWhitespace() } }
        }
        .joinToString("\n\n")
    if (mergedMarkdown.isBlank()) {
        return emptyList()
    }
    val mergedInsightTerms = nodes
        .flatMap { node -> node.resolvedInsightTerms() }
        .map { token -> token.trim() }
        .filter { token -> token.isNotBlank() }
        .distinct()
    val mergedReasoning = nodes
        .firstNotNullOfOrNull { node -> node.reasoning?.takeIf { it.isNotBlank() } }
    return listOf(
        SemanticBlock(
            blockId = "md_root",
            parentNodeId = "md_root",
            blockIndex = 0,
            blockCount = 1,
            markdown = mergedMarkdown,
            plainText = stripMarkdownToPlainText(mergedMarkdown),
            indentLevel = 0,
            type = "paragraph",
            relevanceScore = 1f,
            reasoning = mergedReasoning,
            insightTerms = mergedInsightTerms,
            insightsTags = emptyList()
        )
    )
}

private data class TokenMetaKey(
    val blockId: String,
    val start: Int,
    val end: Int
)

private data class ResolvedAnchorSelection(
    val selection: TokenSelection,
    val anchorData: MobileAnchorData
)

private fun buildTokenMetaKey(blockId: String, start: Int, end: Int): String {
    return "$blockId::$start::$end"
}

private fun buildAnchorContextQuoteSnapshot(
    blockText: String,
    start: Int,
    end: Int,
    contextRadius: Int = 40
): String {
    if (blockText.isBlank()) {
        return ""
    }
    val source = blockText
    val safeStart = start.coerceIn(0, source.length)
    val safeEnd = end.coerceIn(safeStart, source.length)
    if (safeEnd <= safeStart) {
        return source.replace(Regex("\\s+"), " ").trim().take(120)
    }
    val leftStart = (safeStart - contextRadius).coerceAtLeast(0)
    val rightEnd = (safeEnd + contextRadius).coerceAtMost(source.length)
    val prefix = if (leftStart > 0) "..." else ""
    val suffix = if (rightEnd < source.length) "..." else ""
    val left = source.substring(leftStart, safeStart)
    val middle = source.substring(safeStart, safeEnd)
    val right = source.substring(safeEnd, rightEnd)
    return "$prefix$left【$middle】$right$suffix"
        .replace(Regex("\\s+"), " ")
        .trim()
}

private fun parseTokenMetaKey(rawKey: String): TokenMetaKey? {
    val last = rawKey.lastIndexOf("::")
    if (last <= 0 || last >= rawKey.length - 2) {
        return null
    }
    val middle = rawKey.lastIndexOf("::", last - 1)
    if (middle <= 0 || middle >= last - 2) {
        return null
    }
    val blockId = rawKey.substring(0, middle).trim()
    if (blockId.isEmpty()) {
        return null
    }
    val start = rawKey.substring(middle + 2, last).toIntOrNull() ?: return null
    val end = rawKey.substring(last + 2).toIntOrNull() ?: return null
    if (start < 0 || end <= start) {
        return null
    }
    return TokenMetaKey(
        blockId = blockId,
        start = start,
        end = end
    )
}

private fun parseRangeKey(rawKey: String): Pair<Int, Int>? {
    val parts = rawKey.split(':')
    if (parts.size != 2) {
        return null
    }
    val start = parts[0].toIntOrNull() ?: return null
    val end = parts[1].toIntOrNull() ?: return null
    if (start < 0 || end <= start) {
        return null
    }
    return start to end
}

private fun resolveAnchorSelectionForDisplay(
    rangeKey: String,
    anchorData: MobileAnchorData,
    source: String
): TokenSelection? {
    val parsedRange = parseRangeKey(rangeKey)
    val fallbackStart = if (anchorData.startIndex >= 0) anchorData.startIndex else 0
    val fallbackEnd = if (anchorData.endIndex > fallbackStart) anchorData.endIndex else fallbackStart
    val start = parsedRange?.first ?: fallbackStart
    val end = parsedRange?.second ?: fallbackEnd
    if (start >= 0 && end > start) {
        if (source.isNotBlank() && end <= source.length) {
            val selected = source.substring(start, end)
            if (selected.isNotBlank()) {
                return TokenSelection(
                    token = selected,
                    start = start,
                    end = end
                )
            }
        }
        val fallbackToken = anchorData.quote.trim().ifBlank { "anchor" }
        return TokenSelection(
            token = fallbackToken,
            start = start,
            end = end
        )
    }
    if (source.isBlank()) {
        return null
    }
    val quote = anchorData.quote.trim()
    if (quote.isBlank()) {
        return null
    }
    return resolveSelectionByQuoteWithHint(
        source = source,
        quote = quote,
        hintStart = start
    )
}

private fun resolveSelectionByQuoteWithHint(
    source: String,
    quote: String,
    hintStart: Int
): TokenSelection? {
    if (source.isBlank() || quote.isBlank()) {
        return null
    }
    val candidates = mutableListOf<Pair<Int, Int>>()
    var cursor = 0
    while (cursor < source.length) {
        val hit = source.indexOf(quote, startIndex = cursor, ignoreCase = true)
        if (hit < 0) {
            break
        }
        candidates += hit to (hit + quote.length)
        cursor = hit + 1
    }
    if (candidates.isEmpty()) {
        return null
    }
    val safeHint = hintStart.coerceAtLeast(0)
    val selected = candidates.minByOrNull { (start, _) ->
        kotlin.math.abs(start - safeHint)
    } ?: return null
    if (selected.second <= selected.first || selected.second > source.length) {
        return null
    }
    return TokenSelection(
        token = source.substring(selected.first, selected.second),
        start = selected.first,
        end = selected.second
    )
}

private fun groupLikedTokenKeysByBlock(
    tokenLikeState: Map<String, Boolean>
): Map<String, Set<String>> {
    if (tokenLikeState.isEmpty()) {
        return emptyMap()
    }
    val grouped = mutableMapOf<String, MutableSet<String>>()
    tokenLikeState.forEach { (metaKey, liked) ->
        if (!liked) {
            return@forEach
        }
        val parsed = parseTokenMetaKey(metaKey) ?: return@forEach
        val byBlock = grouped.getOrPut(parsed.blockId) { linkedSetOf() }
        byBlock += "${parsed.start}:${parsed.end}"
    }
    return grouped
}

private fun groupTokenAnnotationsByBlock(
    tokenAnnotationsState: Map<String, String>
): Map<String, Map<String, String>> {
    if (tokenAnnotationsState.isEmpty()) {
        return emptyMap()
    }
    val grouped = mutableMapOf<String, MutableMap<String, String>>()
    tokenAnnotationsState.forEach { (metaKey, value) ->
        val parsed = parseTokenMetaKey(metaKey) ?: return@forEach
        val normalized = value.trim()
        if (normalized.isEmpty()) {
            return@forEach
        }
        val byBlock = grouped.getOrPut(parsed.blockId) { linkedMapOf() }
        byBlock["${parsed.start}:${parsed.end}"] = normalized
    }
    return grouped
}

private fun groupAnchorsByBlock(
    anchorsState: Map<String, MobileAnchorData>
): Map<String, Map<String, MobileAnchorData>> {
    if (anchorsState.isEmpty()) {
        return emptyMap()
    }
    val grouped = mutableMapOf<String, MutableMap<String, MobileAnchorData>>()
    anchorsState.forEach { (metaKey, data) ->
        val parsed = parseTokenMetaKey(metaKey)
        val blockId = parsed?.blockId ?: data.blockId.trim()
        val start = parsed?.start ?: data.startIndex
        val end = parsed?.end ?: data.endIndex
        if (blockId.isBlank() || start < 0 || end <= start) {
            return@forEach
        }
        val byBlock = grouped.getOrPut(blockId) { linkedMapOf() }
        byBlock["$start:$end"] = data.copy(
            blockId = blockId,
            startIndex = start,
            endIndex = end
        )
    }
    return grouped
}

private class ParagraphBoundsRef(
    var value: ParagraphOverlayBounds? = null
)
