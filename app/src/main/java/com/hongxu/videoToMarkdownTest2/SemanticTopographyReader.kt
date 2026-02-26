package com.hongxu.videoToMarkdownTest2
import android.annotation.SuppressLint
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.graphics.Typeface
import android.os.Build
import android.os.SystemClock
import android.text.Layout
import android.text.Selection
import android.view.HapticFeedbackConstants
import android.util.LruCache
import android.text.Spannable
import android.text.SpannableStringBuilder
import android.text.Spanned
import android.text.TextPaint
import android.text.style.CharacterStyle
import android.text.style.MetricAffectingSpan
import android.text.style.StyleSpan
import android.text.style.UpdateAppearance
import android.view.GestureDetector
import android.view.MotionEvent
import android.view.ViewConfiguration
import android.widget.Toast
import android.widget.TextView
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
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.gestures.detectTransformGestures
import androidx.compose.foundation.gestures.detectTapGestures
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
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
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
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
import androidx.compose.ui.graphics.StrokeCap
import androidx.compose.ui.graphics.SolidColor
import androidx.compose.ui.graphics.StrokeJoin
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.hapticfeedback.HapticFeedbackType
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.SubcomposeLayout
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.layout.onSizeChanged
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
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.Constraints
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.IntOffset
import androidx.compose.ui.unit.IntSize
import androidx.compose.ui.unit.sp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.graphics.vector.path
import androidx.compose.ui.window.Popup
import androidx.compose.ui.window.PopupProperties
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
fun SemanticTopographyReader(
    nodes: List<SemanticNode>,
    markwon: Markwon,
    renderConfig: MarkdownReaderRenderConfig,
    modifier: Modifier = Modifier,
    taskId: String? = null,
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
    onReadingPositionChanged: (Int, Int) -> Unit = { _, _ -> },
    onGestureEvent: (ParagraphGestureEvent) -> Unit = {},
    onTelemetry: (ReaderTelemetryEvent) -> Unit = {}
) {
    val listState = rememberLazyListState(
        initialFirstVisibleItemIndex = initialFirstVisibleItemIndex.coerceAtLeast(0),
        initialFirstVisibleItemScrollOffset = initialFirstVisibleItemScrollOffset.coerceAtLeast(0)
    )
    val scope = rememberCoroutineScope()
    val lifecycleOwner = LocalLifecycleOwner.current

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
                        tokenAnnotations = tokenAnnotationsSnapshot
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
                            "tokenAnnotationCount" to tokenAnnotationsSnapshot.size.toString()
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

    LaunchedEffect(taskId, pathHint, metaApi) {
        if (taskId.isNullOrBlank() || metaApi == null) {
            favoritesState.clear()
            commentsState.clear()
            deletedState.clear()
            tokenAnnotationsState.clear()
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
                        "tokenAnnotationCount" to payload.tokenAnnotations.size.toString()
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
    val activeParagraphBoundsBlockId = tokenAnnotationEditorState?.blockId ?: tokenAnnotationBubbleState?.blockId

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
        val blocks = remember(nodes) { splitSemanticNodesIntoBlocks(nodes) }

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
                val shouldTrackParagraphBounds = activeParagraphBoundsBlockId == block.blockId
                TopographyParagraph(
                    block = block,
                    index = index,
                    listState = listState,
                    markwon = markwon,
                    renderConfig = renderConfig,
                    selection = tokenSelections[block.blockId],
                    overlayRootWindowOffset = overlayRootWindowOffset,
                    isFavorited = favoritesState[block.blockId] == true,
                    isMarkedDeleted = deletedState[block.blockId] == true,
                    existingComments = commentsState[block.blockId].orEmpty(),
                    likedTokenKeys = emptySet(),
                    tokenAnnotations = blockTokenAnnotations,
                    isCommentPanelExpanded = activeCommentBlockId == block.blockId,
                    shouldTrackParagraphBounds = shouldTrackParagraphBounds,
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
                    onMarkDeleted = {
                        deletedState[block.blockId] = true
                        scheduleMetaSync(reason = "mark_deleted")
                        onMarkDeleted(block.blockId)
                    },
                    onRestoreDeleted = {
                        deletedState.remove(block.blockId)
                        scheduleMetaSync(reason = "restore_deleted")
                    },
                    onResonance = {
                        if (favoritesState[block.blockId] == true) {
                            favoritesState.remove(block.blockId)
                        } else {
                            favoritesState[block.blockId] = true
                        }
                        scheduleMetaSync(reason = "resonance_toggle")
                        onResonance(block.blockId)
                    },
                    onCommentCommitted = { comment ->
                        val merged = (commentsState[block.blockId].orEmpty() + comment)
                            .filter { it.isNotBlank() }
                            .takeLast(30)
                        if (merged.isEmpty()) {
                            commentsState.remove(block.blockId)
                        } else {
                            commentsState[block.blockId] = merged
                        }
                        scheduleMetaSync(reason = "comment")
                    },
                    onRequestOpenCommentPanel = { source ->
                        activeCommentBlockId = block.blockId
                        val commentPanelCenterRatio = when (source) {
                            "selection_action_annotate" -> 0.2f
                            "swipe_right" -> 0.2f
                            else -> 0.22f
                        }
                        scope.launch {
                            delay(220) // 等面板展开动画完成后再滚动定位
                            autoCenterItem(
                                listState = listState,
                                itemIndex = index,
                                centerRatio = commentPanelCenterRatio
                            )
                        }
                        emitTelemetry(
                            ReaderTelemetryEvent(
                                nodeId = block.blockId,
                                eventType = "comment_panel_opened",
                                relevanceScore = block.relevanceScore,
                                payload = mapOf("source" to source)
                            )
                        )
                    },
                    onRequestCloseCommentPanel = { source ->
                        if (activeCommentBlockId == block.blockId) {
                            activeCommentBlockId = null
                            emitTelemetry(
                                ReaderTelemetryEvent(
                                    nodeId = block.blockId,
                                    eventType = "comment_panel_closed",
                                    relevanceScore = block.relevanceScore,
                                    payload = mapOf("source" to source)
                                )
                            )
                        }
                    },
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
    }
}

/**
 * 单段语义块渲染器，负责：
 * 1. 处理段落级交互（选中、收藏、批注、删除）。
 * 2. 触发段落级手势和埋点上报。
 * 3. 在当前段内渲染 markdown 与强调样式。
 */
@Composable
private fun TopographyParagraph(
    block: SemanticBlock,
    index: Int,
    listState: LazyListState,
    markwon: Markwon,
    renderConfig: MarkdownReaderRenderConfig,
    selection: TokenSelection?,
    overlayRootWindowOffset: Offset,
    isFavorited: Boolean,
    isMarkedDeleted: Boolean,
    existingComments: List<String>,
    likedTokenKeys: Set<String>,
    tokenAnnotations: Map<String, String>,
    isCommentPanelExpanded: Boolean,
    shouldTrackParagraphBounds: Boolean,
    onParagraphBoundsChanged: (ParagraphOverlayBounds?) -> Unit,
    onSelectionChanged: (TokenSelection?) -> Unit,
    onInsightTermTapped: (InsightTermTapPayload) -> Unit,
    onToggleTokenLike: (TokenSelection, Boolean) -> Unit,
    onUpsertTokenAnnotation: (TokenSelection, String) -> Unit,
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
    val readerMarkdown = remember(block.markdown) {
        block.markdown
    }
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

    val indentPaddingStart = (block.indentLevel * renderConfig.spacingIndentLevelDp).dp

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
            ParagraphSwipeBackdrop(
                deleteRevealProgress = deleteRevealProgress,
                annotateRevealProgress = annotateRevealProgress,
                hasComments = existingComments.isNotEmpty(),
                isMarkedDeleted = isMarkedDeleted,
                isCommentPanelExpanded = isCommentPanelExpanded
            )
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
                        .pointerInput(block.blockId, deleteRevealLimit, annotateRevealLimit, isMarkedDeleted) {
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
                    Box(modifier = Modifier.padding(top = headingInnerTopPadding)) {
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
                                        markdown = readerMarkdown,
                                        plainText = readerText,
                                        markwon = markwon,
                                        renderConfig = renderConfig,
                                        renderRefreshVersion = textRenderRefreshVersion,
                                        textSizeSp = textSize.value,
                                        lineSpacingMultiplier = lineSpacingMultiplier,
                                        textColor = textColor,
                                        fontWeight = if (useNoiseCapsule) FontWeight.Normal else finalFontWeight,
                                        selection = selection,
                                        isFavorited = isFavorited,
                                        overlayRootWindowOffset = overlayRootWindowOffset,
                                        insightTerms = resolvedInsightTerms,
                                        emphasizedSelections = emphasizedSelections,
                                        likedSelections = likedSelections,
                                        annotatedSelections = annotatedSelections,
                                        modifier = Modifier.fillMaxWidth(),
                                        onSelectionAction = { action, selected, anchor ->
                                            onSelectionChanged(selected)
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
                                                    val annotationAnchor = paragraphBoundsForOverlayRef.value
                                                        ?.toOverlayAnchor()
                                                        ?: anchor
                                                    scope.launch {
                                                        autoCenterItem(
                                                            listState = listState,
                                                            itemIndex = index,
                                                            centerRatio = 0.24f
                                                        )
                                                        onRequestOpenTokenAnnotationEditor(selected, annotationAnchor)
                                                    }
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
                                            }
                                        },
                                        onTokenSingleTap = { cursor, anchor ->
                                            val normalizedCursor = normalizeLexicalCursor(
                                                text = readerText,
                                                cursor = cursor
                                            )
                                            val selection = if (normalizedCursor == null) {
                                                null
                                            } else {
                                                resolveTokenSelection(
                                                    text = readerText,
                                                    cursor = normalizedCursor,
                                                    nativePayload = runCatching {
                                                        LexicalNativeBridge.segmentAt(readerText, normalizedCursor)
                                                    }.getOrNull()
                                                )
                                            }
                                            if (selection != null) {
                                                val note = tokenAnnotations[rangeKey(selection)].orEmpty().trim()
                                                if (note.isNotBlank()) {
                                                    val annotationAnchor = paragraphBoundsForOverlayRef.value
                                                        ?.toOverlayAnchor()
                                                        ?: anchor
                                                    scope.launch {
                                                        autoCenterItem(
                                                            listState = listState,
                                                            itemIndex = index,
                                                            centerRatio = 0.28f
                                                        )
                                                        onRequestOpenTokenAnnotationBubble(selection, annotationAnchor)
                                                    }
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
                                            } else if (existingComments.isNotEmpty()) {
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
                                        onParagraphDoubleTap = { tapOffset ->
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
                    if (isMarkedDeleted) {
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

                    if (existingComments.isNotEmpty()) {
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
                visible = isCommentPanelExpanded,
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
        val foregroundPlaceables = subcompose("foreground", foreground).map {
            it.measure(constraints)
        }

        val layoutWidth = (foregroundPlaceables.maxOfOrNull { it.width } ?: 0)
            .coerceIn(constraints.minWidth, constraints.maxWidth)
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
    } ?: return

    Box(
        modifier = Modifier
            .fillMaxSize()
            .onSizeChanged { viewportSize = it }
    ) {
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
    val widthPx = 280f
    val heightPx = 52f
    val marginPx = 12f
    val fallbackCenterX = (viewportSize.width.toFloat() * 0.5f).coerceAtLeast(marginPx + widthPx / 2f)
    val targetCenterX = paragraphBounds?.centerX ?: anchor?.centerX ?: fallbackCenterX
    val left = (targetCenterX - widthPx / 2f).coerceIn(
        minimumValue = marginPx,
        maximumValue = (viewportSize.width.toFloat() - widthPx - marginPx).coerceAtLeast(marginPx)
    )
    val targetBottomY = paragraphBounds?.bottom ?: anchor?.bottomY ?: marginPx
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
    val targetCenterX = paragraphBounds?.centerX ?: anchor?.centerX ?: fallbackCenterX
    val left = (targetCenterX - widthPx / 2f).coerceIn(
        minimumValue = marginPx,
        maximumValue = (viewportSize.width.toFloat() - widthPx - marginPx).coerceAtLeast(marginPx)
    )
    val targetBottomY = paragraphBounds?.bottom ?: anchor?.bottomY ?: marginPx
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
        textView.isFallbackLineSpacing = false
    }
}

private val MARKDOWN_LIST_LINE_PATTERN = Regex("(?m)^\\s*(?:[-*+]\\s+|\\d+[\\.)]\\s+)")
private const val DISABLE_TEXT_IS_NOISE_JUDGMENT = true
private const val MARKDOWN_CACHE_MAX_ENTRIES = 180
private const val MARKDOWN_CACHE_MAX_TEXT_LENGTH = 12_000
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
    selection: TokenSelection?,
    isFavorited: Boolean,
    overlayRootWindowOffset: Offset,
    insightTerms: List<String>,
    emphasizedSelections: List<TokenSelection>,
    likedSelections: List<TokenSelection>,
    annotatedSelections: List<TokenSelection>,
    onSelectionAction: (SelectionContextAction, TokenSelection, InsightTermAnchor?) -> Unit,
    onTokenSingleTap: (cursor: Int, anchor: InsightTermAnchor?) -> Unit,
    onInsightTermTap: (tap: InsightTermTapPayload) -> Unit,
    onParagraphDoubleTap: (offset: Offset) -> Unit,
    onSelectionModeChanged: (Boolean) -> Unit,
    modifier: Modifier = Modifier
) {
    val latestSelectionAction = androidx.compose.runtime.rememberUpdatedState(onSelectionAction)
    val latestOverlayRootOffset = androidx.compose.runtime.rememberUpdatedState(overlayRootWindowOffset)
    val latestSelectionModeChanged = androidx.compose.runtime.rememberUpdatedState(onSelectionModeChanged)
    AndroidView(
        modifier = modifier,
        factory = { context ->
            val textView = TextView(context).apply {
                applyReaderTextLayoutPolicy(this)
                setTextIsSelectable(true)
                applyReaderParagraphLineSpacing(this, lineSpacingMultiplier)
                letterSpacing = renderConfig.textLetterSpacing
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
                        val anchor = resolveFallbackAnchor(
                            textView = textView,
                            touchX = e.x,
                            touchY = e.y,
                            rootWindowOffset = latestOverlayRootOffset.value
                        )
                        onTokenSingleTap(cursor, anchor)
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
            val previousContext = textView.tag as? InsightTapContext
            val refreshChanged = previousContext?.renderRefreshVersion != renderRefreshVersion
            val markdownChanged = refreshChanged || previousContext?.renderedMarkdown != markdown || textView.text.isNullOrEmpty()
            val widthChanged = previousContext?.appliedWidthPx != textView.width
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
            val insightRanges = if (!sourceChanged && !termsChanged) {
                previousContext.ranges
            } else {
                resolveInsightTermRanges(
                    source = source,
                    terms = insightTerms
                )
            }
            if (markdownChanged || sourceChanged || termsChanged || selectionChanged || emphasisChanged || favoriteChanged || likedChanged || annotationChanged) {
                applySelectionStyle(
                    textView = textView,
                    selection = selection,
                    isFavorited = isFavorited,
                    insightRanges = insightRanges,
                    emphasizedSelections = emphasizedSelections,
                    likedSelections = likedSelections,
                    annotatedSelections = annotatedSelections
                )
            }
            if (markdownChanged || widthChanged || previousContext?.hasMediaBlocks != hasMediaBlocks) {
                applyMediaContainerInset(textView, hasMediaBlocks)
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
                textStyleFingerprint = textStyleFingerprint,
                lineHeightPx = targetLineHeightPx,
                hasMediaBlocks = hasMediaBlocks,
                appliedWidthPx = textView.width
            )
        }
    )
}

private enum class SelectionContextAction {
    Copy,
    ToggleLike,
    ToggleBold,
    Annotate,
    SearchCard
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
    annotatedSelections: List<TokenSelection>
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
    val hasOverlay = insightRanges.isNotEmpty() || hasSelectionOverlay || hasManualEmphasisOverlay || hasFavoriteOverlay || hasLikedOverlay || hasAnnotationOverlay
    val hasMediaSpan = (baseText as? Spanned)?.let(::containsMediaSpan) == true
    val spannable = if (baseText is SpannableStringBuilder) {
        baseText
    } else {
        SpannableStringBuilder(baseText)
    }
    val hasExistingOverlay = spannable
        .getSpans(0, spannable.length, ReaderOverlaySpan::class.java)
        .isNotEmpty()
    if (!hasOverlay || hasMediaSpan) {
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

    val safeInsightRanges = insightRanges.filter { range ->
        range.start >= 0 &&
            range.end <= source.length &&
            range.start < range.end
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

private fun applyMediaContainerInset(textView: TextView, hasMediaBlocks: Boolean) {
    if (!hasMediaBlocks) {
        if (textView.paddingStart != 0 || textView.paddingEnd != 0) {
            textView.setPaddingRelative(0, textView.paddingTop, 0, textView.paddingBottom)
        }
        return
    }
    fun applyInset() {
        val width = textView.width
        if (width <= 0) {
            return
        }
        val inset = (width * 0.05f).roundToInt()
        if (textView.paddingStart != inset || textView.paddingEnd != inset) {
            textView.setPaddingRelative(inset, textView.paddingTop, inset, textView.paddingBottom)
        }
    }
    if (textView.width <= 0) {
        textView.post { applyInset() }
    } else {
        applyInset()
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
        if (originHeight <= 0 || targetHeightPx <= 0 || originHeight == targetHeightPx) {
            return
        }
        val ratio = targetHeightPx.toFloat() / originHeight.toFloat()
        fm.descent = (fm.descent * ratio).roundToInt()
        fm.ascent = fm.descent - targetHeightPx
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

private data class TokenMetaKey(
    val blockId: String,
    val start: Int,
    val end: Int
)

private fun buildTokenMetaKey(blockId: String, start: Int, end: Int): String {
    return "$blockId::$start::$end"
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

private class ParagraphBoundsRef(
    var value: ParagraphOverlayBounds? = null
)
