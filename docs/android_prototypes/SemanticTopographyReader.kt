package com.example.semantictopography

import android.annotation.SuppressLint
import android.graphics.Typeface
import android.text.Spannable
import android.text.SpannableString
import android.text.style.BackgroundColorSpan
import android.text.style.ForegroundColorSpan
import android.text.style.RelativeSizeSpan
import android.text.style.UnderlineSpan
import android.view.GestureDetector
import android.view.MotionEvent
import android.widget.TextView
import androidx.compose.animation.core.Animatable
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.Spring
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.animation.core.spring
import androidx.compose.animation.core.tween
import androidx.compose.foundation.background
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.gestures.detectTapGestures
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyListState
import androidx.compose.foundation.lazy.itemsIndexed
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.hapticfeedback.HapticFeedbackType
import androidx.compose.ui.input.pointer.consume
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.SubcomposeLayout
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.platform.LocalHapticFeedback
import androidx.compose.ui.platform.LocalLifecycleOwner
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.ui.viewinterop.AndroidView
import io.noties.markwon.Markwon
import kotlinx.coroutines.launch
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt

/**
 * 语义地形阅读器主容器。
 *
 * 新增对后端 meta API 的接入能力：
 * 1. 双击共鸣会映射为 favorites[nodeId] = true。
 * 2. 右滑批注保存会写入 comments[nodeId]。
 * 3. 使用 /api/mobile/tasks/{taskId}/meta 读写。
 */
@Composable
fun SemanticTopographyReader(
    nodes: List<SemanticNode>,
    markwon: Markwon,
    modifier: Modifier = Modifier,
    taskId: String? = null,
    pathHint: String? = null,
    metaApi: MobileMarkdownMetaApi? = null,
    telemetryApi: MobileMarkdownTelemetryApi? = null,
    onMarkDeleted: (String) -> Unit = {},
    onBridgeOpen: (String) -> Unit = {},
    onResonance: (String) -> Unit = {},
    onGestureEvent: (ParagraphGestureEvent) -> Unit = {},
    onTelemetry: (ReaderTelemetryEvent) -> Unit = {}
) {
    val listState = rememberLazyListState()
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
        scope.launch {
            runCatching {
                metaApi.updateTaskMeta(
                    taskId = taskId,
                    request = MobileTaskMetaUpdateRequest(
                        path = pathHint,
                        taskTitle = null,
                        favorites = favoriteSnapshot,
                        deleted = deletedSnapshot,
                        comments = commentsSnapshot
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
                            "commentsCount" to commentsSnapshot.size.toString()
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
                        "commentsCount" to payload.comments.size.toString()
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

    LazyColumn(
        state = listState,
        modifier = modifier
            .fillMaxSize()
            .background(Color(0xFFFCFCFC)),
        contentPadding = PaddingValues(vertical = 24.dp),
        verticalArrangement = Arrangement.spacedBy(18.dp)
    ) {
        itemsIndexed(
            items = nodes,
            key = { _, node -> node.id }
        ) { index, node ->
            TopographyParagraph(
                node = node,
                index = index,
                listState = listState,
                markwon = markwon,
                isFavorited = favoritesState[node.id] == true,
                isMarkedDeleted = deletedState[node.id] == true,
                existingComments = commentsState[node.id].orEmpty(),
                onMarkDeleted = {
                    deletedState[node.id] = true
                    scheduleMetaSync(reason = "mark_deleted")
                    onMarkDeleted(node.id)
                },
                onBridgeOpen = onBridgeOpen,
                onResonance = {
                    favoritesState[node.id] = true
                    scheduleMetaSync(reason = "resonance")
                    onResonance(node.id)
                },
                onCommentCommitted = { comment ->
                    val merged = (commentsState[node.id].orEmpty() + comment)
                        .filter { it.isNotBlank() }
                        .takeLast(30)
                    if (merged.isEmpty()) {
                        commentsState.remove(node.id)
                    } else {
                        commentsState[node.id] = merged
                    }
                    scheduleMetaSync(reason = "comment")
                },
                onGestureEvent = onGestureEvent,
                onTelemetry = ::emitTelemetry
            )
        }
    }
}

/**
 * 段落组件，承载段落手势主通道和词句手术刀入口。
 */
@Composable
private fun TopographyParagraph(
    node: SemanticNode,
    index: Int,
    listState: LazyListState,
    markwon: Markwon,
    isFavorited: Boolean,
    isMarkedDeleted: Boolean,
    existingComments: List<String>,
    onMarkDeleted: () -> Unit,
    onBridgeOpen: (String) -> Unit,
    onResonance: () -> Unit,
    onCommentCommitted: (String) -> Unit,
    onGestureEvent: (ParagraphGestureEvent) -> Unit,
    onTelemetry: (ReaderTelemetryEvent) -> Unit
) {
    val scope = rememberCoroutineScope()
    val haptic = LocalHapticFeedback.current
    val offsetX = remember(node.id) {
        Animatable(0f)
    }
    val resonanceScale = remember(node.id) {
        Animatable(1f)
    }

    var paragraphWidthPx by remember(node.id) {
        mutableIntStateOf(1)
    }
    var isBridgeExpanded by remember(node.id) {
        mutableStateOf(false)
    }
    var isNoteExpanded by remember(node.id) {
        mutableStateOf(false)
    }
    var noteDraft by remember(node.id) {
        mutableStateOf("")
    }
    var tokenSelection by remember(node.id) {
        mutableStateOf<TokenSelection?>(null)
    }
    var tokenCard by remember(node.id) {
        mutableStateOf<TokenInsightCard?>(null)
    }

    val hasBridge = !node.bridgeText.isNullOrBlank()
    val isFocused = node.relevanceScore > 0.7f
    val isNoise = node.relevanceScore < 0.3f
    val textSize = if (isFocused) 18.sp else if (isNoise) 14.sp else 16.sp
    val textColor = if (isNoise) Color(0xFF8E8E8E) else Color(0xFF212121)
    val fontWeight = if (isFocused) FontWeight.Bold else FontWeight.Normal

    val rightThreshold = max(140f, paragraphWidthPx * 0.24f)
    val leftThreshold = max(140f, paragraphWidthPx * 0.24f)
    val rightLockOffset = min(paragraphWidthPx * 0.36f, 260f)

    val bridgeRevealProgress = (offsetX.value / rightLockOffset).coerceIn(0f, 1f)
    val breathingAlpha by rememberInfiniteTransition(label = "noise-bridge-breathing").animateFloat(
        initialValue = 0.35f,
        targetValue = 0.95f,
        animationSpec = infiniteRepeatable(
            animation = tween(durationMillis = 1200),
            repeatMode = RepeatMode.Reverse
        ),
        label = "noise-bridge-breathing-alpha"
    )

    SubcomposeAnchorLayout(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 18.dp)
            .onSizeChanged { paragraphWidthPx = max(1, it.width) },
        background = {
            if (hasBridge) {
                BridgeBubble(
                    text = node.bridgeText.orEmpty(),
                    revealProgress = if (isBridgeExpanded) 1f else bridgeRevealProgress
                )
            }
        },
        foregroundOffsetX = offsetX.value.roundToInt(),
        foreground = {
            Surface(
                color = Color(0xFFFCFCFC),
                modifier = Modifier
                    .fillMaxWidth()
                    .clip(RoundedCornerShape(14.dp))
                    .alpha(if (isMarkedDeleted) 0.6f else 1f)
                    .pointerInput(node.id) {
                        detectHorizontalDragGestures(
                            onDragStart = {
                                onTelemetry(
                                    ReaderTelemetryEvent(
                                        nodeId = node.id,
                                        eventType = "paragraph_swipe_start",
                                        relevanceScore = node.relevanceScore,
                                        payload = mapOf(
                                            "offsetX" to offsetX.value.toString()
                                        )
                                    )
                                )
                            },
                            onDragCancel = {
                                scope.launch {
                                    offsetX.animateTo(
                                        targetValue = 0f,
                                        animationSpec = spring(
                                            dampingRatio = Spring.DampingRatioMediumBouncy,
                                            stiffness = Spring.StiffnessMedium
                                        )
                                    )
                                    onGestureEvent(
                                        ParagraphGestureEvent.Settle(
                                            nodeId = node.id,
                                            finalOffsetX = 0f
                                        )
                                    )
                                }
                            },
                            onDragEnd = {
                                scope.launch {
                                    val endOffset = offsetX.value
                                    val swipeDecision = resolveParagraphSwipeDecision(
                                        endOffset = endOffset,
                                        rightThreshold = rightThreshold,
                                        leftThreshold = leftThreshold,
                                        hasBridge = hasBridge
                                    )
                                    when (swipeDecision) {
                                        ParagraphSwipeDecision.OpenBridge,
                                        ParagraphSwipeDecision.OpenNote -> {
                                            onGestureEvent(
                                                ParagraphGestureEvent.SwipeRight(
                                                    nodeId = node.id,
                                                    offsetX = endOffset,
                                                    threshold = rightThreshold,
                                                    hasBridge = hasBridge
                                                )
                                            )
                                            offsetX.animateTo(
                                                targetValue = rightLockOffset,
                                                animationSpec = spring(
                                                    dampingRatio = Spring.DampingRatioNoBouncy,
                                                    stiffness = Spring.StiffnessLow
                                                )
                                            )
                                            val openBridge = swipeDecision == ParagraphSwipeDecision.OpenBridge
                                            isBridgeExpanded = openBridge
                                            isNoteExpanded = !openBridge
                                            haptic.performHapticFeedback(HapticFeedbackType.LongPress)
                                            onBridgeOpen(node.id)
                                            onTelemetry(
                                                ReaderTelemetryEvent(
                                                    nodeId = node.id,
                                                    eventType = if (openBridge) {
                                                        "bridge_opened"
                                                    } else {
                                                        "note_opened"
                                                    },
                                                    relevanceScore = node.relevanceScore,
                                                    payload = mapOf(
                                                        "offsetX" to endOffset.toString(),
                                                        "threshold" to rightThreshold.toString()
                                                    )
                                                )
                                            )
                                            autoCenterItem(
                                                listState = listState,
                                                itemIndex = index,
                                                centerRatio = 0.5f
                                            )
                                        }
                                        ParagraphSwipeDecision.Delete -> {
                                            onGestureEvent(
                                                ParagraphGestureEvent.SwipeLeft(
                                                    nodeId = node.id,
                                                    offsetX = endOffset,
                                                    threshold = leftThreshold
                                                )
                                            )
                                            offsetX.animateTo(
                                                targetValue = -leftThreshold * 0.65f,
                                                animationSpec = tween(durationMillis = 140)
                                            )
                                            offsetX.animateTo(
                                                targetValue = 0f,
                                                animationSpec = spring(
                                                    dampingRatio = Spring.DampingRatioMediumBouncy,
                                                    stiffness = Spring.StiffnessMedium
                                                )
                                            )
                                            onTelemetry(
                                                ReaderTelemetryEvent(
                                                    nodeId = node.id,
                                                    eventType = "paragraph_mark_deleted_by_swipe",
                                                    relevanceScore = node.relevanceScore,
                                                    payload = mapOf(
                                                        "offsetX" to endOffset.toString(),
                                                        "threshold" to leftThreshold.toString()
                                                    )
                                                )
                                            )
                                            onMarkDeleted()
                                        }
                                        ParagraphSwipeDecision.Reset -> {
                                            offsetX.animateTo(
                                                targetValue = 0f,
                                                animationSpec = spring(
                                                    dampingRatio = Spring.DampingRatioMediumBouncy,
                                                    stiffness = Spring.StiffnessMedium
                                                )
                                            )
                                            isBridgeExpanded = false
                                            isNoteExpanded = false
                                            onTelemetry(
                                                ReaderTelemetryEvent(
                                                    nodeId = node.id,
                                                    eventType = "paragraph_swipe_cancelled",
                                                    relevanceScore = node.relevanceScore
                                                )
                                            )
                                        }
                                    }

                                    onGestureEvent(
                                        ParagraphGestureEvent.Settle(
                                            nodeId = node.id,
                                            finalOffsetX = offsetX.value
                                        )
                                    )
                                }
                            },
                            onHorizontalDrag = { change, dragAmount ->
                                change.consume()
                                scope.launch {
                                    val resistance = 1f - (abs(offsetX.value) / 820f).coerceIn(0f, 0.82f)
                                    val next = offsetX.value + dragAmount * resistance
                                    val clamped = next.coerceIn(
                                        minimumValue = -paragraphWidthPx.toFloat(),
                                        maximumValue = paragraphWidthPx.toFloat()
                                    )
                                    offsetX.snapTo(clamped)
                                }
                            }
                        )
                    }
                    .pointerInput(node.id) {
                        detectTapGestures(
                            onDoubleTap = {
                                scope.launch {
                                    resonanceScale.snapTo(1f)
                                    resonanceScale.animateTo(
                                        targetValue = 1.06f,
                                        animationSpec = tween(durationMillis = 90)
                                    )
                                    resonanceScale.animateTo(
                                        targetValue = 1f,
                                        animationSpec = spring(
                                            dampingRatio = Spring.DampingRatioMediumBouncy,
                                            stiffness = Spring.StiffnessMedium
                                        )
                                    )
                                }
                                onGestureEvent(
                                    ParagraphGestureEvent.DoubleTap(nodeId = node.id)
                                )
                                onResonance()
                                haptic.performHapticFeedback(HapticFeedbackType.TextHandleMove)
                                onTelemetry(
                                    ReaderTelemetryEvent(
                                        nodeId = node.id,
                                        eventType = "paragraph_resonance_double_tap",
                                        relevanceScore = node.relevanceScore
                                    )
                                )
                            }
                        )
                    }
            ) {
                Column(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(horizontal = 14.dp, vertical = 12.dp)
                ) {
                    Row(
                        verticalAlignment = Alignment.Top,
                        modifier = Modifier
                            .fillMaxWidth()
                            .alpha(resonanceScale.value)
                    ) {
                        if (isNoise && hasBridge) {
                            Text(
                                text = "✧",
                                color = Color(0xFF2EA8E6),
                                fontSize = 15.sp,
                                modifier = Modifier
                                    .padding(end = 8.dp, top = 2.dp)
                                    .alpha(breathingAlpha)
                            )
                        }

                        MarkdownParagraph(
                            markdown = node.originalMarkdown ?: node.text,
                            plainText = node.text,
                            markwon = markwon,
                            textSizeSp = textSize.value,
                            textColor = textColor,
                            fontWeight = fontWeight,
                            selection = tokenSelection,
                            modifier = Modifier
                                .weight(1f)
                                .alpha(if (isBridgeExpanded || isNoteExpanded) 0.98f else 1f),
                            onTokenSingleTap = { cursor ->
                                val selection = resolveTokenSelection(
                                    text = node.text,
                                    cursor = cursor,
                                    nativePayload = runCatching {
                                        LexicalNativeBridge.segmentAt(node.text, cursor)
                                    }.getOrNull()
                                )
                                if (selection != null) {
                                    tokenSelection = selection
                                    tokenCard = null
                                    haptic.performHapticFeedback(HapticFeedbackType.TextHandleMove)
                                    onTelemetry(
                                        ReaderTelemetryEvent(
                                            nodeId = node.id,
                                            eventType = "lexical_token_selected",
                                            relevanceScore = node.relevanceScore,
                                            payload = mapOf(
                                                "token" to selection.token,
                                                "start" to selection.start.toString(),
                                                "end" to selection.end.toString()
                                            )
                                        )
                                    )
                                }
                            },
                            onTokenDoubleTap = { cursor ->
                                val selection = tokenSelection
                                    ?: resolveTokenSelection(
                                        text = node.text,
                                        cursor = cursor,
                                        nativePayload = null
                                    )
                                if (selection != null) {
                                    tokenSelection = selection
                                    tokenCard = parseTokenInsightCard(
                                        token = selection.token,
                                        nativePayload = runCatching {
                                            LexicalNativeBridge.explainToken(
                                                selection.token,
                                                node.text
                                            )
                                        }.getOrNull()
                                    )
                                    haptic.performHapticFeedback(HapticFeedbackType.LongPress)
                                    onTelemetry(
                                        ReaderTelemetryEvent(
                                            nodeId = node.id,
                                            eventType = "lexical_card_opened",
                                            relevanceScore = node.relevanceScore,
                                            payload = mapOf(
                                                "token" to selection.token
                                            )
                                        )
                                    )
                                    scope.launch {
                                        autoCenterItem(
                                            listState = listState,
                                            itemIndex = index,
                                            centerRatio = 0.45f
                                        )
                                    }
                                }
                            }
                        )
                    }

                    if (isFavorited) {
                        Text(
                            text = "已标记为共鸣段落",
                            fontSize = 12.sp,
                            color = Color(0xFF996C00),
                            modifier = Modifier.padding(top = 6.dp)
                        )
                    }
                    if (isMarkedDeleted) {
                        Text(
                            text = "已标记删除（可在后端二次清理）",
                            fontSize = 12.sp,
                            color = Color(0xFF9C2D2D),
                            modifier = Modifier.padding(top = 6.dp)
                        )
                    }

                    tokenCard?.let { card ->
                        TokenInsightCardView(
                            card = card,
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(top = 12.dp)
                        )
                    }

                    if (isNoteExpanded) {
                        OutlinedTextField(
                            value = noteDraft,
                            onValueChange = { noteDraft = it },
                            label = {
                                Text("段落批注")
                            },
                            placeholder = {
                                Text("记录你的想法，这将成为正向画像信号")
                            },
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(top = 12.dp)
                        )

                        Row(
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(top = 8.dp),
                            horizontalArrangement = Arrangement.End
                        ) {
                            TextButton(
                                onClick = {
                                    noteDraft = ""
                                    isNoteExpanded = false
                                    scope.launch {
                                        offsetX.animateTo(0f, spring())
                                    }
                                }
                            ) {
                                Text("取消")
                            }

                            Button(
                                onClick = {
                                    val normalized = noteDraft.trim()
                                    if (normalized.isNotEmpty()) {
                                        onCommentCommitted(normalized)
                                        onTelemetry(
                                            ReaderTelemetryEvent(
                                                nodeId = node.id,
                                                eventType = "note_saved",
                                                relevanceScore = node.relevanceScore,
                                                payload = mapOf("length" to normalized.length.toString())
                                            )
                                        )
                                        noteDraft = ""
                                    }
                                    isNoteExpanded = false
                                    scope.launch {
                                        offsetX.animateTo(0f, spring())
                                    }
                                }
                            ) {
                                Text("保存")
                            }
                        }
                    }

                    if (existingComments.isNotEmpty()) {
                        Column(
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(top = 10.dp),
                            verticalArrangement = Arrangement.spacedBy(6.dp)
                        ) {
                            existingComments.takeLast(3).forEach { comment ->
                                Text(
                                    text = "• $comment",
                                    color = Color(0xFF55636D),
                                    fontSize = 12.sp,
                                    lineHeight = 18.sp
                                )
                            }
                        }
                    }
                }
            }
        }
    )
}

/**
 * 基于 SubcomposeLayout 叠放桥接层和正文层。
 */
@Composable
private fun SubcomposeAnchorLayout(
    modifier: Modifier,
    background: @Composable () -> Unit,
    foregroundOffsetX: Int,
    foreground: @Composable () -> Unit
) {
    SubcomposeLayout(modifier = modifier) { constraints ->
        val backgroundPlaceables = subcompose("background", background).map {
            it.measure(constraints)
        }
        val foregroundPlaceables = subcompose("foreground", foreground).map {
            it.measure(constraints)
        }

        val layoutWidth = foregroundPlaceables.maxOfOrNull { it.width } ?: constraints.maxWidth
        val layoutHeight = max(
            backgroundPlaceables.maxOfOrNull { it.height } ?: 0,
            foregroundPlaceables.maxOfOrNull { it.height } ?: 0
        )

        layout(layoutWidth, layoutHeight) {
            backgroundPlaceables.forEach { placeable ->
                placeable.placeRelative(x = 0, y = 0)
            }
            foregroundPlaceables.forEach { placeable ->
                placeable.placeRelative(
                    x = foregroundOffsetX,
                    y = 0
                )
            }
        }
    }
}

/**
 * 桥接便签视图。
 */
@Composable
private fun BridgeBubble(
    text: String,
    revealProgress: Float
) {
    val alpha = revealProgress.coerceIn(0f, 1f)

    Card(
        shape = RoundedCornerShape(14.dp),
        colors = CardDefaults.cardColors(
            containerColor = Color(0xFFEFF4F8)
        ),
        modifier = Modifier
            .fillMaxWidth()
            .alpha(alpha)
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 12.dp, vertical = 10.dp),
            verticalAlignment = Alignment.Top
        ) {
            Box(
                modifier = Modifier
                    .size(20.dp)
                    .clip(CircleShape)
                    .background(Color(0xFF3F6A84)),
                contentAlignment = Alignment.Center
            ) {
                Text(
                    text = "AI",
                    color = Color.White,
                    fontSize = 9.sp,
                    fontWeight = FontWeight.Bold
                )
            }

            Text(
                text = " 向导：$text",
                color = Color(0xFF2F4E60),
                fontSize = 13.sp,
                lineHeight = 20.sp,
                modifier = Modifier.padding(start = 8.dp)
            )
        }
    }
}

/**
 * 词句三维解析卡片。
 */
@Composable
private fun TokenInsightCardView(
    card: TokenInsightCard,
    modifier: Modifier = Modifier
) {
    Card(
        modifier = modifier,
        shape = RoundedCornerShape(14.dp),
        colors = CardDefaults.cardColors(
            containerColor = Color(0xFFF7FAFD)
        )
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(12.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Text(
                text = "词句手术刀：${card.token}",
                fontSize = 14.sp,
                color = Color(0xFF20445D),
                fontWeight = FontWeight.Bold
            )
            Text(
                text = "语境化：${card.contextualize}",
                fontSize = 13.sp,
                color = Color(0xFF304A5A),
                lineHeight = 20.sp
            )
            Text(
                text = "第一性：${card.firstPrinciple}",
                fontSize = 13.sp,
                color = Color(0xFF304A5A),
                lineHeight = 20.sp
            )
            Text(
                text = "行业广角：${card.industryHorizon}",
                fontSize = 13.sp,
                color = Color(0xFF304A5A),
                lineHeight = 20.sp
            )
        }
    }
}

/**
 * Markwon 段落渲染视图，附带词句级单击和双击探针。
 */
@SuppressLint("ClickableViewAccessibility")
@Composable
private fun MarkdownParagraph(
    markdown: String,
    plainText: String,
    markwon: Markwon,
    textSizeSp: Float,
    textColor: Color,
    fontWeight: FontWeight,
    selection: TokenSelection?,
    onTokenSingleTap: (cursor: Int) -> Unit,
    onTokenDoubleTap: (cursor: Int) -> Unit,
    modifier: Modifier = Modifier
) {
    AndroidView(
        modifier = modifier,
        factory = { context ->
            val textView = TextView(context).apply {
                includeFontPadding = false
                setTextIsSelectable(false)
                setLineSpacing(0f, 1.6f)
                isLongClickable = false
            }

            val detector = GestureDetector(
                context,
                object : GestureDetector.SimpleOnGestureListener() {
                    override fun onSingleTapConfirmed(e: MotionEvent): Boolean {
                        val cursor = resolveCursorOffset(
                            textView = textView,
                            x = e.x,
                            y = e.y
                        ) ?: return false
                        onTokenSingleTap(cursor)
                        return false
                    }

                    override fun onDoubleTap(e: MotionEvent): Boolean {
                        val cursor = resolveCursorOffset(
                            textView = textView,
                            x = e.x,
                            y = e.y
                        ) ?: return false
                        onTokenDoubleTap(cursor)
                        return true
                    }
                }
            )

            textView.setOnTouchListener { _, event ->
                detector.onTouchEvent(event)
                false
            }

            textView
        },
        update = { textView ->
            textView.textSize = textSizeSp
            textView.setTextColor(textColor.toArgbSafe())
            textView.typeface = when (fontWeight) {
                FontWeight.Bold -> Typeface.create(Typeface.DEFAULT, Typeface.BOLD)
                else -> Typeface.create(Typeface.DEFAULT, Typeface.NORMAL)
            }
            markwon.setMarkdown(textView, markdown)
            applySelectionStyle(
                textView = textView,
                selection = selection,
                fallbackText = plainText
            )
        }
    )
}

/**
 * 将选中词元样式叠加到 Markwon 渲染结果上。
 */
private fun applySelectionStyle(
    textView: TextView,
    selection: TokenSelection?,
    fallbackText: String
) {
    val source = textView.text
        ?.toString()
        .orEmpty()
        .ifBlank { fallbackText }
    val spannable = SpannableString(source)

    if (selection != null &&
        selection.start >= 0 &&
        selection.end <= source.length &&
        selection.start < selection.end
    ) {
        spannable.setSpan(
            UnderlineSpan(),
            selection.start,
            selection.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            ForegroundColorSpan(0xFF176DA2.toInt()),
            selection.start,
            selection.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            BackgroundColorSpan(0x3336A2EB),
            selection.start,
            selection.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
        spannable.setSpan(
            RelativeSizeSpan(1.04f),
            selection.start,
            selection.end,
            Spannable.SPAN_EXCLUSIVE_EXCLUSIVE
        )
    }

    textView.text = spannable
}

/**
 * 将触点映射为 TextView 内字符游标。
 */
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
