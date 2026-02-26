package com.hongxu.videoToMarkdownTest2

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.tween
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInVertically
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
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
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.drawBehind
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.SpanStyle
import androidx.compose.ui.text.buildAnnotatedString
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.text.withStyle
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import kotlinx.coroutines.flow.Flow
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale

// ─── 颜色常量 ────────────────────────────────────────────────────────────────

private val FootprintBg = Color(0xFF0F1117)
private val CardBg = Color(0xFF1A1D28)
private val CardBgHot = Color(0xFF1E2030)
private val TimelineAxisColor = Color(0xFF2A2D3A)
private val AccentBlue = Color(0xFF4A90D9)
private val AccentGold = Color(0xFFD4A843)
private val AccentGreen = Color(0xFF5CB85C)
private val AccentPurple = Color(0xFF9B59B6)
private val SubtitleColor = Color(0xFF8B8FA3)
private val SnippetHighlight = Color(0x33FFDD57)
private val SectionLabelColor = Color(0xFF6B7080)

// ─── Node dot 颜色映射 ───────────────────────────────────────────────────────

private fun nodeColor(eventType: String): Color = when (eventType) {
    FootprintEventTypes.VIDEO_TASK_CREATED -> AccentBlue
    FootprintEventTypes.ARTICLE_OPENED -> AccentBlue
    FootprintEventTypes.PARAGRAPH_BOLD -> AccentGreen
    FootprintEventTypes.TOKEN_DOUBLE_CLICK -> AccentGreen
    FootprintEventTypes.ANNOTATION_ADDED -> AccentPurple
    FootprintEventTypes.INSIGHT_CARD_VIEWED -> AccentGold
    else -> SubtitleColor
}

// ─── Node emoji / label 映射 ─────────────────────────────────────────────────

private fun nodeEmoji(eventType: String): String = when (eventType) {
    FootprintEventTypes.VIDEO_TASK_CREATED -> "🎥"
    FootprintEventTypes.ARTICLE_OPENED -> "📖"
    FootprintEventTypes.PARAGRAPH_BOLD -> "✍️"
    FootprintEventTypes.TOKEN_DOUBLE_CLICK -> "🔍"
    FootprintEventTypes.ANNOTATION_ADDED -> "📝"
    FootprintEventTypes.INSIGHT_CARD_VIEWED -> "💡"
    else -> "📌"
}

private fun nodeLabel(eventType: String): String = when (eventType) {
    FootprintEventTypes.VIDEO_TASK_CREATED -> "提交了视频任务"
    FootprintEventTypes.ARTICLE_OPENED -> "打开了文章"
    FootprintEventTypes.PARAGRAPH_BOLD -> "给段落加粗"
    FootprintEventTypes.TOKEN_DOUBLE_CLICK -> "双击了单词"
    FootprintEventTypes.ANNOTATION_ADDED -> "添加了批注"
    FootprintEventTypes.INSIGHT_CARD_VIEWED -> "查看了洞察卡片"
    else -> "其他操作"
}

private fun sectionLabel(group: FootprintTimeGroup): String = when (group) {
    FootprintTimeGroup.TODAY -> "今天"
    FootprintTimeGroup.YESTERDAY -> "昨天"
    FootprintTimeGroup.THIS_WEEK -> "本周"
    FootprintTimeGroup.EARLIER -> "更早"
}

// ─── 时间格式化 ──────────────────────────────────────────────────────────────

private val TIME_FORMATTER: DateTimeFormatter =
    DateTimeFormatter.ofPattern("HH:mm", Locale.getDefault())

private fun formatTime(timestampMs: Long): String {
    return Instant.ofEpochMilli(timestampMs)
        .atZone(ZoneId.systemDefault())
        .format(TIME_FORMATTER)
}

private val DATE_TIME_FORMATTER: DateTimeFormatter =
    DateTimeFormatter.ofPattern("MM-dd HH:mm", Locale.getDefault())

private fun formatDateTime(timestampMs: Long): String {
    return Instant.ofEpochMilli(timestampMs)
        .atZone(ZoneId.systemDefault())
        .format(DATE_TIME_FORMATTER)
}

// ─── 数据封装（用于回调） ────────────────────────────────────────────────────

/**
 * 足迹点击事件携带的定位信息：
 * 用于从时间轴跳回阅读器的精确位置。
 */
data class FootprintNavigationTarget(
    val taskId: String,
    val taskTitle: String,
    val anchorBlockId: String,
    val anchorTokenStart: Int,
    val anchorTokenEnd: Int,
    val anchorScrollIndex: Int
)

// ─── 主入口 Composable ──────────────────────────────────────────────────────

/**
 * 阅读足迹时间轴页面。
 *
 * @param footprintsFlow 足迹数据 Flow（由 Repository 提供）。
 * @param onBack 返回上一页。
 * @param onNavigateToArticle 点击带锚点的足迹卡片时，回溯到阅读器指定位置。
 */
@Composable
fun ReadingFootprintTimeline(
    footprintsFlow: Flow<List<ReadingFootprintEntity>>,
    onBack: () -> Unit,
    onNavigateToArticle: (FootprintNavigationTarget) -> Unit
) {
    val footprints by footprintsFlow.collectAsState(initial = emptyList())
    val grouped = remember(footprints) { groupFootprintsByTime(footprints) }

    Surface(
        modifier = Modifier.fillMaxSize(),
        color = FootprintBg
    ) {
        Column(modifier = Modifier.fillMaxSize()) {
            // ── 顶部导航栏 ──────────────────────────────────────────────────
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .statusBarsPadding()
                    .padding(horizontal = 12.dp, vertical = 8.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                TextButton(onClick = onBack) {
                    Text("返回", color = AccentBlue)
                }
                Text(
                    text = "阅读足迹",
                    fontWeight = FontWeight.Bold,
                    fontSize = 18.sp,
                    color = Color.White,
                    modifier = Modifier
                        .padding(start = 8.dp)
                        .weight(1f)
                )
                Text(
                    text = "${footprints.size} 条记录",
                    color = SubtitleColor,
                    fontSize = 13.sp
                )
            }

            // ── 空状态 ──────────────────────────────────────────────────────
            if (footprints.isEmpty()) {
                Box(
                    modifier = Modifier
                        .fillMaxSize()
                        .padding(32.dp),
                    contentAlignment = Alignment.Center
                ) {
                    Column(horizontalAlignment = Alignment.CenterHorizontally) {
                        Text("📭", fontSize = 48.sp)
                        Spacer(modifier = Modifier.height(12.dp))
                        Text(
                            text = "还没有阅读足迹",
                            color = SubtitleColor,
                            fontSize = 15.sp
                        )
                        Text(
                            text = "打开文章、提交任务、双击单词等操作都会记录在这里",
                            color = SectionLabelColor,
                            fontSize = 13.sp,
                            modifier = Modifier.padding(top = 6.dp)
                        )
                    }
                }
                return@Surface
            }

            // ── 时间轴主列表 ────────────────────────────────────────────────
            LazyColumn(
                modifier = Modifier
                    .fillMaxSize()
                    .padding(start = 16.dp, end = 16.dp),
                verticalArrangement = Arrangement.spacedBy(0.dp)
            ) {
                grouped.forEach { (group, events) ->
                    // 分组标题
                    item(key = "section_${group.name}") {
                        TimelineSectionHeader(label = sectionLabel(group))
                    }

                    // 事件卡片
                    items(events, key = { "fp_${it.id}" }) { event ->
                        val isLast = event == events.last()
                        TimelineEventCard(
                            event = event,
                            isLastInSection = isLast,
                            onClick = {
                                if (event.anchorBlockId.isNotBlank()) {
                                    onNavigateToArticle(
                                        FootprintNavigationTarget(
                                            taskId = event.taskId,
                                            taskTitle = event.taskTitle,
                                            anchorBlockId = event.anchorBlockId,
                                            anchorTokenStart = event.anchorTokenStart,
                                            anchorTokenEnd = event.anchorTokenEnd,
                                            anchorScrollIndex = event.anchorScrollIndex
                                        )
                                    )
                                } else {
                                    // 宏观事件直接打开整篇文章
                                    onNavigateToArticle(
                                        FootprintNavigationTarget(
                                            taskId = event.taskId,
                                            taskTitle = event.taskTitle,
                                            anchorBlockId = "",
                                            anchorTokenStart = -1,
                                            anchorTokenEnd = -1,
                                            anchorScrollIndex = -1
                                        )
                                    )
                                }
                            }
                        )
                    }

                    // 分组间距
                    item(key = "spacer_${group.name}") {
                        Spacer(modifier = Modifier.height(8.dp))
                    }
                }

                // 底部安全间距
                item {
                    Spacer(modifier = Modifier.height(32.dp))
                }
            }
        }
    }
}

// ─── 分组标题 ────────────────────────────────────────────────────────────────

@Composable
private fun TimelineSectionHeader(label: String) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(top = 16.dp, bottom = 8.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        // 左侧时间轴节点位置占位 + 装饰线
        Box(modifier = Modifier.width(32.dp), contentAlignment = Alignment.Center) {
            Box(
                modifier = Modifier
                    .size(8.dp)
                    .clip(CircleShape)
                    .background(SectionLabelColor)
            )
        }
        Text(
            text = label,
            color = SectionLabelColor,
            fontSize = 13.sp,
            fontWeight = FontWeight.SemiBold,
            letterSpacing = 1.sp,
            modifier = Modifier.padding(start = 12.dp)
        )
    }
}

// ─── 单条足迹卡片（含时间轴结构） ───────────────────────────────────────────

@Composable
private fun TimelineEventCard(
    event: ReadingFootprintEntity,
    isLastInSection: Boolean,
    onClick: () -> Unit
) {
    val dotColor = nodeColor(event.eventType)
    val canNavigate = event.anchorBlockId.isNotBlank() ||
        event.eventType == FootprintEventTypes.ARTICLE_OPENED ||
        event.eventType == FootprintEventTypes.VIDEO_TASK_CREATED

    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(bottom = 4.dp)
    ) {
        // ── 左侧：时间轴竖线 + 圆点 ─────────────────────────────────────
        Box(
            modifier = Modifier
                .width(32.dp)
                .height(80.dp)
                .drawBehind {
                    // 竖线
                    if (!isLastInSection) {
                        drawLine(
                            color = TimelineAxisColor,
                            start = Offset(size.width / 2, 20.dp.toPx()),
                            end = Offset(size.width / 2, size.height),
                            strokeWidth = 1.5.dp.toPx()
                        )
                    }
                },
            contentAlignment = Alignment.TopCenter
        ) {
            // 圆点
            Box(
                modifier = Modifier
                    .padding(top = 6.dp)
                    .size(12.dp)
                    .clip(CircleShape)
                    .background(
                        brush = Brush.radialGradient(
                            colors = listOf(dotColor.copy(alpha = 0.9f), dotColor.copy(alpha = 0.4f))
                        )
                    )
            )
        }

        // ── 右侧：卡片内容 ──────────────────────────────────────────────
        Card(
            modifier = Modifier
                .weight(1f)
                .padding(start = 8.dp, bottom = 6.dp)
                .then(
                    if (canNavigate) Modifier.clickable { onClick() } else Modifier
                ),
            shape = RoundedCornerShape(12.dp),
            colors = CardDefaults.cardColors(
                containerColor = if (event.eventType == FootprintEventTypes.INSIGHT_CARD_VIEWED) {
                    CardBgHot
                } else {
                    CardBg
                }
            ),
            elevation = CardDefaults.cardElevation(defaultElevation = 0.dp)
        ) {
            Column(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 14.dp, vertical = 10.dp)
            ) {
                // 第一行：时间 + 操作类型 emoji
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Text(
                        text = buildAnnotatedString {
                            withStyle(SpanStyle(color = SubtitleColor, fontSize = 12.sp)) {
                                append(formatTime(event.timestampMs))
                            }
                            withStyle(SpanStyle(color = SubtitleColor, fontSize = 12.sp)) {
                                append(" · ")
                            }
                            withStyle(SpanStyle(color = dotColor, fontSize = 12.sp)) {
                                append(nodeLabel(event.eventType))
                            }
                        }
                    )
                    Text(
                        text = nodeEmoji(event.eventType),
                        fontSize = 16.sp
                    )
                }

                Spacer(modifier = Modifier.height(4.dp))

                // 第二行：标题 / 关联内容
                Text(
                    text = event.taskTitle.ifBlank { event.taskId },
                    color = Color.White,
                    fontSize = 14.sp,
                    fontWeight = FontWeight.Medium,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )

                // 第三行：片段文本（如果有）
                if (event.snippetText.isNotBlank()) {
                    Spacer(modifier = Modifier.height(4.dp))
                    Text(
                        text = buildAnnotatedString {
                            when (event.eventType) {
                                FootprintEventTypes.TOKEN_DOUBLE_CLICK -> {
                                    withStyle(
                                        SpanStyle(
                                            color = AccentGold,
                                            fontWeight = FontWeight.SemiBold,
                                            background = SnippetHighlight
                                        )
                                    ) {
                                        append(event.snippetText)
                                    }
                                }
                                FootprintEventTypes.INSIGHT_CARD_VIEWED -> {
                                    withStyle(SpanStyle(color = AccentGold)) {
                                        append("#${event.snippetText}")
                                    }
                                }
                                else -> {
                                    withStyle(SpanStyle(color = SubtitleColor)) {
                                        append(event.snippetText)
                                    }
                                }
                            }
                        },
                        fontSize = 13.sp,
                        maxLines = 2,
                        overflow = TextOverflow.Ellipsis
                    )
                }

                // 第四行：可点击回溯提示
                if (canNavigate && event.anchorBlockId.isNotBlank()) {
                    Spacer(modifier = Modifier.height(4.dp))
                    Text(
                        text = "点击定位到原文 →",
                        color = AccentBlue.copy(alpha = 0.7f),
                        fontSize = 11.sp
                    )
                }
            }
        }
    }
}
