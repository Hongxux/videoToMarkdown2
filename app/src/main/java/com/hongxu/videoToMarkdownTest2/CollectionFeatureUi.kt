package com.hongxu.videoToMarkdownTest2

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.animation.core.tween
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
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.Checkbox
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.rememberModalBottomSheetState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import kotlin.math.roundToInt

@Composable
fun ClipboardPasteBubble(
    candidateUrl: String,
    enabled: Boolean,
    onPaste: () -> Unit,
    onDismiss: () -> Unit
) {
    Card(
        modifier = Modifier.fillMaxWidth(),
        colors = CardDefaults.cardColors(containerColor = Color(0xFFEFF8FF))
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 12.dp, vertical = 10.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp)
        ) {
            Text(
                text = "检测到剪贴板链接",
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
                Button(onClick = onPaste, enabled = enabled) {
                    Text("一键粘贴")
                }
                TextButton(onClick = onDismiss, enabled = enabled) {
                    Text("忽略")
                }
            }
        }
    }
}

@Composable
fun ProbeDetectingSkeleton() {
    val transition = rememberInfiniteTransition(label = "probe-skeleton")
    val alpha by transition.animateFloat(
        initialValue = 0.35f,
        targetValue = 0.82f,
        animationSpec = infiniteRepeatable(
            animation = tween(durationMillis = 900),
            repeatMode = RepeatMode.Reverse
        ),
        label = "probe-skeleton-alpha"
    )
    Card(
        modifier = Modifier.fillMaxWidth(),
        colors = CardDefaults.cardColors(containerColor = Color(0xFFF8F9FC))
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 12.dp, vertical = 12.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Text(
                text = "正在探测视频信息，请稍候...",
                color = Color(0xFF344054),
                fontWeight = FontWeight.Medium
            )
            Box(
                modifier = Modifier
                    .fillMaxWidth()
                    .height(14.dp)
                    .alpha(alpha)
                    .background(Color(0xFFD0D5DD), RoundedCornerShape(10.dp))
            )
            Box(
                modifier = Modifier
                    .fillMaxWidth(0.65f)
                    .height(14.dp)
                    .alpha(alpha)
                    .background(Color(0xFFD0D5DD), RoundedCornerShape(10.dp))
            )
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun ProbeResultBottomSheet(
    probeResult: VideoProbeResult,
    selectedEpisodeNos: Set<Int>,
    submitting: Boolean,
    onDismiss: () -> Unit,
    onSubmitSingle: () -> Unit,
    onSubmitCollection: () -> Unit,
    onSelectAll: () -> Unit,
    onInvertSelection: () -> Unit,
    onToggleEpisode: (Int) -> Unit
) {
    ModalBottomSheet(
        onDismissRequest = onDismiss,
        sheetState = rememberModalBottomSheetState(skipPartiallyExpanded = true)
    ) {
        if (!probeResult.isCollection) {
            SingleVideoProbeResultContent(
                probeResult = probeResult,
                submitting = submitting,
                onSubmit = onSubmitSingle
            )
        } else {
            CollectionProbeResultContent(
                probeResult = probeResult,
                selectedEpisodeNos = selectedEpisodeNos,
                submitting = submitting,
                onSubmitCollection = onSubmitCollection,
                onSelectAll = onSelectAll,
                onInvertSelection = onInvertSelection,
                onToggleEpisode = onToggleEpisode
            )
        }
    }
}

@Composable
private fun SingleVideoProbeResultContent(
    probeResult: VideoProbeResult,
    submitting: Boolean,
    onSubmit: () -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 16.dp, vertical = 8.dp),
        verticalArrangement = Arrangement.spacedBy(14.dp)
    ) {
        Box(
            modifier = Modifier
                .fillMaxWidth()
                .height(180.dp)
                .background(
                    brush = Brush.linearGradient(
                        listOf(Color(0xFF0EA5E9), Color(0xFF111827))
                    ),
                    shape = RoundedCornerShape(18.dp)
                )
        ) {
            Text(
                text = probeResult.title.ifBlank { "未命名视频" },
                modifier = Modifier
                    .align(Alignment.BottomStart)
                    .padding(14.dp),
                color = Color.White,
                fontWeight = FontWeight.SemiBold,
                maxLines = 2,
                overflow = TextOverflow.Ellipsis
            )
        }
        Text("平台：${probeResult.platform.ifBlank { "未知" }}", color = Color(0xFF475467))
        Text("UP主：暂未返回", color = Color(0xFF475467))
        Text("时长：${formatDuration(probeResult.durationSec)}", color = Color(0xFF475467))
        Button(
            onClick = onSubmit,
            enabled = !submitting,
            modifier = Modifier.fillMaxWidth()
        ) {
            if (submitting) {
                CircularProgressIndicator(
                    modifier = Modifier.size(16.dp),
                    strokeWidth = 2.dp,
                    color = Color.White
                )
                Spacer(modifier = Modifier.width(8.dp))
            }
            Text("开始处理")
        }
    }
}

@Composable
private fun CollectionProbeResultContent(
    probeResult: VideoProbeResult,
    selectedEpisodeNos: Set<Int>,
    submitting: Boolean,
    onSubmitCollection: () -> Unit,
    onSelectAll: () -> Unit,
    onInvertSelection: () -> Unit,
    onToggleEpisode: (Int) -> Unit
) {
    Column(modifier = Modifier.fillMaxWidth()) {
        Box(
            modifier = Modifier
                .fillMaxWidth()
                .height(188.dp)
                .background(
                    brush = Brush.verticalGradient(
                        colors = listOf(Color(0xFF0F172A), Color(0xFF1E3A8A), Color(0xFF111827))
                    )
                )
        ) {
            Text(
                text = probeResult.title.ifBlank { probeResult.collectionId },
                color = Color.White,
                fontWeight = FontWeight.Bold,
                maxLines = 2,
                overflow = TextOverflow.Ellipsis,
                modifier = Modifier
                    .align(Alignment.BottomStart)
                    .padding(horizontal = 16.dp, vertical = 14.dp)
            )
        }
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp, vertical = 10.dp),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.SpaceBetween
        ) {
            Text(
                text = "共探测到 ${probeResult.episodes.size} 集",
                fontWeight = FontWeight.SemiBold
            )
            Row(verticalAlignment = Alignment.CenterVertically) {
                Checkbox(
                    checked = selectedEpisodeNos.size == probeResult.episodes.size && probeResult.episodes.isNotEmpty(),
                    onCheckedChange = {
                        if (it) onSelectAll() else onInvertSelection()
                    }
                )
                Text("全选/反选")
            }
        }
        LazyColumn(
            modifier = Modifier
                .fillMaxWidth()
                .height(380.dp)
                .padding(horizontal = 8.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp)
        ) {
            items(probeResult.episodes, key = { episode -> episode.episodeNo }) { episode ->
                val checked = selectedEpisodeNos.contains(episode.episodeNo)
                Card(
                    modifier = Modifier
                        .fillMaxWidth()
                        .clickable { onToggleEpisode(episode.episodeNo) },
                    colors = CardDefaults.cardColors(
                        containerColor = if (checked) Color(0xFFEFF8FF) else Color(0xFFFFFFFF)
                    )
                ) {
                    Row(
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(horizontal = 12.dp, vertical = 10.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Text(
                            text = "P${episode.episodeNo}",
                            modifier = Modifier.width(44.dp),
                            fontWeight = FontWeight.SemiBold,
                            color = Color(0xFF175CD3)
                        )
                        Column(modifier = Modifier.weight(1f)) {
                            Text(
                                text = episode.title.ifBlank { "未命名分集" },
                                maxLines = 1,
                                overflow = TextOverflow.Ellipsis
                            )
                            Text(
                                text = formatDuration(episode.durationSec),
                                color = Color(0xFF667085),
                                style = MaterialTheme.typography.bodySmall
                            )
                        }
                        Checkbox(
                            checked = checked,
                            onCheckedChange = { onToggleEpisode(episode.episodeNo) }
                        )
                    }
                }
            }
        }
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .background(Color(0xFF101828))
                .padding(horizontal = 16.dp, vertical = 14.dp),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.SpaceBetween
        ) {
            Text(
                text = "已选 ${selectedEpisodeNos.size} 集",
                color = Color.White,
                fontWeight = FontWeight.Medium
            )
            Button(onClick = onSubmitCollection, enabled = !submitting && selectedEpisodeNos.isNotEmpty()) {
                if (submitting) {
                    CircularProgressIndicator(
                        modifier = Modifier.size(16.dp),
                        strokeWidth = 2.dp,
                        color = Color.White
                    )
                    Spacer(modifier = Modifier.width(8.dp))
                }
                Text("提交合集")
            }
        }
        Spacer(modifier = Modifier.height(18.dp))
    }
}

@Composable
fun CollectionHubScreen(
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
    Surface(modifier = Modifier.fillMaxSize()) {
        if (detailCollection == null) {
            CollectionStackedList(
                collections = collections,
                onBack = onBackToTasks,
                onRefresh = onRefresh,
                onOpenDetail = onOpenDetail
            )
        } else {
            CollectionDetailScreen(
                collection = detailCollection,
                episodes = detailEpisodes,
                onBack = onCloseDetail,
                onOpenTask = onOpenTask,
                onRetryEpisode = { episode -> onRetryEpisode(detailCollection.collectionId, episode) }
            )
        }
    }
}

@Composable
private fun CollectionStackedList(
    collections: List<CollectionCardUi>,
    onBack: () -> Unit,
    onRefresh: () -> Unit,
    onOpenDetail: (String) -> Unit
) {
    LazyColumn(
        modifier = Modifier
            .fillMaxSize()
            .padding(horizontal = 16.dp, vertical = 14.dp),
        verticalArrangement = Arrangement.spacedBy(14.dp)
    ) {
        item {
            Row(
                modifier = Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                TextButton(onClick = onBack) {
                    Text("返回任务")
                }
                Text("合集管理", fontWeight = FontWeight.Bold, style = MaterialTheme.typography.titleLarge)
                TextButton(onClick = onRefresh) {
                    Text("刷新")
                }
            }
        }
        if (collections.isEmpty()) {
            item {
                Text(
                    text = "暂无合集，先去探测并提交一个合集。",
                    color = Color(0xFF667085)
                )
            }
        }
        items(collections, key = { item -> item.collectionId }) { collection ->
            StackedCollectionCard(
                card = collection,
                onClick = { onOpenDetail(collection.collectionId) }
            )
        }
    }
}

@Composable
private fun StackedCollectionCard(
    card: CollectionCardUi,
    onClick: () -> Unit
) {
    Box(
        modifier = Modifier
            .fillMaxWidth()
            .clickable { onClick() }
    ) {
        Card(
            modifier = Modifier
                .fillMaxWidth()
                .padding(top = 12.dp, start = 12.dp),
            colors = CardDefaults.cardColors(containerColor = Color(0xFFE4E7EC))
        ) {
            Spacer(modifier = Modifier.height(152.dp))
        }
        Card(
            modifier = Modifier
                .fillMaxWidth()
                .padding(top = 6.dp, start = 6.dp),
            colors = CardDefaults.cardColors(containerColor = Color(0xFFF2F4F7))
        ) {
            Spacer(modifier = Modifier.height(152.dp))
        }
        Card(
            modifier = Modifier.fillMaxWidth(),
            colors = CardDefaults.cardColors(containerColor = Color.White)
        ) {
            Column {
                Box(
                    modifier = Modifier
                        .fillMaxWidth()
                        .height(116.dp)
                        .background(
                            Brush.linearGradient(
                                colors = listOf(Color(0xFF0EA5E9), Color(0xFF1D4ED8), Color(0xFF111827))
                            )
                        )
                )
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(horizontal = 12.dp, vertical = 10.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Column(modifier = Modifier.weight(1f)) {
                        Text(
                            text = card.title,
                            fontWeight = FontWeight.SemiBold,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis
                        )
                        Text(
                            text = "${card.totalEpisodes}个视频 · 已提交${card.submittedCount}个",
                            color = Color(0xFF667085)
                        )
                    }
                    Box(contentAlignment = Alignment.Center) {
                        CircularProgressIndicator(
                            progress = { card.progress },
                            modifier = Modifier.size(42.dp),
                            strokeWidth = 4.dp
                        )
                        Text(
                            text = "${(card.progress * 100).roundToInt()}%",
                            style = MaterialTheme.typography.bodySmall
                        )
                    }
                }
            }
        }
    }
}

@Composable
private fun CollectionDetailScreen(
    collection: CollectionCardUi,
    episodes: List<CollectionEpisodeUi>,
    onBack: () -> Unit,
    onOpenTask: (String, String) -> Unit,
    onRetryEpisode: (CollectionEpisodeUi) -> Unit
) {
    LazyColumn(
        modifier = Modifier.fillMaxSize(),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        item {
            Box(
                modifier = Modifier
                    .fillMaxWidth()
                    .height(220.dp)
                    .background(
                        Brush.verticalGradient(
                            listOf(Color(0xFF0F172A), Color(0xFF1D4ED8), Color(0xFF000000))
                        )
                    )
            ) {
                TextButton(
                    onClick = onBack,
                    modifier = Modifier
                        .align(Alignment.TopStart)
                        .padding(top = 16.dp, start = 8.dp)
                ) {
                    Text("返回", color = Color.White)
                }
                Column(
                    modifier = Modifier
                        .align(Alignment.BottomStart)
                        .padding(horizontal = 16.dp, vertical = 16.dp)
                ) {
                    Text(
                        text = collection.title,
                        color = Color.White,
                        fontWeight = FontWeight.Bold,
                        style = MaterialTheme.typography.titleLarge,
                        maxLines = 2,
                        overflow = TextOverflow.Ellipsis
                    )
                    Text(
                        text = "完成 ${collection.completedCount}/${collection.totalEpisodes}",
                        color = Color(0xFFD0D5DD)
                    )
                }
            }
        }
        items(episodes, key = { item -> "${item.collectionId}-${item.episodeNo}" }) { episode ->
            EpisodeStatusCard(
                episode = episode,
                onOpenTask = onOpenTask,
                onRetry = { onRetryEpisode(episode) }
            )
        }
        item { Spacer(modifier = Modifier.height(16.dp)) }
    }
}

@Composable
private fun EpisodeStatusCard(
    episode: CollectionEpisodeUi,
    onOpenTask: (String, String) -> Unit,
    onRetry: () -> Unit
) {
    Card(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 12.dp),
        colors = CardDefaults.cardColors(containerColor = Color(0xFFFFFFFF))
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 12.dp, vertical = 10.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    text = "第${episode.episodeNo}集",
                    color = Color(0xFF175CD3),
                    fontWeight = FontWeight.SemiBold,
                    modifier = Modifier.width(64.dp)
                )
                Column(modifier = Modifier.weight(1f)) {
                    Text(
                        text = episode.title,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis
                    )
                    Text(
                        text = formatDuration(episode.durationSec),
                        color = Color(0xFF667085),
                        style = MaterialTheme.typography.bodySmall
                    )
                }
                when (episode.displayStatus) {
                    EpisodeDisplayStatus.READY -> {
                        OutlinedButton(
                            onClick = {
                                val taskId = episode.taskId
                                if (!taskId.isNullOrBlank()) {
                                    onOpenTask(taskId, episode.title)
                                }
                            }
                        ) {
                            Text("阅读")
                        }
                    }

                    EpisodeDisplayStatus.FAILED -> {
                        OutlinedButton(onClick = onRetry) {
                            Text("重试", color = Color(0xFFB42318))
                        }
                    }

                    else -> {
                        Text(
                            text = statusLabel(episode.displayStatus),
                            color = when (episode.displayStatus) {
                                EpisodeDisplayStatus.PROCESSING -> Color(0xFF175CD3)
                                EpisodeDisplayStatus.QUEUED -> Color(0xFFB54708)
                                EpisodeDisplayStatus.IDLE -> Color(0xFF667085)
                                else -> Color(0xFF667085)
                            }
                        )
                    }
                }
            }
            AnimatedVisibility(
                visible = episode.displayStatus == EpisodeDisplayStatus.PROCESSING ||
                    episode.displayStatus == EpisodeDisplayStatus.QUEUED
            ) {
                val shimmer = rememberInfiniteTransition(label = "episode-${episode.episodeNo}")
                val alpha by shimmer.animateFloat(
                    initialValue = 0.28f,
                    targetValue = 0.76f,
                    animationSpec = infiniteRepeatable(
                        animation = tween(900),
                        repeatMode = RepeatMode.Reverse
                    ),
                    label = "episode-skeleton-alpha"
                )
                Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                    Box(
                        modifier = Modifier
                            .fillMaxWidth()
                            .height(8.dp)
                            .alpha(alpha)
                            .background(Color(0xFFD0D5DD), RoundedCornerShape(8.dp))
                    )
                    LinearProgressIndicator(modifier = Modifier.fillMaxWidth())
                    Text(
                        text = episode.statusMessage ?: "正在解析中，请稍候...",
                        color = Color(0xFF475467),
                        style = MaterialTheme.typography.bodySmall
                    )
                }
            }
            AnimatedVisibility(visible = episode.displayStatus == EpisodeDisplayStatus.FAILED) {
                Text(
                    text = episode.statusMessage ?: "解析失败，请重试",
                    color = Color(0xFFB42318),
                    style = MaterialTheme.typography.bodySmall
                )
            }
        }
    }
}

private fun statusLabel(status: EpisodeDisplayStatus): String {
    return when (status) {
        EpisodeDisplayStatus.IDLE -> "未提交"
        EpisodeDisplayStatus.QUEUED -> "排队中"
        EpisodeDisplayStatus.PROCESSING -> "解析中"
        EpisodeDisplayStatus.READY -> "可阅读"
        EpisodeDisplayStatus.FAILED -> "失败"
    }
}

private fun formatDuration(durationSec: Double?): String {
    val seconds = durationSec ?: return "--:--"
    if (seconds <= 0.0) {
        return "--:--"
    }
    val total = seconds.roundToInt().coerceAtLeast(0)
    val minutes = total / 60
    val remain = total % 60
    return "%d:%02d".format(minutes, remain)
}
