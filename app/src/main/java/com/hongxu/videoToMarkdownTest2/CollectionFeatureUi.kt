package com.hongxu.videoToMarkdownTest2

import android.content.Context
import android.graphics.Bitmap
import android.graphics.pdf.PdfRenderer
import android.net.Uri
import android.os.ParcelFileDescriptor
import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.RepeatMode
import androidx.compose.animation.core.animateFloat
import androidx.compose.animation.core.infiniteRepeatable
import androidx.compose.animation.core.rememberInfiniteTransition
import androidx.compose.animation.core.tween
import androidx.compose.foundation.Image
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
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.rememberModalBottomSheetState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.produceState
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.remember
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
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
                text = "Detected clipboard link",
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
                    Text("Paste")
                }
                TextButton(onClick = onDismiss, enabled = enabled) {
                    Text("Ignore")
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
                text = "Analyzing video metadata and chapter structure, this may take a moment...",
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
    confirmedStartPage: Int?,
    previewDocumentUri: String?,
    submitting: Boolean,
    onDismiss: () -> Unit,
    onSubmitSingle: () -> Unit,
    onSubmitCollection: () -> Unit,
    onSelectAll: () -> Unit,
    onInvertSelection: () -> Unit,
    onToggleEpisode: (Int) -> Unit,
    onConfirmedStartPageChange: (Int?) -> Unit
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
                confirmedStartPage = confirmedStartPage,
                previewDocumentUri = previewDocumentUri,
                submitting = submitting,
                onSubmitCollection = onSubmitCollection,
                onSelectAll = onSelectAll,
                onInvertSelection = onInvertSelection,
                onToggleEpisode = onToggleEpisode,
                onConfirmedStartPageChange = onConfirmedStartPageChange
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
                text = probeResult.title.ifBlank { "Untitled video" },
                modifier = Modifier
                    .align(Alignment.BottomStart)
                    .padding(14.dp),
                color = Color.White,
                fontWeight = FontWeight.SemiBold,
                maxLines = 2,
                overflow = TextOverflow.Ellipsis
            )
        }
        Text("Platform: ${probeResult.platform.ifBlank { "Unknown" }}", color = Color(0xFF475467))
        Text("Duration: ${formatDuration(probeResult.durationSec)}", color = Color(0xFF475467))
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
            Text("Start processing")
        }
    }
}

@Composable
private fun CollectionProbeResultContent(
    probeResult: VideoProbeResult,
    selectedEpisodeNos: Set<Int>,
    confirmedStartPage: Int?,
    previewDocumentUri: String?,
    submitting: Boolean,
    onSubmitCollection: () -> Unit,
    onSelectAll: () -> Unit,
    onInvertSelection: () -> Unit,
    onToggleEpisode: (Int) -> Unit,
    onConfirmedStartPageChange: (Int?) -> Unit
) {
    val isBook = probeResult.isBookProbeResult()
    val chapterGroups = remember(probeResult.episodes) {
        if (isBook) buildBookChapterGroups(probeResult.episodes) else emptyList()
    }
    val chapterExpandedState = remember(probeResult.collectionId, probeResult.episodes) {
        mutableStateMapOf<String, Boolean>()
    }
    val focusedEpisodeNoState = remember(probeResult.collectionId, probeResult.episodes) {
        mutableStateOf<Int?>(null)
    }
    val previewPageState = remember(probeResult.collectionId, probeResult.episodes, confirmedStartPage) {
        mutableStateOf(resolveInitialBookPreviewPage(probeResult, confirmedStartPage))
    }
    val resolvedTotalPages = remember(probeResult.totalPages, probeResult.episodes) {
        if (probeResult.totalPages > 0) {
            probeResult.totalPages
        } else {
            probeResult.episodes.maxOfOrNull { episode ->
                maxOf(episode.startPage ?: 0, episode.endPage ?: 0)
            }?.takeIf { it > 0 } ?: 0
        }
    }

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
                text = "Detected ${probeResult.episodes.size} ${if (isBook) "sections" else "episodes"}",
                fontWeight = FontWeight.SemiBold
            )
            Row(verticalAlignment = Alignment.CenterVertically) {
                Checkbox(
                    checked = selectedEpisodeNos.size == probeResult.episodes.size && probeResult.episodes.isNotEmpty(),
                    onCheckedChange = {
                        if (it) onSelectAll() else onInvertSelection()
                    }
                )
                Text("Select all / invert")
            }
        }

        if (isBook) {
            val appliedOffset = probeResult.appliedPageOffset?.toString() ?: "auto"
            val detectedStart = probeResult.detectedStartPage ?: 1
            Text(
                text = "PDF pages: ${probeResult.totalPages} | detected start: $detectedStart | offset: $appliedOffset",
                modifier = Modifier.padding(horizontal = 16.dp),
                color = Color(0xFF667085),
                style = MaterialTheme.typography.bodySmall
            )
            OutlinedTextField(
                value = confirmedStartPage?.toString().orEmpty(),
                onValueChange = { raw ->
                    val normalized = raw.trim()
                    if (normalized.isEmpty()) {
                        onConfirmedStartPageChange(null)
                        return@OutlinedTextField
                    }
                    val parsed = normalized.toIntOrNull() ?: return@OutlinedTextField
                    val normalizedPage = normalizeBookPage(parsed, resolvedTotalPages)
                    onConfirmedStartPageChange(normalizedPage)
                    previewPageState.value = normalizedPage
                },
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 16.dp, vertical = 8.dp),
                singleLine = true,
                label = { Text("Actual start page") },
                placeholder = { Text("e.g. 13") },
                enabled = !submitting
            )
        }

        if (isBook) {
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .height(430.dp)
                    .padding(horizontal = 8.dp),
                horizontalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                LazyColumn(
                    modifier = Modifier
                        .weight(1f)
                        .fillMaxSize(),
                    verticalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    items(chapterGroups, key = { group -> group.key }) { group ->
                        val expanded = chapterExpandedState[group.key] != false
                        val selectedInChapter = group.sections.count { selectedEpisodeNos.contains(it.episodeNo) }
                        Card(
                            modifier = Modifier.fillMaxWidth(),
                            colors = CardDefaults.cardColors(containerColor = Color(0xFFF8FAFC))
                        ) {
                            Row(
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .clickable { chapterExpandedState[group.key] = !expanded }
                                    .padding(horizontal = 12.dp, vertical = 10.dp),
                                verticalAlignment = Alignment.CenterVertically
                            ) {
                                Text(
                                    text = if (expanded) "v" else ">",
                                    color = Color(0xFF475467),
                                    modifier = Modifier.width(20.dp)
                                )
                                Column(modifier = Modifier.weight(1f)) {
                                    Text(
                                        text = group.title,
                                        fontWeight = FontWeight.SemiBold,
                                        maxLines = 1,
                                        overflow = TextOverflow.Ellipsis
                                    )
                                    Text(
                                        text = "selected $selectedInChapter / ${group.sections.size}",
                                        color = Color(0xFF667085),
                                        style = MaterialTheme.typography.bodySmall
                                    )
                                }
                                Checkbox(
                                    checked = group.sections.isNotEmpty() && selectedInChapter == group.sections.size,
                                    onCheckedChange = { checked ->
                                        for (section in group.sections) {
                                            val selected = selectedEpisodeNos.contains(section.episodeNo)
                                            if (checked && !selected) {
                                                onToggleEpisode(section.episodeNo)
                                            } else if (!checked && selected) {
                                                onToggleEpisode(section.episodeNo)
                                            }
                                        }
                                    }
                                )
                            }
                        }

                        AnimatedVisibility(visible = expanded) {
                            Column(
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .padding(start = 10.dp, end = 8.dp, bottom = 8.dp),
                                verticalArrangement = Arrangement.spacedBy(6.dp)
                            ) {
                                for (episode in group.sections) {
                                    val checked = selectedEpisodeNos.contains(episode.episodeNo)
                                    val focused = focusedEpisodeNoState.value != null && focusedEpisodeNoState.value == episode.episodeNo
                                    val codeLabel = if (episode.chapterIndex > 0 && episode.sectionIndex > 0) {
                                        "C${episode.chapterIndex}.S${episode.sectionIndex}"
                                    } else {
                                        "S${episode.episodeNo}"
                                    }
                                    val startPage = episode.startPage
                                    val endPage = episode.endPage
                                    val metaLabel = when {
                                        startPage != null && endPage != null && endPage >= startPage -> "p$startPage-$endPage"
                                        startPage != null -> "from p$startPage"
                                        else -> "page pending"
                                    }
                                    Card(
                                        modifier = Modifier
                                            .fillMaxWidth()
                                            .clickable {
                                                focusedEpisodeNoState.value = episode.episodeNo
                                                previewPageState.value = resolveEpisodePreviewPage(
                                                    episode = episode,
                                                    fallbackPage = previewPageState.value,
                                                    totalPages = resolvedTotalPages
                                                )
                                            },
                                        colors = CardDefaults.cardColors(
                                            containerColor = when {
                                                focused -> Color(0xFFD8ECFF)
                                                checked -> Color(0xFFEFF8FF)
                                                else -> Color(0xFFFFFFFF)
                                            }
                                        )
                                    ) {
                                        Row(
                                            modifier = Modifier
                                                .fillMaxWidth()
                                                .padding(horizontal = 12.dp, vertical = 10.dp),
                                            verticalAlignment = Alignment.CenterVertically
                                        ) {
                                            Text(
                                                text = codeLabel,
                                                modifier = Modifier.width(68.dp),
                                                fontWeight = FontWeight.SemiBold,
                                                color = Color(0xFF175CD3)
                                            )
                                            Column(modifier = Modifier.weight(1f)) {
                                                Text(
                                                    text = episode.title.ifBlank { "Untitled section" },
                                                    maxLines = 1,
                                                    overflow = TextOverflow.Ellipsis
                                                )
                                                Text(
                                                    text = metaLabel,
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
                        }
                    }
                }

                BookPdfPreviewPane(
                    modifier = Modifier
                        .weight(1f)
                        .fillMaxSize(),
                    previewDocumentUri = previewDocumentUri,
                    fallbackResolvedUrl = probeResult.resolvedUrl,
                    previewPage = previewPageState.value,
                    totalPagesHint = resolvedTotalPages,
                    submitting = submitting,
                    onPreviewPageChange = { page ->
                        previewPageState.value = normalizeBookPage(page, resolvedTotalPages)
                    },
                    onUseCurrentPageAsConfirmedStart = {
                        onConfirmedStartPageChange(previewPageState.value)
                    }
                )
            }
        } else {
            LazyColumn(
                modifier = Modifier
                    .fillMaxWidth()
                    .height(380.dp)
                    .padding(horizontal = 8.dp),
                verticalArrangement = Arrangement.spacedBy(6.dp)
            ) {
                items(probeResult.episodes, key = { episode -> episode.episodeNo }) { episode ->
                    val checked = selectedEpisodeNos.contains(episode.episodeNo)
                    val codeLabel = "P${episode.episodeNo}"
                    val titleLabel = episode.title.ifBlank { "Untitled video" }
                    val metaLabel = formatDuration(episode.durationSec)
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
                                text = codeLabel,
                                modifier = Modifier.width(64.dp),
                                fontWeight = FontWeight.SemiBold,
                                color = Color(0xFF175CD3)
                            )
                            Column(modifier = Modifier.weight(1f)) {
                                Text(
                                    text = titleLabel,
                                    maxLines = 1,
                                    overflow = TextOverflow.Ellipsis
                                )
                                Text(
                                    text = metaLabel,
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
                text = "Selected ${selectedEpisodeNos.size} ${if (isBook) "sections" else "episodes"}",
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
                Text(if (isBook) "Submit book task" else "Submit collection")
            }
        }
        Spacer(modifier = Modifier.height(18.dp))
    }
}

private data class PdfPreviewRenderResult(
    val bitmap: Bitmap?,
    val pageNo: Int,
    val pageCount: Int,
    val errorMessage: String?
)

@Composable
private fun BookPdfPreviewPane(
    modifier: Modifier,
    previewDocumentUri: String?,
    fallbackResolvedUrl: String,
    previewPage: Int,
    totalPagesHint: Int,
    submitting: Boolean,
    onPreviewPageChange: (Int) -> Unit,
    onUseCurrentPageAsConfirmedStart: () -> Unit
) {
    val context = LocalContext.current
    val previewSource = remember(previewDocumentUri, fallbackResolvedUrl) {
        val local = previewDocumentUri?.trim().orEmpty()
        if (local.isNotEmpty()) {
            local
        } else {
            fallbackResolvedUrl.trim()
        }
    }
    val renderResult by produceState(
        initialValue = PdfPreviewRenderResult(
            bitmap = null,
            pageNo = normalizeBookPage(previewPage, totalPagesHint),
            pageCount = totalPagesHint,
            errorMessage = null
        ),
        key1 = previewSource,
        key2 = previewPage
    ) {
        value = renderPdfPreviewPage(
            context = context,
            previewSource = previewSource,
            requestedPage = previewPage,
            totalPagesHint = totalPagesHint
        )
    }
    val resolvedPageCount = if (renderResult.pageCount > 0) renderResult.pageCount else totalPagesHint
    val safePage = normalizeBookPage(renderResult.pageNo, resolvedPageCount)

    Card(
        modifier = modifier,
        colors = CardDefaults.cardColors(containerColor = Color(0xFFF8FAFC))
    ) {
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(8.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                OutlinedButton(
                    onClick = { onPreviewPageChange(safePage - 1) },
                    enabled = !submitting && safePage > 1
                ) {
                    Text("Prev")
                }
                Text(
                    text = if (resolvedPageCount > 0) "Page $safePage/$resolvedPageCount" else "Page $safePage",
                    modifier = Modifier.weight(1f),
                    color = Color(0xFF344054),
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
                OutlinedButton(
                    onClick = { onPreviewPageChange(safePage + 1) },
                    enabled = !submitting && (resolvedPageCount <= 0 || safePage < resolvedPageCount)
                ) {
                    Text("Next")
                }
            }

            OutlinedButton(
                onClick = onUseCurrentPageAsConfirmedStart,
                enabled = !submitting,
                modifier = Modifier.fillMaxWidth()
            ) {
                Text("Use current page as actual start")
            }

            Box(
                modifier = Modifier
                    .fillMaxWidth()
                    .weight(1f)
                    .background(Color(0xFFE4E7EC), RoundedCornerShape(12.dp)),
                contentAlignment = Alignment.Center
            ) {
                val bitmap = renderResult.bitmap
                val errorMessage = renderResult.errorMessage
                if (bitmap != null) {
                    Image(
                        bitmap = bitmap.asImageBitmap(),
                        contentDescription = "PDF preview page",
                        modifier = Modifier
                            .fillMaxSize()
                            .padding(6.dp),
                        contentScale = ContentScale.Fit
                    )
                } else if (errorMessage.isNullOrBlank()) {
                    CircularProgressIndicator()
                } else {
                    Text(
                        text = errorMessage,
                        color = Color(0xFF667085),
                        modifier = Modifier.padding(12.dp)
                    )
                }
            }
        }
    }
}

private suspend fun renderPdfPreviewPage(
    context: Context,
    previewSource: String,
    requestedPage: Int,
    totalPagesHint: Int
): PdfPreviewRenderResult {
    return withContext(Dispatchers.IO) {
        val source = previewSource.trim()
        if (source.isEmpty()) {
            return@withContext PdfPreviewRenderResult(
                bitmap = null,
                pageNo = normalizeBookPage(requestedPage, totalPagesHint),
                pageCount = totalPagesHint,
                errorMessage = "Preview source is empty"
            )
        }

        var descriptor: ParcelFileDescriptor? = null
        var renderer: PdfRenderer? = null
        var page: PdfRenderer.Page? = null
        try {
            descriptor = openPdfDescriptor(context, source)
            if (descriptor == null) {
                return@withContext PdfPreviewRenderResult(
                    bitmap = null,
                    pageNo = normalizeBookPage(requestedPage, totalPagesHint),
                    pageCount = totalPagesHint,
                    errorMessage = "Preview is available for uploaded local PDF only"
                )
            }
            renderer = PdfRenderer(descriptor)
            val pageCount = renderer.pageCount
            if (pageCount <= 0) {
                return@withContext PdfPreviewRenderResult(
                    bitmap = null,
                    pageNo = normalizeBookPage(requestedPage, totalPagesHint),
                    pageCount = totalPagesHint,
                    errorMessage = "No pages detected"
                )
            }
            val safePageNo = normalizeBookPage(requestedPage, pageCount)
            page = renderer.openPage(safePageNo - 1)
            val targetWidth = (page.width * 2).coerceIn(640, 2200)
            val targetHeight = (page.height * targetWidth.toFloat() / page.width.toFloat())
                .roundToInt()
                .coerceAtLeast(1)
            val bitmap = Bitmap.createBitmap(targetWidth, targetHeight, Bitmap.Config.ARGB_8888)
            bitmap.eraseColor(android.graphics.Color.WHITE)
            page.render(bitmap, null, null, PdfRenderer.Page.RENDER_MODE_FOR_DISPLAY)
            PdfPreviewRenderResult(
                bitmap = bitmap,
                pageNo = safePageNo,
                pageCount = pageCount,
                errorMessage = null
            )
        } catch (error: Exception) {
            PdfPreviewRenderResult(
                bitmap = null,
                pageNo = normalizeBookPage(requestedPage, totalPagesHint),
                pageCount = totalPagesHint,
                errorMessage = error.message ?: "Failed to render PDF preview"
            )
        } finally {
            try {
                page?.close()
            } catch (_: Exception) {
            }
            try {
                renderer?.close()
            } catch (_: Exception) {
            }
            try {
                descriptor?.close()
            } catch (_: Exception) {
            }
        }
    }
}

private fun openPdfDescriptor(context: Context, source: String): ParcelFileDescriptor? {
    return try {
        when {
            source.startsWith("content://", ignoreCase = true) -> {
                context.contentResolver.openFileDescriptor(Uri.parse(source), "r")
            }

            source.startsWith("file://", ignoreCase = true) -> {
                val path = Uri.parse(source).path ?: return null
                val file = File(path)
                if (!file.exists() || file.isDirectory) {
                    null
                } else {
                    ParcelFileDescriptor.open(file, ParcelFileDescriptor.MODE_READ_ONLY)
                }
            }

            source.startsWith("/") || Regex("^[a-zA-Z]:\\\\").containsMatchIn(source) -> {
                val normalizedPath = source.replace('\\', File.separatorChar)
                val file = File(normalizedPath)
                if (!file.exists() || file.isDirectory) {
                    null
                } else {
                    ParcelFileDescriptor.open(file, ParcelFileDescriptor.MODE_READ_ONLY)
                }
            }

            else -> null
        }
    } catch (_: Exception) {
        null
    }
}

private fun resolveInitialBookPreviewPage(result: VideoProbeResult, confirmedStartPage: Int?): Int {
    val candidate = confirmedStartPage
        ?: result.confirmedStartPage
        ?: result.detectedStartPage
        ?: result.appliedPageOffset?.let { it + 1 }
        ?: result.episodes.firstNotNullOfOrNull { episode -> episode.startPage }
        ?: 1
    return normalizeBookPage(candidate, result.totalPages)
}

private fun resolveEpisodePreviewPage(
    episode: VideoProbeEpisode,
    fallbackPage: Int,
    totalPages: Int
): Int {
    val candidate = episode.startPage ?: episode.endPage ?: fallbackPage
    return normalizeBookPage(candidate, totalPages)
}

private fun normalizeBookPage(candidate: Int?, totalPages: Int): Int {
    val safe = (candidate ?: 1).coerceAtLeast(1)
    return if (totalPages > 0) {
        safe.coerceIn(1, totalPages)
    } else {
        safe
    }
}
private data class BookChapterGroup(
    val key: String,
    val title: String,
    val sections: List<VideoProbeEpisode>
)

private fun buildBookChapterGroups(episodes: List<VideoProbeEpisode>): List<BookChapterGroup> {
    if (episodes.isEmpty()) {
        return emptyList()
    }
    data class MutableChapter(
        val chapterIndex: Int,
        var chapterTitle: String,
        val sections: MutableList<VideoProbeEpisode>
    )
    val grouped = LinkedHashMap<String, MutableChapter>()
    for (episode in episodes) {
        val chapterIndex = episode.chapterIndex.takeIf { it > 0 } ?: 0
        val defaultChapterTitle = if (chapterIndex > 0) {
            "Chapter $chapterIndex"
        } else {
            "Ungrouped Sections"
        }
        val rawChapterTitle = episode.chapterTitle.trim()
        val chapterTitle = if (rawChapterTitle.isBlank()) defaultChapterTitle else rawChapterTitle
        val key = if (chapterIndex > 0) {
            "chapter-$chapterIndex"
        } else {
            "chapter-unknown-${chapterTitle.lowercase()}"
        }
        val chapter = grouped.getOrPut(key) {
            MutableChapter(
                chapterIndex = chapterIndex,
                chapterTitle = chapterTitle,
                sections = mutableListOf()
            )
        }
        if (chapter.chapterTitle.isBlank()) {
            chapter.chapterTitle = chapterTitle
        }
        chapter.sections.add(episode)
    }
    return grouped.entries.map { entry ->
        val chapter = entry.value
        val displayTitle = if (chapter.chapterIndex > 0) {
            "C${chapter.chapterIndex} ${chapter.chapterTitle}"
        } else {
            chapter.chapterTitle
        }
        BookChapterGroup(
            key = entry.key,
            title = displayTitle,
            sections = chapter.sections.toList()
        )
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
                    Text("Back to tasks")
                }
                Text("Collection Manager", fontWeight = FontWeight.Bold, style = MaterialTheme.typography.titleLarge)
                TextButton(onClick = onRefresh) {
                    Text("Refresh")
                }
            }
        }
        if (collections.isEmpty()) {
            item {
                Text(
                    text = "No collections yet. Probe and submit one first.",
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
                            text = "${card.totalEpisodes} videos | submitted ${card.submittedCount}",
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
                    Text("Back", color = Color.White)
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
                        text = "Progress ${collection.completedCount}/${collection.totalEpisodes}",
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
                    text = "E${episode.episodeNo}",
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
                            Text("Read")
                        }
                    }

                    EpisodeDisplayStatus.FAILED -> {
                        OutlinedButton(onClick = onRetry) {
                            Text("Retry", color = Color(0xFFB42318))
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
                        text = episode.statusMessage ?: "Processing in background, status will update automatically...",
                        color = Color(0xFF475467),
                        style = MaterialTheme.typography.bodySmall
                    )
                }
            }
            AnimatedVisibility(visible = episode.displayStatus == EpisodeDisplayStatus.FAILED) {
                Text(
                    text = episode.statusMessage ?: "Processing failed, please retry",
                    color = Color(0xFFB42318),
                    style = MaterialTheme.typography.bodySmall
                )
            }
        }
    }
}

private fun statusLabel(status: EpisodeDisplayStatus): String {
    return when (status) {
        EpisodeDisplayStatus.IDLE -> "Not submitted"
        EpisodeDisplayStatus.QUEUED -> "Queued"
        EpisodeDisplayStatus.PROCESSING -> "Processing"
        EpisodeDisplayStatus.READY -> "Ready"
        EpisodeDisplayStatus.FAILED -> "Failed"
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
