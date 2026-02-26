package com.hongxu.videoToMarkdownTest2

import android.content.Context
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.withContext
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId

/**
 * 阅读足迹仓库。
 *
 * 封装对 [ReadingFootprintDao] 的操作，提供面向业务的写入 API
 * （如 recordVideoTaskCreated、recordArticleOpened 等）
 * 以及面向 UI 的查询 API（按天分组的 Flow）。
 */
class ReadingFootprintRepository(context: Context) {

    private val dao: ReadingFootprintDao =
        ReadingFootprintDatabase.getInstance(context.applicationContext).footprintDao()

    // ──────────────────────────────────────────────────────────────────────────
    // 写入 API：各种场景的足迹记录
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * 记录：视频任务被创建/提交。
     */
    suspend fun recordVideoTaskCreated(
        taskId: String,
        taskTitle: String
    ) {
        dao.insert(
            ReadingFootprintEntity(
                eventType = FootprintEventTypes.VIDEO_TASK_CREATED,
                timestampMs = System.currentTimeMillis(),
                taskId = taskId,
                taskTitle = taskTitle
            )
        )
    }

    /**
     * 记录：文章/任务被打开阅读。
     */
    suspend fun recordArticleOpened(
        taskId: String,
        taskTitle: String
    ) {
        dao.insert(
            ReadingFootprintEntity(
                eventType = FootprintEventTypes.ARTICLE_OPENED,
                timestampMs = System.currentTimeMillis(),
                taskId = taskId,
                taskTitle = taskTitle
            )
        )
    }

    /**
     * 记录：段落级加粗/收藏。
     */
    suspend fun recordParagraphBold(
        taskId: String,
        taskTitle: String,
        blockId: String,
        snippetText: String,
        scrollIndex: Int
    ) {
        dao.insert(
            ReadingFootprintEntity(
                eventType = FootprintEventTypes.PARAGRAPH_BOLD,
                timestampMs = System.currentTimeMillis(),
                taskId = taskId,
                taskTitle = taskTitle,
                snippetText = snippetText.take(200),
                anchorBlockId = blockId,
                anchorScrollIndex = scrollIndex
            )
        )
    }

    /**
     * 记录：Token 级双击分词。
     */
    suspend fun recordTokenDoubleClick(
        taskId: String,
        taskTitle: String,
        blockId: String,
        token: String,
        tokenStart: Int,
        tokenEnd: Int,
        scrollIndex: Int
    ) {
        dao.insert(
            ReadingFootprintEntity(
                eventType = FootprintEventTypes.TOKEN_DOUBLE_CLICK,
                timestampMs = System.currentTimeMillis(),
                taskId = taskId,
                taskTitle = taskTitle,
                snippetText = token.take(100),
                anchorBlockId = blockId,
                anchorTokenStart = tokenStart,
                anchorTokenEnd = tokenEnd,
                anchorScrollIndex = scrollIndex
            )
        )
    }

    /**
     * 记录：段落级批注。
     */
    suspend fun recordAnnotationAdded(
        taskId: String,
        taskTitle: String,
        blockId: String,
        annotationText: String,
        scrollIndex: Int
    ) {
        dao.insert(
            ReadingFootprintEntity(
                eventType = FootprintEventTypes.ANNOTATION_ADDED,
                timestampMs = System.currentTimeMillis(),
                taskId = taskId,
                taskTitle = taskTitle,
                snippetText = annotationText.take(300),
                anchorBlockId = blockId,
                anchorScrollIndex = scrollIndex
            )
        )
    }

    /**
     * 记录：点击 insight_tag 查看卡片。
     */
    suspend fun recordInsightCardViewed(
        taskId: String,
        taskTitle: String,
        blockId: String,
        insightTag: String,
        scrollIndex: Int
    ) {
        dao.insert(
            ReadingFootprintEntity(
                eventType = FootprintEventTypes.INSIGHT_CARD_VIEWED,
                timestampMs = System.currentTimeMillis(),
                taskId = taskId,
                taskTitle = taskTitle,
                snippetText = insightTag.take(100),
                anchorBlockId = blockId,
                anchorScrollIndex = scrollIndex
            )
        )
    }

    // ──────────────────────────────────────────────────────────────────────────
    // 查询 API
    // ──────────────────────────────────────────────────────────────────────────

    /**
     * 观察所有足迹事件（时间倒序），用于时间轴视图渲染。
     */
    fun observeAllFootprints(): Flow<List<ReadingFootprintEntity>> {
        return dao.observeAllFootprints()
    }

    /**
     * 观察最近 N 条足迹。
     */
    fun observeRecentFootprints(limit: Int = 50): Flow<List<ReadingFootprintEntity>> {
        return dao.observeRecentFootprints(limit)
    }

    /**
     * 删除单条足迹。
     */
    suspend fun deleteFootprint(id: Long) {
        dao.deleteById(id)
    }

    /**
     * 清空所有足迹。
     */
    suspend fun clearAll() {
        dao.deleteAll()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// 时间分组工具
// ──────────────────────────────────────────────────────────────────────────────

/**
 * 足迹时间分组标签。
 */
enum class FootprintTimeGroup {
    TODAY,
    YESTERDAY,
    THIS_WEEK,
    EARLIER
}

/**
 * 将足迹列表按时间分组（今天、昨天、本周、更早），返回有序的分组映射。
 */
fun groupFootprintsByTime(
    events: List<ReadingFootprintEntity>,
    zone: ZoneId = ZoneId.systemDefault()
): Map<FootprintTimeGroup, List<ReadingFootprintEntity>> {
    val today = LocalDate.now(zone)
    val yesterday = today.minusDays(1)
    val weekStart = today.minusDays(today.dayOfWeek.value.toLong() - 1)

    val groups = linkedMapOf<FootprintTimeGroup, MutableList<ReadingFootprintEntity>>()
    FootprintTimeGroup.entries.forEach { group ->
        groups[group] = mutableListOf()
    }

    events.forEach { event ->
        val eventDate = Instant.ofEpochMilli(event.timestampMs)
            .atZone(zone)
            .toLocalDate()
        val group = when {
            eventDate == today -> FootprintTimeGroup.TODAY
            eventDate == yesterday -> FootprintTimeGroup.YESTERDAY
            !eventDate.isBefore(weekStart) -> FootprintTimeGroup.THIS_WEEK
            else -> FootprintTimeGroup.EARLIER
        }
        groups[group]!!.add(event)
    }

    return groups.filterValues { it.isNotEmpty() }
}
