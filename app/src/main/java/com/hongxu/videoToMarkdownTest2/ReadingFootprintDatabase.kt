package com.hongxu.videoToMarkdownTest2

import android.content.Context
import androidx.room.ColumnInfo
import androidx.room.Dao
import androidx.room.Database
import androidx.room.Entity
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.PrimaryKey
import androidx.room.Query
import androidx.room.Room
import androidx.room.RoomDatabase
import kotlinx.coroutines.flow.Flow

// ─── Footprint event types ───────────────────────────────────────────────────

/**
 * 足迹事件类型枚举。
 * 用于区分宏观任务事件和微观阅读交互事件。
 */
object FootprintEventTypes {
    /** 视频任务被创建/提交。 */
    const val VIDEO_TASK_CREATED = "VIDEO_TASK_CREATED"

    /** 文章/任务被打开阅读。 */
    const val ARTICLE_OPENED = "ARTICLE_OPENED"

    /** 段落级加粗（左滑/右滑收藏）。 */
    const val PARAGRAPH_BOLD = "PARAGRAPH_BOLD"

    /** Token 级双击分词。 */
    const val TOKEN_DOUBLE_CLICK = "TOKEN_DOUBLE_CLICK"

    /** 段落级批注。 */
    const val ANNOTATION_ADDED = "ANNOTATION_ADDED"

    /** 点击 insight_tag 查看卡片。 */
    const val INSIGHT_CARD_VIEWED = "INSIGHT_CARD_VIEWED"
}

// ─── Room Entity ─────────────────────────────────────────────────────────────

/**
 * 阅读足迹事件实体。
 *
 * 既存储宏观任务事件（如视频任务创建、文章打开），
 * 也存储微粒度交互事件（如段落加粗、Token 双击、批注、Insight 卡片查看）。
 * 所有微粒度事件都携带 [anchorBlockId] 和 [anchorTokenStart]/[anchorTokenEnd]
 * 用于实现"点击回溯到原文精确位置"。
 */
@Entity(tableName = "reading_footprints")
data class ReadingFootprintEntity(
    @PrimaryKey(autoGenerate = true)
    @ColumnInfo(name = "id")
    val id: Long = 0L,

    /** 事件类型，见 [FootprintEventTypes]。 */
    @ColumnInfo(name = "event_type")
    val eventType: String,

    /** 事件发生的毫秒级时间戳。 */
    @ColumnInfo(name = "timestamp_ms")
    val timestampMs: Long,

    /**
     * 关联的任务 ID（对于 ARTICLE_OPENED、PARAGRAPH_BOLD 等操作，
     * 指向被阅读的任务；对于 VIDEO_TASK_CREATED，指向新创建的任务）。
     */
    @ColumnInfo(name = "task_id")
    val taskId: String,

    /** 文章/任务的标题（冗余存储，避免查询时再 join）。 */
    @ColumnInfo(name = "task_title")
    val taskTitle: String,

    /**
     * 交互上下文文本片段。
     * - 对于 TOKEN_DOUBLE_CLICK：被双击的 token 文本。
     * - 对于 PARAGRAPH_BOLD：被加粗段落的摘要/首句。
     * - 对于 ANNOTATION_ADDED：用户输入的批注内容。
     * - 对于 INSIGHT_CARD_VIEWED：insight tag 名称。
     * - 对于 VIDEO_TASK_CREATED / ARTICLE_OPENED：可为空。
     */
    @ColumnInfo(name = "snippet_text")
    val snippetText: String = "",

    // ── 空间锚点（Semantic Anchor）──────────────────────────────────────────
    // 用于"点击回溯原文"功能的精准定位。

    /** 该交互发生在哪个 SemanticBlock 内（对应 SemanticNode.id）。 */
    @ColumnInfo(name = "anchor_block_id")
    val anchorBlockId: String = "",

    /** Token 级偏移起始位置（仅 TOKEN_DOUBLE_CLICK 等 Token 级操作有值）。 */
    @ColumnInfo(name = "anchor_token_start")
    val anchorTokenStart: Int = -1,

    /** Token 级偏移结束位置。 */
    @ColumnInfo(name = "anchor_token_end")
    val anchorTokenEnd: Int = -1,

    /** 粗略的 LazyColumn 第一可见项索引（用于平滑恢复滚动）。 */
    @ColumnInfo(name = "anchor_scroll_index")
    val anchorScrollIndex: Int = -1
)

// ─── Room DAO ────────────────────────────────────────────────────────────────

@Dao
interface ReadingFootprintDao {

    /**
     * 插入一条足迹事件。
     */
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insert(event: ReadingFootprintEntity): Long

    /**
     * 批量插入足迹事件（用于批量写入）。
     */
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insertAll(events: List<ReadingFootprintEntity>)

    /**
     * 查询某一天范围内的足迹事件，按时间倒序排列。
     * [startMs] / [endMs] 是 UTC 毫秒时间戳。
     */
    @Query(
        """
        SELECT * FROM reading_footprints
        WHERE timestamp_ms >= :startMs AND timestamp_ms < :endMs
        ORDER BY timestamp_ms DESC
        """
    )
    fun observeFootprintsBetween(startMs: Long, endMs: Long): Flow<List<ReadingFootprintEntity>>

    /**
     * 查询所有足迹事件，按时间倒序排列（用于分组渲染今天/昨天/更早）。
     */
    @Query("SELECT * FROM reading_footprints ORDER BY timestamp_ms DESC")
    fun observeAllFootprints(): Flow<List<ReadingFootprintEntity>>

    /**
     * 查询最近 N 条足迹（用于首页快速预览）。
     */
    @Query("SELECT * FROM reading_footprints ORDER BY timestamp_ms DESC LIMIT :limit")
    fun observeRecentFootprints(limit: Int): Flow<List<ReadingFootprintEntity>>

    /**
     * 删除单条足迹。
     */
    @Query("DELETE FROM reading_footprints WHERE id = :id")
    suspend fun deleteById(id: Long)

    /**
     * 清空所有足迹（谨慎使用）。
     */
    @Query("DELETE FROM reading_footprints")
    suspend fun deleteAll()

    /**
     * 按任务 ID 查询足迹数量（用于显示"这篇文章产生了 N 次足迹"）。
     */
    @Query("SELECT COUNT(*) FROM reading_footprints WHERE task_id = :taskId")
    suspend fun countByTaskId(taskId: String): Int
}

// ─── Room Database ───────────────────────────────────────────────────────────

@Database(
    entities = [ReadingFootprintEntity::class],
    version = 1,
    exportSchema = false
)
abstract class ReadingFootprintDatabase : RoomDatabase() {
    abstract fun footprintDao(): ReadingFootprintDao

    companion object {
        @Volatile
        private var instance: ReadingFootprintDatabase? = null

        fun getInstance(context: Context): ReadingFootprintDatabase {
            return instance ?: synchronized(this) {
                instance ?: Room.databaseBuilder(
                    context.applicationContext,
                    ReadingFootprintDatabase::class.java,
                    "reading_footprints.db"
                ).build().also { created ->
                    instance = created
                }
            }
        }
    }
}
