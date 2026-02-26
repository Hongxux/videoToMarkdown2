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

@Entity(tableName = "collection_cards")
data class CollectionEntity(
    @PrimaryKey
    @ColumnInfo(name = "collection_id")
    val collectionId: String,
    @ColumnInfo(name = "title")
    val title: String,
    @ColumnInfo(name = "platform")
    val platform: String,
    @ColumnInfo(name = "canonical_id")
    val canonicalId: String,
    @ColumnInfo(name = "total_episodes")
    val totalEpisodes: Int,
    @ColumnInfo(name = "updated_at_epoch_ms")
    val updatedAtEpochMs: Long
)

@Entity(
    tableName = "collection_episodes",
    primaryKeys = ["collection_id", "episode_no"]
)
data class CollectionEpisodeEntity(
    @ColumnInfo(name = "collection_id")
    val collectionId: String,
    @ColumnInfo(name = "episode_no")
    val episodeNo: Int,
    @ColumnInfo(name = "title")
    val title: String,
    @ColumnInfo(name = "episode_url")
    val episodeUrl: String,
    @ColumnInfo(name = "duration_sec")
    val durationSec: Double?,
    @ColumnInfo(name = "task_id")
    val taskId: String?,
    @ColumnInfo(name = "status")
    val status: String?,
    @ColumnInfo(name = "status_message")
    val statusMessage: String?,
    @ColumnInfo(name = "updated_at_epoch_ms")
    val updatedAtEpochMs: Long
)

data class CollectionSummaryProjection(
    @ColumnInfo(name = "collection_id")
    val collectionId: String,
    @ColumnInfo(name = "title")
    val title: String,
    @ColumnInfo(name = "platform")
    val platform: String,
    @ColumnInfo(name = "canonical_id")
    val canonicalId: String,
    @ColumnInfo(name = "total_episodes")
    val totalEpisodes: Int,
    @ColumnInfo(name = "submitted_count")
    val submittedCount: Int,
    @ColumnInfo(name = "completed_count")
    val completedCount: Int,
    @ColumnInfo(name = "updated_at_epoch_ms")
    val updatedAtEpochMs: Long
)

@Dao
interface CollectionFeatureDao {
    @Query(
        """
        SELECT
            c.collection_id,
            c.title,
            c.platform,
            c.canonical_id,
            c.total_episodes,
            COUNT(CASE WHEN e.task_id IS NOT NULL AND e.task_id != '' THEN 1 END) AS submitted_count,
            COUNT(CASE WHEN UPPER(COALESCE(e.status, '')) IN ('COMPLETED', 'SUCCESS') THEN 1 END) AS completed_count,
            c.updated_at_epoch_ms
        FROM collection_cards c
        LEFT JOIN collection_episodes e
          ON e.collection_id = c.collection_id
        GROUP BY c.collection_id, c.title, c.platform, c.canonical_id, c.total_episodes, c.updated_at_epoch_ms
        ORDER BY c.updated_at_epoch_ms DESC
        """
    )
    fun observeCollectionSummaries(): Flow<List<CollectionSummaryProjection>>

    @Query(
        """
        SELECT * FROM collection_episodes
        WHERE collection_id = :collectionId
        ORDER BY episode_no ASC
        """
    )
    fun observeEpisodes(collectionId: String): Flow<List<CollectionEpisodeEntity>>

    @Query("SELECT * FROM collection_cards WHERE collection_id = :collectionId LIMIT 1")
    suspend fun findCollection(collectionId: String): CollectionEntity?

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertCollections(collections: List<CollectionEntity>)

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertEpisodes(episodes: List<CollectionEpisodeEntity>)

    @Query("DELETE FROM collection_episodes WHERE collection_id = :collectionId")
    suspend fun deleteEpisodesByCollection(collectionId: String)

    @Query("SELECT task_id FROM collection_episodes WHERE collection_id = :collectionId AND task_id IS NOT NULL AND task_id != ''")
    suspend fun listTaskIdsByCollection(collectionId: String): List<String>

    @Query(
        """
        UPDATE collection_episodes
        SET task_id = :taskId,
            status = :status,
            status_message = :statusMessage,
            updated_at_epoch_ms = :updatedAt
        WHERE collection_id = :collectionId AND episode_no = :episodeNo
        """
    )
    suspend fun updateEpisodeSubmission(
        collectionId: String,
        episodeNo: Int,
        taskId: String?,
        status: String?,
        statusMessage: String?,
        updatedAt: Long
    )

    @Query(
        """
        UPDATE collection_episodes
        SET status = :status,
            status_message = :statusMessage,
            updated_at_epoch_ms = :updatedAt
        WHERE task_id = :taskId
        """
    )
    suspend fun updateEpisodeStatusByTaskId(
        taskId: String,
        status: String,
        statusMessage: String?,
        updatedAt: Long
    )
}

@Database(
    entities = [CollectionEntity::class, CollectionEpisodeEntity::class],
    version = 1,
    exportSchema = false
)
abstract class CollectionFeatureDatabase : RoomDatabase() {
    abstract fun collectionDao(): CollectionFeatureDao

    companion object {
        @Volatile
        private var instance: CollectionFeatureDatabase? = null

        fun getInstance(context: Context): CollectionFeatureDatabase {
            return instance ?: synchronized(this) {
                instance ?: Room.databaseBuilder(
                    context.applicationContext,
                    CollectionFeatureDatabase::class.java,
                    "collection_feature.db"
                ).build().also { created ->
                    instance = created
                }
            }
        }
    }
}
