package com.hongxu.videoToMarkdownTest2

import android.content.Context
import androidx.room.withTransaction
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import retrofit2.HttpException
import java.util.Locale

data class CollectionCardUi(
    val collectionId: String,
    val title: String,
    val platform: String,
    val canonicalId: String,
    val totalEpisodes: Int,
    val submittedCount: Int,
    val completedCount: Int,
    val progress: Float
)

data class CollectionEpisodeUi(
    val collectionId: String,
    val episodeNo: Int,
    val title: String,
    val episodeUrl: String,
    val durationSec: Double?,
    val taskId: String?,
    val status: String?,
    val statusMessage: String?,
    val displayStatus: EpisodeDisplayStatus
)

enum class EpisodeDisplayStatus {
    IDLE,
    QUEUED,
    PROCESSING,
    READY,
    FAILED
}

class CollectionFeatureRepository(
    context: Context,
    private val apiBaseUrl: String
) {
    private val mobileUserId = MobileClientIdentity.resolveUserId(context.applicationContext)
    private val database = CollectionFeatureDatabase.getInstance(context)
    private val dao = database.collectionDao()
    private val api = CollectionApiFactory.create(apiBaseUrl)
    private val repositoryScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val realtimeClient = CollectionRealtimeClient(
        wsEndpoint = CollectionApiFactory.toWebSocketUrl(apiBaseUrl),
        userId = mobileUserId,
        onTaskUpdate = { taskId, status, statusMessage ->
            repositoryScope.launch {
                dao.updateEpisodeStatusByTaskId(
                    taskId = taskId,
                    status = status,
                    statusMessage = statusMessage,
                    updatedAt = System.currentTimeMillis()
                )
            }
        }
    )

    fun observeCollectionCards(): Flow<List<CollectionCardUi>> {
        return dao.observeCollectionSummaries().map { rows ->
            rows.map { row ->
                val safeTotal = row.totalEpisodes.coerceAtLeast(1)
                val progress = (row.completedCount.toFloat() / safeTotal.toFloat()).coerceIn(0f, 1f)
                CollectionCardUi(
                    collectionId = row.collectionId,
                    title = row.title.ifBlank { row.collectionId },
                    platform = row.platform,
                    canonicalId = row.canonicalId,
                    totalEpisodes = row.totalEpisodes,
                    submittedCount = row.submittedCount,
                    completedCount = row.completedCount,
                    progress = progress
                )
            }
        }
    }

    fun observeEpisodes(collectionId: String): Flow<List<CollectionEpisodeUi>> {
        return dao.observeEpisodes(collectionId).map { rows ->
            rows.map { row ->
                CollectionEpisodeUi(
                    collectionId = row.collectionId,
                    episodeNo = row.episodeNo,
                    title = row.title.ifBlank { "第${row.episodeNo}集" },
                    episodeUrl = row.episodeUrl,
                    durationSec = row.durationSec,
                    taskId = row.taskId,
                    status = row.status,
                    statusMessage = row.statusMessage,
                    displayStatus = toDisplayStatus(row.status, row.taskId)
                )
            }
        }
    }

    suspend fun findCollection(collectionId: String): CollectionEntity? {
        return dao.findCollection(collectionId)
    }

    suspend fun probeVideoInfo(videoInput: String, pageOffset: Int? = null): VideoProbeResult {
        return try {
            api.probeVideoInfoMobile(videoInput, pageOffset).toDomain()
        } catch (error: HttpException) {
            if (error.code() != 404) {
                throw error
            }
            api.probeVideoInfoLegacy(videoInput, pageOffset).toDomain()
        }
    }

    suspend fun submitSingleTask(
        videoUrl: String,
        collectionId: String? = null,
        episodeNo: Int? = null,
        chapterSelector: String? = null,
        sectionSelector: String? = null,
        splitByChapter: Boolean? = null,
        splitBySection: Boolean? = null,
        pageOffset: Int? = null
    ): MobileTaskSubmitResponseDto {
        return api.submitTask(
            MobileTaskSubmitRequestDto(
                videoUrl = videoUrl,
                collectionId = collectionId?.takeIf { it.isNotBlank() },
                episodeNo = episodeNo,
                userId = mobileUserId,
                chapterSelector = chapterSelector?.takeIf { it.isNotBlank() },
                sectionSelector = sectionSelector?.takeIf { it.isNotBlank() },
                splitByChapter = splitByChapter,
                splitBySection = splitBySection,
                pageOffset = pageOffset
            )
        )
    }

    suspend fun refreshCollections(): List<MobileCollectionSummary> {
        val response = api.listCollections()
        val collections = response.collections.map { it.toDomain() }
        val now = System.currentTimeMillis()
        database.withTransaction {
            collections.forEach { collection ->
                dao.upsertCollections(
                    listOf(
                        CollectionEntity(
                            collectionId = collection.collectionId,
                            title = collection.title,
                            platform = collection.platform,
                            canonicalId = collection.canonicalId,
                            totalEpisodes = collection.totalEpisodes,
                            updatedAtEpochMs = now
                        )
                    )
                )
                dao.deleteEpisodesByCollection(collection.collectionId)
                dao.upsertEpisodes(
                    collection.episodes.map { episode ->
                        CollectionEpisodeEntity(
                            collectionId = collection.collectionId,
                            episodeNo = episode.episodeNo,
                            title = episode.title,
                            episodeUrl = episode.episodeUrl,
                            durationSec = episode.durationSec,
                            taskId = episode.taskId?.trim()?.ifEmpty { null },
                            status = episode.status?.trim()?.ifEmpty { null },
                            statusMessage = null,
                            updatedAtEpochMs = now
                        )
                    }
                )
            }
        }
        return collections
    }

    suspend fun submitCollectionBatch(
        collectionId: String,
        episodeNos: List<Int>,
        userId: String? = mobileUserId,
        outputDir: String? = null
    ): CollectionBatchSubmitResult {
        val response = api.submitBatch(
            collectionId = collectionId,
            request = CollectionBatchSubmitRequestDto(
                episodeNos = episodeNos.ifEmpty { null },
                userId = userId,
                outputDir = outputDir
            )
        ).toDomain()
        val now = System.currentTimeMillis()
        response.submitted.forEach { item ->
            dao.updateEpisodeSubmission(
                collectionId = collectionId,
                episodeNo = item.episodeNo,
                taskId = item.taskId,
                status = item.status,
                statusMessage = "已提交到队列",
                updatedAt = now
            )
        }
        refreshCollections()
        return response
    }

    suspend fun subscribeCollectionTasks(collectionId: String) {
        realtimeClient.connectOrUpdate(collectionId)
    }

    fun stopRealtime() {
        realtimeClient.disconnect()
    }

    private fun toDisplayStatus(status: String?, taskId: String?): EpisodeDisplayStatus {
        val normalizedStatus = status?.trim()?.uppercase(Locale.ROOT).orEmpty()
        if (normalizedStatus.isEmpty()) {
            return if (taskId.isNullOrBlank()) EpisodeDisplayStatus.IDLE else EpisodeDisplayStatus.QUEUED
        }
        if (normalizedStatus == "COMPLETED" || normalizedStatus == "SUCCESS") {
            return EpisodeDisplayStatus.READY
        }
        if (normalizedStatus == "FAILED" || normalizedStatus == "ERROR") {
            return EpisodeDisplayStatus.FAILED
        }
        if (normalizedStatus == "QUEUED" || normalizedStatus == "PENDING") {
            return EpisodeDisplayStatus.QUEUED
        }
        return EpisodeDisplayStatus.PROCESSING
    }
}
