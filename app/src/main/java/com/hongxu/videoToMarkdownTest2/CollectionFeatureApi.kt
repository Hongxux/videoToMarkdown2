package com.hongxu.videoToMarkdownTest2

import com.google.gson.annotations.SerializedName
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Path
import retrofit2.http.Query
import java.net.URI
import java.util.Locale

data class VideoProbeEpisode(
    val episodeNo: Int,
    val title: String,
    val durationSec: Double?,
    val episodeUrl: String
)

data class VideoProbeResult(
    val success: Boolean,
    val title: String,
    val platform: String,
    val resolvedUrl: String,
    val canonicalId: String,
    val collectionId: String,
    val isCollection: Boolean,
    val totalEpisodes: Int,
    val durationSec: Double?,
    val episodes: List<VideoProbeEpisode>
)

data class MobileCollectionSummary(
    val collectionId: String,
    val title: String,
    val platform: String,
    val canonicalId: String,
    val totalEpisodes: Int,
    val completedCount: Int,
    val episodes: List<MobileCollectionEpisode>
)

data class MobileCollectionEpisode(
    val episodeNo: Int,
    val title: String,
    val episodeUrl: String,
    val durationSec: Double?,
    val taskId: String?,
    val status: String?
)

data class CollectionBatchSubmitResult(
    val success: Boolean,
    val collectionId: String,
    val submittedCount: Int,
    val skippedCount: Int,
    val submitted: List<CollectionBatchSubmittedItem>,
    val skipped: List<CollectionBatchSkippedItem>,
    val message: String
)

data class CollectionBatchSubmittedItem(
    val episodeNo: Int,
    val title: String,
    val taskId: String,
    val status: String,
    val normalizedVideoUrl: String
)

data class CollectionBatchSkippedItem(
    val episodeNo: Int,
    val title: String,
    val reason: String,
    val taskId: String?
)

interface CollectionRetrofitApi {
    @GET("/api/mobile/video-info")
    suspend fun probeVideoInfoMobile(@Query("videoInput") videoInput: String): VideoProbeResponseDto

    @GET("/api/video-info")
    suspend fun probeVideoInfoLegacy(@Query("videoInput") videoInput: String): VideoProbeResponseDto

    @POST("/api/mobile/tasks/submit")
    suspend fun submitTask(@Body request: MobileTaskSubmitRequestDto): MobileTaskSubmitResponseDto

    @GET("/api/mobile/collections")
    suspend fun listCollections(): CollectionListResponseDto

    @POST("/api/mobile/collections/{collectionId}/submit-batch")
    suspend fun submitBatch(
        @Path("collectionId") collectionId: String,
        @Body request: CollectionBatchSubmitRequestDto
    ): CollectionBatchSubmitResponseDto
}

data class MobileTaskSubmitRequestDto(
    @SerializedName("videoUrl")
    val videoUrl: String,
    @SerializedName("collectionId")
    val collectionId: String? = null,
    @SerializedName("episodeNo")
    val episodeNo: Int? = null,
    @SerializedName("userId")
    val userId: String? = null,
    @SerializedName("outputDir")
    val outputDir: String? = null
)

data class MobileTaskSubmitResponseDto(
    @SerializedName("success")
    val success: Boolean = false,
    @SerializedName("taskId")
    val taskId: String = "",
    @SerializedName("status")
    val status: String = "",
    @SerializedName("message")
    val message: String = ""
)

data class CollectionBatchSubmitRequestDto(
    @SerializedName("episodeNos")
    val episodeNos: List<Int>? = null,
    @SerializedName("userId")
    val userId: String? = null,
    @SerializedName("outputDir")
    val outputDir: String? = null
)

data class VideoProbeResponseDto(
    @SerializedName("success")
    val success: Boolean = false,
    @SerializedName("title")
    val title: String = "",
    @SerializedName("platform")
    val platform: String = "",
    @SerializedName("resolvedUrl")
    val resolvedUrl: String = "",
    @SerializedName("canonicalId")
    val canonicalId: String = "",
    @SerializedName("collectionId")
    val collectionId: String = "",
    @SerializedName("isCollection")
    val isCollection: Boolean = false,
    @SerializedName("totalEpisodes")
    val totalEpisodes: Int = 0,
    @SerializedName("durationSec")
    val durationSec: Double? = null,
    @SerializedName("episodes")
    val episodes: List<VideoProbeEpisodeDto> = emptyList()
)

data class VideoProbeEpisodeDto(
    @SerializedName("index")
    val index: Int = 0,
    @SerializedName("title")
    val title: String = "",
    @SerializedName("durationSec")
    val durationSec: Double? = null,
    @SerializedName("episodeUrl")
    val episodeUrl: String = ""
)

data class CollectionListResponseDto(
    @SerializedName("collections")
    val collections: List<CollectionSummaryDto> = emptyList()
)

data class CollectionSummaryDto(
    @SerializedName("collectionId")
    val collectionId: String = "",
    @SerializedName("title")
    val title: String = "",
    @SerializedName("platform")
    val platform: String = "",
    @SerializedName("canonicalId")
    val canonicalId: String = "",
    @SerializedName("totalEpisodes")
    val totalEpisodes: Int = 0,
    @SerializedName("completedCount")
    val completedCount: Int = 0,
    @SerializedName("episodes")
    val episodes: List<CollectionEpisodeDto> = emptyList()
)

data class CollectionEpisodeDto(
    @SerializedName("episodeNo")
    val episodeNo: Int = 0,
    @SerializedName("title")
    val title: String = "",
    @SerializedName("episodeUrl")
    val episodeUrl: String = "",
    @SerializedName("durationSec")
    val durationSec: Double? = null,
    @SerializedName("taskId")
    val taskId: String? = null,
    @SerializedName("status")
    val status: String? = null
)

data class CollectionBatchSubmitResponseDto(
    @SerializedName("success")
    val success: Boolean = false,
    @SerializedName("collectionId")
    val collectionId: String = "",
    @SerializedName("submittedCount")
    val submittedCount: Int = 0,
    @SerializedName("skippedCount")
    val skippedCount: Int = 0,
    @SerializedName("submitted")
    val submitted: List<CollectionBatchSubmittedDto> = emptyList(),
    @SerializedName("skipped")
    val skipped: List<CollectionBatchSkippedDto> = emptyList(),
    @SerializedName("message")
    val message: String = ""
)

data class CollectionBatchSubmittedDto(
    @SerializedName("episodeNo")
    val episodeNo: Int = 0,
    @SerializedName("title")
    val title: String = "",
    @SerializedName("taskId")
    val taskId: String = "",
    @SerializedName("status")
    val status: String = "",
    @SerializedName("normalizedVideoUrl")
    val normalizedVideoUrl: String = ""
)

data class CollectionBatchSkippedDto(
    @SerializedName("episodeNo")
    val episodeNo: Int = 0,
    @SerializedName("title")
    val title: String = "",
    @SerializedName("reason")
    val reason: String = "",
    @SerializedName("taskId")
    val taskId: String? = null
)

object CollectionApiFactory {
    fun create(baseUrl: String): CollectionRetrofitApi {
        val logging = HttpLoggingInterceptor().apply {
            level = HttpLoggingInterceptor.Level.NONE
        }
        val client = OkHttpClient.Builder()
            .addInterceptor(logging)
            .build()
        return Retrofit.Builder()
            .baseUrl(normalizeBaseUrl(baseUrl))
            .client(client)
            .addConverterFactory(GsonConverterFactory.create())
            .build()
            .create(CollectionRetrofitApi::class.java)
    }

    fun toWebSocketUrl(baseUrl: String): String {
        val normalized = normalizeBaseUrl(baseUrl)
        val uri = URI(normalized)
        val scheme = when (uri.scheme?.lowercase(Locale.ROOT)) {
            "https" -> "wss"
            "http" -> "ws"
            else -> "ws"
        }
        val host = uri.host ?: "localhost"
        val portPart = if (uri.port > 0) ":${uri.port}" else ""
        return "$scheme://$host$portPart/ws/tasks"
    }

    private fun normalizeBaseUrl(baseUrl: String): String {
        val trimmed = baseUrl.trim().ifEmpty { BuildConfig.MOBILE_API_BASE_URL }.trimEnd('/')
        return "$trimmed/"
    }
}

fun VideoProbeResponseDto.toDomain(): VideoProbeResult {
    return VideoProbeResult(
        success = success,
        title = title,
        platform = platform,
        resolvedUrl = resolvedUrl,
        canonicalId = canonicalId,
        collectionId = collectionId,
        isCollection = isCollection,
        totalEpisodes = totalEpisodes,
        durationSec = durationSec,
        episodes = episodes.map { episode ->
            VideoProbeEpisode(
                episodeNo = episode.index,
                title = episode.title,
                durationSec = episode.durationSec,
                episodeUrl = episode.episodeUrl
            )
        }
    )
}

fun CollectionSummaryDto.toDomain(): MobileCollectionSummary {
    return MobileCollectionSummary(
        collectionId = collectionId,
        title = title,
        platform = platform,
        canonicalId = canonicalId,
        totalEpisodes = totalEpisodes,
        completedCount = completedCount,
        episodes = episodes.map { it.toDomain() }
    )
}

fun CollectionEpisodeDto.toDomain(): MobileCollectionEpisode {
    return MobileCollectionEpisode(
        episodeNo = episodeNo,
        title = title,
        episodeUrl = episodeUrl,
        durationSec = durationSec,
        taskId = taskId,
        status = status
    )
}

fun CollectionBatchSubmitResponseDto.toDomain(): CollectionBatchSubmitResult {
    return CollectionBatchSubmitResult(
        success = success,
        collectionId = collectionId,
        submittedCount = submittedCount,
        skippedCount = skippedCount,
        submitted = submitted.map {
            CollectionBatchSubmittedItem(
                episodeNo = it.episodeNo,
                title = it.title,
                taskId = it.taskId,
                status = it.status,
                normalizedVideoUrl = it.normalizedVideoUrl
            )
        },
        skipped = skipped.map {
            CollectionBatchSkippedItem(
                episodeNo = it.episodeNo,
                title = it.title,
                reason = it.reason,
                taskId = it.taskId
            )
        },
        message = message
    )
}
