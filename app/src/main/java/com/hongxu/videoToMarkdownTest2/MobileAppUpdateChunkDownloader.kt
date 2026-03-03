package com.hongxu.videoToMarkdownTest2

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.security.MessageDigest
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.ceil
import kotlin.math.max
import kotlin.math.min

internal data class MobileAppChunkDownloadRequest(
    val downloadUrl: String,
    val destinationFile: File,
    val expectedSha256: String
)

internal class MobileAppUpdateChunkDownloader(
    private val chunkSizeBytes: Long = resolveChunkSizeBytes(),
    private val minChunkedDownloadBytes: Long = resolveMinChunkedDownloadBytes(),
    private val maxParallelChunks: Int = resolveMaxParallelChunks(),
    private val httpClient: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .callTimeout(0, TimeUnit.MILLISECONDS)
        .build()
) {
    suspend fun downloadApk(
        request: MobileAppChunkDownloadRequest,
        onProgress: (Int?) -> Unit
    ) = withContext(Dispatchers.IO) {
        val destination = request.destinationFile
        val expectedSha = request.expectedSha256.trim()
        if (destination.exists() && verifySha256File(destination, expectedSha)) {
            onProgress(100)
            return@withContext
        }

        val parent = destination.parentFile
            ?: throw IOException("apk target path has no parent directory: ${destination.absolutePath}")
        if (!parent.exists() && !parent.mkdirs()) {
            throw IOException("unable to create apk directory: ${parent.absolutePath}")
        }

        val probe = probeDownloadTarget(request.downloadUrl)
        val useChunkedDownload = probe.supportsRanges &&
            probe.contentLength > 0L &&
            probe.contentLength >= minChunkedDownloadBytes
        if (useChunkedDownload) {
            downloadByChunks(
                request = request,
                totalBytes = probe.contentLength,
                onProgress = onProgress
            )
        } else {
            downloadBySingleStream(
                request = request,
                totalBytesHint = probe.contentLength,
                onProgress = onProgress
            )
        }

        if (!verifySha256File(destination, expectedSha)) {
            runCatching { destination.delete() }
            throw IOException("update apk checksum mismatch after download")
        }
        onProgress(100)
    }

    private suspend fun downloadByChunks(
        request: MobileAppChunkDownloadRequest,
        totalBytes: Long,
        onProgress: (Int?) -> Unit
    ) {
        val destination = request.destinationFile
        val partDir = File(destination.parentFile, "${destination.name}.parts")
        if (!partDir.exists() && !partDir.mkdirs()) {
            throw IOException("unable to create chunk directory: ${partDir.absolutePath}")
        }

        val chunkCount = ceil(totalBytes.toDouble() / chunkSizeBytes.toDouble()).toInt().coerceAtLeast(1)
        val chunks = ArrayList<ChunkDescriptor>(chunkCount)
        var completedBytes = 0L
        for (chunkIndex in 0 until chunkCount) {
            val start = chunkIndex * chunkSizeBytes
            val end = min(totalBytes - 1L, start + chunkSizeBytes - 1L)
            val expectedLength = end - start + 1L
            val chunkFile = File(partDir, "part-$chunkIndex.bin")
            var existingLength = if (chunkFile.exists()) chunkFile.length() else 0L
            if (existingLength > expectedLength) {
                FileOutputStream(chunkFile, false).channel.use { channel ->
                    channel.truncate(expectedLength)
                }
                existingLength = expectedLength
            }
            completedBytes += existingLength
            chunks += ChunkDescriptor(
                index = chunkIndex,
                start = start,
                end = end,
                expectedLength = expectedLength,
                file = chunkFile
            )
        }

        val progressReporter = ProgressReporter(totalBytes = totalBytes, onProgress = onProgress)
        val downloadedCounter = AtomicLong(completedBytes)
        progressReporter.report(downloadedCounter.get(), force = true)

        val semaphore = Semaphore(maxParallelChunks)
        coroutineScope {
            chunks.map { chunk ->
                async {
                    semaphore.withPermit {
                        downloadSingleChunk(
                            request = request,
                            chunk = chunk,
                            downloadedCounter = downloadedCounter,
                            progressReporter = progressReporter
                        )
                    }
                }
            }.awaitAll()
        }

        val mergedFile = File(destination.parentFile, "${destination.name}.merge")
        if (mergedFile.exists()) {
            mergedFile.delete()
        }
        FileOutputStream(mergedFile, false).use { output ->
            chunks.sortedBy { it.index }.forEach { chunk ->
                chunk.file.inputStream().use { input ->
                    input.copyTo(output)
                }
            }
        }
        if (mergedFile.length() != totalBytes) {
            runCatching { mergedFile.delete() }
            throw IOException(
                "merged apk size mismatch: expected=$totalBytes actual=${mergedFile.length()}"
            )
        }
        if (destination.exists() && !destination.delete()) {
            throw IOException("unable to replace existing apk file: ${destination.absolutePath}")
        }
        if (!mergedFile.renameTo(destination)) {
            throw IOException("unable to finalize apk file: ${destination.absolutePath}")
        }
        partDir.deleteRecursively()
    }

    private fun downloadSingleChunk(
        request: MobileAppChunkDownloadRequest,
        chunk: ChunkDescriptor,
        downloadedCounter: AtomicLong,
        progressReporter: ProgressReporter
    ) {
        val expectedLength = chunk.expectedLength
        var existingLength = if (chunk.file.exists()) chunk.file.length() else 0L
        if (existingLength >= expectedLength) {
            return
        }

        val rangeStart = chunk.start + existingLength
        val rangeHeader = "bytes=$rangeStart-${chunk.end}"
        val httpRequest = Request.Builder()
            .url(request.downloadUrl)
            .addHeader("Range", rangeHeader)
            .get()
            .build()
        httpClient.newCall(httpRequest).execute().use { response ->
            if (response.code != 206) {
                throw IOException("chunk request failed with status=${response.code}, range=$rangeHeader")
            }
            val body = response.body ?: throw IOException("chunk response body is empty")
            FileOutputStream(chunk.file, true).use { output ->
                val input = body.byteStream()
                val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                while (true) {
                    val readBytes = input.read(buffer)
                    if (readBytes <= 0) {
                        break
                    }
                    output.write(buffer, 0, readBytes)
                    downloadedCounter.addAndGet(readBytes.toLong())
                    progressReporter.report(downloadedCounter.get())
                }
            }
            existingLength = chunk.file.length()
            if (existingLength < expectedLength) {
                throw IOException("chunk download interrupted: index=${chunk.index}")
            }
            if (existingLength > expectedLength) {
                FileOutputStream(chunk.file, false).channel.use { channel ->
                    channel.truncate(expectedLength)
                }
                val overflow = existingLength - expectedLength
                downloadedCounter.addAndGet(-overflow)
                existingLength = expectedLength
                progressReporter.report(downloadedCounter.get())
            }
        }
    }

    private fun downloadBySingleStream(
        request: MobileAppChunkDownloadRequest,
        totalBytesHint: Long,
        onProgress: (Int?) -> Unit
    ) {
        val destination = request.destinationFile
        val partialFile = File(destination.parentFile, "${destination.name}.partial")
        var existingLength = if (partialFile.exists()) partialFile.length() else 0L

        val requestBuilder = Request.Builder().url(request.downloadUrl).get()
        if (existingLength > 0L) {
            requestBuilder.addHeader("Range", "bytes=$existingLength-")
        }
        val httpRequest = requestBuilder.build()
        httpClient.newCall(httpRequest).execute().use { response ->
            val body = response.body ?: throw IOException("single stream response body is empty")
            val append = response.code == 206 && existingLength > 0L
            if (!append) {
                existingLength = 0L
            }
            val totalBytes = resolveTotalBytesFromResponse(
                responseContentLength = body.contentLength(),
                contentRangeHeader = response.header("Content-Range"),
                existingBytes = existingLength,
                fallback = totalBytesHint
            )
            val progressReporter = ProgressReporter(totalBytes = totalBytes, onProgress = onProgress)
            val output = FileOutputStream(partialFile, append)
            output.use { stream ->
                val input = body.byteStream()
                val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                var writtenBytes = existingLength
                progressReporter.report(writtenBytes, force = true)
                while (true) {
                    val readBytes = input.read(buffer)
                    if (readBytes <= 0) {
                        break
                    }
                    stream.write(buffer, 0, readBytes)
                    writtenBytes += readBytes
                    progressReporter.report(writtenBytes)
                }
            }
        }

        if (destination.exists() && !destination.delete()) {
            throw IOException("unable to replace existing apk file: ${destination.absolutePath}")
        }
        if (!partialFile.renameTo(destination)) {
            throw IOException("unable to finalize apk file: ${destination.absolutePath}")
        }
    }

    private fun probeDownloadTarget(downloadUrl: String): DownloadProbe {
        var contentLength = -1L
        var supportsRanges = false

        val headRequest = Request.Builder().url(downloadUrl).head().build()
        runCatching {
            httpClient.newCall(headRequest).execute().use { response ->
                if (response.isSuccessful) {
                    contentLength = response.header("Content-Length")?.trim()?.toLongOrNull() ?: -1L
                    supportsRanges = response.header("Accept-Ranges")
                        ?.lowercase(Locale.ROOT)
                        ?.contains("bytes") == true
                }
            }
        }

        val rangeProbeRequest = Request.Builder()
            .url(downloadUrl)
            .addHeader("Range", "bytes=0-0")
            .get()
            .build()
        runCatching {
            httpClient.newCall(rangeProbeRequest).execute().use { response ->
                if (response.code == 206) {
                    supportsRanges = true
                    val contentRange = response.header("Content-Range").orEmpty()
                    val totalFromRange = contentRange
                        .substringAfter('/', "")
                        .trim()
                        .toLongOrNull()
                        ?: -1L
                    if (totalFromRange > 0L) {
                        contentLength = totalFromRange
                    }
                } else if (contentLength <= 0L) {
                    val value = response.header("Content-Length")?.trim()?.toLongOrNull() ?: -1L
                    if (value > 0L) {
                        contentLength = value
                    }
                }
            }
        }

        return DownloadProbe(
            contentLength = contentLength,
            supportsRanges = supportsRanges
        )
    }

    private fun resolveTotalBytesFromResponse(
        responseContentLength: Long,
        contentRangeHeader: String?,
        existingBytes: Long,
        fallback: Long
    ): Long {
        val rangeTotal = contentRangeHeader
            ?.substringAfter('/', "")
            ?.trim()
            ?.toLongOrNull()
            ?: -1L
        if (rangeTotal > 0L) {
            return rangeTotal
        }
        if (responseContentLength > 0L) {
            return responseContentLength + existingBytes
        }
        return fallback
    }

    private data class DownloadProbe(
        val contentLength: Long,
        val supportsRanges: Boolean
    )

    private data class ChunkDescriptor(
        val index: Int,
        val start: Long,
        val end: Long,
        val expectedLength: Long,
        val file: File
    )

    private class ProgressReporter(
        private val totalBytes: Long,
        private val onProgress: (Int?) -> Unit
    ) {
        private val lastPercent = AtomicInteger(MobileAppUpdateStateStore.APP_UPDATE_PROGRESS_UNKNOWN)
        private val lastEmitTime = AtomicLong(0L)

        fun report(downloadedBytes: Long, force: Boolean = false) {
            if (totalBytes <= 0L) {
                if (force) {
                    onProgress(null)
                }
                return
            }
            val normalized = ((downloadedBytes * 100L) / totalBytes)
                .toInt()
                .coerceIn(0, 100)
            val now = System.currentTimeMillis()
            val previous = lastPercent.get()
            val intervalReached = now - lastEmitTime.get() >= PROGRESS_EMIT_INTERVAL_MS
            if (!force && normalized == previous && !intervalReached) {
                return
            }
            if (!force && normalized < previous) {
                return
            }
            lastPercent.set(normalized)
            lastEmitTime.set(now)
            onProgress(normalized)
        }
    }

    companion object {
        private const val FALLBACK_CHUNK_SIZE_BYTES = 2L * 1024L * 1024L
        private const val MIN_CHUNK_SIZE_BYTES = 256L * 1024L
        private const val FALLBACK_MIN_CHUNKED_DOWNLOAD_BYTES = 4L * 1024L * 1024L
        private const val MIN_CHUNKED_DOWNLOAD_BYTES = 2L * 1024L * 1024L
        private const val FALLBACK_MAX_PARALLEL_CHUNKS = 4
        private const val MIN_PARALLEL_CHUNKS = 1
        private const val MAX_PARALLEL_CHUNKS = 8
        private const val PROGRESS_EMIT_INTERVAL_MS = 300L

        private fun resolveChunkSizeBytes(): Long {
            return BuildConfig.MOBILE_APP_UPDATE_CHUNK_SIZE_BYTES
                .coerceAtLeast(MIN_CHUNK_SIZE_BYTES)
                .takeIf { it > 0L }
                ?: FALLBACK_CHUNK_SIZE_BYTES
        }

        private fun resolveMinChunkedDownloadBytes(): Long {
            val configured = BuildConfig.MOBILE_APP_UPDATE_MIN_CHUNKED_DOWNLOAD_BYTES
                .coerceAtLeast(MIN_CHUNKED_DOWNLOAD_BYTES)
            val chunkBasedLowerBound = resolveChunkSizeBytes() * 2L
            return max(configured, chunkBasedLowerBound)
                .takeIf { it > 0L }
                ?: FALLBACK_MIN_CHUNKED_DOWNLOAD_BYTES
        }

        private fun resolveMaxParallelChunks(): Int {
            return BuildConfig.MOBILE_APP_UPDATE_MAX_PARALLEL_CHUNKS
                .coerceIn(MIN_PARALLEL_CHUNKS, MAX_PARALLEL_CHUNKS)
                .takeIf { it > 0 }
                ?: FALLBACK_MAX_PARALLEL_CHUNKS
        }
    }
}

internal fun verifySha256File(apkFile: File, expectedSha256: String): Boolean {
    val expected = expectedSha256.trim().lowercase(Locale.ROOT)
    if (expected.isEmpty()) {
        return true
    }
    if (!apkFile.exists() || !apkFile.isFile) {
        return false
    }
    val actual = sha256Hex(apkFile)
    return actual == expected
}

internal fun sha256Hex(file: File): String {
    val digest = MessageDigest.getInstance("SHA-256")
    file.inputStream().use { input ->
        val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
        while (true) {
            val read = input.read(buffer)
            if (read <= 0) {
                break
            }
            digest.update(buffer, 0, read)
        }
    }
    val hash = digest.digest()
    val builder = StringBuilder(hash.size * 2)
    hash.forEach { value ->
        val number = value.toInt() and 0xFF
        if (number < 16) {
            builder.append('0')
        }
        builder.append(number.toString(16))
    }
    return builder.toString()
}
