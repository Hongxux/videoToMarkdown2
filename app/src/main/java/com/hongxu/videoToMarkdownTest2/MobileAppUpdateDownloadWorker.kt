package com.hongxu.videoToMarkdownTest2

import android.content.Context
import android.os.Environment
import androidx.work.CoroutineWorker
import androidx.work.Data
import androidx.work.WorkerParameters
import androidx.work.workDataOf
import java.io.File
import java.io.IOException

class MobileAppUpdateDownloadWorker(
    appContext: Context,
    params: WorkerParameters
) : CoroutineWorker(appContext, params) {
    private val stateStore = MobileAppUpdateStateStore(appContext)
    private val chunkDownloader = MobileAppUpdateChunkDownloader()

    override suspend fun doWork(): Result {
        val versionCode = inputData.getInt(KEY_VERSION_CODE, -1)
        val versionName = inputData.getString(KEY_VERSION_NAME)?.trim().orEmpty()
        val downloadUrl = inputData.getString(KEY_DOWNLOAD_URL)?.trim().orEmpty()
        val sha256 = inputData.getString(KEY_SHA256)?.trim().orEmpty()
        val fileName = inputData.getString(KEY_FILE_NAME)?.trim().orEmpty()
        val forceUpdate = inputData.getBoolean(KEY_FORCE_UPDATE, false)

        if (versionCode <= 0 || versionName.isEmpty() || downloadUrl.isEmpty() || fileName.isEmpty()) {
            return Result.failure(
                workDataOf(
                    KEY_FAILURE_MESSAGE to "invalid update download worker input"
                )
            )
        }
        val targetFile = resolveManagedUpdateApkFile(applicationContext, fileName)
            ?: return Result.failure(
                workDataOf(
                    KEY_FAILURE_MESSAGE to "unable to resolve update target file path"
                )
            )

        return try {
            chunkDownloader.downloadApk(
                request = MobileAppChunkDownloadRequest(
                    downloadUrl = downloadUrl,
                    destinationFile = targetFile,
                    expectedSha256 = sha256
                ),
                onProgress = { progress ->
                    val normalized = progress ?: MobileAppUpdateStateStore.APP_UPDATE_PROGRESS_UNKNOWN
                    stateStore.updatePendingProgress(progress)
                    setProgressAsync(
                        workDataOf(
                            KEY_PROGRESS_PERCENT to normalized
                        )
                    )
                }
            )
            stateStore.writeReadyInstallState(
                ReadyInstallState(
                    versionCode = versionCode,
                    versionName = versionName,
                    sha256 = sha256,
                    fileName = fileName,
                    forceUpdate = forceUpdate
                )
            )
            stateStore.clearPendingDownloadState()
            Result.success(
                workDataOf(
                    KEY_PROGRESS_PERCENT to 100
                )
            )
        } catch (error: IOException) {
            if (runAttemptCount >= MAX_RETRY_ATTEMPTS) {
                Result.failure(
                    workDataOf(
                        KEY_FAILURE_MESSAGE to error.message.orEmpty().ifBlank { "update download failed" }
                    )
                )
            } else {
                Result.retry()
            }
        } catch (error: Exception) {
            Result.failure(
                workDataOf(
                    KEY_FAILURE_MESSAGE to error.message.orEmpty().ifBlank { "update download failed" }
                )
            )
        }
    }

    companion object {
        internal const val KEY_VERSION_CODE = "version_code"
        internal const val KEY_VERSION_NAME = "version_name"
        internal const val KEY_DOWNLOAD_URL = "download_url"
        internal const val KEY_SHA256 = "sha256"
        internal const val KEY_FILE_NAME = "file_name"
        internal const val KEY_FORCE_UPDATE = "force_update"
        internal const val KEY_PROGRESS_PERCENT = "progress_percent"
        internal const val KEY_FAILURE_MESSAGE = "failure_message"

        private const val MAX_RETRY_ATTEMPTS = 2

        internal fun buildInputData(
            versionCode: Int,
            versionName: String,
            downloadUrl: String,
            sha256: String,
            fileName: String,
            forceUpdate: Boolean
        ): Data {
            return workDataOf(
                KEY_VERSION_CODE to versionCode,
                KEY_VERSION_NAME to versionName,
                KEY_DOWNLOAD_URL to downloadUrl,
                KEY_SHA256 to sha256,
                KEY_FILE_NAME to fileName,
                KEY_FORCE_UPDATE to forceUpdate
            )
        }
    }
}

internal fun resolveManagedUpdateApkFile(context: Context, fileName: String): File? {
    if (fileName.isBlank()) {
        return null
    }
    val root = context.applicationContext.getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS)
        ?: return null
    return File(root, fileName)
}
