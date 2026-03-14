package com.hongxu.videoToMarkdownTest2

import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.provider.Settings
import androidx.core.content.FileProvider
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkInfo
import androidx.work.WorkManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.TimeUnit

class MobileAppAutoUpdateManager(
    context: Context,
    apiBaseUrl: String
) {
    sealed class AutoUpdateAction {
        object NoOp : AutoUpdateAction()
        data class DownloadStarted(
            val versionCode: Int,
            val versionName: String,
            val forceUpdate: Boolean
        ) : AutoUpdateAction()

        data class DownloadInProgress(
            val versionCode: Int,
            val versionName: String,
            val progressPercent: Int?,
            val forceUpdate: Boolean
        ) : AutoUpdateAction()

        data class ReadyToInstall(
            val versionCode: Int,
            val versionName: String,
            val forceUpdate: Boolean
        ) : AutoUpdateAction()

        data class InstallPrompted(
            val versionCode: Int,
            val versionName: String,
            val forceUpdate: Boolean
        ) : AutoUpdateAction()

        data class InstallPermissionRequired(
            val versionCode: Int,
            val versionName: String,
            val forceUpdate: Boolean
        ) : AutoUpdateAction()

        data class Failed(
            val message: String,
            val forceUpdate: Boolean = false,
            val versionCode: Int? = null,
            val versionName: String = ""
        ) : AutoUpdateAction()
    }

    private val appContext = context.applicationContext
    private val normalizedApiBaseUrl = apiBaseUrl.trim().trimEnd('/')
    private val stateStore = MobileAppUpdateStateStore(appContext)
    private val workManager = WorkManager.getInstance(appContext)
    private val checkMutex = Mutex()

    suspend fun checkAndAutoUpdate(): AutoUpdateAction {
        return checkMutex.withLock {
            try {
                withContext(Dispatchers.IO) {
                    val currentVersion = stateStore.readCurrentVersion()
                    stateStore.pruneStateByCurrentVersion(currentVersion.versionCode)

                    val pending = stateStore.readPendingDownloadState()
                    if (pending != null) {
                        val pendingAction = handlePendingDownload(pending, currentVersion)
                        if (pendingAction !is AutoUpdateAction.NoOp) {
                            return@withContext pendingAction
                        }
                    }

                    val ready = stateStore.readReadyInstallState()
                    if (ready != null && ready.versionCode > currentVersion.versionCode) {
                        val readyAction = resolveReadyInstallAction(
                            state = ready,
                            autoPromptInstaller = true
                        )
                        if (readyAction !is AutoUpdateAction.NoOp) {
                            return@withContext readyAction
                        }
                    }

                    val checkPayload = fetchUpdateCheckPayload(
                        currentVersionCode = currentVersion.versionCode,
                        currentVersionName = currentVersion.versionName
                    )
                    if (!checkPayload.hasUpdate || checkPayload.latestVersionCode <= currentVersion.versionCode) {
                        return@withContext AutoUpdateAction.NoOp
                    }

                    val existingReady = stateStore.readReadyInstallState()
                    if (existingReady != null && existingReady.versionCode == checkPayload.latestVersionCode) {
                        val readyAction = resolveReadyInstallAction(
                                state = existingReady,
                                autoPromptInstaller = true
                            )
                        if (readyAction !is AutoUpdateAction.NoOp) {
                            return@withContext readyAction
                        }
                    }

                    val existingPending = stateStore.readPendingDownloadState()
                    if (existingPending != null) {
                        if (existingPending.versionCode == checkPayload.latestVersionCode) {
                            val pendingAction = handlePendingDownload(existingPending, currentVersion)
                            if (pendingAction !is AutoUpdateAction.NoOp) {
                                return@withContext pendingAction
                            }
                        } else {
                            cancelPendingDownload(existingPending)
                        }
                    }

                    enqueueUpdateDownload(checkPayload)
                }
            } catch (error: Exception) {
                AutoUpdateAction.Failed(
                    error.message
                        ?.trim()
                        .orEmpty()
                        .ifEmpty { "check update failed" }
                )
            }
        }
    }

    suspend fun promptInstallReadyUpdate(): AutoUpdateAction {
        return checkMutex.withLock {
            try {
                withContext(Dispatchers.IO) {
                    val currentVersion = stateStore.readCurrentVersion()
                    stateStore.pruneStateByCurrentVersion(currentVersion.versionCode)
                    val ready = stateStore.readReadyInstallState() ?: return@withContext AutoUpdateAction.NoOp
                    val installAction = launchInstallerIfReady(ready)
                    if (installAction is AutoUpdateAction.InstallPrompted) {
                        stateStore.markReadyInstallPrompted(ready.versionCode)
                    }
                    installAction
                }
            } catch (error: Exception) {
                AutoUpdateAction.Failed(
                    error.message
                        ?.trim()
                        .orEmpty()
                        .ifEmpty { "install update failed" }
                )
            }
        }
    }

    private suspend fun handlePendingDownload(
        state: PendingDownloadState,
        currentVersion: AppVersionSnapshot
    ): AutoUpdateAction {
        if (state.versionCode <= currentVersion.versionCode) {
            stateStore.clearPendingDownloadState()
            return AutoUpdateAction.NoOp
        }

        val workInfo = queryWorkInfoById(state.workerId)
        if (workInfo == null) {
            val completedFile = resolveManagedUpdateApkFile(appContext, state.fileName)
            if (completedFile != null && completedFile.exists() && verifySha256File(completedFile, state.sha256)) {
                stateStore.clearPendingDownloadState()
                val readyState = ReadyInstallState(
                    versionCode = state.versionCode,
                    versionName = state.versionName,
                    sha256 = state.sha256,
                    fileName = state.fileName,
                    forceUpdate = state.forceUpdate
                )
                stateStore.writeReadyInstallState(readyState)
                return resolveReadyInstallAction(
                    state = readyState,
                    autoPromptInstaller = true
                )
            }
            stateStore.clearPendingDownloadState()
            return AutoUpdateAction.NoOp
        }

        return when (workInfo.state) {
            WorkInfo.State.ENQUEUED,
            WorkInfo.State.RUNNING,
            WorkInfo.State.BLOCKED -> {
                val progress = resolveWorkProgress(workInfo, state.progressPercent)
                if (progress != state.progressPercent) {
                    stateStore.updatePendingProgress(progress)
                }
                AutoUpdateAction.DownloadInProgress(
                    versionCode = state.versionCode,
                    versionName = state.versionName,
                    progressPercent = progress,
                    forceUpdate = state.forceUpdate
                )
            }

            WorkInfo.State.SUCCEEDED -> {
                finalizeCompletedPendingDownload(state)
            }

            WorkInfo.State.FAILED,
            WorkInfo.State.CANCELLED -> {
                stateStore.clearPendingDownloadState()
                val message = workInfo.outputData
                    .getString(MobileAppUpdateDownloadWorker.KEY_FAILURE_MESSAGE)
                    ?.trim()
                    .orEmpty()
                    .ifEmpty { "update download failed" }
                AutoUpdateAction.Failed(
                    message = message,
                    forceUpdate = state.forceUpdate,
                    versionCode = state.versionCode,
                    versionName = state.versionName
                )
            }
        }
    }

    private suspend fun finalizeCompletedPendingDownload(state: PendingDownloadState): AutoUpdateAction {
        val completedFile = resolveManagedUpdateApkFile(appContext, state.fileName)
        if (completedFile == null || !completedFile.exists()) {
            stateStore.clearPendingDownloadState()
            return AutoUpdateAction.Failed(
                message = "update apk file missing after download",
                forceUpdate = state.forceUpdate,
                versionCode = state.versionCode,
                versionName = state.versionName
            )
        }
        if (!verifySha256File(completedFile, state.sha256)) {
            stateStore.clearPendingDownloadState()
            runCatching { completedFile.delete() }
            return AutoUpdateAction.Failed(
                message = "update apk checksum mismatch",
                forceUpdate = state.forceUpdate,
                versionCode = state.versionCode,
                versionName = state.versionName
            )
        }

        stateStore.clearPendingDownloadState()
        val readyState = ReadyInstallState(
            versionCode = state.versionCode,
            versionName = state.versionName,
            sha256 = state.sha256,
            fileName = state.fileName,
            forceUpdate = state.forceUpdate
        )
        stateStore.writeReadyInstallState(readyState)
        return resolveReadyInstallAction(
            state = readyState,
            autoPromptInstaller = true
        )
    }

    private fun resolveWorkProgress(workInfo: WorkInfo, fallbackProgress: Int?): Int? {
        val value = workInfo.progress.getInt(
            MobileAppUpdateDownloadWorker.KEY_PROGRESS_PERCENT,
            MobileAppUpdateStateStore.APP_UPDATE_PROGRESS_UNKNOWN
        )
        if (value in 0..100) {
            return value
        }
        return fallbackProgress
    }

    private fun cancelPendingDownload(state: PendingDownloadState) {
        val workerId = runCatching { UUID.fromString(state.workerId) }.getOrNull()
        if (workerId != null) {
            runCatching { workManager.cancelWorkById(workerId) }
        }
        cleanupDownloadArtifacts(state.fileName)
        stateStore.clearPendingDownloadState()
    }

    private fun cleanupDownloadArtifacts(fileName: String) {
        val file = resolveManagedUpdateApkFile(appContext, fileName) ?: return
        runCatching { if (file.exists()) file.delete() }
        runCatching {
            val partDir = File(file.parentFile, "${file.name}.parts")
            if (partDir.exists()) {
                partDir.deleteRecursively()
            }
        }
        runCatching {
            val partial = File(file.parentFile, "${file.name}.partial")
            if (partial.exists()) {
                partial.delete()
            }
        }
        runCatching {
            val merged = File(file.parentFile, "${file.name}.merge")
            if (merged.exists()) {
                merged.delete()
            }
        }
    }

    private fun enqueueUpdateDownload(
        payload: UpdateCheckPayload,
        targetFileNameOverride: String? = null
    ): AutoUpdateAction {
        val parsedUri = runCatching { Uri.parse(payload.downloadUrl) }.getOrNull()
            ?: return AutoUpdateAction.Failed(
                message = "invalid update url",
                forceUpdate = payload.forceUpdate,
                versionCode = payload.latestVersionCode,
                versionName = payload.latestVersionName
            )
        if (parsedUri.scheme.isNullOrEmpty()) {
            return AutoUpdateAction.Failed(
                message = "invalid update url",
                forceUpdate = payload.forceUpdate,
                versionCode = payload.latestVersionCode,
                versionName = payload.latestVersionName
            )
        }

        val targetFileName = targetFileNameOverride?.trim().orEmpty().ifBlank {
            payload.suggestedFileName
        }.ifBlank {
            buildManagedApkFileName(payload.latestVersionCode, payload.latestVersionName)
        }

        val workerInput = MobileAppUpdateDownloadWorker.buildInputData(
            versionCode = payload.latestVersionCode,
            versionName = payload.latestVersionName,
            downloadUrl = payload.downloadUrl,
            sha256 = payload.sha256,
            fileName = targetFileName,
            forceUpdate = payload.forceUpdate
        )
        val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()
        val workRequest = OneTimeWorkRequestBuilder<MobileAppUpdateDownloadWorker>()
            .setInputData(workerInput)
            .setConstraints(constraints)
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
            .addTag(APP_UPDATE_DOWNLOAD_TAG)
            .build()
        workManager.enqueueUniqueWork(
            APP_UPDATE_DOWNLOAD_UNIQUE_WORK,
            ExistingWorkPolicy.REPLACE,
            workRequest
        )
        stateStore.writePendingDownloadState(
            PendingDownloadState(
                workerId = workRequest.id.toString(),
                versionCode = payload.latestVersionCode,
                versionName = payload.latestVersionName,
                sha256 = payload.sha256,
                fileName = targetFileName,
                downloadUrl = payload.downloadUrl,
                forceUpdate = payload.forceUpdate,
                progressPercent = null
            )
        )
        return AutoUpdateAction.DownloadStarted(
            versionCode = payload.latestVersionCode,
            versionName = payload.latestVersionName,
            forceUpdate = payload.forceUpdate
        )
    }

    private suspend fun resolveReadyInstallAction(
        state: ReadyInstallState,
        autoPromptInstaller: Boolean
    ): AutoUpdateAction {
        val apkFile = resolveManagedUpdateApkFile(appContext, state.fileName)
        if (apkFile == null || !apkFile.exists()) {
            stateStore.clearReadyInstallState()
            return AutoUpdateAction.NoOp
        }
        if (!verifySha256File(apkFile, state.sha256)) {
            stateStore.clearReadyInstallState()
            runCatching { apkFile.delete() }
            return AutoUpdateAction.Failed(
                message = "cached update apk checksum mismatch",
                forceUpdate = state.forceUpdate,
                versionCode = state.versionCode,
                versionName = state.versionName
            )
        }
        if (autoPromptInstaller && !stateStore.wasReadyInstallPrompted(state.versionCode)) {
            val installAction = launchInstallerIfReady(state)
            if (installAction is AutoUpdateAction.InstallPrompted) {
                stateStore.markReadyInstallPrompted(state.versionCode)
            }
            if (installAction !is AutoUpdateAction.ReadyToInstall && installAction !is AutoUpdateAction.NoOp) {
                return installAction
            }
        }
        return AutoUpdateAction.ReadyToInstall(
            versionCode = state.versionCode,
            versionName = state.versionName,
            forceUpdate = state.forceUpdate
        )
    }

    private suspend fun launchInstallerIfReady(state: ReadyInstallState): AutoUpdateAction {
        val apkFile = resolveManagedUpdateApkFile(appContext, state.fileName)
        if (apkFile == null || !apkFile.exists()) {
            stateStore.clearReadyInstallState()
            return AutoUpdateAction.NoOp
        }
        if (!verifySha256File(apkFile, state.sha256)) {
            stateStore.clearReadyInstallState()
            runCatching { apkFile.delete() }
            return AutoUpdateAction.Failed("cached update apk checksum mismatch")
        }
        return withContext(Dispatchers.Main) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O &&
                !appContext.packageManager.canRequestPackageInstalls()
            ) {
                val settingsIntent = Intent(Settings.ACTION_MANAGE_UNKNOWN_APP_SOURCES).apply {
                    data = Uri.parse("package:${appContext.packageName}")
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                }
                runCatching { appContext.startActivity(settingsIntent) }
                AutoUpdateAction.InstallPermissionRequired(
                    versionCode = state.versionCode,
                    versionName = state.versionName,
                    forceUpdate = state.forceUpdate
                )
            } else {
                val authority = "${appContext.packageName}.fileprovider"
                val apkUri = FileProvider.getUriForFile(appContext, authority, apkFile)
                val installIntent = Intent(Intent.ACTION_VIEW).apply {
                    setDataAndType(apkUri, APK_MIME_TYPE)
                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                    addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                }
                runCatching { appContext.startActivity(installIntent) }.fold(
                    onSuccess = {
                        AutoUpdateAction.InstallPrompted(
                            versionCode = state.versionCode,
                            versionName = state.versionName,
                            forceUpdate = state.forceUpdate
                        )
                    },
                    onFailure = { error ->
                        AutoUpdateAction.Failed(
                            message = error.message?.trim().orEmpty().ifEmpty {
                                "open installer failed"
                            },
                            forceUpdate = state.forceUpdate,
                            versionCode = state.versionCode,
                            versionName = state.versionName
                        )
                    }
                )
            }
        }
    }

    private fun fetchUpdateCheckPayload(
        currentVersionCode: Int,
        currentVersionName: String
    ): UpdateCheckPayload {
        if (normalizedApiBaseUrl.isBlank()) {
            throw IllegalStateException("mobile api base url is blank")
        }
        val encodedVersionName = URLEncoder.encode(
            currentVersionName,
            StandardCharsets.UTF_8.toString()
        )
        val endpoint = "$normalizedApiBaseUrl/app/update/check?versionCode=$currentVersionCode&versionName=$encodedVersionName"
        val connection = (URL(endpoint).openConnection() as HttpURLConnection).apply {
            requestMethod = "GET"
            connectTimeout = 10_000
            readTimeout = 20_000
            setRequestProperty("Accept", "application/json")
        }
        return connection.useJsonPayload { json ->
            val success = json.optBoolean("success", true)
            if (!success) {
                val message = json.optString("message").trim().ifEmpty { "check update failed" }
                throw IllegalStateException(message)
            }
            val hasUpdate = json.optBoolean("hasUpdate", false)
            val forceUpdate = json.optBoolean("forceUpdate", false)
            val latestVersionCode = json.optInt("latestVersionCode", -1)
            val latestVersionName = json.optString("latestVersionName").trim()
            val downloadUrl = json.optString("downloadUrl").trim()
            if (hasUpdate && latestVersionCode <= 0) {
                throw IllegalStateException("invalid latestVersionCode from update payload")
            }
            if (hasUpdate && latestVersionName.isEmpty()) {
                throw IllegalStateException("invalid latestVersionName from update payload")
            }
            if (hasUpdate && downloadUrl.isEmpty()) {
                throw IllegalStateException("empty download url from update payload")
            }
            val suggestedName = parseFileNameFromUrl(downloadUrl)
            UpdateCheckPayload(
                hasUpdate = hasUpdate,
                latestVersionCode = latestVersionCode,
                latestVersionName = latestVersionName,
                downloadUrl = downloadUrl,
                sha256 = json.optString("sha256").trim(),
                suggestedFileName = suggestedName,
                forceUpdate = forceUpdate
            )
        }
    }

    private fun queryWorkInfoById(workerId: String): WorkInfo? {
        val uuid = runCatching { UUID.fromString(workerId.trim()) }.getOrNull() ?: return null
        return runCatching { workManager.getWorkInfoById(uuid).get() }.getOrNull()
    }

    private fun parseFileNameFromUrl(downloadUrl: String): String {
        if (downloadUrl.isBlank()) {
            return ""
        }
        val path = runCatching { Uri.parse(downloadUrl).lastPathSegment.orEmpty() }.getOrDefault("")
        val normalized = path.substringAfterLast('/').trim()
        if (normalized.isBlank()) {
            return ""
        }
        if (!normalized.lowercase().endsWith(APK_SUFFIX)) {
            return ""
        }
        return normalized.replace(ILLEGAL_FILE_NAME_CHARS, "_")
    }

    private fun buildManagedApkFileName(versionCode: Int, versionName: String): String {
        val safeVersionName = versionName.trim()
            .ifEmpty { "latest" }
            .replace(ILLEGAL_FILE_NAME_CHARS, "_")
        return "videoToMarkdown-$versionCode-$safeVersionName.apk"
    }

    private data class UpdateCheckPayload(
        val hasUpdate: Boolean,
        val latestVersionCode: Int,
        val latestVersionName: String,
        val downloadUrl: String,
        val sha256: String,
        val suggestedFileName: String,
        val forceUpdate: Boolean
    )

    companion object {
        private const val APP_UPDATE_DOWNLOAD_UNIQUE_WORK = "mobile_app_auto_update_download"
        private const val APP_UPDATE_DOWNLOAD_TAG = "mobile_app_auto_update"
        private const val APK_SUFFIX = ".apk"
        private const val APK_MIME_TYPE = "application/vnd.android.package-archive"
        private val ILLEGAL_FILE_NAME_CHARS = Regex("[^0-9A-Za-z._-]")
    }
}

private inline fun <T> HttpURLConnection.useJsonPayload(parse: (JSONObject) -> T): T {
    try {
        val statusCode = responseCode
        val stream = if (statusCode in 200..299) inputStream else errorStream
        val responseText = stream?.use {
            BufferedReader(InputStreamReader(it, StandardCharsets.UTF_8)).readText()
        }.orEmpty()
        if (statusCode !in 200..299) {
            val message = runCatching {
                JSONObject(responseText).optString("message").trim().ifEmpty { "HTTP $statusCode" }
            }.getOrDefault("HTTP $statusCode")
            throw IllegalStateException(message)
        }
        val payload = if (responseText.isBlank()) JSONObject() else JSONObject(responseText)
        return parse(payload)
    } finally {
        disconnect()
    }
}
