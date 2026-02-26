package com.hongxu.videoToMarkdownTest2

import android.app.DownloadManager
import android.content.Context
import android.content.Intent
import android.database.Cursor
import android.net.Uri
import android.os.Build
import android.os.Environment
import android.provider.Settings
import androidx.core.content.FileProvider
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
import java.security.MessageDigest

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
    private val downloadManager = appContext.getSystemService(DownloadManager::class.java)
    private val preferences = appContext.getSharedPreferences(
        PREFS_APP_UPDATE,
        Context.MODE_PRIVATE
    )
    private val checkMutex = Mutex()

    suspend fun checkAndAutoUpdate(): AutoUpdateAction {
        return checkMutex.withLock {
            try {
                withContext(Dispatchers.IO) {
                    if (downloadManager == null) {
                        return@withContext AutoUpdateAction.Failed("download manager unavailable")
                    }
                    val currentVersion = readCurrentVersion()
                    pruneStateByCurrentVersion(currentVersion.versionCode)

                    val pending = readPendingDownloadState()
                    if (pending != null) {
                        val pendingAction = handlePendingDownload(pending, currentVersion)
                        if (pendingAction !is AutoUpdateAction.NoOp) {
                            return@withContext pendingAction
                        }
                    }

                    val readyInstall = readReadyInstallState()
                    if (readyInstall != null && readyInstall.versionCode > currentVersion.versionCode) {
                        val readyAction = launchInstallerIfReady(readyInstall)
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

                    val existingReady = readReadyInstallState()
                    if (existingReady != null && existingReady.versionCode == checkPayload.latestVersionCode) {
                        val readyAction = launchInstallerIfReady(existingReady)
                        if (readyAction !is AutoUpdateAction.NoOp) {
                            return@withContext readyAction
                        }
                    }

                    val existingPending = readPendingDownloadState()
                    if (existingPending != null && existingPending.versionCode == checkPayload.latestVersionCode) {
                        val pendingAction = handlePendingDownload(existingPending, currentVersion)
                        if (pendingAction !is AutoUpdateAction.NoOp) {
                            return@withContext pendingAction
                        }
                    }

                    enqueueUpdateDownload(checkPayload)
                }
            } catch (error: Exception) {
                AutoUpdateAction.Failed(error.message?.trim().orEmpty().ifEmpty { "check update failed" })
            }
        }
    }

    private suspend fun handlePendingDownload(
        state: PendingDownloadState,
        currentVersion: AppVersionSnapshot
    ): AutoUpdateAction {
        if (state.versionCode <= currentVersion.versionCode) {
            clearPendingDownloadState()
            return AutoUpdateAction.NoOp
        }
        val snapshot = queryDownloadSnapshot(state.downloadId)
        if (snapshot == null) {
            clearPendingDownloadState()
            return AutoUpdateAction.NoOp
        }
        return when (snapshot.status) {
            DownloadManager.STATUS_PENDING,
            DownloadManager.STATUS_RUNNING,
            DownloadManager.STATUS_PAUSED -> {
                AutoUpdateAction.DownloadInProgress(
                    versionCode = state.versionCode,
                    versionName = state.versionName,
                    progressPercent = snapshot.progressPercent,
                    forceUpdate = state.forceUpdate
                )
            }

            DownloadManager.STATUS_FAILED -> {
                clearPendingDownloadState()
                AutoUpdateAction.Failed(
                    message = "update download failed: code=${snapshot.reason}",
                    forceUpdate = state.forceUpdate,
                    versionCode = state.versionCode,
                    versionName = state.versionName
                )
            }

            DownloadManager.STATUS_SUCCESSFUL -> {
                val completedFile = resolveDownloadedFile(state, snapshot.localUri)
                if (completedFile == null || !completedFile.exists()) {
                    clearPendingDownloadState()
                    AutoUpdateAction.Failed(
                        message = "update apk file missing after download",
                        forceUpdate = state.forceUpdate,
                        versionCode = state.versionCode,
                        versionName = state.versionName
                    )
                } else if (!verifySha256(completedFile, state.sha256)) {
                    clearPendingDownloadState()
                    completedFile.delete()
                    AutoUpdateAction.Failed(
                        message = "update apk checksum mismatch",
                        forceUpdate = state.forceUpdate,
                        versionCode = state.versionCode,
                        versionName = state.versionName
                    )
                } else {
                    clearPendingDownloadState()
                    writeReadyInstallState(
                        ReadyInstallState(
                            versionCode = state.versionCode,
                            versionName = state.versionName,
                            sha256 = state.sha256,
                            fileName = state.fileName,
                            forceUpdate = state.forceUpdate
                        )
                    )
                    launchInstallerIfReady(
                        ReadyInstallState(
                            versionCode = state.versionCode,
                            versionName = state.versionName,
                            sha256 = state.sha256,
                            fileName = state.fileName,
                            forceUpdate = state.forceUpdate
                        )
                    )
                }
            }

            else -> AutoUpdateAction.NoOp
        }
    }

    private suspend fun launchInstallerIfReady(state: ReadyInstallState): AutoUpdateAction {
        val apkFile = resolveManagedDownloadFile(state.fileName)
        if (apkFile == null || !apkFile.exists()) {
            clearReadyInstallState()
            return AutoUpdateAction.NoOp
        }
        if (!verifySha256(apkFile, state.sha256)) {
            clearReadyInstallState()
            apkFile.delete()
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
                runCatching {
                    appContext.startActivity(installIntent)
                }.fold(
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

    private fun enqueueUpdateDownload(payload: UpdateCheckPayload): AutoUpdateAction {
        val manager = downloadManager ?: return AutoUpdateAction.Failed(
            message = "download manager unavailable",
            forceUpdate = payload.forceUpdate,
            versionCode = payload.latestVersionCode,
            versionName = payload.latestVersionName
        )
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
        val targetFileName = payload.suggestedFileName.ifBlank {
            buildManagedApkFileName(payload.latestVersionCode, payload.latestVersionName)
        }
        resolveManagedDownloadFile(targetFileName)?.let { existing ->
            if (existing.exists()) {
                existing.delete()
            }
        }
        val request = DownloadManager.Request(parsedUri).apply {
            setMimeType(APK_MIME_TYPE)
            setTitle("New version ${payload.latestVersionName}")
            setDescription("Installer will open automatically after download")
            setAllowedOverMetered(true)
            setAllowedOverRoaming(true)
            setNotificationVisibility(DownloadManager.Request.VISIBILITY_VISIBLE_NOTIFY_COMPLETED)
            setDestinationInExternalFilesDir(
                appContext,
                Environment.DIRECTORY_DOWNLOADS,
                targetFileName
            )
        }
        val downloadId = manager.enqueue(request)
        writePendingDownloadState(
            PendingDownloadState(
                downloadId = downloadId,
                versionCode = payload.latestVersionCode,
                versionName = payload.latestVersionName,
                sha256 = payload.sha256,
                fileName = targetFileName,
                forceUpdate = payload.forceUpdate
            )
        )
        return AutoUpdateAction.DownloadStarted(
            versionCode = payload.latestVersionCode,
            versionName = payload.latestVersionName,
            forceUpdate = payload.forceUpdate
        )
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

    private fun queryDownloadSnapshot(downloadId: Long): DownloadSnapshot? {
        val manager = downloadManager ?: return null
        val cursor = manager.query(DownloadManager.Query().setFilterById(downloadId)) ?: return null
        cursor.use { row ->
            if (!row.moveToFirst()) {
                return null
            }
            val status = row.readInt(DownloadManager.COLUMN_STATUS, DownloadManager.STATUS_FAILED)
            val reason = row.readInt(DownloadManager.COLUMN_REASON, 0)
            val soFar = row.readLong(DownloadManager.COLUMN_BYTES_DOWNLOADED_SO_FAR, -1L)
            val total = row.readLong(DownloadManager.COLUMN_TOTAL_SIZE_BYTES, -1L)
            val localUri = row.readString(DownloadManager.COLUMN_LOCAL_URI)
            val progressPercent = if (soFar >= 0L && total > 0L) {
                ((soFar * 100L) / total).toInt().coerceIn(0, 100)
            } else {
                null
            }
            return DownloadSnapshot(
                status = status,
                reason = reason,
                localUri = localUri,
                progressPercent = progressPercent
            )
        }
    }

    private fun resolveDownloadedFile(
        pendingState: PendingDownloadState,
        localUri: String?
    ): File? {
        val managed = resolveManagedDownloadFile(pendingState.fileName)
        if (managed != null && managed.exists()) {
            return managed
        }
        if (localUri.isNullOrBlank()) {
            return null
        }
        val parsed = runCatching { Uri.parse(localUri) }.getOrNull() ?: return null
        if (parsed.scheme.equals("file", ignoreCase = true)) {
            val path = parsed.path ?: return null
            return File(path)
        }
        return null
    }

    private fun resolveManagedDownloadFile(fileName: String): File? {
        val root = appContext.getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS) ?: return null
        return File(root, fileName)
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

    private fun verifySha256(apkFile: File, expectedSha256: String): Boolean {
        val expected = expectedSha256.trim().lowercase()
        if (expected.isEmpty()) {
            return true
        }
        val actual = sha256Hex(apkFile)
        return expected == actual
    }

    private fun sha256Hex(file: File): String {
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
        hash.forEach { byte ->
            val value = byte.toInt() and 0xFF
            val hex = value.toString(16)
            if (hex.length == 1) {
                builder.append('0')
            }
            builder.append(hex)
        }
        return builder.toString()
    }

    private fun pruneStateByCurrentVersion(currentVersionCode: Int) {
        val pending = readPendingDownloadState()
        if (pending != null && pending.versionCode <= currentVersionCode) {
            clearPendingDownloadState()
        }
        val ready = readReadyInstallState()
        if (ready != null && ready.versionCode <= currentVersionCode) {
            clearReadyInstallState()
        }
    }

    private fun readCurrentVersion(): AppVersionSnapshot {
        val packageInfo = appContext.packageManager.getPackageInfo(appContext.packageName, 0)
        val versionCode = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            packageInfo.longVersionCode.toInt()
        } else {
            @Suppress("DEPRECATION")
            packageInfo.versionCode
        }
        val versionName = packageInfo.versionName?.trim().orEmpty().ifEmpty { versionCode.toString() }
        return AppVersionSnapshot(versionCode = versionCode, versionName = versionName)
    }

    private fun readPendingDownloadState(): PendingDownloadState? {
        val downloadId = preferences.getLong(KEY_PENDING_DOWNLOAD_ID, -1L)
        val versionCode = preferences.getInt(KEY_PENDING_VERSION_CODE, -1)
        val versionName = preferences.getString(KEY_PENDING_VERSION_NAME, "")?.trim().orEmpty()
        val fileName = preferences.getString(KEY_PENDING_FILE_NAME, "")?.trim().orEmpty()
        if (downloadId <= 0L || versionCode <= 0 || versionName.isEmpty() || fileName.isEmpty()) {
            return null
        }
        return PendingDownloadState(
            downloadId = downloadId,
            versionCode = versionCode,
            versionName = versionName,
            sha256 = preferences.getString(KEY_PENDING_SHA256, "")?.trim().orEmpty(),
            fileName = fileName,
            forceUpdate = preferences.getBoolean(KEY_PENDING_FORCE_UPDATE, false)
        )
    }

    private fun writePendingDownloadState(state: PendingDownloadState) {
        preferences.edit()
            .putLong(KEY_PENDING_DOWNLOAD_ID, state.downloadId)
            .putInt(KEY_PENDING_VERSION_CODE, state.versionCode)
            .putString(KEY_PENDING_VERSION_NAME, state.versionName)
            .putString(KEY_PENDING_SHA256, state.sha256)
            .putString(KEY_PENDING_FILE_NAME, state.fileName)
            .putBoolean(KEY_PENDING_FORCE_UPDATE, state.forceUpdate)
            .apply()
    }

    private fun clearPendingDownloadState() {
        preferences.edit()
            .remove(KEY_PENDING_DOWNLOAD_ID)
            .remove(KEY_PENDING_VERSION_CODE)
            .remove(KEY_PENDING_VERSION_NAME)
            .remove(KEY_PENDING_SHA256)
            .remove(KEY_PENDING_FILE_NAME)
            .remove(KEY_PENDING_FORCE_UPDATE)
            .apply()
    }

    private fun readReadyInstallState(): ReadyInstallState? {
        val versionCode = preferences.getInt(KEY_READY_VERSION_CODE, -1)
        val versionName = preferences.getString(KEY_READY_VERSION_NAME, "")?.trim().orEmpty()
        val fileName = preferences.getString(KEY_READY_FILE_NAME, "")?.trim().orEmpty()
        if (versionCode <= 0 || versionName.isEmpty() || fileName.isEmpty()) {
            return null
        }
        return ReadyInstallState(
            versionCode = versionCode,
            versionName = versionName,
            sha256 = preferences.getString(KEY_READY_SHA256, "")?.trim().orEmpty(),
            fileName = fileName,
            forceUpdate = preferences.getBoolean(KEY_READY_FORCE_UPDATE, false)
        )
    }

    private fun writeReadyInstallState(state: ReadyInstallState) {
        preferences.edit()
            .putInt(KEY_READY_VERSION_CODE, state.versionCode)
            .putString(KEY_READY_VERSION_NAME, state.versionName)
            .putString(KEY_READY_SHA256, state.sha256)
            .putString(KEY_READY_FILE_NAME, state.fileName)
            .putBoolean(KEY_READY_FORCE_UPDATE, state.forceUpdate)
            .apply()
    }

    private fun clearReadyInstallState() {
        preferences.edit()
            .remove(KEY_READY_VERSION_CODE)
            .remove(KEY_READY_VERSION_NAME)
            .remove(KEY_READY_SHA256)
            .remove(KEY_READY_FILE_NAME)
            .remove(KEY_READY_FORCE_UPDATE)
            .apply()
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

    private data class DownloadSnapshot(
        val status: Int,
        val reason: Int,
        val localUri: String?,
        val progressPercent: Int?
    )

    private data class PendingDownloadState(
        val downloadId: Long,
        val versionCode: Int,
        val versionName: String,
        val sha256: String,
        val fileName: String,
        val forceUpdate: Boolean
    )

    private data class ReadyInstallState(
        val versionCode: Int,
        val versionName: String,
        val sha256: String,
        val fileName: String,
        val forceUpdate: Boolean
    )

    private data class AppVersionSnapshot(
        val versionCode: Int,
        val versionName: String
    )

    companion object {
        private const val PREFS_APP_UPDATE = "mobile_app_auto_update"
        private const val KEY_PENDING_DOWNLOAD_ID = "pending_download_id"
        private const val KEY_PENDING_VERSION_CODE = "pending_version_code"
        private const val KEY_PENDING_VERSION_NAME = "pending_version_name"
        private const val KEY_PENDING_SHA256 = "pending_sha256"
        private const val KEY_PENDING_FILE_NAME = "pending_file_name"
        private const val KEY_PENDING_FORCE_UPDATE = "pending_force_update"
        private const val KEY_READY_VERSION_CODE = "ready_version_code"
        private const val KEY_READY_VERSION_NAME = "ready_version_name"
        private const val KEY_READY_SHA256 = "ready_sha256"
        private const val KEY_READY_FILE_NAME = "ready_file_name"
        private const val KEY_READY_FORCE_UPDATE = "ready_force_update"
        private const val APK_SUFFIX = ".apk"
        private const val APK_MIME_TYPE = "application/vnd.android.package-archive"
        private val ILLEGAL_FILE_NAME_CHARS = Regex("[^0-9A-Za-z._-]")
    }
}

private fun Cursor.readInt(columnName: String, defaultValue: Int): Int {
    val columnIndex = getColumnIndex(columnName)
    if (columnIndex < 0) {
        return defaultValue
    }
    return getInt(columnIndex)
}

private fun Cursor.readLong(columnName: String, defaultValue: Long): Long {
    val columnIndex = getColumnIndex(columnName)
    if (columnIndex < 0) {
        return defaultValue
    }
    return getLong(columnIndex)
}

private fun Cursor.readString(columnName: String): String? {
    val columnIndex = getColumnIndex(columnName)
    if (columnIndex < 0 || isNull(columnIndex)) {
        return null
    }
    return getString(columnIndex)
}

private inline fun <T> HttpURLConnection.useJsonPayload(parse: (JSONObject) -> T): T {
    try {
        val statusCode = responseCode
        val stream = if (statusCode in 200..299) {
            inputStream
        } else {
            errorStream
        }
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
