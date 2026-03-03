package com.hongxu.videoToMarkdownTest2

import android.content.Context
import android.os.Build

internal data class PendingDownloadState(
    val workerId: String,
    val versionCode: Int,
    val versionName: String,
    val sha256: String,
    val fileName: String,
    val downloadUrl: String,
    val forceUpdate: Boolean,
    val progressPercent: Int?
)

internal data class ReadyInstallState(
    val versionCode: Int,
    val versionName: String,
    val sha256: String,
    val fileName: String,
    val forceUpdate: Boolean
)

internal data class AppVersionSnapshot(
    val versionCode: Int,
    val versionName: String
)

internal class MobileAppUpdateStateStore(
    context: Context
) {
    private val appContext = context.applicationContext
    private val preferences = appContext.getSharedPreferences(
        PREFS_APP_UPDATE,
        Context.MODE_PRIVATE
    )

    fun readPendingDownloadState(): PendingDownloadState? {
        val workerId = preferences.getString(KEY_PENDING_WORKER_ID, "")?.trim().orEmpty()
        val versionCode = preferences.getInt(KEY_PENDING_VERSION_CODE, -1)
        val versionName = preferences.getString(KEY_PENDING_VERSION_NAME, "")?.trim().orEmpty()
        val fileName = preferences.getString(KEY_PENDING_FILE_NAME, "")?.trim().orEmpty()
        if (workerId.isEmpty() || versionCode <= 0 || versionName.isEmpty() || fileName.isEmpty()) {
            return null
        }
        val progressValue = preferences.getInt(KEY_PENDING_PROGRESS_PERCENT, APP_UPDATE_PROGRESS_UNKNOWN)
        val progressPercent = if (progressValue in 0..100) progressValue else null
        return PendingDownloadState(
            workerId = workerId,
            versionCode = versionCode,
            versionName = versionName,
            sha256 = preferences.getString(KEY_PENDING_SHA256, "")?.trim().orEmpty(),
            fileName = fileName,
            downloadUrl = preferences.getString(KEY_PENDING_DOWNLOAD_URL, "")?.trim().orEmpty(),
            forceUpdate = preferences.getBoolean(KEY_PENDING_FORCE_UPDATE, false),
            progressPercent = progressPercent
        )
    }

    fun writePendingDownloadState(state: PendingDownloadState) {
        val progressValue = state.progressPercent ?: APP_UPDATE_PROGRESS_UNKNOWN
        preferences.edit()
            .putString(KEY_PENDING_WORKER_ID, state.workerId)
            .putInt(KEY_PENDING_VERSION_CODE, state.versionCode)
            .putString(KEY_PENDING_VERSION_NAME, state.versionName)
            .putString(KEY_PENDING_SHA256, state.sha256)
            .putString(KEY_PENDING_FILE_NAME, state.fileName)
            .putString(KEY_PENDING_DOWNLOAD_URL, state.downloadUrl)
            .putBoolean(KEY_PENDING_FORCE_UPDATE, state.forceUpdate)
            .putInt(KEY_PENDING_PROGRESS_PERCENT, progressValue)
            .apply()
    }

    fun updatePendingProgress(progressPercent: Int?) {
        val normalized = progressPercent?.coerceIn(0, 100) ?: APP_UPDATE_PROGRESS_UNKNOWN
        preferences.edit()
            .putInt(KEY_PENDING_PROGRESS_PERCENT, normalized)
            .apply()
    }

    fun clearPendingDownloadState() {
        preferences.edit()
            .remove(KEY_PENDING_WORKER_ID)
            .remove(KEY_PENDING_VERSION_CODE)
            .remove(KEY_PENDING_VERSION_NAME)
            .remove(KEY_PENDING_SHA256)
            .remove(KEY_PENDING_FILE_NAME)
            .remove(KEY_PENDING_DOWNLOAD_URL)
            .remove(KEY_PENDING_FORCE_UPDATE)
            .remove(KEY_PENDING_PROGRESS_PERCENT)
            .apply()
    }

    fun readReadyInstallState(): ReadyInstallState? {
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

    fun writeReadyInstallState(state: ReadyInstallState) {
        preferences.edit()
            .putInt(KEY_READY_VERSION_CODE, state.versionCode)
            .putString(KEY_READY_VERSION_NAME, state.versionName)
            .putString(KEY_READY_SHA256, state.sha256)
            .putString(KEY_READY_FILE_NAME, state.fileName)
            .putBoolean(KEY_READY_FORCE_UPDATE, state.forceUpdate)
            .apply()
    }

    fun clearReadyInstallState() {
        preferences.edit()
            .remove(KEY_READY_VERSION_CODE)
            .remove(KEY_READY_VERSION_NAME)
            .remove(KEY_READY_SHA256)
            .remove(KEY_READY_FILE_NAME)
            .remove(KEY_READY_FORCE_UPDATE)
            .apply()
    }

    fun pruneStateByCurrentVersion(currentVersionCode: Int) {
        val pending = readPendingDownloadState()
        if (pending != null && pending.versionCode <= currentVersionCode) {
            clearPendingDownloadState()
        }
        val ready = readReadyInstallState()
        if (ready != null && ready.versionCode <= currentVersionCode) {
            clearReadyInstallState()
        }
    }

    fun readCurrentVersion(): AppVersionSnapshot {
        val packageInfo = appContext.packageManager.getPackageInfo(appContext.packageName, 0)
        val versionCode = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            packageInfo.longVersionCode.toInt()
        } else {
            @Suppress("DEPRECATION")
            packageInfo.versionCode
        }
        val versionName = packageInfo.versionName
            ?.trim()
            .orEmpty()
            .ifEmpty { versionCode.toString() }
        return AppVersionSnapshot(versionCode = versionCode, versionName = versionName)
    }

    companion object {
        internal const val PREFS_APP_UPDATE = "mobile_app_auto_update"
        internal const val APP_UPDATE_PROGRESS_UNKNOWN = -1
        internal const val KEY_PENDING_WORKER_ID = "pending_worker_id"
        internal const val KEY_PENDING_VERSION_CODE = "pending_version_code"
        internal const val KEY_PENDING_VERSION_NAME = "pending_version_name"
        internal const val KEY_PENDING_SHA256 = "pending_sha256"
        internal const val KEY_PENDING_FILE_NAME = "pending_file_name"
        internal const val KEY_PENDING_DOWNLOAD_URL = "pending_download_url"
        internal const val KEY_PENDING_FORCE_UPDATE = "pending_force_update"
        internal const val KEY_PENDING_PROGRESS_PERCENT = "pending_progress_percent"
        internal const val KEY_READY_VERSION_CODE = "ready_version_code"
        internal const val KEY_READY_VERSION_NAME = "ready_version_name"
        internal const val KEY_READY_SHA256 = "ready_sha256"
        internal const val KEY_READY_FILE_NAME = "ready_file_name"
        internal const val KEY_READY_FORCE_UPDATE = "ready_force_update"
    }
}
