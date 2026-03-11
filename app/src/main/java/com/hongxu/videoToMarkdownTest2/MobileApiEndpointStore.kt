package com.hongxu.videoToMarkdownTest2

import android.content.Context

object MobileApiEndpointStore {
    private const val PREFS_NAME = "mobile_api_endpoint"
    private const val KEY_ROOT_URL = "root_url"
    private const val DEFAULT_ROOT_URL = "https://216d0ee2.r9.cpolar.cn"

    fun resolveRootUrl(context: Context): String {
        val preferences = context.applicationContext.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        val stored = preferences.getString(KEY_ROOT_URL, null).orEmpty()
        return normalizeRootUrl(stored).ifBlank { DEFAULT_ROOT_URL }
    }

    fun saveRootUrl(context: Context, rawRootUrl: String): String {
        val normalized = normalizeRootUrl(rawRootUrl).ifBlank { DEFAULT_ROOT_URL }
        context.applicationContext
            .getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            .edit()
            .putString(KEY_ROOT_URL, normalized)
            .apply()
        return normalized
    }

    fun resolveApiBaseUrl(context: Context): String {
        return toApiBaseUrl(resolveRootUrl(context))
    }

    fun toApiBaseUrl(rootUrl: String): String {
        val normalizedRoot = normalizeRootUrl(rootUrl).ifBlank { DEFAULT_ROOT_URL }
        return if (normalizedRoot.endsWith("/api/mobile")) {
            normalizedRoot
        } else {
            "$normalizedRoot/api/mobile"
        }
    }

    fun normalizeRootUrl(rawRootUrl: String): String {
        val trimmed = rawRootUrl.trim().trimEnd('/')
        if (trimmed.isBlank()) {
            return ""
        }
        val normalizedScheme = when {
            trimmed.startsWith("https://", ignoreCase = true) -> trimmed
            trimmed.startsWith("http://", ignoreCase = true) -> trimmed
            else -> "https://$trimmed"
        }
        return normalizedScheme.removeSuffix("/api/mobile")
    }
}
