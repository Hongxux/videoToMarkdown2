package com.hongxu.videoToMarkdownTest2

import android.content.Context
import java.util.UUID

object MobileClientIdentity {
    private const val PREFS_NAME = "mobile_client_identity"
    private const val KEY_USER_ID = "user_id"

    fun resolveUserId(context: Context): String {
        val appContext = context.applicationContext
        val preferences = appContext.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        val cached = preferences.getString(KEY_USER_ID, null)?.trim().orEmpty()
        if (cached.isNotEmpty()) {
            return cached
        }
        val generated = "android_${UUID.randomUUID()}"
        preferences.edit().putString(KEY_USER_ID, generated).apply()
        return generated
    }
}
