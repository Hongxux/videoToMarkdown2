package com.hongxu.videoToMarkdownTest2

import android.content.Context
import android.graphics.Typeface
import android.os.Build
import androidx.core.content.res.ResourcesCompat
import java.util.concurrent.ConcurrentHashMap

object MarkdownTypefaceResolver {
    private val bundledCache = ConcurrentHashMap<String, Typeface>()
    private val weightedCache = ConcurrentHashMap<String, Typeface>()

    fun resolve(
        context: Context,
        fontFamily: String,
        style: Int
    ): Typeface {
        val key = "resolve|${fontFamily.trim().lowercase()}|$style"
        return weightedCache.getOrPut(key) {
            val bundled = resolveBundledTypeface(context, fontFamily)
            if (bundled != null) {
                Typeface.create(bundled, style)
            } else {
                Typeface.create(fontFamily, style)
            }
        }
    }

    fun resolveWithWeight(
        context: Context,
        fontFamily: String,
        weight: Int,
        italic: Boolean = false
    ): Typeface {
        val key = "weight|${fontFamily.trim().lowercase()}|$weight|$italic"
        return weightedCache.getOrPut(key) {
            val bundled = resolveBundledTypeface(context, fontFamily)
            if (bundled != null) {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
                    Typeface.create(bundled, weight, italic)
                } else {
                    Typeface.create(bundled, if (weight >= 500) Typeface.BOLD else Typeface.NORMAL)
                }
            } else {
                val base = Typeface.create(fontFamily, Typeface.NORMAL)
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
                    Typeface.create(base, weight, italic)
                } else {
                    Typeface.create(base, if (weight >= 500) Typeface.BOLD else Typeface.NORMAL)
                }
            }
        }
    }

    private fun resolveBundledTypeface(
        context: Context,
        fontFamily: String
    ): Typeface? {
        if (!isNotoSerifFamily(fontFamily)) {
            return null
        }
        val normalized = fontFamily
            .lowercase()
            .replace("-", "")
            .replace("_", "")
            .replace(" ", "")
        val resId = when {
            normalized.contains("bold") -> R.font.noto_serif_bold
            normalized.contains("medium") -> R.font.noto_serif_medium
            else -> R.font.noto_serif_regular
        }
        val key = "bundled|$resId"
        val cached = bundledCache[key]
        if (cached != null) {
            return cached
        }
        val loaded = ResourcesCompat.getFont(context, resId) ?: return null
        bundledCache.putIfAbsent(key, loaded)
        return bundledCache[key] ?: loaded
    }

    private fun isNotoSerifFamily(fontFamily: String): Boolean {
        val normalized = fontFamily
            .trim()
            .lowercase()
            .replace("-", "")
            .replace("_", "")
            .replace(" ", "")
        return normalized.startsWith("notoserif")
    }
}
