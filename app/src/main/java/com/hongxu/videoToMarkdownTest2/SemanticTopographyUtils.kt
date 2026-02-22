package com.hongxu.videoToMarkdownTest2

import androidx.compose.foundation.gestures.animateScrollBy
import androidx.compose.foundation.lazy.LazyListState
import androidx.compose.ui.graphics.Color
import org.json.JSONObject
import kotlin.math.abs
import kotlin.math.roundToInt

fun resolveTokenSelection(
    text: String,
    cursor: Int,
    nativePayload: String?
): TokenSelection? {
    val fromNative = parseSegmentPayload(
        text = text,
        payload = nativePayload
    )
    if (fromNative != null) {
        return fromNative
    }
    return fallbackTokenSelection(text, cursor)
}

private fun parseSegmentPayload(
    text: String,
    payload: String?
): TokenSelection? {
    if (payload.isNullOrBlank()) {
        return null
    }
    return runCatching {
        val json = JSONObject(payload)
        val start = json.optInt("start", -1)
        val end = json.optInt("end", -1)
        val token = json.optString("token", "")
        if (start < 0 || end <= start || end > text.length) {
            null
        } else {
            val normalizedToken = if (token.isNotBlank()) {
                token
            } else {
                text.substring(start, end)
            }
            TokenSelection(
                token = normalizedToken,
                start = start,
                end = end
            )
        }
    }.getOrNull()
}

private fun fallbackTokenSelection(
    text: String,
    cursor: Int
): TokenSelection? {
    if (text.isEmpty()) {
        return null
    }
    val boundedCursor = cursor.coerceIn(0, text.lastIndex)
    if (!isTokenChar(text[boundedCursor])) {
        return null
    }

    var start = boundedCursor
    var end = boundedCursor + 1
    while (start > 0 && isTokenChar(text[start - 1])) {
        start -= 1
    }
    while (end < text.length && isTokenChar(text[end])) {
        end += 1
    }
    if (start >= end) {
        return null
    }

    return TokenSelection(
        token = text.substring(start, end),
        start = start,
        end = end
    )
}

private fun isTokenChar(value: Char): Boolean {
    return value.isLetterOrDigit() || value == '_' || (value.code in 0x4E00..0x9FFF)
}

fun parseTokenInsightCard(
    token: String,
    nativePayload: String?
): TokenInsightCard {
    val parsed = runCatching {
        val json = JSONObject(nativePayload.orEmpty())
        TokenInsightCard(
            token = token,
            contextualize = json.optString("contextualize"),
            firstPrinciple = json.optString("first_principle"),
            industryHorizon = json.optString("industry_horizon")
        )
    }.getOrNull()

    if (parsed != null &&
        parsed.contextualize.isNotBlank() &&
        parsed.firstPrinciple.isNotBlank() &&
        parsed.industryHorizon.isNotBlank()
    ) {
        return parsed
    }

    return TokenInsightCard(
        token = token,
        contextualize = "In this context, \"$token\" carries key semantic weight and changes reading intent.",
        firstPrinciple = "From first principles, \"$token\" compresses complex meaning and reduces cognitive jumps.",
        industryHorizon = "In practice, \"$token\" can map to observable metrics or operational decisions."
    )
}

suspend fun autoCenterItem(
    listState: LazyListState,
    itemIndex: Int,
    centerRatio: Float
) {
    val ratio = centerRatio.coerceIn(0.35f, 0.65f)
    val firstLookup = listState.layoutInfo.visibleItemsInfo.firstOrNull { it.index == itemIndex }
    if (firstLookup == null) {
        listState.animateScrollToItem(itemIndex)
    }

    val target = listState.layoutInfo.visibleItemsInfo.firstOrNull { it.index == itemIndex }
        ?: return
    val viewportStart = listState.layoutInfo.viewportStartOffset
    val viewportEnd = listState.layoutInfo.viewportEndOffset
    val targetCenter = viewportStart + ((viewportEnd - viewportStart) * ratio).roundToInt()
    val itemCenter = target.offset + target.size / 2
    val delta = (itemCenter - targetCenter).toFloat()
    if (abs(delta) > 1f) {
        listState.animateScrollBy(delta)
    }
}

fun Color.toArgbSafe(): Int {
    val a = (alpha * 255).roundToInt().coerceIn(0, 255)
    val r = (red * 255).roundToInt().coerceIn(0, 255)
    val g = (green * 255).roundToInt().coerceIn(0, 255)
    val b = (blue * 255).roundToInt().coerceIn(0, 255)
    return (a shl 24) or (r shl 16) or (g shl 8) or b
}
