package com.example.semantictopography

import androidx.compose.foundation.gestures.animateScrollBy
import androidx.compose.foundation.lazy.LazyListState
import androidx.compose.ui.graphics.Color
import org.json.JSONObject
import kotlin.math.abs
import kotlin.math.roundToInt

/**
 * 优先使用 JNI 分词结果，失败时回退到本地边界探测。
 */
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

/**
 * 解析 native 返回的分词片段协议。
 */
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

/**
 * 本地回退的词元边界计算。
 */
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

/**
 * 词元字符定义。
 */
private fun isTokenChar(value: Char): Boolean {
    return value.isLetterOrDigit() || value == '_' || (value.code in 0x4E00..0x9FFF)
}

/**
 * 解析 native 返回的三维解析卡片。
 */
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
        contextualize = "当前上下文里，“$token”承担关键语义锚点，影响这段话的理解路径。",
        firstPrinciple = "从第一性视角看，“$token”用于压缩复杂概念，减少读者的认知跳跃成本。",
        industryHorizon = "在行业实践中，可把“$token”映射为可观测指标或流程决策节点，提升复盘效率。"
    )
}

/**
 * 将段落滚动到视觉中心附近。
 */
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

/**
 * 在原型文件中避免直接依赖 Compose Color 扩展的版本差异。
 */
fun Color.toArgbSafe(): Int {
    val a = (alpha * 255).roundToInt().coerceIn(0, 255)
    val r = (red * 255).roundToInt().coerceIn(0, 255)
    val g = (green * 255).roundToInt().coerceIn(0, 255)
    val b = (blue * 255).roundToInt().coerceIn(0, 255)
    return (a shl 24) or (r shl 16) or (g shl 8) or b
}
