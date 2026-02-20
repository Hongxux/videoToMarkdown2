package com.example.semantictopography

/**
 * 段落滑动结算动作。
 *
 * 这是纯决策层，不依赖 UI 细节：
 * 1. 输入当前偏移和阈值。
 * 2. 输出业务语义动作。
 */
sealed class ParagraphSwipeDecision {
    data object OpenBridge : ParagraphSwipeDecision()

    data object OpenNote : ParagraphSwipeDecision()

    data object Delete : ParagraphSwipeDecision()

    data object Reset : ParagraphSwipeDecision()
}

/**
 * 段落滑动结算 reducer。
 */
fun resolveParagraphSwipeDecision(
    endOffset: Float,
    rightThreshold: Float,
    leftThreshold: Float,
    hasBridge: Boolean
): ParagraphSwipeDecision {
    return when {
        endOffset > rightThreshold && hasBridge -> ParagraphSwipeDecision.OpenBridge
        endOffset > rightThreshold && !hasBridge -> ParagraphSwipeDecision.OpenNote
        endOffset < -leftThreshold -> ParagraphSwipeDecision.Delete
        else -> ParagraphSwipeDecision.Reset
    }
}
