package com.hongxu.videoToMarkdownTest2

import android.content.Context
import java.io.InputStreamReader
import java.util.Properties

data class MarkdownReaderRenderConfig(
    val bodyFontFamily: String,
    val mediumFontFamily: String,
    val headingFontFamily: String,
    val monospaceFontFamily: String,
    val textSizeDefaultSp: Float,
    val textSizeNoiseSp: Float,
    val textSizeFocusMinSp: Float,
    val textSizeFocusScaleSp: Float,
    val lineSpacingDefault: Float,
    val lineSpacingNoise: Float,
    val lineSpacingFocus: Float,
    val textLetterSpacing: Float,
    val listIndentInputUnitSpaces: Int,
    val listIndentOutputUnitSpaces: Int,
    val listIndentMaxDepth: Int,
    val listIndentTabSpaces: Int,
    val markwonTableCellPadding: Int,
    val markwonTableBorderWidth: Int,
    val markwonBlockMargin: Int,
    val markwonBulletWidth: Int,
    val markwonBulletStrokeWidth: Int,
    val markwonCodeBlockMargin: Int,
    val markwonBlockquoteWidth: Int,
    val markwonHeadingBreakHeight: Int,
    val markwonHeadingTextSizeMultipliers: FloatArray,
    val markwonLatexTextSizeSp: Float,
    val noiseChevronColorArgb: Int,
    val noiseChevronRotateDurationMs: Int,
    val spacingHeadingTopDp: Int,
    val spacingHeadingBottomDp: Int,
    val spacingBlockquoteTopDp: Int,
    val spacingDefaultTopDp: Int,
    val spacingDefaultBottomDp: Int,
    val spacingSubblockTopDp: Int,
    val spacingSubblockBottomDp: Int,
    val spacingIndentLevelDp: Int
) {
    companion object {
        fun defaults(): MarkdownReaderRenderConfig = MarkdownReaderRenderConfig(
            bodyFontFamily = "Source Han Sans",
            mediumFontFamily = "Source Han Sans",
            headingFontFamily = "Source Han Sans",
            monospaceFontFamily = "monospace",
            textSizeDefaultSp = 15f,
            textSizeNoiseSp = 13f,
            textSizeFocusMinSp = 19f,
            textSizeFocusScaleSp = 21f,
            lineSpacingDefault = 1.16f,
            lineSpacingNoise = 1.10f,
            lineSpacingFocus = 1.22f,
            textLetterSpacing = 0.01f,
            listIndentInputUnitSpaces = 4,
            listIndentOutputUnitSpaces = 5,
            listIndentMaxDepth = 8,
            listIndentTabSpaces = 5,
            markwonTableCellPadding = 22,
            markwonTableBorderWidth = 2,
            markwonBlockMargin = 4,
            markwonBulletWidth = 34,
            markwonBulletStrokeWidth = 2,
            markwonCodeBlockMargin = 6,
            markwonBlockquoteWidth = 10,
            markwonHeadingBreakHeight = 0,
            markwonHeadingTextSizeMultipliers = floatArrayOf(1.92f, 1.62f, 1.38f, 1.2f, 1.08f, 1.0f),
            markwonLatexTextSizeSp = 48f,
            noiseChevronColorArgb = 0xFF8DA4B5.toInt(),
            noiseChevronRotateDurationMs = 220,
            spacingHeadingTopDp = 16,
            spacingHeadingBottomDp = 4,
            spacingBlockquoteTopDp = 14,
            spacingDefaultTopDp = 6,
            spacingDefaultBottomDp = 6,
            spacingSubblockTopDp = 1,
            spacingSubblockBottomDp = 1,
            spacingIndentLevelDp = 24
        )
    }
}

object MarkdownReaderRenderConfigLoader {
    private const val CONFIG_ASSET_PATH = "markdown_reader_render.properties"

    fun load(context: Context): MarkdownReaderRenderConfig {
        val defaults = MarkdownReaderRenderConfig.defaults()
        val properties = Properties()
        runCatching {
            context.assets.open(CONFIG_ASSET_PATH).use { stream ->
                InputStreamReader(stream, Charsets.UTF_8).use { reader ->
                    properties.load(reader)
                }
            }
        }
        return MarkdownReaderRenderConfig(
            bodyFontFamily = properties.readFontFamily("font.body", defaults.bodyFontFamily),
            mediumFontFamily = properties.readFontFamily("font.medium", defaults.mediumFontFamily),
            headingFontFamily = properties.readFontFamily("font.heading", defaults.headingFontFamily),
            monospaceFontFamily = properties.readFontFamily("font.monospace", defaults.monospaceFontFamily),
            textSizeDefaultSp = properties.readFloat("text.size.default.sp", defaults.textSizeDefaultSp),
            textSizeNoiseSp = properties.readFloat("text.size.noise.sp", defaults.textSizeNoiseSp),
            textSizeFocusMinSp = properties.readFloat("text.size.focus.min.sp", defaults.textSizeFocusMinSp),
            textSizeFocusScaleSp = properties.readFloat("text.size.focus.scale.sp", defaults.textSizeFocusScaleSp),
            lineSpacingDefault = properties.readFloat("line.spacing.default", defaults.lineSpacingDefault),
            lineSpacingNoise = properties.readFloat("line.spacing.noise", defaults.lineSpacingNoise),
            lineSpacingFocus = properties.readFloat("line.spacing.focus", defaults.lineSpacingFocus),
            textLetterSpacing = properties.readFloat("text.letter.spacing.em", defaults.textLetterSpacing),
            listIndentInputUnitSpaces = properties.readInt("list.indent.input.unit.spaces", defaults.listIndentInputUnitSpaces),
            listIndentOutputUnitSpaces = properties.readInt("list.indent.output.unit.spaces", defaults.listIndentOutputUnitSpaces),
            listIndentMaxDepth = properties.readInt("list.indent.max.depth", defaults.listIndentMaxDepth),
            listIndentTabSpaces = properties.readInt("list.indent.tab.spaces", defaults.listIndentTabSpaces),
            markwonTableCellPadding = properties.readInt("markwon.table.cell.padding", defaults.markwonTableCellPadding),
            markwonTableBorderWidth = properties.readInt("markwon.table.border.width", defaults.markwonTableBorderWidth),
            markwonBlockMargin = properties.readInt("markwon.block.margin", defaults.markwonBlockMargin),
            markwonBulletWidth = properties.readInt("markwon.bullet.width", defaults.markwonBulletWidth),
            markwonBulletStrokeWidth = properties.readInt("markwon.bullet.stroke.width", defaults.markwonBulletStrokeWidth),
            markwonCodeBlockMargin = properties.readInt("markwon.code.block.margin", defaults.markwonCodeBlockMargin),
            markwonBlockquoteWidth = properties.readInt("markwon.blockquote.width", defaults.markwonBlockquoteWidth),
            markwonHeadingBreakHeight = properties.readInt("markwon.heading.break.height", defaults.markwonHeadingBreakHeight),
            markwonHeadingTextSizeMultipliers = properties.readFloatArray(
                key = "markwon.heading.text.multipliers",
                defaultValue = defaults.markwonHeadingTextSizeMultipliers
            ),
            markwonLatexTextSizeSp = properties.readFloat("markwon.latex.text.size.sp", defaults.markwonLatexTextSizeSp),
            noiseChevronColorArgb = properties.readColorArgb("noise.chevron.color", defaults.noiseChevronColorArgb),
            noiseChevronRotateDurationMs = properties.readInt("noise.chevron.rotate.duration.ms", defaults.noiseChevronRotateDurationMs),
            spacingHeadingTopDp = properties.readInt("spacing.heading.top.dp", defaults.spacingHeadingTopDp),
            spacingHeadingBottomDp = properties.readInt("spacing.heading.bottom.dp", defaults.spacingHeadingBottomDp),
            spacingBlockquoteTopDp = properties.readInt("spacing.blockquote.top.dp", defaults.spacingBlockquoteTopDp),
            spacingDefaultTopDp = properties.readInt("spacing.default.top.dp", defaults.spacingDefaultTopDp),
            spacingDefaultBottomDp = properties.readInt("spacing.default.bottom.dp", defaults.spacingDefaultBottomDp),
            spacingSubblockTopDp = properties.readInt("spacing.subblock.top.dp", defaults.spacingSubblockTopDp),
            spacingSubblockBottomDp = properties.readInt("spacing.subblock.bottom.dp", defaults.spacingSubblockBottomDp),
            spacingIndentLevelDp = properties.readInt("spacing.indent.level.dp", defaults.spacingIndentLevelDp)
        )
    }
}

private fun Properties.readFloat(key: String, defaultValue: Float): Float {
    val raw = getProperty(key)?.trim().orEmpty()
    return raw.toFloatOrNull() ?: defaultValue
}

private fun Properties.readInt(key: String, defaultValue: Int): Int {
    val raw = getProperty(key)?.trim().orEmpty()
    return raw.toIntOrNull() ?: defaultValue
}

private fun Properties.readFloatArray(key: String, defaultValue: FloatArray): FloatArray {
    val raw = getProperty(key)?.trim().orEmpty()
    if (raw.isEmpty()) {
        return defaultValue
    }
    val parsed = raw
        .split(',')
        .mapNotNull { token -> token.trim().toFloatOrNull() }
    return if (parsed.isEmpty()) defaultValue else parsed.toFloatArray()
}

private fun Properties.readFontFamily(key: String, defaultValue: String): String {
    val raw = getProperty(key)?.trim().orEmpty()
    if (raw.isEmpty()) {
        return defaultValue
    }
    val first = raw
        .split(',')
        .firstOrNull()
        .orEmpty()
        .trim()
        .trim('"', '\'')
    return if (first.isNotEmpty()) first else defaultValue
}

private fun Properties.readColorArgb(key: String, defaultValue: Int): Int {
    val raw = getProperty(key)?.trim().orEmpty()
    if (raw.isEmpty()) {
        return defaultValue
    }
    val normalized = raw.removePrefix("#")
    return when (normalized.length) {
        6 -> normalized.toLongOrNull(16)?.let { (0xFF000000 or it).toInt() } ?: defaultValue
        8 -> normalized.toLongOrNull(16)?.toInt() ?: defaultValue
        else -> defaultValue
    }
}
