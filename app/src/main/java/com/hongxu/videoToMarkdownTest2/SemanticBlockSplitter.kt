package com.hongxu.videoToMarkdownTest2

/**
 * 将 SemanticNode 按 Markdown 语义块拆分为 SemanticBlock 列表。
 *
 * 拆分规则：
 * 1. 先按双换行拆为 top-level 块
 * 2. 每个 top-level 块内识别 heading / blockquote / list / code-fence / 普通段落
 * 3. 连续 list items 合并为一个块（保持 Markwon 渲染完整性）
 * 4. bridgeText / reasoning 只保留在首块
 * 5. 若 node 只产出一个块，blockId == node.id（向后兼容）
 */

/**
 * 语义块 — 拆分后的最小手势交互单元。
 */
data class SemanticBlock(
    /** 唯一标识。单块 node 时等于 parentNodeId，多块时为 "parentNodeId#index"。 */
    val blockId: String,
    /** 原始 SemanticNode.id。 */
    val parentNodeId: String,
    /** 在父 node 中的序号（0-based）。 */
    val blockIndex: Int,
    /** 父 node 拆出的总块数。 */
    val blockCount: Int,
    /** 本块的 Markdown 内容（已去除前导缩进空格，用于 Markwon 渲染）。 */
    val markdown: String,
    /** 本块的纯文本（用于选词/计数等）。 */
    val plainText: String,
    /** 缩进层级（0=无缩进，1=4空格，2=8空格...），用于 UI 左侧偏移。 */
    val indentLevel: Int,
    /** 继承自父 node。 */
    val type: String,
    /** 继承自父 node。 */
    val relevanceScore: Float,
    /** 仅 blockIndex==0 时有值。 */
    val bridgeText: String?,
    /** 仅 blockIndex==0 时有值。 */
    val reasoning: String?,
    /** 继承自父 node。 */
    val insightTerms: List<String>,
    /** 继承自父 node。 */
    val insightsTags: List<String>
) {
    fun resolvedInsightTerms(): List<String> {
        return (insightTerms + insightsTags)
            .map { it.trim() }
            .filter { it.isNotBlank() }
            .distinct()
    }
}

/**
 * 入口：将 node 列表展开为 block 列表。
 */
fun splitSemanticNodesIntoBlocks(nodes: List<SemanticNode>): List<SemanticBlock> {
    return nodes.flatMap { node -> splitSingleNode(node) }
}

// ─── 内部实现 ─────────────────────────────────────────────

/**
 * 拆分单个 SemanticNode。
 */
internal fun splitSingleNode(node: SemanticNode): List<SemanticBlock> {
    val markdown = node.originalMarkdown ?: node.text
    if (markdown.isBlank()) {
        return listOf(nodeToSingleBlock(node))
    }
    val rawBlocks = splitMarkdownIntoSemanticBlocks(markdown)
    if (rawBlocks.size <= 1) {
        return listOf(nodeToSingleBlock(node))
    }
    return rawBlocks.mapIndexed { index, blockMarkdown ->
        val stripped = stripLeadingIndent(blockMarkdown)
        SemanticBlock(
            blockId = "${node.id}#$index",
            parentNodeId = node.id,
            blockIndex = index,
            blockCount = rawBlocks.size,
            markdown = stripped.text,
            plainText = stripMarkdownToPlainText(stripped.text),
            indentLevel = stripped.indentLevel,
            type = node.type,
            relevanceScore = node.relevanceScore,
            bridgeText = if (index == 0) node.bridgeText else null,
            reasoning = if (index == 0) node.reasoning else null,
            insightTerms = node.insightTerms,
            insightsTags = node.insightsTags
        )
    }
}

/**
 * 不拆分时直接包装为单块。blockId == node.id 以保持向后兼容。
 */
private fun nodeToSingleBlock(node: SemanticNode): SemanticBlock {
    val markdown = node.originalMarkdown ?: node.text
    return SemanticBlock(
        blockId = node.id,
        parentNodeId = node.id,
        blockIndex = 0,
        blockCount = 1,
        markdown = markdown,
        plainText = node.text,
        indentLevel = 0,
        type = node.type,
        relevanceScore = node.relevanceScore,
        bridgeText = node.bridgeText,
        reasoning = node.reasoning,
        insightTerms = node.insightTerms,
        insightsTags = node.insightsTags
    )
}

// ─── Markdown 语义块拆分核心 ──────────────────────────────

/**
 * 将 Markdown 文本拆分为语义块列表。
 *
 * 识别规则（优先级从高到低）：
 * 1. Fenced code block (```) — 整个 fence 作为一个块
 * 2. Heading (# / ## / ...) — 独立一块
 * 3. Blockquote (> ...) — 连续 blockquote 行合并为一块
 * 4. List items (- / * / + / 1. ) — 连续 list items 合并为一块（含缩进子项）
 * 5. 普通段落 — 连续非空行合并为一块
 * 6. 空行分隔 — 空行本身被丢弃
 */
internal fun splitMarkdownIntoSemanticBlocks(markdown: String): List<String> {
    val lines = markdown.lines()
    val blocks = mutableListOf<String>()
    val currentBlock = mutableListOf<String>()
    var currentType = BlockType.NONE
    var inFencedCode = false

    fun flushCurrent() {
        if (currentBlock.isNotEmpty()) {
            blocks.add(currentBlock.joinToString("\n"))
            currentBlock.clear()
        }
        currentType = BlockType.NONE
    }

    for (line in lines) {
        // ── fenced code block 处理 ──
        if (FENCED_CODE_PATTERN.matches(line.trimEnd())) {
            if (!inFencedCode) {
                // 开始 fence：先 flush 之前的块
                flushCurrent()
                inFencedCode = true
                currentBlock.add(line)
                currentType = BlockType.CODE_FENCE
                continue
            } else {
                // 结束 fence
                currentBlock.add(line)
                flushCurrent()
                inFencedCode = false
                continue
            }
        }
        if (inFencedCode) {
            currentBlock.add(line)
            continue
        }

        // ── 空行 → flush ──
        if (line.isBlank()) {
            flushCurrent()
            continue
        }

        val lineType = classifyLine(line)

        when (lineType) {
            BlockType.HEADING -> {
                // heading 总是独立一块
                flushCurrent()
                currentBlock.add(line)
                flushCurrent()
            }
            BlockType.BLOCKQUOTE -> {
                if (currentType != BlockType.BLOCKQUOTE) {
                    flushCurrent()
                }
                currentBlock.add(line)
                currentType = BlockType.BLOCKQUOTE
            }
            BlockType.LIST_ITEM -> {
                // 每个 list item 独立一块
                flushCurrent()
                currentBlock.add(line)
                currentType = BlockType.LIST_ITEM
            }
            BlockType.LIST_CONTINUATION -> {
                // 缩进行：如果前面是 list item，归入当前 list item（子项）；否则归入段落
                if (currentType == BlockType.LIST_ITEM || currentType == BlockType.LIST_CONTINUATION) {
                    currentBlock.add(line)
                    currentType = BlockType.LIST_CONTINUATION
                } else {
                    // 当作段落延续
                    currentBlock.add(line)
                    if (currentType == BlockType.NONE) {
                        currentType = BlockType.PARAGRAPH
                    }
                }
            }
            BlockType.PARAGRAPH -> {
                if (currentType != BlockType.PARAGRAPH) {
                    flushCurrent()
                }
                currentBlock.add(line)
                currentType = BlockType.PARAGRAPH
            }
            else -> {
                currentBlock.add(line)
                if (currentType == BlockType.NONE) {
                    currentType = BlockType.PARAGRAPH
                }
            }
        }
    }

    // flush 残余
    flushCurrent()

    return blocks.filter { it.isNotBlank() }
}

/**
 * 行分类。
 */
private fun classifyLine(line: String): BlockType {
    val trimmed = line.trimStart()
    // 先用 trimmed 内容判断是否为 list marker（无论缩进深度）
    val isList = UNORDERED_LIST_PATTERN.matches(trimmed) || ORDERED_LIST_PATTERN.matches(trimmed)
    if (isList) {
        return BlockType.LIST_ITEM // 每个 list item（含缩进子项）都独立拆分
    }
    // 缩进行但不是 list marker → continuation
    val isIndented = line.startsWith("  ") || line.startsWith("\t")
    if (isIndented) {
        return BlockType.LIST_CONTINUATION
    }
    return when {
        HEADING_PATTERN.matches(trimmed) -> BlockType.HEADING
        BLOCKQUOTE_PATTERN.matches(trimmed) -> BlockType.BLOCKQUOTE
        else -> BlockType.PARAGRAPH
    }
}

private enum class BlockType {
    NONE,
    HEADING,
    BLOCKQUOTE,
    LIST_ITEM,
    LIST_CONTINUATION,
    PARAGRAPH,
    CODE_FENCE
}

// ─── 正则模式 ─────────────────────────────────────────────

private val FENCED_CODE_PATTERN = Regex("^\\s*(`{3,}|~{3,}).*$")
private val HEADING_PATTERN = Regex("^#{1,6}\\s+.*")
private val BLOCKQUOTE_PATTERN = Regex("^>.*")
private val UNORDERED_LIST_PATTERN = Regex("^[-*+•]\\s+.*")
private val ORDERED_LIST_PATTERN = Regex("^\\d+[.)\\u3001]\\s+.*")

// ─── 辅助工具 ─────────────────────────────────────────────

/**
 * 粗略去除 Markdown 标记，用于 plainText 字段。
 * 不需要精确——仅用于计数和词选择。
 */
internal fun stripMarkdownToPlainText(markdown: String): String {
    var text = markdown
    // 去 heading 标记
    text = text.replace(Regex("(?m)^#{1,6}\\s+"), "")
    // 去 blockquote 标记
    text = text.replace(Regex("(?m)^>\\s?"), "")
    // 去 bold/italic
    text = text.replace(Regex("\\*{1,3}([^*]+)\\*{1,3}"), "$1")
    // 去 inline code
    text = text.replace(Regex("`([^`]+)`"), "$1")
    // 去 links [text](url)
    text = text.replace(Regex("\\[([^]]+)]\\([^)]+\\)"), "$1")
    // 去 images ![[...]] or ![...](...)
    text = text.replace(Regex("!\\[\\[([^]]*)]\\]"), "")
    text = text.replace(Regex("!\\[([^]]*)]\\([^)]+\\)"), "")
    return text.trim()
}

// ─── 缩进剥离 ─────────────────────────────────────────────

internal data class StrippedBlock(
    val text: String,
    val indentLevel: Int
)

/**
 * 剥离 block 的前导缩进空格，返回去除缩进后的文本和缩进层级。
 * 缩进层级按首行的前导空格数计算，每 4 空格为一级（tab 算 4 空格）。
 */
internal fun stripLeadingIndent(blockMarkdown: String): StrippedBlock {
    val lines = blockMarkdown.lines()
    if (lines.isEmpty()) {
        return StrippedBlock(blockMarkdown, 0)
    }
    // 用首行的前导空格数来决定缩进量
    val firstLine = lines.first()
    val leadingSpaces = firstLine.length - firstLine.trimStart().length
    if (leadingSpaces == 0) {
        return StrippedBlock(blockMarkdown, 0)
    }
    val indentLevel = (leadingSpaces + 3) / 4 // 向上取整：1-4 空格=1级，5-8=2级
    // 从每一行去掉最多 leadingSpaces 个前导空格
    val stripped = lines.joinToString("\n") { line ->
        if (line.length >= leadingSpaces && line.substring(0, leadingSpaces).isBlank()) {
            line.substring(leadingSpaces)
        } else {
            line.trimStart()
        }
    }
    return StrippedBlock(stripped, indentLevel)
}

