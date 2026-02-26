package com.hongxu.videoToMarkdownTest2

import org.junit.Assert.*
import org.junit.Test

class SemanticBlockSplitterTest {

    // ─── 基础场景 ─────────────────────────────────────────

    @Test
    fun `single paragraph node - no split`() {
        val node = makeNode(id = "n1", text = "Hello world, this is a single paragraph.")
        val blocks = splitSemanticNodesIntoBlocks(listOf(node))
        assertEquals(1, blocks.size)
        assertEquals("n1", blocks[0].blockId) // 向后兼容：单块 ID == node ID
        assertEquals("n1", blocks[0].parentNodeId)
        assertEquals(0, blocks[0].blockIndex)
        assertEquals(1, blocks[0].blockCount)
    }

    @Test
    fun `blank node - no crash`() {
        val node = makeNode(id = "n0", text = "")
        val blocks = splitSemanticNodesIntoBlocks(listOf(node))
        assertEquals(1, blocks.size)
        assertEquals("n0", blocks[0].blockId)
    }

    // ─── 多块拆分 ─────────────────────────────────────────

    @Test
    fun `heading + paragraph - splits into 2 blocks`() {
        val md = """
            |### 标题
            |
            |这是一段正文。
        """.trimMargin()
        val node = makeNode(id = "n2", text = md, originalMarkdown = md)
        val blocks = splitSemanticNodesIntoBlocks(listOf(node))
        assertEquals(2, blocks.size)
        assertEquals("n2#0", blocks[0].blockId)
        assertEquals("n2#1", blocks[1].blockId)
        assertTrue(blocks[0].markdown.contains("### 标题"))
        assertTrue(blocks[1].markdown.contains("这是一段正文"))
    }

    @Test
    fun `heading + blockquote + list items - splits into 5 blocks`() {
        val md = """
            |## AI 趋势
            |
            |> 核心论点：AI正在取代人类。
            |
            |- 第一点：效率提升
            |- 第二点：成本降低
            |    - 子项：降低50%
        """.trimMargin()
        val node = makeNode(id = "n3", text = md, originalMarkdown = md)
        val blocks = splitSemanticNodesIntoBlocks(listOf(node))
        assertEquals(5, blocks.size)
        assertTrue(blocks[0].markdown.contains("## AI 趋势"))
        assertTrue(blocks[1].markdown.contains("> 核心论点"))
        assertTrue(blocks[2].markdown.contains("- 第一点"))
        assertTrue(blocks[3].markdown.contains("- 第二点"))
        assertTrue(blocks[4].markdown.contains("- 子项"))
    }

    // ─── bridgeText 仅首块 ────────────────────────────────

    @Test
    fun `bridgeText only on first block`() {
        val md = """
            |### 标题
            |
            |正文段落。
        """.trimMargin()
        val node = makeNode(
            id = "n4",
            text = md,
            originalMarkdown = md,
            bridgeText = "桥接引导文本",
            reasoning = "推理说明"
        )
        val blocks = splitSemanticNodesIntoBlocks(listOf(node))
        assertEquals(2, blocks.size)
        assertEquals("桥接引导文本", blocks[0].bridgeText)
        assertEquals("推理说明", blocks[0].reasoning)
        assertNull(blocks[1].bridgeText)
        assertNull(blocks[1].reasoning)
    }

    // ─── 分数继承 ─────────────────────────────────────────

    @Test
    fun `all blocks inherit relevance score`() {
        val md = """
            |## 标题
            |
            |段落一。
            |
            |段落二。
        """.trimMargin()
        val node = makeNode(id = "n5", text = md, originalMarkdown = md, relevanceScore = 0.87f)
        val blocks = splitSemanticNodesIntoBlocks(listOf(node))
        assertEquals(3, blocks.size)
        blocks.forEach { block ->
            assertEquals(0.87f, block.relevanceScore, 0.001f)
        }
    }

    // ─── Fenced code block 不拆开 ─────────────────────────

    @Test
    fun `fenced code block stays together`() {
        val md = """
            |段落前。
            |
            |```kotlin
            |val x = 1
            |val y = 2
            |```
            |
            |段落后。
        """.trimMargin()
        val node = makeNode(id = "n6", text = md, originalMarkdown = md)
        val blocks = splitSemanticNodesIntoBlocks(listOf(node))
        assertEquals(3, blocks.size)
        assertTrue(blocks[0].markdown.contains("段落前"))
        assertTrue(blocks[1].markdown.contains("```kotlin"))
        assertTrue(blocks[1].markdown.contains("val x = 1"))
        assertTrue(blocks[1].markdown.contains("```"))
        assertTrue(blocks[2].markdown.contains("段落后"))
    }

    // ─── 连续列表不被拆散 ─────────────────────────────────

    @Test
    fun `continuous list items split into individual blocks`() {
        val md = """
            |- 第一点
            |- 第二点
            |- 第三点
        """.trimMargin()
        val node = makeNode(id = "n7", text = md, originalMarkdown = md)
        val blocks = splitSemanticNodesIntoBlocks(listOf(node))
        assertEquals(3, blocks.size)
        assertEquals("n7#0", blocks[0].blockId)
        assertEquals("n7#1", blocks[1].blockId)
        assertEquals("n7#2", blocks[2].blockId)
        assertTrue(blocks[0].markdown.contains("第一点"))
        assertTrue(blocks[1].markdown.contains("第二点"))
        assertTrue(blocks[2].markdown.contains("第三点"))
    }

    @Test
    fun `list item with indented sub-items splits each item`() {
        val md = """
            |- 主项目
            |    - 子项A
            |    - 子项B
        """.trimMargin()
        val node = makeNode(id = "n7b", text = md, originalMarkdown = md)
        val blocks = splitSemanticNodesIntoBlocks(listOf(node))
        assertEquals(3, blocks.size)
        assertEquals("n7b#0", blocks[0].blockId)
        assertEquals("n7b#1", blocks[1].blockId)
        assertEquals("n7b#2", blocks[2].blockId)
        assertTrue(blocks[0].markdown.contains("主项目"))
        assertTrue(blocks[1].markdown.contains("子项A"))
        assertTrue(blocks[2].markdown.contains("子项B"))
    }

    @Test
    fun `heading then list - each item including sub-items is separate block`() {
        val md = """
            |### 列表标题
            |
            |- 项目A
            |- 项目B
            |    - 子项
        """.trimMargin()
        val node = makeNode(id = "n8", text = md, originalMarkdown = md)
        val blocks = splitSemanticNodesIntoBlocks(listOf(node))
        assertEquals(4, blocks.size)
        assertTrue(blocks[0].markdown.contains("### 列表标题"))
        assertTrue(blocks[1].markdown.contains("- 项目A"))
        assertTrue(blocks[2].markdown.contains("- 项目B"))
        assertTrue(blocks[3].markdown.contains("- 子项"))
    }

    // ─── 多个 node 一起拆分 ────────────────────────────────

    @Test
    fun `multiple nodes expand correctly`() {
        val nodes = listOf(
            makeNode(id = "a", text = "Simple node"),
            makeNode(
                id = "b",
                text = "## Title\n\nParagraph",
                originalMarkdown = "## Title\n\nParagraph"
            )
        )
        val blocks = splitSemanticNodesIntoBlocks(nodes)
        assertEquals(3, blocks.size)
        assertEquals("a", blocks[0].blockId)       // 单块保持
        assertEquals("b#0", blocks[1].blockId)      // 拆分
        assertEquals("b#1", blocks[2].blockId)
        assertEquals("b", blocks[1].parentNodeId)
        assertEquals("b", blocks[2].parentNodeId)
    }

    // ─── insightTerms 继承 ────────────────────────────────

    @Test
    fun `insight terms inherited by all blocks`() {
        val md = """
            |## 标题
            |
            |正文。
        """.trimMargin()
        val terms = listOf("AI", "效率")
        val tags = listOf("趋势")
        val node = makeNode(
            id = "n9",
            text = md,
            originalMarkdown = md,
            insightTerms = terms,
            insightsTags = tags
        )
        val blocks = splitSemanticNodesIntoBlocks(listOf(node))
        assertEquals(2, blocks.size)
        blocks.forEach { block ->
            assertEquals(terms, block.insightTerms)
            assertEquals(tags, block.insightsTags)
        }
    }

    // ─── blockquote 连续合并 ──────────────────────────────

    @Test
    fun `consecutive blockquote lines merged`() {
        val md = """
            |> 引用行一
            |> 引用行二
            |
            |正文。
        """.trimMargin()
        val node = makeNode(id = "n10", text = md, originalMarkdown = md)
        val blocks = splitSemanticNodesIntoBlocks(listOf(node))
        assertEquals(2, blocks.size)
        assertTrue(blocks[0].markdown.contains("> 引用行一"))
        assertTrue(blocks[0].markdown.contains("> 引用行二"))
        assertTrue(blocks[1].markdown.contains("正文"))
    }

    // ─── stripMarkdownToPlainText ─────────────────────────

    @Test
    fun `strip markdown removes formatting`() {
        val md = "### **加粗标题** `code` [链接](http://example.com)"
        val plain = stripMarkdownToPlainText(md)
        assertFalse(plain.contains("###"))
        assertFalse(plain.contains("**"))
        assertFalse(plain.contains("`"))
        assertFalse(plain.contains("["))
        assertTrue(plain.contains("加粗标题"))
        assertTrue(plain.contains("code"))
        assertTrue(plain.contains("链接"))
    }

    // ─── 辅助工厂方法 ─────────────────────────────────────

    private fun makeNode(
        id: String,
        text: String,
        originalMarkdown: String? = null,
        type: String = "paragraph",
        relevanceScore: Float = 0.5f,
        bridgeText: String? = null,
        reasoning: String? = null,
        insightTerms: List<String> = emptyList(),
        insightsTags: List<String> = emptyList()
    ): SemanticNode {
        return SemanticNode(
            id = id,
            text = text,
            type = type,
            originalMarkdown = originalMarkdown,
            relevanceScore = relevanceScore,
            bridgeText = bridgeText,
            reasoning = reasoning,
            insightTerms = insightTerms,
            insightsTags = insightsTags
        )
    }
}
