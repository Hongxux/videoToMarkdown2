package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;

class BookMarkdownProtectionUtilsTest {

    @Test
    void protectAndRestoreShouldKeepImageCodeTableAndFormula() {
        String markdown = String.join("\n",
                "# Sample",
                "",
                "This is a paragraph before image.",
                "",
                "![image-1](./assets/a.png)",
                "",
                "| colA | colB |",
                "| --- | --- |",
                "| v1 | v2 |",
                "",
                "```python",
                "print('hello')",
                "```",
                "",
                "$$",
                "E = mc^2",
                "$$",
                "",
                "Tail paragraph."
        );

        BookMarkdownProtectionUtils.ProtectionResult protectedResult =
                BookMarkdownProtectionUtils.protectMarkdown(markdown);
        Assertions.assertNotNull(protectedResult);
        Assertions.assertTrue(protectedResult.getProtectedCount() >= 4);

        List<BookMarkdownProtectionUtils.ProtectedBlock> blocks = protectedResult.getBlocks();
        StringBuilder protectedMarkdown = new StringBuilder();
        for (BookMarkdownProtectionUtils.ProtectedBlock block : blocks) {
            if (block == null) {
                continue;
            }
            if (protectedMarkdown.length() > 0) {
                protectedMarkdown.append("\n\n");
            }
            protectedMarkdown.append(block.getWorkingText());
        }

        String restored = BookMarkdownProtectionUtils.restoreProtectedBlocks(
                protectedMarkdown.toString(),
                new LinkedHashMap<>(protectedResult.getTokenToOriginalBlock())
        );

        Assertions.assertTrue(restored.contains("![image-1](./assets/a.png)"));
        Assertions.assertTrue(restored.contains("```python"));
        Assertions.assertTrue(restored.contains("| colA | colB |"));
        Assertions.assertTrue(restored.contains("E = mc^2"));
    }

    @Test
    void generatedTokenShouldNotCollideWithSourceToken() {
        String existingToken = "[[SYS_MEDIA_ABCDEF1234567890ABCD]]";
        String markdown = String.join("\n",
                existingToken,
                "正文",
                "![image-1](./assets/a.png)"
        );

        BookMarkdownProtectionUtils.ProtectionResult protectedResult =
                BookMarkdownProtectionUtils.protectMarkdown(markdown);
        Assertions.assertNotNull(protectedResult);
        Assertions.assertFalse(protectedResult.getTokenToOriginalBlock().isEmpty());
        for (String token : protectedResult.getTokenToOriginalBlock().keySet()) {
            Assertions.assertNotEquals(existingToken, token);
        }
    }
}
