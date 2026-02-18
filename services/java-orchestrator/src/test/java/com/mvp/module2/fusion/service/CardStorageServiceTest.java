package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CardStorageServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldWriteFrontmatterAndBody() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        CardStorageService.CardWriteOptions options = new CardStorageService.CardWriteOptions();
        options.type = "concept";
        options.created = "2024-03-21";
        options.tags = List.of("physics", "philosophy");

        CardStorageService.CardSaveResult save = service.saveCard(
                "entropy",
                "## Entropy\n\nA short note.",
                options
        );
        assertNotNull(save);

        CardStorageService.CardReadResult read = service.readCard("entropy");
        assertTrue(read.exists);
        assertEquals("2024-03-21", read.created);
        assertEquals("concept", read.type);
        assertEquals(List.of("physics", "philosophy"), read.tags);
        assertTrue(read.markdown.contains("## Entropy"));
        assertFalse(read.markdown.contains("反向链接"));

        String persisted = Files.readString(read.path, StandardCharsets.UTF_8);
        assertTrue(persisted.startsWith("---\n"));
        assertTrue(persisted.contains("title: \"entropy\""));
        assertTrue(persisted.contains("created: \"2024-03-21\""));
        assertTrue(persisted.contains("tags: [\"physics\", \"philosophy\"]"));
        assertTrue(persisted.contains("type: \"concept\""));
    }

    @Test
    void shouldPreserveExistingCreatedWhenSavingAgain() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        CardStorageService.CardWriteOptions first = new CardStorageService.CardWriteOptions();
        first.created = "2024-01-01";
        first.type = "concept";
        first.tags = List.of("a");
        service.saveCard("first_principles", "draft-v1", first);

        CardStorageService.CardWriteOptions second = new CardStorageService.CardWriteOptions();
        second.type = "context";
        second.tags = List.of("b");
        service.saveCard("first_principles", "draft-v2", second);

        CardStorageService.CardReadResult read = service.readCard("first_principles");
        assertEquals("2024-01-01", read.created);
        assertEquals("context", read.type);
        assertEquals(List.of("b"), read.tags);
        assertTrue(read.markdown.contains("draft-v1"));
        assertTrue(read.markdown.contains("draft-v2"));
        assertTrue(read.markdown.contains("```markdown"));
    }

    @Test
    void shouldAppendBacklinkSectionWithoutOverwritingManualEntries() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        service.saveCard(
                "entropy",
                """
                ## Note
                line-1

                ## 🔗 反向链接
                - manual-entry
                """,
                new CardStorageService.CardWriteOptions()
        );

        CardStorageService.CardWriteOptions options = new CardStorageService.CardWriteOptions();
        options.sourceTaskId = "task-a";
        options.sourcePath = "tasks/task-a/notes.md";
        service.saveCard("entropy", "line-1", options);
        service.saveCard("entropy", "line-1", options);

        String persisted = Files.readString(
                tempDir.resolve("cards").resolve("entropy.md"),
                StandardCharsets.UTF_8
        );
        assertTrue(persisted.contains("## 🔗 反向链接"));
        assertTrue(persisted.contains("- manual-entry"));
        assertTrue(persisted.contains("- task `task-a` @ `tasks/task-a/notes.md`"), persisted);

        int firstIndex = persisted.indexOf("- task `task-a` @ `tasks/task-a/notes.md`");
        int lastIndex = persisted.lastIndexOf("- task `task-a` @ `tasks/task-a/notes.md`");
        assertEquals(firstIndex, lastIndex);
    }

    @Test
    void shouldPreserveManualDesktopEditsOnConflict() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        CardStorageService.CardWriteOptions base = new CardStorageService.CardWriteOptions();
        base.created = "2024-03-21";
        base.type = "concept";
        service.saveCard("entropy", "initial", base);

        Path file = tempDir.resolve("cards").resolve("entropy.md");
        Files.writeString(file, """
                ---
                title: "entropy"
                created: "2024-03-21"
                tags: []
                type: "concept"
                ---

                desktop-manual-edit
                """, StandardCharsets.UTF_8);

        CardStorageService.CardWriteOptions stale = new CardStorageService.CardWriteOptions();
        stale.type = "concept";
        stale.sourceTaskId = "task-stale";
        stale.sourcePath = "desktop/manual.md";
        service.saveCard("entropy", "mobile-stale-edit", stale);

        String persisted = Files.readString(file, StandardCharsets.UTF_8);
        assertTrue(persisted.contains("desktop-manual-edit"));
        assertTrue(persisted.contains("待合并草稿"));
        assertTrue(persisted.contains("mobile-stale-edit"));
        assertTrue(persisted.contains("- task `task-stale` @ `desktop/manual.md`"), persisted);
    }

    @Test
    void shouldReturnNotFoundWhenCardMissing() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        CardStorageService.CardReadResult read = service.readCard("missing-card");
        assertFalse(read.exists);
        assertEquals("", read.markdown);
    }

    @Test
    void shouldInsertTearCalloutAfterAnchorParagraph() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        Path source = tempDir.resolve("notes").resolve("demo.md");
        Files.createDirectories(source.getParent());
        Files.writeString(source, """
                第一段内容，包含 anchor 关键词。

                第二段内容。
                """, StandardCharsets.UTF_8);

        CardStorageService.CardSaveResult save = service.saveThought(
                source.toString(),
                "anchor 关键词",
                "这是当下思考"
        );

        String updated = Files.readString(source, StandardCharsets.UTF_8);
        assertTrue(updated.contains("> [!TEAR]\n> 这是当下思考"));
        assertEquals("local", save.targetType);
        assertEquals(source.toString(), save.targetPath);
        assertEquals("local", save.type);
    }

    @Test
    void shouldRejectThoughtSaveWhenAnchorMissing() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        Path source = tempDir.resolve("notes").resolve("demo.md");
        Files.createDirectories(source.getParent());
        Files.writeString(source, "只是一段正文", StandardCharsets.UTF_8);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> service.saveThought(source.toString(), "不存在锚点", "内容")
        );
        assertTrue(error.getMessage().contains("anchor"));
    }

    @Test
    void shouldSanitizeWindowsReservedAndIllegalTitles() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));

        service.saveCard("CON", "reserved", null);
        CardStorageService.CardReadResult con = service.readCard("CON");
        assertTrue(con.exists);
        assertEquals("_CON", con.title);
        assertEquals("_CON.md", con.path.getFileName().toString());

        service.saveCard("AUX.txt", "reserved-with-ext", null);
        CardStorageService.CardReadResult aux = service.readCard("AUX.txt");
        assertTrue(aux.exists);
        assertEquals("_AUX.txt", aux.title);
        assertEquals("_AUX.txt.md", aux.path.getFileName().toString());

        service.saveCard("complex:term?*<>|", "illegal", null);
        CardStorageService.CardReadResult illegal = service.readCard("complex:term?*<>|");
        assertTrue(illegal.exists);
        assertEquals("complex_term_____", illegal.title);
        assertEquals("complex_term_____.md", illegal.path.getFileName().toString());

        service.saveCard("concept   .", "trailing-dot", null);
        CardStorageService.CardReadResult trailing = service.readCard("concept   .");
        assertTrue(trailing.exists);
        assertEquals("concept", trailing.title);
        assertEquals("concept.md", trailing.path.getFileName().toString());
    }

    @Test
    void shouldListBacklinksByWikilinkTitle() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        service.saveCard("热力学第二定律", "基础概念", null);
        service.saveCard("熵增", "参考 [[热力学第二定律]] 与 [[热力学第二定律|二律背反]]", null);
        service.saveCard("系统论", "和 [[热力学第二定律]] 有交叉", null);
        service.saveCard("无关卡片", "这里只提到 [[别的概念]]", null);

        List<CardStorageService.CardBacklinkItem> backlinks = service.listBacklinks("热力学第二定律");
        assertEquals(2, backlinks.size());
        int entropyCount = backlinks.stream()
                .filter(item -> "熵增".equals(item.sourceTitle))
                .mapToInt(item -> item.count)
                .findFirst()
                .orElse(0);
        int systemCount = backlinks.stream()
                .filter(item -> "系统论".equals(item.sourceTitle))
                .mapToInt(item -> item.count)
                .findFirst()
                .orElse(0);
        assertEquals(2, entropyCount);
        assertEquals(1, systemCount);
    }

    private CardStorageService createService(Path cardsRoot) throws Exception {
        CardStorageService service = new CardStorageService();
        setField(service, "configuredCardsRoot", cardsRoot.toString());
        service.init();
        return service;
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
