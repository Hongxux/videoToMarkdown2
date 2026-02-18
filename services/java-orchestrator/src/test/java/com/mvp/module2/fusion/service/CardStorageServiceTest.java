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
        assertFalse(read.markdown.contains("Backlinks"));

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
    void shouldPersistThoughtTypeWhenRequested() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        CardStorageService.CardWriteOptions options = new CardStorageService.CardWriteOptions();
        options.type = "thought";
        options.tags = List.of("zettel");

        service.saveCard(
                "apple",
                """
                - claim: a thought card should keep thought type.
                - evidence: this is a simplified test payload.
                """,
                options
        );

        CardStorageService.CardReadResult read = service.readCard("apple");
        assertEquals("thought", read.type);
        assertEquals(List.of("zettel"), read.tags);
    }

    @Test
    void shouldNotAppendBacklinkEntryWhenSourceMetadataProvided() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        service.saveCard(
                "entropy",
                """
                ## Note
                line-1

                ## Backlinks
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
        assertTrue(persisted.contains("manual-entry"));
        assertTrue(persisted.contains("- manual-entry"));
        assertFalse(persisted.contains("- task `task-a` @ `tasks/task-a/notes.md`"), persisted);
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
        assertTrue(persisted.contains("```markdown"));
        assertTrue(persisted.contains("mobile-stale-edit"));
        assertFalse(persisted.contains("- task `task-stale` @ `desktop/manual.md`"), persisted);
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
                first paragraph with anchor keyword.

                second paragraph.
                """, StandardCharsets.UTF_8);

        CardStorageService.CardSaveResult save = service.saveThought(
                source.toString(),
                "anchor keyword",
                "this is a local thought"
        );

        String updated = Files.readString(source, StandardCharsets.UTF_8);
        assertTrue(updated.contains("> [!TEAR]\n> this is a local thought"));
        assertEquals("local", save.targetType);
        assertEquals(source.toString(), save.targetPath);
        assertEquals("local", save.type);
    }

    @Test
    void shouldRejectThoughtSaveWhenAnchorMissing() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        Path source = tempDir.resolve("notes").resolve("demo.md");
        Files.createDirectories(source.getParent());
        Files.writeString(source, "only one paragraph", StandardCharsets.UTF_8);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> service.saveThought(source.toString(), "missing anchor", "content")
        );
        assertTrue(error.getMessage().contains("anchor"));
    }

    @Test
    void shouldSanitizeWindowsReservedAndIllegalTitles() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));

        service.saveCard("CON", "reserved", null);
        CardStorageService.CardReadResult con = service.readCard("CON");
        assertTrue(con.exists);
        assertEquals("CON", con.title);
        assertEquals("_CON.md", con.path.getFileName().toString());

        service.saveCard("AUX.txt", "reserved-with-ext", null);
        CardStorageService.CardReadResult aux = service.readCard("AUX.txt");
        assertTrue(aux.exists);
        assertEquals("AUX.txt", aux.title);
        assertEquals("_AUX.txt.md", aux.path.getFileName().toString());

        service.saveCard("complex:term?*<>|", "illegal", null);
        CardStorageService.CardReadResult illegal = service.readCard("complex:term?*<>|");
        assertTrue(illegal.exists);
        assertEquals("complex:term?*<>|", illegal.title);
        assertEquals("complex_term_____.md", illegal.path.getFileName().toString());

        service.saveCard("concept   .", "trailing-dot", null);
        CardStorageService.CardReadResult trailing = service.readCard("concept   .");
        assertTrue(trailing.exists);
        assertEquals("concept .", trailing.title);
        assertEquals("concept.md", trailing.path.getFileName().toString());
    }

    @Test
    void shouldListBacklinksByWikilinkTitle() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        service.saveCard("second_law", "base concept", null);
        service.saveCard("entropy", "[[second_law]] and [[second_law|display]]", null);
        service.saveCard("system", "relates to [[second_law]]", null);
        service.saveCard("unrelated", "mentions [[other_concept]] only", null);

        List<CardStorageService.CardBacklinkItem> backlinks = service.listBacklinks("second_law");
        assertEquals(2, backlinks.size());
        int entropyCount = backlinks.stream()
                .filter(item -> "entropy".equals(item.sourceTitle))
                .mapToInt(item -> item.count)
                .findFirst()
                .orElse(0);
        int systemCount = backlinks.stream()
                .filter(item -> "system".equals(item.sourceTitle))
                .mapToInt(item -> item.count)
                .findFirst()
                .orElse(0);
        assertEquals(2, entropyCount);
        assertEquals(1, systemCount);
    }

    @Test
    void shouldResolveAliasesToExistingStorageFile() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        Path cardPath = tempDir.resolve("cards").resolve("thermo-note.md");
        Files.writeString(cardPath, """
                ---
                title: "Thermodynamics Second Law"
                created: "2024-06-01"
                tags: ["physics"]
                type: "concept"
                aliases: ["Second Law", "Entropy Law"]
                ---

                content-v1
                """, StandardCharsets.UTF_8);

        service.init();

        List<String> titles = service.listTitles();
        assertTrue(titles.contains("Thermodynamics Second Law"));
        assertTrue(titles.contains("Second Law"));
        assertTrue(titles.contains("Entropy Law"));

        CardStorageService.CardReadResult byAlias = service.readCard("Second Law");
        assertTrue(byAlias.exists);
        assertEquals("Thermodynamics Second Law", byAlias.title);
        assertEquals("thermo-note.md", byAlias.path.getFileName().toString());

        CardStorageService.CardSaveResult save = service.saveCard("Entropy Law", "content-v1", null);
        assertEquals("thermo-note.md", save.path.getFileName().toString());
        assertFalse(Files.exists(tempDir.resolve("cards").resolve("Entropy Law.md")));
    }

    @Test
    void shouldCountBacklinksWhenWikilinkUsesAlias() throws Exception {
        CardStorageService service = createService(tempDir.resolve("cards"));
        Path targetPath = tempDir.resolve("cards").resolve("thermo-note.md");
        Files.writeString(targetPath, """
                ---
                title: "Thermodynamics Second Law"
                created: "2024-06-01"
                tags: []
                type: "concept"
                aliases: ["Second Law", "Entropy Law"]
                ---

                base
                """, StandardCharsets.UTF_8);
        service.init();

        service.saveCard("source_one", "[[Second Law]] and [[Thermodynamics Second Law]]", null);
        service.saveCard("source_two", "[[Entropy Law]]", null);

        List<CardStorageService.CardBacklinkItem> backlinks = service.listBacklinks("Thermodynamics Second Law");
        assertEquals(2, backlinks.size());

        int sourceOneCount = backlinks.stream()
                .filter(item -> "source_one".equals(item.sourceTitle))
                .mapToInt(item -> item.count)
                .findFirst()
                .orElse(0);
        int sourceTwoCount = backlinks.stream()
                .filter(item -> "source_two".equals(item.sourceTitle))
                .mapToInt(item -> item.count)
                .findFirst()
                .orElse(0);
        assertEquals(2, sourceOneCount);
        assertEquals(1, sourceTwoCount);
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
