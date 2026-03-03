package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

class BookMarkdownServiceProbeManualTest {

    @Test
    void probeDistributedSystemsPdfChapters() throws Exception {
        Path pdfPath = Paths.get("D:\\videoToMarkdownTest2\\var\\Distributed_Systems_4.pdf");
        Assertions.assertTrue(Files.isRegularFile(pdfPath), "pdf not found: " + pdfPath);

        Path outputDir = Paths.get("D:\\videoToMarkdownTest2\\var\\tmp_book_probe_ds4");
        deleteDirIfExists(outputDir);
        Files.createDirectories(outputDir);

        BookMarkdownService service = new BookMarkdownService();
        BookMarkdownService.BookProcessingResult result = service.processBook(
                "manual_probe_ds4",
                pdfPath.toString(),
                outputDir.toString(),
                null
        );

        Assertions.assertTrue(result.success, "book processing failed: " + result.errorMessage);
        Assertions.assertTrue(result.chapterCount > 0, "no chapter recognized");

        Path markdownPath = Paths.get(result.markdownPath);
        String markdown = Files.readString(markdownPath, StandardCharsets.UTF_8);
        List<String> chapterLines = markdown.lines()
                .map(String::trim)
                .filter(line -> line.matches("^\\d+\\. \\[.*\\]\\(.*\\)$"))
                .collect(Collectors.toList());

        System.out.println("PROBE_SUCCESS=true");
        System.out.println("PROBE_CHAPTER_COUNT=" + result.chapterCount);
        System.out.println("PROBE_SECTION_COUNT=" + result.sectionCount);
        System.out.println("PROBE_MARKDOWN_PATH=" + markdownPath);
        int sampleSize = Math.min(12, chapterLines.size());
        for (int i = 0; i < sampleSize; i++) {
            System.out.println("PROBE_CHAPTER_LINE_" + (i + 1) + "=" + chapterLines.get(i));
        }
    }

    private void deleteDirIfExists(Path root) throws Exception {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var stream = Files.walk(root)) {
            stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                }
            });
        }
    }
}
