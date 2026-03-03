package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

class BookMarkdownServiceSectionExtractManualTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Path PDF_PATH = Paths.get("D:\\videoToMarkdownTest2\\var\\Distributed_Systems_4.pdf");

    @Test
    void extractSingleSectionBySelectorWithManualPageOffset() throws Exception {
        Assertions.assertTrue(Files.isRegularFile(PDF_PATH), "pdf not found: " + PDF_PATH);

        BookMarkdownService service = new BookMarkdownService();
        BookMarkdownService.BookProbeResult probe = service.probeBook(PDF_PATH.toString(), 8);
        Assertions.assertTrue(probe.success, "probe failed: " + probe.errorMessage);

        Map<String, Object> sectionProbe = probe.sections.stream()
                .filter(item -> "c1s1".equalsIgnoreCase(String.valueOf(item.get("sectionSelector"))))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("probe section c1s1 not found"));
        int expectedStartPage = asInt(sectionProbe.get("startPage"), -1);
        int expectedEndPage = asInt(sectionProbe.get("endPage"), -1);
        Assertions.assertTrue(expectedStartPage > 0, "invalid expected start page from probe");
        Assertions.assertTrue(expectedEndPage >= expectedStartPage, "invalid expected page range from probe");

        Path outputDir = Paths.get("D:\\videoToMarkdownTest2\\var\\tmp_book_section_extract_ds4_offset8");
        deleteDirIfExists(outputDir);
        Files.createDirectories(outputDir);

        BookMarkdownService.BookProcessingOptions options = new BookMarkdownService.BookProcessingOptions();
        options.sectionSelector = "c1s1";
        options.splitByChapter = true;
        options.splitBySection = true;
        options.pageOffset = 8;

        BookMarkdownService.BookProcessingResult result = service.processBook(
                "manual_section_extract_ds4_offset8",
                PDF_PATH.toString(),
                outputDir.toString(),
                options
        );

        Assertions.assertTrue(result.success, "section extract failed: " + result.errorMessage);
        Assertions.assertEquals(1, result.chapterCount, "selected chapter count mismatch");
        Assertions.assertEquals(1, result.sectionCount, "selected section count mismatch");

        Map<String, Object> metadata = OBJECT_MAPPER.readValue(
                Files.readString(Paths.get(result.metadataPath), StandardCharsets.UTF_8),
                new TypeReference<Map<String, Object>>() {
                }
        );
        Assertions.assertEquals(8, asInt(metadata.get("applied_page_offset"), Integer.MIN_VALUE));
        Assertions.assertEquals("outline", String.valueOf(metadata.get("page_map_strategy")));

        List<Map<String, Object>> units = castListOfMap(metadata.get("semantic_units"));
        Assertions.assertEquals(1, units.size(), "semantic unit size mismatch");
        Map<String, Object> unit = units.get(0);
        Assertions.assertEquals("c1s1", String.valueOf(unit.get("section_selector")));
        Assertions.assertEquals(expectedStartPage, asInt(unit.get("start_page"), -1));
        Assertions.assertEquals(expectedEndPage, asInt(unit.get("end_page"), -1));

        Optional<Path> sectionFile = Files.walk(outputDir)
                .filter(path -> Files.isRegularFile(path))
                .filter(path -> path.getFileName().toString().startsWith("section-"))
                .filter(path -> path.getFileName().toString().endsWith(".md"))
                .findFirst();
        Assertions.assertTrue(sectionFile.isPresent(), "section markdown file not found");
        String sectionMarkdown = Files.readString(sectionFile.get(), StandardCharsets.UTF_8);
        Assertions.assertTrue(sectionMarkdown.contains("- Start Page: " + expectedStartPage));
        Assertions.assertTrue(sectionMarkdown.contains("- End Page: " + expectedEndPage));

        System.out.println("SECTION_SELECTOR_TEST_SUCCESS=true");
        System.out.println("SECTION_SELECTOR_TEST_PAGE_RANGE=" + expectedStartPage + "-" + expectedEndPage);
        System.out.println("SECTION_SELECTOR_TEST_OUTPUT=" + outputDir);
    }

    @Test
    void extractChapterKeepsImagesInRelativeTextPosition() throws Exception {
        Assertions.assertTrue(Files.isRegularFile(PDF_PATH), "pdf not found: " + PDF_PATH);

        BookMarkdownService service = new BookMarkdownService();
        Path outputDir = Paths.get("D:\\videoToMarkdownTest2\\var\\tmp_book_chapter2_extract_ds4_offset8");
        deleteDirIfExists(outputDir);
        Files.createDirectories(outputDir);

        BookMarkdownService.BookProcessingOptions options = new BookMarkdownService.BookProcessingOptions();
        options.chapterSelector = "2";
        options.splitByChapter = true;
        options.splitBySection = true;
        options.pageOffset = 8;

        BookMarkdownService.BookProcessingResult result = service.processBook(
                "manual_chapter_extract_ds4_offset8",
                PDF_PATH.toString(),
                outputDir.toString(),
                options
        );

        Assertions.assertTrue(result.success, "chapter extract failed: " + result.errorMessage);
        List<Path> sectionFiles = Files.walk(outputDir)
                .filter(path -> Files.isRegularFile(path))
                .filter(path -> path.getFileName().toString().startsWith("section-"))
                .filter(path -> path.getFileName().toString().endsWith(".md"))
                .collect(Collectors.toList());
        Assertions.assertFalse(sectionFiles.isEmpty(), "chapter section markdown files not generated");

        int imageMarkerCount = 0;
        boolean hasInterleavedImage = false;
        for (Path sectionFile : sectionFiles) {
            List<String> lines = Files.readAllLines(sectionFile, StandardCharsets.UTF_8);
            for (int i = 0; i < lines.size(); i++) {
                String line = normalize(lines.get(i));
                if (!line.startsWith("![image-")) {
                    continue;
                }
                imageMarkerCount += 1;
                boolean hasTextBefore = hasBodyText(lines, 0, i);
                boolean hasTextAfter = hasBodyText(lines, i + 1, lines.size());
                if (hasTextBefore && hasTextAfter) {
                    hasInterleavedImage = true;
                }
            }
        }

        Assertions.assertTrue(imageMarkerCount > 0, "no extracted image marker found in chapter markdown");
        Assertions.assertTrue(hasInterleavedImage, "images were not inserted into relative text position");

        System.out.println("CHAPTER_IMAGE_TEST_SUCCESS=true");
        System.out.println("CHAPTER_IMAGE_TEST_SECTION_FILE_COUNT=" + sectionFiles.size());
        System.out.println("CHAPTER_IMAGE_TEST_IMAGE_MARKERS=" + imageMarkerCount);
        System.out.println("CHAPTER_IMAGE_TEST_INTERLEAVED=" + hasInterleavedImage);
        System.out.println("CHAPTER_IMAGE_TEST_OUTPUT=" + outputDir);
    }

    private boolean hasBodyText(List<String> lines, int start, int endExclusive) {
        for (int i = Math.max(0, start); i < Math.min(lines.size(), endExclusive); i++) {
            String line = normalize(lines.get(i));
            if (line.isBlank()) {
                continue;
            }
            if (line.startsWith("#")) {
                continue;
            }
            if (line.startsWith("- ")) {
                continue;
            }
            if (line.startsWith("![image-")) {
                continue;
            }
            if (line.startsWith("|")) {
                continue;
            }
            if ("_No content extracted._".equals(line)) {
                continue;
            }
            return true;
        }
        return false;
    }

    private int asInt(Object value, int fallback) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value == null) {
            return fallback;
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(text);
        } catch (Exception ignored) {
            return fallback;
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> castListOfMap(Object value) {
        if (!(value instanceof List<?>)) {
            return List.of();
        }
        return ((List<?>) value).stream()
                .filter(item -> item instanceof Map<?, ?>)
                .map(item -> (Map<String, Object>) item)
                .collect(Collectors.toList());
    }

    private String normalize(String raw) {
        return raw == null ? "" : raw.trim();
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
