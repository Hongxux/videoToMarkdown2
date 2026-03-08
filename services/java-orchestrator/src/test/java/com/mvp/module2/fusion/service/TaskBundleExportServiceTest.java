package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskBundleExportServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldFlattenMarkdownAnchorNoteAndRewriteLocalAssetLinks() throws Exception {
        Path taskRoot = tempDir.resolve("task_export_case");
        Path mainDir = taskRoot.resolve("docs");
        Path assetDir = taskRoot.resolve("assets");
        Path anchorDir = taskRoot.resolve("thinking").resolve("anchor_a1").resolve("rev_r1");
        Files.createDirectories(mainDir);
        Files.createDirectories(assetDir);
        Files.createDirectories(anchorDir);

        Path mainMarkdown = mainDir.resolve("enhanced_output.md");
        Path coverImage = assetDir.resolve("cover.png");
        Path clipVideo = taskRoot.resolve("clip.mp4");
        Path anchorNote = anchorDir.resolve("note.md");
        Path anchorImage = anchorDir.resolve("note-image.png");

        Files.writeString(coverImage, "cover", StandardCharsets.UTF_8);
        Files.writeString(clipVideo, "video", StandardCharsets.UTF_8);
        Files.writeString(anchorImage, "note-image", StandardCharsets.UTF_8);
        Files.writeString(anchorNote, "![anchor-image](./note-image.png)\n", StandardCharsets.UTF_8);
        Files.writeString(
                mainMarkdown,
                "![cover](../assets/cover.png)\n<video src=\"../clip.mp4\"></video>\n[anchor-note](../thinking/anchor_a1/rev_r1/note.md)\n",
                StandardCharsets.UTF_8
        );

        Files.writeString(
                taskRoot.resolve("mobile_task_meta.json"),
                """
                {
                  "version": "1.0",
                  "notesByMarkdown": {
                    "docs/enhanced_output.md": {
                      "anchors": {
                        "a1": {
                          "mountedPath": "thinking/anchor_a1/rev_r1/note.md",
                          "revisions": [
                            {
                              "revisionId": "r1",
                              "notePath": "thinking/anchor_a1/rev_r1/note.md",
                              "files": [
                                { "path": "thinking/anchor_a1/rev_r1/note.md" },
                                { "path": "thinking/anchor_a1/rev_r1/note-image.png" }
                              ]
                            }
                          ]
                        }
                      }
                    }
                  }
                }
                """,
                StandardCharsets.UTF_8
        );

        TaskBundleExportService service = new TaskBundleExportService();
        TaskBundleExportService.FlatTaskExportPlan plan = service.planFlatExport("task-1", taskRoot, mainMarkdown);
        assertEquals(2, plan.markdownCount());
        assertEquals(3, plan.binaryCount());
        assertFalse(plan.mainMarkdownEntryName().contains("/"));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        TaskBundleExportService.ExportZipResult result;
        try (ZipOutputStream zos = new ZipOutputStream(outputStream, StandardCharsets.UTF_8)) {
            result = service.writeFlatZipStreaming(plan, zos);
            zos.finish();
        }

        Map<String, byte[]> zipEntries = unzip(outputStream.toByteArray());
        assertEquals(plan.entries().size(), result.exportedCount());
        assertTrue(zipEntries.containsKey("export_manifest.json"));
        assertTrue(zipEntries.keySet().stream().allMatch(name -> !name.contains("/")));

        String mainMarkdownText = new String(zipEntries.get(plan.mainMarkdownEntryName()), StandardCharsets.UTF_8);
        assertTrue(mainMarkdownText.contains("cover.png"));
        assertTrue(mainMarkdownText.contains("clip.mp4"));

        String anchorNoteEntryName = plan.entries().stream()
                .filter(entry -> "anchor_note".equals(entry.role()))
                .findFirst()
                .orElseThrow()
                .entryName();
        String anchorImageEntryName = plan.entries().stream()
                .filter(entry -> entry.originalPath().endsWith("note-image.png"))
                .findFirst()
                .orElseThrow()
                .entryName();
        assertTrue(mainMarkdownText.contains("(" + anchorNoteEntryName + ")"));

        String anchorMarkdownText = new String(zipEntries.get(anchorNoteEntryName), StandardCharsets.UTF_8);
        assertTrue(anchorMarkdownText.contains(anchorImageEntryName));
        assertNotNull(zipEntries.get(anchorImageEntryName));
    }

    private Map<String, byte[]> unzip(byte[] zipBytes) throws Exception {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes), StandardCharsets.UTF_8)) {
            java.util.zip.ZipEntry entry;
            byte[] buffer = new byte[4096];
            while ((entry = zis.getNextEntry()) != null) {
                ByteArrayOutputStream one = new ByteArrayOutputStream();
                int read;
                while ((read = zis.read(buffer)) >= 0) {
                    if (read > 0) {
                        one.write(buffer, 0, read);
                    }
                }
                entries.put(entry.getName(), one.toByteArray());
                zis.closeEntry();
            }
        }
        return entries;
    }
}