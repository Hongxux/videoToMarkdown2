package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VideoMetaServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldReadTitleDomainAndMainTopicFromVideoMeta() throws Exception {
        Path taskDir = tempDir.resolve("task-a");
        Files.createDirectories(taskDir);
        Files.writeString(
                taskDir.resolve("video_meta.json"),
                """
                {
                  "title": "  test title  ",
                  "domain": "  数字营销与内容创作  ",
                  "main_topic": "  探讨价值内容创作策略与AI时代内容质量  "
                }
                """,
                StandardCharsets.UTF_8
        );

        VideoMetaService service = new VideoMetaService();
        VideoMetaService.VideoMetaSnapshot snapshot = service.read(taskDir);

        assertEquals("test title", snapshot.title);
        assertEquals("数字营销与内容创作", snapshot.domain);
        assertEquals("探讨价值内容创作策略与AI时代内容质量", snapshot.mainTopic);
    }

    @Test
    void shouldFallbackToCamelMainTopicAndReturnEmptyNodeWhenMissing() throws Exception {
        Path taskDir = tempDir.resolve("task-b");
        Files.createDirectories(taskDir);
        Files.writeString(
                taskDir.resolve("video_meta.json"),
                """
                {
                  "domain": "knowledge",
                  "mainTopic": "fallback topic"
                }
                """,
                StandardCharsets.UTF_8
        );

        VideoMetaService service = new VideoMetaService();
        VideoMetaService.VideoMetaSnapshot snapshot = service.read(taskDir);
        assertNull(snapshot.title);
        assertEquals("knowledge", snapshot.domain);
        assertEquals("fallback topic", snapshot.mainTopic);

        Path missingDir = tempDir.resolve("missing-task");
        Files.createDirectories(missingDir);
        ObjectNode emptyNode = service.readOrCreateNode(missingDir);
        assertTrue(emptyNode.isObject());
        assertEquals(0, emptyNode.size());
    }
}
