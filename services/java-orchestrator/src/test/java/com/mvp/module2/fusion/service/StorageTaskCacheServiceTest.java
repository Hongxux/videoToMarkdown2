package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StorageTaskCacheServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldPreferInputVideoUrlWhenMultipleSourceFieldsExist() throws Exception {
        String storageKey = "task_input_first";
        String metrics = """
                {
                  "success": true,
                  "generated_at": "2026-02-18T00:00:00Z",
                  "input_video_url": "https://www.bilibili.com/video/BV1XKIJBSEBJ",
                  "video_url": "https://www.youtube.com/watch?v=YFjfBk8HI5o",
                  "source_url": "https://v.douyin.com/SUKDNrJiJ3k/",
                  "original_video_url": "https://example.com/original",
                  "video_path": "D:/videoToMarkdownTest2/var/storage/storage/task_input_first/video.mp4"
                }
                """;

        StorageTaskCacheService.CachedTask task = refreshAndGetTask(storageKey, metrics);
        assertEquals("https://www.bilibili.com/video/BV1XKIJBSEBJ", task.videoUrl);
    }

    @Test
    void shouldFallbackToVideoUrlAliasWhenInputVideoUrlMissing() throws Exception {
        String storageKey = "task_video_url_alias";
        String metrics = """
                {
                  "success": true,
                  "generated_at": "2026-02-18T00:00:00Z",
                  "video_url": "https://www.youtube.com/watch?v=YFjfBk8HI5o",
                  "source_url": "https://v.douyin.com/SUKDNrJiJ3k/",
                  "original_video_url": "https://example.com/original",
                  "video_path": "D:/videoToMarkdownTest2/var/storage/storage/task_video_url_alias/video.mp4"
                }
                """;

        StorageTaskCacheService.CachedTask task = refreshAndGetTask(storageKey, metrics);
        assertEquals("https://www.youtube.com/watch?v=YFjfBk8HI5o", task.videoUrl);
    }

    @Test
    void shouldFallbackToSourceUrlAliasWhenVideoUrlMissing() throws Exception {
        String storageKey = "task_source_url_alias";
        String metrics = """
                {
                  "success": true,
                  "generated_at": "2026-02-18T00:00:00Z",
                  "source_url": "https://v.douyin.com/SUKDNrJiJ3k/",
                  "original_video_url": "https://example.com/original",
                  "video_path": "D:/videoToMarkdownTest2/var/storage/storage/task_source_url_alias/video.mp4"
                }
                """;

        StorageTaskCacheService.CachedTask task = refreshAndGetTask(storageKey, metrics);
        assertEquals("https://v.douyin.com/SUKDNrJiJ3k/", task.videoUrl);
    }

    @Test
    void shouldFallbackToOriginalVideoUrlAliasWhenOnlyOriginalExists() throws Exception {
        String storageKey = "task_original_url_alias";
        String metrics = """
                {
                  "success": true,
                  "generated_at": "2026-02-18T00:00:00Z",
                  "original_video_url": "https://example.com/original",
                  "video_path": "D:/videoToMarkdownTest2/var/storage/storage/task_original_url_alias/video.mp4"
                }
                """;

        StorageTaskCacheService.CachedTask task = refreshAndGetTask(storageKey, metrics);
        assertEquals("https://example.com/original", task.videoUrl);
    }

    @Test
    void shouldFallbackToVideoPathWhenNoUrlAliasExists() throws Exception {
        String storageKey = "task_video_path_only";
        String metrics = """
                {
                  "success": true,
                  "generated_at": "2026-02-18T00:00:00Z",
                  "video_path": "D:/videoToMarkdownTest2/var/storage/storage/task_video_path_only/video.mp4"
                }
                """;

        StorageTaskCacheService.CachedTask task = refreshAndGetTask(storageKey, metrics);
        assertEquals("D:/videoToMarkdownTest2/var/storage/storage/task_video_path_only/video.mp4", task.videoUrl);
    }

    private StorageTaskCacheService.CachedTask refreshAndGetTask(String storageKey, String metricsJson) throws Exception {
        Path storageRoot = tempDir.resolve("storage");
        Path taskDir = storageRoot.resolve(storageKey);
        Path metricsPath = taskDir.resolve("intermediates").resolve("task_metrics_latest.json");
        Files.createDirectories(metricsPath.getParent());
        Files.writeString(metricsPath, metricsJson, StandardCharsets.UTF_8);

        StorageTaskCacheService service = new StorageTaskCacheService();
        Field resolvedStorageRoot = StorageTaskCacheService.class.getDeclaredField("resolvedStorageRoot");
        resolvedStorageRoot.setAccessible(true);
        resolvedStorageRoot.set(service, storageRoot);

        Method doRefresh = StorageTaskCacheService.class.getDeclaredMethod("doRefresh");
        doRefresh.setAccessible(true);
        doRefresh.invoke(service);

        StorageTaskCacheService.CachedTask task = service.getTask(storageKey).orElse(null);
        assertNotNull(task, "缓存刷新后应能读取到目标任务");
        return task;
    }
}
