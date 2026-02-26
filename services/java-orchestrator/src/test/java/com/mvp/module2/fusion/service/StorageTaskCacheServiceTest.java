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

    @Test
    void shouldPreferVideoMetaTitleWhenPresent() throws Exception {
        String storageKey = "task_video_meta_title";
        String metrics = """
                {
                  "success": true,
                  "generated_at": "2026-02-18T00:00:00Z",
                  "video_title": "metrics_title_should_not_win",
                  "video_path": "D:/videoToMarkdownTest2/var/storage/storage/task_video_meta_title/video.mp4"
                }
                """;

        StorageTaskCacheService.CachedTask task = refreshAndGetTask(storageKey, metrics, "video_meta_title_should_win");
        assertEquals("video_meta_title_should_win", task.title);
    }

    @Test
    void shouldResolveCachedTaskByTaskIdWhenStorageKeyIsHashed() throws Exception {
        String storageKey = "0a9d3176320428da26316e1c4c891d64";
        String taskId = "VT_1771907724319_4";
        String metrics = """
                {
                  "success": true,
                  "generated_at": "2026-02-24T04:45:25.201837400Z",
                  "task_id": "VT_1771907724319_4",
                  "video_path": "D:/videoToMarkdownTest2/var/storage/storage/0a9d3176320428da26316e1c4c891d64/video.mp4"
                }
                """;

        StorageTaskCacheService service = refreshService(storageKey, metrics, null);
        StorageTaskCacheService.CachedTask task = service.getTaskByTaskId(taskId).orElse(null);
        assertNotNull(task, "应支持通过 task_id 反查哈希目录任务");
        assertEquals(storageKey, task.storageKey);
        assertEquals(taskId, task.taskId);
    }

    private StorageTaskCacheService.CachedTask refreshAndGetTask(String storageKey, String metricsJson) throws Exception {
        return refreshAndGetTask(storageKey, metricsJson, null);
    }

    private StorageTaskCacheService.CachedTask refreshAndGetTask(
            String storageKey,
            String metricsJson,
            String videoMetaTitle) throws Exception {
        StorageTaskCacheService service = refreshService(storageKey, metricsJson, videoMetaTitle);
        StorageTaskCacheService.CachedTask task = service.getTask(storageKey).orElse(null);
        assertNotNull(task, "缓存刷新后应能读取到目标任务");
        return task;
    }

    private StorageTaskCacheService refreshService(
            String storageKey,
            String metricsJson,
            String videoMetaTitle) throws Exception {
        Path storageRoot = tempDir.resolve("storage");
        Path taskDir = storageRoot.resolve(storageKey);
        Path metricsPath = taskDir.resolve("intermediates").resolve("task_metrics_latest.json");
        Files.createDirectories(metricsPath.getParent());
        Files.writeString(metricsPath, metricsJson, StandardCharsets.UTF_8);
        if (videoMetaTitle != null) {
            String videoMetaJson = "{\n  \"title\": \"" + videoMetaTitle + "\"\n}\n";
            Files.writeString(taskDir.resolve("video_meta.json"), videoMetaJson, StandardCharsets.UTF_8);
        }

        StorageTaskCacheService service = new StorageTaskCacheService();
        Field resolvedStorageRoot = StorageTaskCacheService.class.getDeclaredField("resolvedStorageRoot");
        resolvedStorageRoot.setAccessible(true);
        resolvedStorageRoot.set(service, storageRoot);

        Method doRefresh = StorageTaskCacheService.class.getDeclaredMethod("doRefresh");
        doRefresh.setAccessible(true);
        doRefresh.invoke(service);
        return service;
    }
}
