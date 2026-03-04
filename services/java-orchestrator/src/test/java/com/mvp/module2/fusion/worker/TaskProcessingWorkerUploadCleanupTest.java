package com.mvp.module2.fusion.worker;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.VideoProcessingOrchestrator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskProcessingWorkerUploadCleanupTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldDeleteUploadedSourceAfterCompletionWhenNoSiblingTaskReferencesSource() throws Exception {
        Path uploadRoot = Files.createDirectories(tempDir.resolve("uploads"));
        Path uploadedSource = uploadRoot.resolve("book.pdf");
        Files.writeString(uploadedSource, "book", StandardCharsets.UTF_8);

        TaskQueueManager queueManager = new TaskQueueManager(1);
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "user-a",
                uploadedSource.toString(),
                null,
                TaskQueueManager.Priority.NORMAL
        );
        moveTaskToCompleted(queueManager, task);

        TaskProcessingWorker worker = buildWorker(queueManager, uploadRoot);
        invokeCleanup(worker, task);

        assertFalse(Files.exists(uploadedSource), "uploaded source should be deleted after completion");
    }

    @Test
    void shouldKeepUploadedSourceWhenAnotherTaskStillUsesSameSource() throws Exception {
        Path uploadRoot = Files.createDirectories(tempDir.resolve("uploads"));
        Path uploadedSource = uploadRoot.resolve("shared-book.pdf");
        Files.writeString(uploadedSource, "shared", StandardCharsets.UTF_8);

        TaskQueueManager queueManager = new TaskQueueManager(1);
        TaskQueueManager.TaskEntry firstTask = queueManager.submitTask(
                "user-a",
                uploadedSource.toString(),
                null,
                TaskQueueManager.Priority.NORMAL
        );
        TaskQueueManager.TaskEntry secondTask = queueManager.submitTask(
                "user-a",
                uploadedSource.toString(),
                null,
                TaskQueueManager.Priority.NORMAL
        );

        moveTaskToCompleted(queueManager, firstTask);

        TaskProcessingWorker worker = buildWorker(queueManager, uploadRoot);
        invokeCleanup(worker, firstTask);
        assertTrue(Files.exists(uploadedSource), "source should stay when another active task still references it");

        moveTaskToCompleted(queueManager, secondTask);
        invokeCleanup(worker, secondTask);
        assertFalse(Files.exists(uploadedSource), "source should be deleted when the last referencing task finishes");
    }

    @Test
    void shouldNotDeleteFileOutsideUploadRoot() throws Exception {
        Path uploadRoot = Files.createDirectories(tempDir.resolve("uploads"));
        Path outsideRoot = Files.createDirectories(tempDir.resolve("outside"));
        Path localSource = outsideRoot.resolve("local-video.mp4");
        Files.writeString(localSource, "video", StandardCharsets.UTF_8);

        TaskQueueManager queueManager = new TaskQueueManager(1);
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "user-a",
                localSource.toString(),
                null,
                TaskQueueManager.Priority.NORMAL
        );
        moveTaskToCompleted(queueManager, task);

        TaskProcessingWorker worker = buildWorker(queueManager, uploadRoot);
        invokeCleanup(worker, task);

        assertTrue(Files.exists(localSource), "file outside upload root must not be deleted");
    }

    @Test
    void shouldDeleteDownloadedSourceUnderStorageRootAfterCompletion() throws Exception {
        Path uploadRoot = Files.createDirectories(tempDir.resolve("uploads"));
        Path storageRoot = Files.createDirectories(tempDir.resolve("var").resolve("storage").resolve("storage"));
        Path downloadedSource = Files.createDirectories(storageRoot.resolve("task-a")).resolve("source.mp4");
        Files.writeString(downloadedSource, "video", StandardCharsets.UTF_8);

        TaskQueueManager queueManager = new TaskQueueManager(1);
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "user-a",
                "https://example.com/video",
                null,
                TaskQueueManager.Priority.NORMAL
        );
        moveTaskToCompleted(queueManager, task);

        TaskProcessingWorker worker = buildWorker(queueManager, uploadRoot, storageRoot);
        VideoProcessingOrchestrator.ProcessingResult result = new VideoProcessingOrchestrator.ProcessingResult();
        result.cleanupSourcePath = downloadedSource.toString();
        invokeDownloadedCleanup(worker, task, result);

        assertFalse(Files.exists(downloadedSource), "downloaded source under storage root should be deleted");
    }

    @Test
    void shouldKeepDownloadedSourceWhenAnotherActiveTaskStillReferencesIt() throws Exception {
        Path uploadRoot = Files.createDirectories(tempDir.resolve("uploads"));
        Path storageRoot = Files.createDirectories(tempDir.resolve("var").resolve("storage").resolve("storage"));
        Path downloadedSource = Files.createDirectories(storageRoot.resolve("task-b")).resolve("source.pdf");
        Files.writeString(downloadedSource, "book", StandardCharsets.UTF_8);

        TaskQueueManager queueManager = new TaskQueueManager(1);
        TaskQueueManager.TaskEntry firstTask = queueManager.submitTask(
                "user-a",
                "https://example.com/book-1.pdf",
                null,
                TaskQueueManager.Priority.NORMAL
        );
        TaskQueueManager.TaskEntry secondTask = queueManager.submitTask(
                "user-a",
                "https://example.com/book-2.pdf",
                null,
                TaskQueueManager.Priority.NORMAL
        );
        queueManager.updateCleanupSourcePath(secondTask.taskId, downloadedSource.toString());
        moveTaskToCompleted(queueManager, firstTask);

        TaskProcessingWorker worker = buildWorker(queueManager, uploadRoot, storageRoot);
        VideoProcessingOrchestrator.ProcessingResult firstResult = new VideoProcessingOrchestrator.ProcessingResult();
        firstResult.cleanupSourcePath = downloadedSource.toString();
        invokeDownloadedCleanup(worker, firstTask, firstResult);
        assertTrue(Files.exists(downloadedSource), "downloaded source should stay while another task still references it");

        moveTaskToCompleted(queueManager, secondTask);
        VideoProcessingOrchestrator.ProcessingResult secondResult = new VideoProcessingOrchestrator.ProcessingResult();
        secondResult.cleanupSourcePath = downloadedSource.toString();
        invokeDownloadedCleanup(worker, secondTask, secondResult);
        assertFalse(Files.exists(downloadedSource), "downloaded source should be deleted after last reference finishes");
    }

    @Test
    void shouldNotDeleteDownloadedSourceOutsideStorageRoot() throws Exception {
        Path uploadRoot = Files.createDirectories(tempDir.resolve("uploads"));
        Path storageRoot = Files.createDirectories(tempDir.resolve("var").resolve("storage").resolve("storage"));
        Path outsideRoot = Files.createDirectories(tempDir.resolve("outside-storage"));
        Path downloadedSource = outsideRoot.resolve("source.mp4");
        Files.writeString(downloadedSource, "video", StandardCharsets.UTF_8);

        TaskQueueManager queueManager = new TaskQueueManager(1);
        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "user-a",
                "https://example.com/video",
                null,
                TaskQueueManager.Priority.NORMAL
        );
        moveTaskToCompleted(queueManager, task);

        TaskProcessingWorker worker = buildWorker(queueManager, uploadRoot, storageRoot);
        VideoProcessingOrchestrator.ProcessingResult result = new VideoProcessingOrchestrator.ProcessingResult();
        result.cleanupSourcePath = downloadedSource.toString();
        invokeDownloadedCleanup(worker, task, result);

        assertTrue(Files.exists(downloadedSource), "downloaded source outside storage root must not be deleted");
    }

    private TaskProcessingWorker buildWorker(TaskQueueManager queueManager, Path uploadRoot) throws Exception {
        return buildWorker(queueManager, uploadRoot, null);
    }

    private TaskProcessingWorker buildWorker(
            TaskQueueManager queueManager,
            Path uploadRoot,
            Path storageRoot
    ) throws Exception {
        TaskProcessingWorker worker = new TaskProcessingWorker();
        setField(worker, "taskQueueManager", queueManager);
        setField(worker, "uploadDir", uploadRoot.toString());
        if (storageRoot != null) {
            setField(worker, "configuredStorageRoot", storageRoot.toString());
        }
        return worker;
    }

    private void moveTaskToCompleted(TaskQueueManager queueManager, TaskQueueManager.TaskEntry task) throws Exception {
        TaskQueueManager.TaskEntry polled = queueManager.pollNextTask(1, TimeUnit.SECONDS);
        assertNotNull(polled, "expected queued task to be polled");
        assertEquals(task.taskId, polled.taskId, "expected FIFO order for equal-priority task submissions");
        queueManager.completeTask(task.taskId, "out.md");
    }

    private void invokeCleanup(TaskProcessingWorker worker, TaskQueueManager.TaskEntry task) throws Exception {
        Method method = TaskProcessingWorker.class.getDeclaredMethod(
                "cleanupUploadedSourceAfterCompletion",
                TaskQueueManager.TaskEntry.class
        );
        method.setAccessible(true);
        method.invoke(worker, task);
    }

    private void invokeDownloadedCleanup(
            TaskProcessingWorker worker,
            TaskQueueManager.TaskEntry task,
            VideoProcessingOrchestrator.ProcessingResult result
    ) throws Exception {
        Method method = TaskProcessingWorker.class.getDeclaredMethod(
                "cleanupDownloadedSourceAfterCompletion",
                TaskQueueManager.TaskEntry.class,
                VideoProcessingOrchestrator.ProcessingResult.class
        );
        method.setAccessible(true);
        method.invoke(worker, task, result);
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
