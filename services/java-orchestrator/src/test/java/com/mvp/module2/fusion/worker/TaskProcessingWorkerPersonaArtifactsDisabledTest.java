package com.mvp.module2.fusion.worker;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.PersonaAwareReadingService;
import com.mvp.module2.fusion.service.PersonaInsightCardService;
import com.mvp.module2.fusion.service.VideoProcessingOrchestrator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TaskProcessingWorkerPersonaArtifactsDisabledTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldKeepPostCompletionPersonaArtifactsHookDisabled() throws Exception {
        TaskProcessingWorker worker = new TaskProcessingWorker();
        RecordingPersonaAwareReadingService readingService = new RecordingPersonaAwareReadingService();
        RecordingPersonaInsightCardService insightService = new RecordingPersonaInsightCardService();
        setField(worker, "personaAwareReadingService", readingService);
        setField(worker, "personaInsightCardService", insightService);

        Path markdownPath = tempDir.resolve("article.md");
        Files.writeString(markdownPath, "# Title\nbody", StandardCharsets.UTF_8);

        TaskQueueManager.TaskEntry task = new TaskQueueManager.TaskEntry();
        task.taskId = "VT_worker_persona_disabled_001";
        task.userId = "reader_worker";

        VideoProcessingOrchestrator.ProcessingResult result = new VideoProcessingOrchestrator.ProcessingResult();
        result.markdownPath = markdownPath.toString();

        invokePostCompletionHook(worker, task, result);

        assertFalse((Boolean) readField(worker, "postCompletionPersonaArtifactsEnabled"));
        assertEquals(0, readingService.loadOrComputeCalls);
        assertEquals(0, insightService.generateAsyncCalls);
    }

    private void invokePostCompletionHook(
            TaskProcessingWorker worker,
            TaskQueueManager.TaskEntry task,
            VideoProcessingOrchestrator.ProcessingResult result
    ) throws Exception {
        Method method = TaskProcessingWorker.class.getDeclaredMethod(
                "triggerPersonaArtifactsAfterCompletion",
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

    private Object readField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static class RecordingPersonaAwareReadingService extends PersonaAwareReadingService {
        private int loadOrComputeCalls;

        @Override
        public PersonalizedReadingPayload loadOrCompute(String taskId, String userId, Path markdownPath, String markdown) {
            loadOrComputeCalls++;
            PersonalizedReadingPayload payload = new PersonalizedReadingPayload();
            payload.nodes = List.of(Map.of("node_id", "n1"));
            return payload;
        }
    }

    private static class RecordingPersonaInsightCardService extends PersonaInsightCardService {
        private int generateAsyncCalls;

        @Override
        public void generateAsync(
                String taskId,
                String userId,
                Path markdownPath,
                List<Map<String, Object>> personalizedNodes
        ) {
            generateAsyncCalls++;
        }
    }
}
