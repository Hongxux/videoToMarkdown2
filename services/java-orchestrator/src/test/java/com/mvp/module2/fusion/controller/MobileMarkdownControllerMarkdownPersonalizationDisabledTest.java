package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.PersonaAwareReadingService;
import com.mvp.module2.fusion.service.PersonaInsightCardService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestParam;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class MobileMarkdownControllerMarkdownPersonalizationDisabledTest {

    @TempDir
    Path tempDir;

    @Test
    void markdownEndpointShouldIgnorePersonalizationRequest() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();
        RecordingPersonaAwareReadingService readingService = new RecordingPersonaAwareReadingService();
        RecordingPersonaInsightCardService insightService = new RecordingPersonaInsightCardService();
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "personaAwareReadingService", readingService);
        injectField(controller, "personaInsightCardService", insightService);

        Path markdownPath = tempDir.resolve("article.md");
        Files.writeString(markdownPath, "# Title\nbody");

        TaskQueueManager.TaskEntry task = queueManager.submitTask(
                "u_markdown_disable",
                "https://example.com/video",
                tempDir.toString(),
                TaskQueueManager.Priority.NORMAL
        );
        task.resultPath = markdownPath.toString();
        task.status = TaskQueueManager.TaskStatus.COMPLETED;
        task.title = "Demo";

        ResponseEntity<?> response = controller.getTaskMarkdown(task.taskId, "reader_A", true);

        assertEquals(200, response.getStatusCode().value());
        Map<String, Object> body = responseBody(response);
        assertEquals("# Title\nbody", body.get("markdown"));
        assertEquals(false, body.get("personalizationIncluded"));
        assertEquals("disabled", body.get("personalizationWarmupStatus"));
        assertFalse(body.containsKey("personalizedNodes"));
        assertFalse(body.containsKey("insightCardIndex"));
        assertEquals(0, readingService.loadOrComputeCalls);
        assertEquals(0, readingService.precomputeAsyncCalls);
        assertEquals(0, insightService.loadIndexSnapshotCalls);
    }

    @Test
    void markdownEndpointsShouldDefaultIncludePersonalizationToFalse() throws Exception {
        assertEquals(
                "false",
                requestParamDefaultValue(
                        "getTaskMarkdown",
                        2,
                        String.class,
                        String.class,
                        boolean.class
                )
        );
        assertEquals(
                "false",
                requestParamDefaultValue(
                        "getTaskMarkdownByRelativePath",
                        2,
                        String.class,
                        String.class,
                        boolean.class,
                        String.class
                )
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> responseBody(ResponseEntity<?> response) {
        Object body = response.getBody();
        assertInstanceOf(Map.class, body);
        return (Map<String, Object>) body;
    }

    private static String requestParamDefaultValue(String methodName, int parameterIndex, Class<?>... parameterTypes)
            throws Exception {
        Method method = MobileMarkdownController.class.getMethod(methodName, parameterTypes);
        Annotation[] annotations = method.getParameterAnnotations()[parameterIndex];
        for (Annotation annotation : annotations) {
            if (annotation instanceof RequestParam requestParam) {
                return requestParam.defaultValue();
            }
        }
        throw new IllegalStateException("RequestParam annotation not found on parameter index " + parameterIndex);
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class RecordingPersonaAwareReadingService extends PersonaAwareReadingService {
        private int loadOrComputeCalls;
        private int precomputeAsyncCalls;

        @Override
        public void precomputeAsync(String taskId, String userId, String markdownPath) {
            precomputeAsyncCalls++;
        }

        @Override
        public PersonalizedReadingPayload loadOrCompute(String taskId, String userId, Path markdownPath, String markdown) {
            loadOrComputeCalls++;
            return new PersonalizedReadingPayload();
        }
    }

    private static class RecordingPersonaInsightCardService extends PersonaInsightCardService {
        private int loadIndexSnapshotCalls;

        @Override
        public Map<String, Object> loadIndexSnapshot(String taskId, Path markdownPath) {
            loadIndexSnapshotCalls++;
            return Map.of("unexpected", true);
        }
    }
}
