package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.FileReuseService;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MobileMarkdownControllerUploadReuseTest {

    @Test
    void reuseCheckShouldSubmitTaskWhenReusableFileExists() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();
        Path reusableFile = Files.createTempFile("mobile-reuse", ".mp4");
        try {
            FileReuseService reuseService = mock(FileReuseService.class);
            FileReuseService.FileFingerprint fingerprint =
                    new FileReuseService.FileFingerprint("d41d8cd98f00b204e9800998ecf8427e", ".mp4");
            when(reuseService.normalizeFingerprint(any(), any(), any())).thenReturn(Optional.of(fingerprint));
            when(reuseService.findReusablePath(any())).thenReturn(Optional.of(reusableFile));

            injectField(controller, "taskQueueManager", queueManager);
            injectField(controller, "fileReuseService", reuseService);

            MobileMarkdownController.UploadReuseCheckRequest request =
                    new MobileMarkdownController.UploadReuseCheckRequest();
            request.userId = "mobile_user_1";
            request.fileName = "demo.mp4";
            request.fileMd5 = "d41d8cd98f00b204e9800998ecf8427e";
            request.fileExt = ".mp4";
            request.autoSubmit = true;
            request.probeOnly = false;

            ResponseEntity<Map<String, Object>> response = controller.checkUploadFileReuse(request);

            assertEquals(200, response.getStatusCode().value());
            assertTrue(response.getBody() != null);
            Map<String, Object> payload = response.getBody();
            assertEquals(true, payload.get("success"));
            assertEquals(true, payload.get("reused"));
            String taskId = String.valueOf(payload.get("taskId"));
            assertTrue(!taskId.isBlank());
            assertNotNull(queueManager.getTask(taskId));
        } finally {
            Files.deleteIfExists(reusableFile);
        }
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
