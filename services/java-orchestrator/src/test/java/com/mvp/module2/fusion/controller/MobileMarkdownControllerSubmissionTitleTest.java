package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MobileMarkdownControllerSubmissionTitleTest {

    @Test
    void submitTaskShouldLockVideoTitleFromVideoInfo() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();

        PythonGrpcClient.VideoInfoResult videoInfo = new PythonGrpcClient.VideoInfoResult();
        videoInfo.success = true;
        videoInfo.videoTitle = "Video Title From Probe";
        videoInfo.canonicalId = "BV1ABCDEF123";

        StubPythonGrpcClient stubGrpc = new StubPythonGrpcClient(videoInfo);
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "pythonGrpcClient", stubGrpc);
        injectField(controller, "mobileVideoInfoTimeoutSeconds", 20);

        MobileMarkdownController.TaskSubmitRequest request = new MobileMarkdownController.TaskSubmitRequest();
        request.userId = "u_001";
        request.videoUrl = "https://www.bilibili.com/video/BV1ABCDEF123?p=2";

        ResponseEntity<Map<String, Object>> response = controller.submitTaskFromMobile(request);

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody() != null);
        String taskId = String.valueOf(response.getBody().get("taskId"));
        TaskQueueManager.TaskEntry task = queueManager.getTask(taskId);
        assertNotNull(task);
        assertEquals("Video Title From Probe", task.title);
        assertEquals("Video Title From Probe", response.getBody().get("title"));
        assertEquals(request.videoUrl, stubGrpc.lastVideoInput);
    }

    @Test
    void submitTaskShouldFallbackToCanonicalIdWhenVideoTitleMissing() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();

        PythonGrpcClient.VideoInfoResult videoInfo = new PythonGrpcClient.VideoInfoResult();
        videoInfo.success = true;
        videoInfo.videoTitle = "   ";
        videoInfo.canonicalId = "BV9CANONICAL9";

        StubPythonGrpcClient stubGrpc = new StubPythonGrpcClient(videoInfo);
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "pythonGrpcClient", stubGrpc);
        injectField(controller, "mobileVideoInfoTimeoutSeconds", 20);

        MobileMarkdownController.TaskSubmitRequest request = new MobileMarkdownController.TaskSubmitRequest();
        request.userId = "u_001";
        request.videoUrl = "https://www.bilibili.com/video/BV9CANONICAL9";

        ResponseEntity<Map<String, Object>> response = controller.submitTaskFromMobile(request);

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody() != null);
        String taskId = String.valueOf(response.getBody().get("taskId"));
        TaskQueueManager.TaskEntry task = queueManager.getTask(taskId);
        assertNotNull(task);
        assertEquals("BV9CANONICAL9", task.title);
        assertEquals("BV9CANONICAL9", response.getBody().get("title"));
    }

    @Test
    void submitTaskShouldFallbackToResolverWhenVideoInfoUnavailable() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();

        PythonGrpcClient.VideoInfoResult videoInfo = new PythonGrpcClient.VideoInfoResult();
        videoInfo.success = false;

        StubPythonGrpcClient stubGrpc = new StubPythonGrpcClient(videoInfo);
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "pythonGrpcClient", stubGrpc);
        injectField(controller, "mobileVideoInfoTimeoutSeconds", 20);

        MobileMarkdownController.TaskSubmitRequest request = new MobileMarkdownController.TaskSubmitRequest();
        request.userId = "u_001";
        request.videoUrl = "https://www.bilibili.com/video/BV1XKIJBSEBJ?p=1";

        ResponseEntity<Map<String, Object>> response = controller.submitTaskFromMobile(request);

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody() != null);
        String taskId = String.valueOf(response.getBody().get("taskId"));
        TaskQueueManager.TaskEntry task = queueManager.getTask(taskId);
        assertNotNull(task);
        assertEquals("BV1XKIJBSEBJ", task.title);
        assertEquals("BV1XKIJBSEBJ", response.getBody().get("title"));
    }

    @Test
    void submitTaskShouldSkipVideoInfoProbeForPdfInput() throws Exception {
        MobileMarkdownController controller = new MobileMarkdownController();
        TaskQueueManager queueManager = new TaskQueueManager();

        PythonGrpcClient.VideoInfoResult videoInfo = new PythonGrpcClient.VideoInfoResult();
        videoInfo.success = true;
        videoInfo.videoTitle = "Should Not Be Used";
        videoInfo.canonicalId = "SHOULD_NOT_BE_USED";

        StubPythonGrpcClient stubGrpc = new StubPythonGrpcClient(videoInfo);
        injectField(controller, "taskQueueManager", queueManager);
        injectField(controller, "pythonGrpcClient", stubGrpc);
        injectField(controller, "mobileVideoInfoTimeoutSeconds", 20);

        MobileMarkdownController.TaskSubmitRequest request = new MobileMarkdownController.TaskSubmitRequest();
        request.userId = "u_001";
        request.videoUrl = "D:\\videoToMarkdownTest2\\var\\uploads\\book_sample.pdf";

        ResponseEntity<Map<String, Object>> response = controller.submitTaskFromMobile(request);

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody() != null);
        String taskId = String.valueOf(response.getBody().get("taskId"));
        TaskQueueManager.TaskEntry task = queueManager.getTask(taskId);
        assertNotNull(task);
        assertEquals("book_sample.pdf", task.title);
        assertEquals("book_sample.pdf", response.getBody().get("title"));
        assertNull(stubGrpc.lastVideoInput);
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class StubPythonGrpcClient extends PythonGrpcClient {
        private final VideoInfoResult fixedResult;
        private String lastVideoInput;

        private StubPythonGrpcClient(VideoInfoResult fixedResult) {
            this.fixedResult = fixedResult;
        }

        @Override
        public VideoInfoResult getVideoInfo(String taskId, String videoInput, int timeoutSec) {
            this.lastVideoInput = videoInput;
            return fixedResult;
        }
    }
}

