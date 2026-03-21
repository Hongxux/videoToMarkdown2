package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.websocket.TaskWebSocketHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/admin/websocket-benchmark")
public class WebSocketBenchmarkController {

    private static final int MAX_MESSAGE_COUNT = 20_000;
    private static final int MAX_INTERVAL_MS = 60_000;
    private static final int MAX_PADDING_BYTES = 64 * 1024;

    @Autowired
    private TaskWebSocketHandler taskWebSocketHandler;

    @Value("${websocket.benchmark.enabled:false}")
    private boolean benchmarkEnabled;

    @GetMapping("/connections")
    public ResponseEntity<Map<String, Object>> getConnectionSnapshot(
            @RequestParam(defaultValue = "") String userId
    ) {
        ensureBenchmarkEnabled();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("userId", normalizeText(userId));
        payload.put("totalConnections", taskWebSocketHandler.getConnectionCount());
        payload.put("userConnections", taskWebSocketHandler.getUserConnectionCount(userId));
        payload.put("serverTime", System.currentTimeMillis());
        return ResponseEntity.ok(payload);
    }

    @PostMapping("/broadcast")
    public ResponseEntity<Map<String, Object>> broadcastBenchmarkMessages(
            @RequestBody BenchmarkBroadcastRequest request
    ) throws InterruptedException {
        ensureBenchmarkEnabled();
        String userId = normalizeText(request.userId);
        String taskId = normalizeText(request.taskId);
        String collectionId = normalizeText(request.collectionId);
        if (userId.isBlank() && taskId.isBlank() && collectionId.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "userId / taskId / collectionId 至少需要一个"
            ));
        }

        int messageCount = boundedPositiveInt(request.messageCount, 1, MAX_MESSAGE_COUNT);
        int intervalMs = boundedPositiveInt(request.intervalMs, 0, MAX_INTERVAL_MS);
        int payloadBytes = boundedPositiveInt(request.payloadBytes, 0, MAX_PADDING_BYTES);
        String runId = normalizeText(request.runId);
        if (runId.isBlank()) {
            runId = "ws-bench-" + System.currentTimeMillis();
        }
        String eventType = normalizeText(request.eventType);
        if (eventType.isBlank()) {
            eventType = "benchmarkEvent";
        }
        String padding = payloadBytes > 0 ? "x".repeat(payloadBytes) : "";
        long startedAt = System.currentTimeMillis();

        for (int sequence = 1; sequence <= messageCount; sequence++) {
            long sentAt = System.currentTimeMillis();
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("type", eventType);
            payload.put("benchmarkRunId", runId);
            payload.put("sequence", sequence);
            payload.put("messageCount", messageCount);
            payload.put("serverSendTime", sentAt);
            payload.put("payloadBytes", payloadBytes);
            payload.put("routeUserId", userId);
            if (!taskId.isBlank()) {
                payload.put("routeTaskId", taskId);
            }
            if (!collectionId.isBlank()) {
                payload.put("routeCollectionId", collectionId);
            }
            if (!padding.isEmpty()) {
                payload.put("padding", padding);
            }
            taskWebSocketHandler.broadcastBenchmarkEvent(userId, taskId, collectionId, payload);
            if (intervalMs > 0 && sequence < messageCount) {
                Thread.sleep(intervalMs);
            }
        }

        long finishedAt = System.currentTimeMillis();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", true);
        response.put("runId", runId);
        response.put("eventType", eventType);
        response.put("messageCount", messageCount);
        response.put("intervalMs", intervalMs);
        response.put("payloadBytes", payloadBytes);
        response.put("userId", userId);
        response.put("taskId", taskId);
        response.put("collectionId", collectionId);
        response.put("serverStartedAt", startedAt);
        response.put("serverFinishedAt", finishedAt);
        response.put("serverElapsedMs", Math.max(0L, finishedAt - startedAt));
        response.put("targetUserConnections", taskWebSocketHandler.getUserConnectionCount(userId));
        response.put("totalConnections", taskWebSocketHandler.getConnectionCount());
        return ResponseEntity.ok(response);
    }

    private void ensureBenchmarkEnabled() {
        if (!benchmarkEnabled) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND,
                    "websocket benchmark disabled; set websocket.benchmark.enabled=true"
            );
        }
    }

    private int boundedPositiveInt(Integer rawValue, int defaultValue, int maxValue) {
        int resolved = rawValue != null ? rawValue : defaultValue;
        if (resolved < 0) {
            resolved = defaultValue;
        }
        return Math.min(maxValue, resolved);
    }

    private String normalizeText(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    public static class BenchmarkBroadcastRequest {
        public String runId;
        public String eventType;
        public String userId;
        public String taskId;
        public String collectionId;
        public Integer messageCount;
        public Integer intervalMs;
        public Integer payloadBytes;
    }
}
