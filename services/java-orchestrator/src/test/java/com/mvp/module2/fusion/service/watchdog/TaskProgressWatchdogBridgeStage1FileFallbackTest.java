package com.mvp.module2.fusion.service.watchdog;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskProgressWatchdogBridgeStage1FileFallbackTest {

    @Test
    void startMonitorShouldReadStage1DedicatedHeartbeatFile() throws Exception {
        TaskProgressWatchdogBridge bridge = new TaskProgressWatchdogBridge();
        setField(bridge, "grpcStreamEnabled", false);
        setField(bridge, "grpcFallbackFilePolling", true);
        setField(bridge, "filePollIntervalMs", 25L);

        Path outputDir = Files.createTempDirectory("stage1-watchdog-file");
        Path heartbeatPath = outputDir.resolve("intermediates").resolve("stage1_watchdog_heartbeat.json");
        Files.createDirectories(heartbeatPath.getParent());

        CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Double> progresses = new CopyOnWriteArrayList<>();
        TaskProgressWatchdogBridge.SignalEmitter emitter = (progress, message) -> {
            progresses.add(progress);
            messages.add(message);
        };

        TaskProgressWatchdogBridge.MonitorHandle handle =
                bridge.startMonitor("task-stage1-file", outputDir.toString(), "stage1", emitter);
        try {
            Files.writeString(
                    heartbeatPath,
                    "{" +
                            "\"schema\":\"stage_watchdog.v1\"," +
                            "\"stage\":\"stage1\"," +
                            "\"status\":\"running\"," +
                            "\"checkpoint\":\"pipeline_pending\"," +
                            "\"completed\":0," +
                            "\"pending\":6," +
                            "\"seq\":1," +
                            "\"updated_at_ms\":1774007761000," +
                            "\"signal_type\":\"soft\"" +
                            "}",
                    StandardCharsets.UTF_8
            );

            waitUntil(() -> !messages.isEmpty(), 1000L);

            assertFalse(messages.isEmpty());
            assertTrue(messages.get(0).contains("\"stage\":\"stage1\""));
            assertFalse(progresses.isEmpty());
            assertTrue(progresses.get(0) >= 0.25d && progresses.get(0) <= 0.35d);
        } finally {
            bridge.stopMonitor("task-stage1-file", handle, emitter);
        }
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25L);
        }
        throw new AssertionError("condition not met within timeout");
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}