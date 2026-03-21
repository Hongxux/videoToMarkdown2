import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TaskWebSocketHeartbeatBenchmark {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern PING_ACTION_PATTERN =
            Pattern.compile("\"action\"\\s*:\\s*\"ping\"", Pattern.CASE_INSENSITIVE);
    private static final Pattern CLIENT_TIME_PATTERN =
            Pattern.compile("\"clientTime\"\\s*:\\s*(-?\\d+)");
    private static final int SESSION_COUNT = 20_000;
    private static final int BACKGROUND_SESSION_COUNT = 8_000;
    private static final int ACTIVE_SESSION_COUNT = SESSION_COUNT - BACKGROUND_SESSION_COUNT;
    private static final int DURATION_SECONDS = 300;
    private static final int HEARTBEAT_INTERVAL_SECONDS = 20;
    private static final int LEGACY_REAP_SECONDS = 5;
    private static final int LEGACY_PING_SECONDS = 15;
    private static final int TICK_MILLIS = 100;
    private static final int PING_BENCH_WARMUP = 20_000;
    private static final int PING_BENCH_ITERATIONS = 200_000;
    private static final int PONG_BENCH_WARMUP = 20_000;
    private static final int PONG_BENCH_ITERATIONS = 200_000;

    private TaskWebSocketHeartbeatBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        Path outputDir = resolveOutputDir(args);
        Files.createDirectories(outputDir);

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("generatedAt", LocalDateTime.now().toString());
        summary.put("scenario", buildScenario());
        summary.put("pingDecode", benchmarkPingDecode());
        summary.put("pongEncode", benchmarkPongEncode());
        summary.put("scheduler", simulateScheduler());
        summary.put("fanout", simulateProgressFanout());

        Path jsonPath = outputDir.resolve("summary.json");
        Path markdownPath = outputDir.resolve("report.md");
        OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(jsonPath.toFile(), summary);
        Files.writeString(markdownPath, buildMarkdown(summary));

        System.out.println("WebSocket heartbeat benchmark written to: " + outputDir.toAbsolutePath());
        System.out.println("JSON: " + jsonPath.toAbsolutePath());
        System.out.println("Markdown: " + markdownPath.toAbsolutePath());
    }

    private static Map<String, Object> buildScenario() {
        Map<String, Object> scenario = new LinkedHashMap<>();
        scenario.put("sessionCount", SESSION_COUNT);
        scenario.put("activeSessionCount", ACTIVE_SESSION_COUNT);
        scenario.put("backgroundSessionCount", BACKGROUND_SESSION_COUNT);
        scenario.put("durationSeconds", DURATION_SECONDS);
        scenario.put("heartbeatIntervalSeconds", HEARTBEAT_INTERVAL_SECONDS);
        scenario.put("browserApplicationSuspendSeconds", 60);
        scenario.put("legacyReapSeconds", LEGACY_REAP_SECONDS);
        scenario.put("legacyTransportPingSeconds", LEGACY_PING_SECONDS);
        scenario.put("tickMillis", TICK_MILLIS);
        return scenario;
    }

    private static Map<String, Object> benchmarkPingDecode() throws Exception {
        String payload = "{\"action\":\"ping\",\"clientTime\":1773894636659}";
        for (int i = 0; i < PING_BENCH_WARMUP; i++) {
            legacyDecodePing(payload);
            fastDecodePing(payload);
        }

        BenchSample legacy = runLoop(PING_BENCH_ITERATIONS, () -> legacyDecodePing(payload));
        BenchSample fast = runLoop(PING_BENCH_ITERATIONS, () -> fastDecodePing(payload));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("iterations", PING_BENCH_ITERATIONS);
        result.put("legacyOpsPerSec", legacy.opsPerSec());
        result.put("fastPathOpsPerSec", fast.opsPerSec());
        result.put("throughputGainPercent", gainPercent(legacy.opsPerSec(), fast.opsPerSec()));
        result.put("checksum", legacy.checksum + fast.checksum);
        return result;
    }

    private static Map<String, Object> benchmarkPongEncode() throws Exception {
        long serverTime = 1773894636659L;
        long clientTime = 1773894636000L;
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "pong");
        payload.put("serverTime", serverTime);
        payload.put("clientTime", clientTime);

        for (int i = 0; i < PONG_BENCH_WARMUP; i++) {
            legacyEncodePong(payload);
            fastEncodePong(serverTime, clientTime);
        }

        BenchSample legacy = runLoop(PONG_BENCH_ITERATIONS, () -> legacyEncodePong(payload).length());
        BenchSample fast = runLoop(PONG_BENCH_ITERATIONS, () -> fastEncodePong(serverTime, clientTime).length());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("iterations", PONG_BENCH_ITERATIONS);
        result.put("legacyOpsPerSec", legacy.opsPerSec());
        result.put("fastPathOpsPerSec", fast.opsPerSec());
        result.put("throughputGainPercent", gainPercent(legacy.opsPerSec(), fast.opsPerSec()));
        result.put("checksum", legacy.checksum + fast.checksum);
        return result;
    }

    private static Map<String, Object> simulateScheduler() {
        int durationTicks = (DURATION_SECONDS * 1000) / TICK_MILLIS;
        int heartbeatIntervalTicks = (HEARTBEAT_INTERVAL_SECONDS * 1000) / TICK_MILLIS;
        int legacyReapTicks = (LEGACY_REAP_SECONDS * 1000) / TICK_MILLIS;
        int legacyPingTicks = (LEGACY_PING_SECONDS * 1000) / TICK_MILLIS;

        long[] wheelBuckets = new long[durationTicks + heartbeatIntervalTicks + 2];
        long[] legacyBuckets = new long[durationTicks + heartbeatIntervalTicks + 2];
        Random random = new Random(20260319L);

        for (int sessionIndex = 0; sessionIndex < SESSION_COUNT; sessionIndex++) {
            int offsetTick = 1 + random.nextInt(heartbeatIntervalTicks);
            for (int tick = offsetTick; tick <= durationTicks; tick += heartbeatIntervalTicks) {
                wheelBuckets[tick] += 1L;
            }
        }
        for (int tick = legacyReapTicks; tick <= durationTicks; tick += legacyReapTicks) {
            legacyBuckets[tick] += SESSION_COUNT;
        }
        for (int tick = legacyPingTicks; tick <= durationTicks; tick += legacyPingTicks) {
            legacyBuckets[tick] += SESSION_COUNT;
        }

        BucketStats legacyStats = bucketStats(legacyBuckets);
        BucketStats wheelStats = bucketStats(wheelBuckets);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("legacyHeartbeatInspections", legacyStats.total);
        result.put("wheelHeartbeatInspections", wheelStats.total);
        result.put("inspectionReductionPercent", reductionPercent(legacyStats.total, wheelStats.total));
        result.put("legacyPeakBatch", legacyStats.max);
        result.put("wheelPeakBatch", wheelStats.max);
        result.put("peakBatchReductionPercent", reductionPercent(legacyStats.max, wheelStats.max));
        result.put("legacyP95Batch", legacyStats.p95);
        result.put("wheelP95Batch", wheelStats.p95);
        return result;
    }

    private static Map<String, Object> simulateProgressFanout() {
        long legacyAttempts = 0L;
        long wheelAttempts = 0L;
        long backgroundPushesSaved = 0L;
        for (int second = 1; second <= DURATION_SECONDS; second++) {
            legacyAttempts += SESSION_COUNT;
            if (second <= 60) {
                wheelAttempts += SESSION_COUNT;
                continue;
            }
            wheelAttempts += ACTIVE_SESSION_COUNT;
            backgroundPushesSaved += BACKGROUND_SESSION_COUNT;
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("legacyProcessingFanoutAttempts", legacyAttempts);
        result.put("wheelProcessingFanoutAttempts", wheelAttempts);
        result.put("fanoutReductionPercent", reductionPercent(legacyAttempts, wheelAttempts));
        result.put("backgroundPushesSaved", backgroundPushesSaved);
        result.put("browserBackgroundDetectionSeconds", 60);
        result.put("legacyBackgroundDetection", "not_detectable");
        return result;
    }

    private static BenchSample runLoop(int iterations, ThrowingIntSupplier supplier) throws Exception {
        long checksum = 0L;
        long startedAt = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            checksum += supplier.getAsInt();
        }
        long elapsedNanos = System.nanoTime() - startedAt;
        return new BenchSample(iterations, elapsedNanos, checksum);
    }

    private static int legacyDecodePing(String payload) throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, Object> parsed = OBJECT_MAPPER.readValue(payload, Map.class);
        Object clientTime = parsed.get("clientTime");
        long value;
        if (clientTime instanceof Number number) {
            value = number.longValue();
        } else {
            value = Long.parseLong(String.valueOf(clientTime));
        }
        return Long.hashCode(value);
    }

    private static int fastDecodePing(String payload) {
        if (payload == null || !payload.contains("\"action\"") || !payload.contains("\"ping\"")) {
            return 0;
        }
        if (!PING_ACTION_PATTERN.matcher(payload).find()) {
            return 0;
        }
        Matcher matcher = CLIENT_TIME_PATTERN.matcher(payload);
        if (!matcher.find()) {
            return 0;
        }
        return Long.hashCode(Long.parseLong(matcher.group(1)));
    }

    private static String legacyEncodePong(Map<String, Object> payload) throws Exception {
        return OBJECT_MAPPER.writeValueAsString(payload);
    }

    private static String fastEncodePong(long serverTime, long clientTime) {
        return "{\"type\":\"pong\",\"serverTime\":" + serverTime + ",\"clientTime\":" + clientTime + "}";
    }

    private static BucketStats bucketStats(long[] buckets) {
        long total = 0L;
        long max = 0L;
        List<Long> nonZero = new ArrayList<>();
        for (long bucket : buckets) {
            total += bucket;
            if (bucket > max) {
                max = bucket;
            }
            if (bucket > 0) {
                nonZero.add(bucket);
            }
        }
        nonZero.sort(Long::compareTo);
        long p95 = nonZero.isEmpty()
                ? 0L
                : nonZero.get(Math.min(nonZero.size() - 1, (int) Math.floor((nonZero.size() - 1) * 0.95)));
        return new BucketStats(total, max, p95);
    }

    private static double gainPercent(double baseline, double current) {
        if (baseline <= 0.0d) {
            return 0.0d;
        }
        return round(((current - baseline) / baseline) * 100.0d);
    }

    private static double reductionPercent(long baseline, long current) {
        if (baseline <= 0L) {
            return 0.0d;
        }
        return round(((double) (baseline - current) / baseline) * 100.0d);
    }

    private static double round(double value) {
        return Math.round(value * 100.0d) / 100.0d;
    }

    private static Path resolveOutputDir(String[] args) {
        if (args != null && args.length >= 2 && "--output-dir".equals(args[0])) {
            return Path.of(args[1]).toAbsolutePath().normalize();
        }
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return Path.of("var", "artifacts", "benchmarks", "websocket_heartbeat_" + timestamp)
                .toAbsolutePath()
                .normalize();
    }

    private static String buildMarkdown(Map<String, Object> summary) {
        @SuppressWarnings("unchecked")
        Map<String, Object> scenario = (Map<String, Object>) summary.get("scenario");
        @SuppressWarnings("unchecked")
        Map<String, Object> pingDecode = (Map<String, Object>) summary.get("pingDecode");
        @SuppressWarnings("unchecked")
        Map<String, Object> pongEncode = (Map<String, Object>) summary.get("pongEncode");
        @SuppressWarnings("unchecked")
        Map<String, Object> scheduler = (Map<String, Object>) summary.get("scheduler");
        @SuppressWarnings("unchecked")
        Map<String, Object> fanout = (Map<String, Object>) summary.get("fanout");

        StringBuilder builder = new StringBuilder();
        builder.append("# WebSocket Heartbeat Benchmark\n\n");
        builder.append("- Sessions: ").append(scenario.get("sessionCount")).append('\n');
        builder.append("- Active sessions: ").append(scenario.get("activeSessionCount")).append('\n');
        builder.append("- Background sessions: ").append(scenario.get("backgroundSessionCount")).append('\n');
        builder.append("- Duration: ").append(scenario.get("durationSeconds")).append("s\n\n");

        builder.append("## Ping decode\n");
        builder.append("- Legacy ops/s: ").append(formatNumber(pingDecode.get("legacyOpsPerSec"))).append('\n');
        builder.append("- Fast path ops/s: ").append(formatNumber(pingDecode.get("fastPathOpsPerSec"))).append('\n');
        builder.append("- Gain: ").append(pingDecode.get("throughputGainPercent")).append("%\n\n");

        builder.append("## Pong encode\n");
        builder.append("- Legacy ops/s: ").append(formatNumber(pongEncode.get("legacyOpsPerSec"))).append('\n');
        builder.append("- Fast path ops/s: ").append(formatNumber(pongEncode.get("fastPathOpsPerSec"))).append('\n');
        builder.append("- Gain: ").append(pongEncode.get("throughputGainPercent")).append("%\n\n");

        builder.append("## Scheduler\n");
        builder.append("- Legacy inspections: ").append(formatNumber(scheduler.get("legacyHeartbeatInspections"))).append('\n');
        builder.append("- Wheel inspections: ").append(formatNumber(scheduler.get("wheelHeartbeatInspections"))).append('\n');
        builder.append("- Inspection reduction: ").append(scheduler.get("inspectionReductionPercent")).append("%\n");
        builder.append("- Legacy peak batch: ").append(formatNumber(scheduler.get("legacyPeakBatch"))).append('\n');
        builder.append("- Wheel peak batch: ").append(formatNumber(scheduler.get("wheelPeakBatch"))).append('\n');
        builder.append("- Peak batch reduction: ").append(scheduler.get("peakBatchReductionPercent")).append("%\n\n");

        builder.append("## Fanout\n");
        builder.append("- Legacy processing attempts: ").append(formatNumber(fanout.get("legacyProcessingFanoutAttempts"))).append('\n');
        builder.append("- Wheel processing attempts: ").append(formatNumber(fanout.get("wheelProcessingFanoutAttempts"))).append('\n');
        builder.append("- Fanout reduction: ").append(fanout.get("fanoutReductionPercent")).append("%\n");
        builder.append("- Background pushes saved: ").append(formatNumber(fanout.get("backgroundPushesSaved"))).append('\n');
        builder.append("- Browser background detection: ").append(fanout.get("browserBackgroundDetectionSeconds")).append("s\n");
        return builder.toString();
    }

    private static String formatNumber(Object value) {
        if (value instanceof Number number) {
            if (value instanceof Float || value instanceof Double) {
                return String.format(Locale.ROOT, "%,.2f", number.doubleValue());
            }
            return String.format(Locale.ROOT, "%,d", number.longValue());
        }
        return String.valueOf(value);
    }

    private record BenchSample(int iterations, long elapsedNanos, long checksum) {
        private double opsPerSec() {
            if (elapsedNanos <= 0L) {
                return 0.0d;
            }
            return round((iterations * 1_000_000_000.0d) / elapsedNanos);
        }
    }

    private record BucketStats(long total, long max, long p95) {
    }

    @FunctionalInterface
    private interface ThrowingIntSupplier {
        int getAsInt() throws Exception;
    }
}
