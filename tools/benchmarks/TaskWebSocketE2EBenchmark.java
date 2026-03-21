import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public final class TaskWebSocketE2EBenchmark {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private TaskWebSocketE2EBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.parse(args);
        Files.createDirectories(config.outputDir());

        MetricsCollector metrics = new MetricsCollector();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
                Math.max(4, Math.min(32, config.connections())),
                new NamedThreadFactory("ws-e2e-bench")
        );
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(config.connectTimeoutMs()))
                .executor(scheduler)
                .build();

        List<BenchWebSocketClient> clients = new ArrayList<>(config.connections());
        try {
            connectClients(config, httpClient, scheduler, metrics, clients);
            if (config.warmupMs() > 0) {
                Thread.sleep(config.warmupMs());
            }

            Map<String, Object> beforeSnapshot = fetchConnectionSnapshot(config, httpClient);
            Map<String, Object> triggerResponse = triggerBroadcast(config, httpClient);

            int connectedClients = metrics.connectedClients();
            long expectedMessages = (long) connectedClients * config.broadcastCount();
            waitForBenchmarkMessages(config, metrics, expectedMessages);

            if (config.observeAfterBroadcastMs() > 0) {
                Thread.sleep(config.observeAfterBroadcastMs());
            }

            Map<String, Object> summary = buildSummary(config, metrics, beforeSnapshot, triggerResponse, expectedMessages);
            writeOutputs(config.outputDir(), summary);

            System.out.println("WebSocket e2e benchmark completed.");
            System.out.println("Output: " + config.outputDir().toAbsolutePath());
            System.out.println("Connected clients: " + connectedClients + "/" + config.connections());
            System.out.println("Benchmark deliveries: " + metrics.benchmarkMessagesReceived() + "/" + expectedMessages);
        } finally {
            closeClients(clients);
            scheduler.shutdownNow();
        }
    }

    private static void connectClients(
            BenchmarkConfig config,
            HttpClient httpClient,
            ScheduledExecutorService scheduler,
            MetricsCollector metrics,
            List<BenchWebSocketClient> clients
    ) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(config.connections());
        for (int i = 0; i < config.connections(); i++) {
            BenchWebSocketClient client = new BenchWebSocketClient(
                    i,
                    config,
                    httpClient,
                    scheduler,
                    metrics,
                    latch
            );
            clients.add(client);
            client.connect();
            if (config.connectRampMs() > 0 && i < config.connections() - 1) {
                Thread.sleep(config.connectRampMs());
            }
        }
        latch.await(config.connectTimeoutMs() * Math.max(1L, config.connections()), TimeUnit.MILLISECONDS);
    }

    private static Map<String, Object> fetchConnectionSnapshot(BenchmarkConfig config, HttpClient httpClient) {
        try {
            String url = config.baseUrl() + "/api/admin/websocket-benchmark/connections?userId="
                    + urlEncode(config.userId());
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofMillis(config.httpTimeoutMs()))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return OBJECT_MAPPER.readValue(response.body(), MAP_TYPE);
        } catch (Exception error) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", false);
            payload.put("message", error.getMessage());
            return payload;
        }
    }

    private static Map<String, Object> triggerBroadcast(BenchmarkConfig config, HttpClient httpClient) throws Exception {
        Map<String, Object> requestPayload = new LinkedHashMap<>();
        requestPayload.put("runId", config.runId());
        requestPayload.put("eventType", "benchmarkEvent");
        requestPayload.put("userId", config.userId());
        requestPayload.put("taskId", config.taskId());
        requestPayload.put("collectionId", config.collectionId());
        requestPayload.put("messageCount", config.broadcastCount());
        requestPayload.put("intervalMs", config.broadcastIntervalMs());
        requestPayload.put("payloadBytes", config.payloadBytes());

        HttpRequest request = HttpRequest.newBuilder(URI.create(config.baseUrl() + "/api/admin/websocket-benchmark/broadcast"))
                .timeout(Duration.ofMillis(config.httpTimeoutMs() + Math.max(0L, (long) config.broadcastCount() * config.broadcastIntervalMs())))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(requestPayload)))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IOException("broadcast trigger failed: status=" + response.statusCode() + ", body=" + response.body());
        }
        return OBJECT_MAPPER.readValue(response.body(), MAP_TYPE);
    }

    private static void waitForBenchmarkMessages(BenchmarkConfig config, MetricsCollector metrics, long expectedMessages)
            throws InterruptedException {
        if (expectedMessages <= 0L) {
            return;
        }
        long deadline = System.currentTimeMillis() + config.broadcastAwaitTimeoutMs();
        while (System.currentTimeMillis() < deadline) {
            if (metrics.benchmarkMessagesReceived() >= expectedMessages) {
                return;
            }
            Thread.sleep(200L);
        }
    }

    private static void closeClients(List<BenchWebSocketClient> clients) {
        for (BenchWebSocketClient client : clients) {
            try {
                client.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static Map<String, Object> buildSummary(
            BenchmarkConfig config,
            MetricsCollector metrics,
            Map<String, Object> beforeSnapshot,
            Map<String, Object> triggerResponse,
            long expectedMessages
    ) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("generatedAt", LocalDateTime.now().toString());
        summary.put("config", config.toMap());
        summary.put("serverConnectionSnapshot", beforeSnapshot);
        summary.put("broadcastTrigger", triggerResponse);

        Map<String, Object> connection = new LinkedHashMap<>();
        connection.put("requestedConnections", config.connections());
        connection.put("connectedConnections", metrics.connectedClients());
        connection.put("connectErrors", metrics.connectErrors());
        connection.put("closeEvents", metrics.closeEvents());
        connection.put("connectSuccessRate", percentage(metrics.connectedClients(), config.connections()));
        connection.put("connectLatencyMs", metrics.summaryOf(metrics.connectLatencySamples()));
        summary.put("connection", connection);

        Map<String, Object> heartbeat = new LinkedHashMap<>();
        heartbeat.put("pingSent", metrics.pingsSent());
        heartbeat.put("pongReceived", metrics.pongsReceived());
        heartbeat.put("pongReceiveRate", percentage(metrics.pongsReceived(), metrics.pingsSent()));
        heartbeat.put("pongRttMs", metrics.summaryOf(metrics.pongRttSamples()));
        summary.put("heartbeat", heartbeat);

        Map<String, Object> broadcast = new LinkedHashMap<>();
        broadcast.put("expectedMessages", expectedMessages);
        broadcast.put("receivedMessages", metrics.benchmarkMessagesReceived());
        broadcast.put("deliveryRate", percentage(metrics.benchmarkMessagesReceived(), expectedMessages));
        broadcast.put("latencyMs", metrics.summaryOf(metrics.benchmarkLatencySamples()));
        broadcast.put("unexpectedMessages", metrics.unexpectedMessages());
        broadcast.put("clientErrors", metrics.clientErrors());
        summary.put("broadcast", broadcast);
        return summary;
    }

    private static void writeOutputs(Path outputDir, Map<String, Object> summary) throws Exception {
        Path jsonPath = outputDir.resolve("summary.json");
        Path markdownPath = outputDir.resolve("report.md");
        OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(jsonPath.toFile(), summary);
        Files.writeString(markdownPath, buildMarkdown(summary));
    }

    private static String buildMarkdown(Map<String, Object> summary) {
        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) summary.get("config");
        @SuppressWarnings("unchecked")
        Map<String, Object> snapshot = (Map<String, Object>) summary.get("serverConnectionSnapshot");
        @SuppressWarnings("unchecked")
        Map<String, Object> connection = (Map<String, Object>) summary.get("connection");
        @SuppressWarnings("unchecked")
        Map<String, Object> heartbeat = (Map<String, Object>) summary.get("heartbeat");
        @SuppressWarnings("unchecked")
        Map<String, Object> broadcast = (Map<String, Object>) summary.get("broadcast");

        StringBuilder builder = new StringBuilder();
        builder.append("# WebSocket E2E Benchmark\n\n");
        builder.append("- Base URL: ").append(config.get("baseUrl")).append('\n');
        builder.append("- WebSocket URL: ").append(config.get("wsUrl")).append('\n');
        builder.append("- User route: ").append(config.get("userId")).append('\n');
        builder.append("- Connections: ").append(config.get("connections")).append('\n');
        builder.append("- Broadcast count: ").append(config.get("broadcastCount")).append('\n');
        builder.append("- Payload bytes: ").append(config.get("payloadBytes")).append('\n');
        builder.append("- Server sees user connections: ").append(snapshot.getOrDefault("userConnections", "unknown")).append("\n\n");

        builder.append("## Connection\n");
        builder.append("- Connected: ").append(connection.get("connectedConnections"))
                .append('/').append(connection.get("requestedConnections")).append('\n');
        builder.append("- Connect success rate: ").append(connection.get("connectSuccessRate")).append("%\n");
        builder.append("- Connect latency: ").append(connection.get("connectLatencyMs")).append("\n\n");

        builder.append("## Heartbeat\n");
        builder.append("- Ping sent: ").append(heartbeat.get("pingSent")).append('\n');
        builder.append("- Pong received: ").append(heartbeat.get("pongReceived")).append('\n');
        builder.append("- Pong receive rate: ").append(heartbeat.get("pongReceiveRate")).append("%\n");
        builder.append("- Pong RTT: ").append(heartbeat.get("pongRttMs")).append("\n\n");

        builder.append("## Broadcast\n");
        builder.append("- Expected messages: ").append(broadcast.get("expectedMessages")).append('\n');
        builder.append("- Received messages: ").append(broadcast.get("receivedMessages")).append('\n');
        builder.append("- Delivery rate: ").append(broadcast.get("deliveryRate")).append("%\n");
        builder.append("- Latency: ").append(broadcast.get("latencyMs")).append('\n');
        builder.append("- Unexpected messages: ").append(broadcast.get("unexpectedMessages")).append('\n');
        builder.append("- Client errors: ").append(broadcast.get("clientErrors")).append('\n');
        return builder.toString();
    }

    private static double percentage(long value, long total) {
        if (total <= 0L) {
            return 0.0d;
        }
        return round(value * 100.0d / total);
    }

    private static double round(double value) {
        return Math.round(value * 100.0d) / 100.0d;
    }

    private static String urlEncode(String value) {
        return URLEncoder.encode(value != null ? value : "", StandardCharsets.UTF_8);
    }

    private static final class BenchWebSocketClient implements WebSocket.Listener {

        private final BenchmarkConfig config;
        private final HttpClient httpClient;
        private final ScheduledExecutorService scheduler;
        private final MetricsCollector metrics;
        private final CountDownLatch connectLatch;
        private final ConcurrentHashMap<Long, Long> pendingPingSentAt = new ConcurrentHashMap<>();
        private final StringBuilder partialMessage = new StringBuilder();
        private final CompletableFuture<WebSocket> openFuture = new CompletableFuture<>();
        private volatile WebSocket webSocket;

        private BenchWebSocketClient(
                int clientIndex,
                BenchmarkConfig config,
                HttpClient httpClient,
                ScheduledExecutorService scheduler,
                MetricsCollector metrics,
                CountDownLatch connectLatch
        ) {
            this.config = config;
            this.httpClient = httpClient;
            this.scheduler = scheduler;
            this.metrics = metrics;
            this.connectLatch = connectLatch;
        }

        private void connect() {
            long startedAt = System.nanoTime();
            URI targetUri = URI.create(buildWebSocketUrl());
            httpClient.newWebSocketBuilder()
                    .connectTimeout(Duration.ofMillis(config.connectTimeoutMs()))
                    .buildAsync(targetUri, this)
                    .whenComplete((socket, error) -> {
                        if (error != null) {
                            metrics.recordConnectError();
                            openFuture.completeExceptionally(error);
                            connectLatch.countDown();
                            return;
                        }
                        this.webSocket = socket;
                        metrics.recordConnectLatency(elapsedMillis(startedAt));
                    });
        }

        private String buildWebSocketUrl() {
            String separator = config.wsUrl().contains("?") ? "&" : "?";
            StringBuilder builder = new StringBuilder(config.wsUrl())
                    .append(separator)
                    .append("userId=").append(urlEncode(config.userId()))
                    .append("&clientType=").append(urlEncode(config.clientType()))
                    .append("&streamKey=").append(urlEncode(config.streamKey()));
            if (!config.lastAckedTerminalEventId().isBlank()) {
                builder.append("&lastAckedTerminalEventId=").append(urlEncode(config.lastAckedTerminalEventId()));
            }
            return builder.toString();
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            openFuture.complete(webSocket);
            metrics.recordConnectedClient();
            webSocket.request(1L);
            if (!config.taskId().isBlank()) {
                sendJson(Map.of("action", "subscribe", "taskId", config.taskId()));
            }
            if (!config.collectionId().isBlank()) {
                sendJson(Map.of("action", "subscribeCollection", "collectionId", config.collectionId()));
            }
            if (config.pingIntervalMs() > 0) {
                scheduler.scheduleAtFixedRate(
                        this::sendHeartbeatPing,
                        config.pingIntervalMs(),
                        config.pingIntervalMs(),
                        TimeUnit.MILLISECONDS
                );
            }
            connectLatch.countDown();
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            partialMessage.append(data);
            if (last) {
                String payload = partialMessage.toString();
                partialMessage.setLength(0);
                handleText(payload);
            }
            webSocket.request(1L);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            metrics.recordClientError();
            if (!openFuture.isDone()) {
                openFuture.completeExceptionally(error);
                connectLatch.countDown();
            }
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            metrics.recordCloseEvent();
            if (!openFuture.isDone()) {
                openFuture.complete(webSocket);
                connectLatch.countDown();
            }
            return CompletableFuture.completedFuture(null);
        }

        private void handleText(String payload) {
            try {
                Map<String, Object> message = OBJECT_MAPPER.readValue(payload, MAP_TYPE);
                String type = Objects.toString(message.getOrDefault("type", ""), "").trim();
                if ("pong".equalsIgnoreCase(type)) {
                    metrics.recordPong();
                    long clientTime = readLong(message.get("clientTime"));
                    Long sentAt = pendingPingSentAt.remove(clientTime);
                    if (sentAt != null) {
                        metrics.recordPongRtt(System.currentTimeMillis() - sentAt);
                    }
                    return;
                }
                if ("benchmarkEvent".equals(type)) {
                    String runId = Objects.toString(message.getOrDefault("benchmarkRunId", ""), "");
                    if (!config.runId().equals(runId)) {
                        metrics.recordUnexpectedMessage();
                        return;
                    }
                    long serverSendTime = readLong(message.get("serverSendTime"));
                    metrics.recordBenchmarkMessage(System.currentTimeMillis() - serverSendTime);
                    return;
                }
                metrics.recordUnexpectedMessage();
            } catch (Exception error) {
                metrics.recordClientError();
            }
        }

        private void sendHeartbeatPing() {
            WebSocket socket = webSocket;
            if (socket == null || openFuture.isCompletedExceptionally()) {
                return;
            }
            long now = System.currentTimeMillis();
            pendingPingSentAt.put(now, now);
            metrics.recordPingSent();
            sendJson(Map.of("action", "ping", "clientTime", now));
        }

        private void sendJson(Map<String, Object> payload) {
            try {
                WebSocket socket = openFuture.getNow(null);
                if (socket != null) {
                    socket.sendText(OBJECT_MAPPER.writeValueAsString(payload), true);
                }
            } catch (Exception error) {
                metrics.recordClientError();
            }
        }

        private void close() {
            WebSocket socket = openFuture.getNow(null);
            if (socket != null) {
                try {
                    socket.sendClose(WebSocket.NORMAL_CLOSURE, "benchmark done").join();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static final class MetricsCollector {
        private final LongAdder connectedClients = new LongAdder();
        private final LongAdder connectErrors = new LongAdder();
        private final LongAdder closeEvents = new LongAdder();
        private final LongAdder pingsSent = new LongAdder();
        private final LongAdder pongsReceived = new LongAdder();
        private final LongAdder benchmarkMessagesReceived = new LongAdder();
        private final LongAdder unexpectedMessages = new LongAdder();
        private final LongAdder clientErrors = new LongAdder();
        private final ConcurrentLinkedQueue<Long> connectLatencySamples = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Long> pongRttSamples = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Long> benchmarkLatencySamples = new ConcurrentLinkedQueue<>();

        private void recordConnectedClient() {
            connectedClients.increment();
        }

        private void recordConnectError() {
            connectErrors.increment();
        }

        private void recordCloseEvent() {
            closeEvents.increment();
        }

        private void recordConnectLatency(long latencyMs) {
            connectLatencySamples.add(Math.max(0L, latencyMs));
        }

        private void recordPingSent() {
            pingsSent.increment();
        }

        private void recordPong() {
            pongsReceived.increment();
        }

        private void recordPongRtt(long latencyMs) {
            pongRttSamples.add(Math.max(0L, latencyMs));
        }

        private void recordBenchmarkMessage(long latencyMs) {
            benchmarkMessagesReceived.increment();
            benchmarkLatencySamples.add(Math.max(0L, latencyMs));
        }

        private void recordUnexpectedMessage() {
            unexpectedMessages.increment();
        }

        private void recordClientError() {
            clientErrors.increment();
        }

        private int connectedClients() {
            return connectedClients.intValue();
        }

        private long connectErrors() {
            return connectErrors.longValue();
        }

        private long closeEvents() {
            return closeEvents.longValue();
        }

        private long pingsSent() {
            return pingsSent.longValue();
        }

        private long pongsReceived() {
            return pongsReceived.longValue();
        }

        private long benchmarkMessagesReceived() {
            return benchmarkMessagesReceived.longValue();
        }

        private long unexpectedMessages() {
            return unexpectedMessages.longValue();
        }

        private long clientErrors() {
            return clientErrors.longValue();
        }

        private ConcurrentLinkedQueue<Long> connectLatencySamples() {
            return connectLatencySamples;
        }

        private ConcurrentLinkedQueue<Long> pongRttSamples() {
            return pongRttSamples;
        }

        private ConcurrentLinkedQueue<Long> benchmarkLatencySamples() {
            return benchmarkLatencySamples;
        }

        private Map<String, Object> summaryOf(ConcurrentLinkedQueue<Long> samples) {
            List<Long> values = new ArrayList<>(samples);
            Collections.sort(values);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("count", values.size());
            if (values.isEmpty()) {
                payload.put("min", 0L);
                payload.put("p50", 0L);
                payload.put("p95", 0L);
                payload.put("p99", 0L);
                payload.put("max", 0L);
                payload.put("avg", 0.0d);
                return payload;
            }
            long total = 0L;
            for (Long value : values) {
                total += value;
            }
            payload.put("min", values.get(0));
            payload.put("p50", percentile(values, 0.50d));
            payload.put("p95", percentile(values, 0.95d));
            payload.put("p99", percentile(values, 0.99d));
            payload.put("max", values.get(values.size() - 1));
            payload.put("avg", round(total * 1.0d / values.size()));
            return payload;
        }

        private long percentile(List<Long> values, double ratio) {
            if (values.isEmpty()) {
                return 0L;
            }
            int index = Math.min(values.size() - 1, (int) Math.floor((values.size() - 1) * ratio));
            return values.get(index);
        }
    }

    private record BenchmarkConfig(
            String baseUrl,
            String wsUrl,
            String userId,
            String clientType,
            String streamKey,
            String taskId,
            String collectionId,
            int connections,
            int connectRampMs,
            int connectTimeoutMs,
            int httpTimeoutMs,
            int pingIntervalMs,
            int warmupMs,
            int broadcastCount,
            int broadcastIntervalMs,
            int broadcastAwaitTimeoutMs,
            int observeAfterBroadcastMs,
            int payloadBytes,
            String lastAckedTerminalEventId,
            String runId,
            Path outputDir
    ) {
        private static BenchmarkConfig parse(String[] args) {
            Map<String, String> values = parseArgs(args);
            String baseUrl = values.getOrDefault("base-url", "http://127.0.0.1:8080");
            String wsUrl = values.getOrDefault("ws-url", baseUrl.replaceFirst("^http", "ws") + "/ws/tasks");
            String runId = values.getOrDefault(
                    "run-id",
                    "ws-e2e-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
            );
            Path outputDir = values.containsKey("output-dir")
                    ? Path.of(values.get("output-dir")).toAbsolutePath().normalize()
                    : Path.of("var", "artifacts", "benchmarks", runId).toAbsolutePath().normalize();
            return new BenchmarkConfig(
                    baseUrl,
                    wsUrl,
                    values.getOrDefault("user-id", "ws-bench-user"),
                    values.getOrDefault("client-type", "browser"),
                    values.getOrDefault("stream-key", "web-task-updates"),
                    values.getOrDefault("task-id", ""),
                    values.getOrDefault("collection-id", ""),
                    parseInt(values.get("connections"), 100),
                    parseInt(values.get("connect-ramp-ms"), 5),
                    parseInt(values.get("connect-timeout-ms"), 5_000),
                    parseInt(values.get("http-timeout-ms"), 30_000),
                    parseInt(values.get("ping-interval-ms"), 10_000),
                    parseInt(values.get("warmup-ms"), 5_000),
                    parseInt(values.get("broadcast-count"), 30),
                    parseInt(values.get("broadcast-interval-ms"), 1_000),
                    parseInt(values.get("broadcast-await-timeout-ms"), 120_000),
                    parseInt(values.get("observe-after-broadcast-ms"), 2_000),
                    parseInt(values.get("payload-bytes"), 0),
                    values.getOrDefault("last-acked-terminal-event-id", ""),
                    runId,
                    outputDir
            );
        }

        private Map<String, Object> toMap() {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("baseUrl", baseUrl);
            payload.put("wsUrl", wsUrl);
            payload.put("userId", userId);
            payload.put("clientType", clientType);
            payload.put("streamKey", streamKey);
            payload.put("taskId", taskId);
            payload.put("collectionId", collectionId);
            payload.put("connections", connections);
            payload.put("connectRampMs", connectRampMs);
            payload.put("connectTimeoutMs", connectTimeoutMs);
            payload.put("httpTimeoutMs", httpTimeoutMs);
            payload.put("pingIntervalMs", pingIntervalMs);
            payload.put("warmupMs", warmupMs);
            payload.put("broadcastCount", broadcastCount);
            payload.put("broadcastIntervalMs", broadcastIntervalMs);
            payload.put("broadcastAwaitTimeoutMs", broadcastAwaitTimeoutMs);
            payload.put("observeAfterBroadcastMs", observeAfterBroadcastMs);
            payload.put("payloadBytes", payloadBytes);
            payload.put("runId", runId);
            payload.put("outputDir", outputDir.toString());
            return payload;
        }

        private static Map<String, String> parseArgs(String[] args) {
            Map<String, String> values = new LinkedHashMap<>();
            if (args == null) {
                return values;
            }
            for (int i = 0; i < args.length; i++) {
                String raw = Optional.ofNullable(args[i]).orElse("").trim();
                if (!raw.startsWith("--")) {
                    continue;
                }
                String key = raw.substring(2);
                if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
                    values.put(key, "true");
                    continue;
                }
                values.put(key, args[++i]);
            }
            return values;
        }

        private static int parseInt(String rawValue, int defaultValue) {
            try {
                return Integer.parseInt(rawValue);
            } catch (Exception ignored) {
                return defaultValue;
            }
        }
    }

    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger nextId = new AtomicInteger(1);

        private NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, prefix + "-" + nextId.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }

    private static long elapsedMillis(long startedAtNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
    }

    private static long readLong(Object rawValue) {
        if (rawValue instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(rawValue));
        } catch (Exception ignored) {
            return 0L;
        }
    }
}
