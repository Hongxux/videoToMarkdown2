package com.mvp.module2.fusion.grpc;

import com.mvp.videoprocessing.grpc.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Python gRPC 瀹㈡埛绔?
 * 
 * 负责与 Python Worker 通信，调用：
 * 1. DownloadVideo
 * 2. TranscribeVideo
 * 3. ProcessStage1
 * 4. AnalyzeSemanticUnits (Phase2A)
 * 5. AssembleRichText (Phase2B)
 */
@Component
public class PythonGrpcClient {
    
    private static final Logger logger = LoggerFactory.getLogger(PythonGrpcClient.class);
    private static final String GRPC_PROTO_PACKAGE = "com.mvp.videoprocessing.grpc";
    // 做什么：集中声明 Java 侧直接依赖的 protobuf 消息类型。
    // 为什么：启动自检、磁盘校验、单测共享同一份清单，避免以后只补 response 忘补 request。
    private static final List<String> REQUIRED_GRPC_PROTO_MESSAGE_TYPES = List.of(
        "ActionUnitForClassification",
        "ActionUnitForMaterialGeneration",
        "AnalyzeRequest",
        "AnalyzeResponse",
        "AssembleRequest",
        "AssembleResponse",
        "CVValidationRequest",
        "CVValidationResponse",
        "ClipRequest",
        "ClipSegment",
        "DownloadRequest",
        "DownloadResponse",
        "EpisodeInfo",
        "ExtractBookPdfRequest",
        "ExtractBookPdfResponse",
        "GenerateMaterialRequestsRequest",
        "GenerateMaterialRequestsResponse",
        "HealthCheckRequest",
        "HealthCheckResponse",
        "KnowledgeClassificationRequest",
        "KnowledgeClassificationResponse",
        "KnowledgeClassificationResult",
        "ReleaseResourcesRequest",
        "ReleaseResourcesResponse",
        "ScreenshotRequest",
        "SemanticUnitForCV",
        "SemanticUnitForClassification",
        "SemanticUnitForMaterialGeneration",
        "SemanticUnitsInline",
        "SemanticUnitsRef",
        "StableIsland",
        "Stage1Request",
        "Stage1Response",
        "SubtitleForClassification",
        "TranscriptSegment",
        "TranscribeRequest",
        "TranscribeResponse",
        "VLAnalysisRequest",
        "VLAnalysisResponse",
        "VideoInfoRequest",
        "VideoInfoResponse",
        "WatchdogSignalEvent",
        "WatchdogSignalStreamRequest"
    );
    private static final List<String> REQUIRED_GRPC_PROTO_SYNTHETIC_TYPES = List.of(
        "CVValidationRequest$1",
        "ScreenshotRequest$1",
        "TranscribeRequest$1"
    );
    
    @Value("${grpc.python.host:127.0.0.1}")
    private String pythonHost;
    
    @Value("${grpc.python.port:50051}")
    private int pythonPort;
    
    @Value("${grpc.python.timeout-seconds:300}")
    private int defaultTimeoutSeconds;

    @Value("${grpc.python.keepalive.enabled:false}")
    private boolean grpcKeepaliveEnabled;

    @Value("${grpc.python.keepalive-time-seconds:600}")
    private long grpcKeepaliveTimeSeconds;

    @Value("${grpc.python.keepalive-timeout-seconds:20}")
    private long grpcKeepaliveTimeoutSeconds;

    @Value("${grpc.python.keepalive-without-calls:false}")
    private boolean grpcKeepaliveWithoutCalls;

    @Value("${grpc.proto.self-check.auto-heal.enabled:true}")
    private boolean grpcProtoSelfHealEnabled;

    @Value("${grpc.proto.self-check.auto-heal.command:}")
    private String grpcProtoSelfHealCommand;

    @Value("${grpc.proto.self-check.auto-heal.timeout-seconds:300}")
    private long grpcProtoSelfHealTimeoutSeconds;
    
    private ManagedChannel channel;
    private VideoProcessingServiceGrpc.VideoProcessingServiceBlockingStub blockingStub;
    
    @PostConstruct
    public void init() {
        String resolvedHost = resolveGrpcHost(pythonHost);
        logger.info("Initializing Python gRPC client: {}:{} (configuredHost={})", resolvedHost, pythonPort, pythonHost);

        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(resolvedHost, pythonPort)
            .usePlaintext()  // 寮€鍙戠幆澧冧娇鐢ㄦ槑鏂?
            .maxInboundMessageSize(100 * 1024 * 1024);  // 100MB
        if (grpcKeepaliveEnabled) {
            // 按配置启用 gRPC keepalive。默认保持保守档位，避免服务端触发 too_many_pings。
            builder = builder
                .keepAliveTime(grpcKeepaliveTimeSeconds, TimeUnit.SECONDS)
                .keepAliveTimeout(grpcKeepaliveTimeoutSeconds, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(grpcKeepaliveWithoutCalls);
            logger.info(
                "Python gRPC keepalive enabled: time={}s timeout={}s withoutCalls={}",
                grpcKeepaliveTimeSeconds,
                grpcKeepaliveTimeoutSeconds,
                grpcKeepaliveWithoutCalls
            );
        } else {
            logger.info("Python gRPC keepalive disabled to avoid server-side too_many_pings throttling");
        }
        channel = builder.build();

        blockingStub = VideoProcessingServiceGrpc.newBlockingStub(channel);
        verifyGrpcProtoSelfCheckWithAutoHeal();
        
        logger.info("Python gRPC client initialized");
    }

    private void verifyGrpcProtoSelfCheckWithAutoHeal() {
        try {
            runGrpcProtoSelfCheck(false);
            logger.info("gRPC protobuf self-check passed");
            return;
        } catch (Throwable checkError) {
            logger.error("gRPC protobuf self-check failed: {}", rootCauseSummary(checkError), checkError);
            if (!grpcProtoSelfHealEnabled) {
                throw new IllegalStateException("gRPC protobuf self-check failed and auto-heal is disabled", checkError);
            }
            logger.warn("gRPC protobuf auto-heal enabled, trying rebuild workflow");
        }

        boolean healed = runGrpcProtoAutoHealWorkflow();
        if (!healed) {
            throw new IllegalStateException("gRPC protobuf auto-heal failed, startup aborted");
        }

        try {
            runGrpcProtoSelfCheck(true);
            logger.info("gRPC protobuf self-check passed after auto-heal");
        } catch (Throwable healedCheckError) {
            throw new IllegalStateException(
                "gRPC protobuf self-check still failed after auto-heal: " + rootCauseSummary(healedCheckError),
                healedCheckError
            );
        }
    }

    private void runGrpcProtoSelfCheck(boolean verifyDiskLocations) throws Exception {
        ClassLoader loader = PythonGrpcClient.class.getClassLoader();
        Class.forName(GRPC_PROTO_PACKAGE + ".VideoProcessingServiceGrpc", true, loader);
        for (String simpleName : REQUIRED_GRPC_PROTO_MESSAGE_TYPES) {
            Class<?> messageType = Class.forName(GRPC_PROTO_PACKAGE + "." + simpleName, true, loader);
            messageType.getMethod("parser").invoke(null);
        }
        for (String syntheticType : REQUIRED_GRPC_PROTO_SYNTHETIC_TYPES) {
            Class.forName(GRPC_PROTO_PACKAGE + "." + syntheticType, true, loader);
        }

        if (!verifyDiskLocations) {
            return;
        }
        Path repoRoot = resolveRepoRootDirectory();
        verifyGrpcProtoPythonGeneratedFiles(repoRoot);
        verifyGrpcProtoGeneratedFiles(repoRoot);
        verifyGrpcProtoCompiledClasses(repoRoot);
    }

    private boolean runGrpcProtoAutoHealWorkflow() {
        Path repoRoot = resolveRepoRootDirectory();
        String healCommand = resolveGrpcProtoAutoHealCommand(repoRoot);
        if (healCommand.isBlank()) {
            logger.error("gRPC protobuf auto-heal command is empty");
            return false;
        }
        logger.info("Running gRPC protobuf auto-heal command: {}", healCommand);
        boolean commandOk = runShellCommand(healCommand, repoRoot, grpcProtoSelfHealTimeoutSeconds);
        if (!commandOk) {
            return false;
        }
        try {
            verifyGrpcProtoPythonGeneratedFiles(repoRoot);
            verifyGrpcProtoGeneratedFiles(repoRoot);
            verifyGrpcProtoCompiledClasses(repoRoot);
            logger.info("gRPC protobuf auto-heal artifacts verified under expected locations");
            return true;
        } catch (Exception verifyError) {
            logger.error("gRPC protobuf artifact location verification failed: {}", rootCauseSummary(verifyError), verifyError);
            return false;
        }
    }

    private String resolveGrpcProtoAutoHealCommand(Path repoRoot) {
        String manualCommand = grpcProtoSelfHealCommand == null ? "" : grpcProtoSelfHealCommand.trim();
        if (!manualCommand.isEmpty()) {
            return manualCommand;
        }
        Path scriptPath = repoRoot.resolve("scripts").resolve("build").resolve("generate_grpc.ps1");
        Path orchestratorPom = repoRoot.resolve("services").resolve("java-orchestrator").resolve("pom.xml");
        if (Files.exists(scriptPath)) {
            String quotedScript = quoteForShell(scriptPath.toAbsolutePath().toString());
            String quotedPom = quoteForShell(orchestratorPom.toAbsolutePath().toString());
            return "powershell -NoProfile -ExecutionPolicy Bypass -File " + quotedScript
                + " && mvn -f " + quotedPom + " -DskipTests compile -q";
        }
        String quotedPom = quoteForShell(orchestratorPom.toAbsolutePath().toString());
        return "mvn -f " + quotedPom + " -DskipTests compile -q";
    }

    private boolean runShellCommand(String command, Path workDir, long timeoutSeconds) {
        List<String> shellCommand = new ArrayList<>();
        boolean isWindows = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
        if (isWindows) {
            shellCommand.add("cmd");
            shellCommand.add("/c");
            shellCommand.add(command);
        } else {
            shellCommand.add("bash");
            shellCommand.add("-lc");
            shellCommand.add(command);
        }

        ProcessBuilder processBuilder = new ProcessBuilder(shellCommand);
        processBuilder.directory(workDir.toFile());
        processBuilder.redirectErrorStream(true);

        StringBuffer output = new StringBuffer();
        try {
            Process process = processBuilder.start();
            Thread outputDrainThread = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        output.append(line).append(System.lineSeparator());
                    }
                } catch (Exception readError) {
                    output
                        .append("[output-read-error] ")
                        .append(rootCauseSummary(readError))
                        .append(System.lineSeparator());
                }
            }, "grpc-proto-auto-heal-output-drain");
            outputDrainThread.setDaemon(true);
            outputDrainThread.start();

            boolean finished = process.waitFor(Math.max(1L, timeoutSeconds), TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
                outputDrainThread.join(1000);
                logger.error("gRPC protobuf auto-heal command timeout after {}s", timeoutSeconds);
                return false;
            }
            outputDrainThread.join(2000);
            int exitCode = process.exitValue();
            if (exitCode != 0) {
                logger.error("gRPC protobuf auto-heal command failed with exitCode={}, output:\n{}", exitCode, output);
                return false;
            }
            logger.info("gRPC protobuf auto-heal command succeeded");
            if (output.length() > 0) {
                logger.info("gRPC protobuf auto-heal output:\n{}", output);
            }
            return true;
        } catch (InterruptedException interruptedError) {
            Thread.currentThread().interrupt();
            logger.error(
                "gRPC protobuf auto-heal command interrupted: {}",
                rootCauseSummary(interruptedError),
                interruptedError
            );
            return false;
        } catch (Exception execError) {
            logger.error("gRPC protobuf auto-heal command execution failed: {}", rootCauseSummary(execError), execError);
            return false;
        }
    }

    private Path resolveRepoRootDirectory() {
        Path current = Paths.get(System.getProperty("user.dir", ".")).toAbsolutePath().normalize();
        Path cursor = current;
        for (int i = 0; i < 8 && cursor != null; i++) {
            Path protoPath = cursor.resolve("contracts").resolve("proto").resolve("video_processing.proto");
            Path orchestratorPom = cursor.resolve("services").resolve("java-orchestrator").resolve("pom.xml");
            if (Files.exists(protoPath) && Files.exists(orchestratorPom)) {
                return cursor;
            }
            cursor = cursor.getParent();
        }
        throw new IllegalStateException("Cannot resolve repo root from user.dir=" + current);
    }

    private void verifyGrpcProtoGeneratedFiles(Path repoRoot) {
        Path messageGeneratedDir = repoRoot
            .resolve("services")
            .resolve("java-orchestrator")
            .resolve("target")
            .resolve("generated-sources")
            .resolve("protobuf")
            .resolve("java")
            .resolve("com")
            .resolve("mvp")
            .resolve("videoprocessing")
            .resolve("grpc");
        List<String> missingMessageFiles = requiredGrpcProtoGeneratedMessageFiles().stream()
            .filter(name -> !Files.exists(messageGeneratedDir.resolve(name)))
            .collect(Collectors.toList());
        if (!missingMessageFiles.isEmpty()) {
            throw new IllegalStateException(
                "Missing generated protobuf message java files under "
                    + messageGeneratedDir
                    + ": "
                    + missingMessageFiles
            );
        }

        Path grpcGeneratedDir = repoRoot
            .resolve("services")
            .resolve("java-orchestrator")
            .resolve("target")
            .resolve("generated-sources")
            .resolve("protobuf")
            .resolve("grpc-java")
            .resolve("com")
            .resolve("mvp")
            .resolve("videoprocessing")
            .resolve("grpc");
        if (!Files.exists(grpcGeneratedDir.resolve("VideoProcessingServiceGrpc.java"))) {
            throw new IllegalStateException(
                "Missing generated grpc stub java file under "
                    + grpcGeneratedDir
                    + ": [VideoProcessingServiceGrpc.java]"
            );
        }
    }

    private void verifyGrpcProtoPythonGeneratedFiles(Path repoRoot) {
        Path pythonGeneratedDir = repoRoot
            .resolve("contracts")
            .resolve("gen")
            .resolve("python");
        List<String> requiredPythonFiles = List.of(
            "video_processing_pb2.py",
            "video_processing_pb2_grpc.py"
        );
        List<String> missing = requiredPythonFiles.stream()
            .filter(name -> !Files.exists(pythonGeneratedDir.resolve(name)))
            .collect(Collectors.toList());
        if (!missing.isEmpty()) {
            throw new IllegalStateException("Missing generated python grpc files under " + pythonGeneratedDir + ": " + missing);
        }
    }

    private void verifyGrpcProtoCompiledClasses(Path repoRoot) {
        Path classesDir = repoRoot
            .resolve("services")
            .resolve("java-orchestrator")
            .resolve("target")
            .resolve("classes")
            .resolve("com")
            .resolve("mvp")
            .resolve("videoprocessing")
            .resolve("grpc");
        List<String> missing = requiredGrpcProtoCompiledClassFiles().stream()
            .filter(name -> !Files.exists(classesDir.resolve(name)))
            .collect(Collectors.toList());
        if (!missing.isEmpty()) {
            throw new IllegalStateException("Missing compiled protobuf classes under " + classesDir + ": " + missing);
        }
    }

    static List<String> requiredGrpcProtoMessageSimpleNamesForSelfCheck() {
        return REQUIRED_GRPC_PROTO_MESSAGE_TYPES;
    }

    static List<String> requiredGrpcProtoGeneratedMessageFiles() {
        return REQUIRED_GRPC_PROTO_MESSAGE_TYPES.stream()
            .map(simpleName -> simpleName + ".java")
            .collect(Collectors.toList());
    }

    static List<String> requiredGrpcProtoCompiledClassFiles() {
        List<String> requiredClasses = new ArrayList<>();
        for (String simpleName : REQUIRED_GRPC_PROTO_MESSAGE_TYPES) {
            requiredClasses.add(simpleName + ".class");
        }
        for (String syntheticType : REQUIRED_GRPC_PROTO_SYNTHETIC_TYPES) {
            requiredClasses.add(syntheticType + ".class");
        }
        requiredClasses.add("VideoProcessingServiceGrpc.class");
        return requiredClasses;
    }

    private String quoteForShell(String text) {
        return "\"" + text.replace("\"", "\\\"") + "\"";
    }

    private String resolveGrpcHost(String rawHost) {
        String normalized = rawHost != null ? rawHost.trim() : "";
        if (normalized.isEmpty()) {
            return "127.0.0.1";
        }
        String lower = normalized.toLowerCase(Locale.ROOT);
        if ("localhost".equals(lower) || "::1".equals(lower) || "[::1]".equals(lower)) {
            return "127.0.0.1";
        }
        return normalized;
    }
    
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down Python gRPC client");
        if (channel != null) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                channel.shutdownNow();
            }
        }
    }
    
    // ========== Step 1: Download Video ==========
    
    public static class DownloadResult {
        public boolean success;
        public String videoPath;
        public long fileSizeBytes;
        public double durationSec;
        public String errorMsg;
        public String resolvedUrl;
        public String videoTitle;
        public String sourcePlatform;
        public String canonicalId;
        public String linkResolver;
        public String contentType;
    }

    public static class EpisodeInfo {
        public int index;
        public String title;
        public double durationSec;
        public String episodeUrl;
        public String episodeCoverUrl;
    }

    public static class VideoInfoResult {
        public boolean success;
        public String errorMsg;
        public String rawInput;
        public String resolvedUrl;
        public String sourcePlatform;
        public String canonicalId;
        public String videoTitle;
        public double durationSec;
        public boolean isCollection;
        public int totalEpisodes;
        public int currentEpisodeIndex;
        public String currentEpisodeTitle;
        public List<EpisodeInfo> episodes = new ArrayList<>();
        public String linkResolver;
        public String contentType;
        public String coverUrl;
    }
    
    public CompletableFuture<DownloadResult> downloadVideoAsync(
            String taskId, String videoUrl, String outputDir, int timeoutSec) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.info("[{}] Calling DownloadVideo: {}", taskId, videoUrl);
                
                DownloadRequest request = DownloadRequest.newBuilder()
                    .setTaskId(taskId)
                    .setVideoUrl(videoUrl)
                    .setOutputDir(outputDir)
                    .build();
                
                DownloadResponse response = blockingStub
                    .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                    .downloadVideo(request);
                
                DownloadResult result = new DownloadResult();
                result.success = response.getSuccess();
                result.videoPath = response.getVideoPath();
                result.fileSizeBytes = response.getFileSizeBytes();
                result.durationSec = response.getDurationSec();
                result.errorMsg = response.getErrorMsg();
                result.resolvedUrl = response.getResolvedUrl();
                result.videoTitle = response.getVideoTitle();
                result.sourcePlatform = response.getSourcePlatform();
                result.canonicalId = response.getCanonicalId();
                result.linkResolver = response.getLinkResolver();
                result.contentType = response.getContentType();
                
                return result;
                
            } catch (StatusRuntimeException e) {
                logger.error("[{}] DownloadVideo failed: {}", taskId, e.getStatus());
                DownloadResult result = new DownloadResult();
                result.success = false;
                result.errorMsg = statusDescriptionOrCode(e);
                return result;
            }
        });
    }

    public VideoInfoResult getVideoInfo(String taskId, String videoInput, int timeoutSec) {
        try {
            logger.info("[{}] Calling GetVideoInfo: {}", taskId, videoInput);

            VideoInfoRequest request = VideoInfoRequest.newBuilder()
                .setTaskId(taskId)
                .setVideoInput(videoInput != null ? videoInput : "")
                .build();

            VideoInfoResponse response = blockingStub
                .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                .getVideoInfo(request);

            VideoInfoResult result = new VideoInfoResult();
            result.success = response.getSuccess();
            result.errorMsg = response.getErrorMsg();
            result.rawInput = response.getRawInput();
            result.resolvedUrl = response.getResolvedUrl();
            result.sourcePlatform = response.getSourcePlatform();
            result.canonicalId = response.getCanonicalId();
            result.videoTitle = response.getVideoTitle();
            result.durationSec = response.getDurationSec();
            result.isCollection = response.getIsCollection();
            result.totalEpisodes = response.getTotalEpisodes();
            result.currentEpisodeIndex = response.getCurrentEpisodeIndex();
            result.currentEpisodeTitle = response.getCurrentEpisodeTitle();
            result.linkResolver = response.getLinkResolver();
            result.contentType = response.getContentType();
            result.coverUrl = response.getCoverUrl();
            for (com.mvp.videoprocessing.grpc.EpisodeInfo episode : response.getEpisodesList()) {
                EpisodeInfo mapped = new EpisodeInfo();
                mapped.index = episode.getIndex();
                mapped.title = episode.getTitle();
                mapped.durationSec = episode.getDurationSec();
                mapped.episodeUrl = episode.getEpisodeUrl();
                mapped.episodeCoverUrl = episode.getEpisodeCoverUrl();
                result.episodes.add(mapped);
            }
            return result;
        } catch (StatusRuntimeException e) {
            logger.error("[{}] GetVideoInfo failed: {}", taskId, e.getStatus());
            VideoInfoResult result = new VideoInfoResult();
            result.success = false;
            result.errorMsg = statusDescriptionOrCode(e);
            return result;
        }
    }
    
    // ========== Step 2: Transcribe with Whisper ==========
    
    public static class TranscribeResult {
        public boolean success;
        public String subtitlePath;
        public String subtitleText;
        public String errorMsg;
    }
    
    public TranscribeResult transcribeVideo(
            String taskId, String videoPath, String language, int timeoutSec) {
        try {
            logger.info("[{}] Calling TranscribeVideo", taskId);

            TranscribeRequest request = TranscribeRequest.newBuilder()
                .setTaskId(taskId)
                .setVideoPath(videoPath)
                .setLanguage(language)
                .build();

            TranscribeResponse response = blockingStub
                .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                .transcribeVideo(request);

            TranscribeResult result = new TranscribeResult();
            result.success = response.getSuccess();
            result.subtitlePath = response.getSubtitlePath();
            result.subtitleText = response.getSubtitleText();
            result.errorMsg = response.getErrorMsg();
            return result;
        } catch (StatusRuntimeException e) {
            logger.error("[{}] TranscribeVideo failed: {}", taskId, e.getStatus());
            TranscribeResult result = new TranscribeResult();
            result.success = false;
            result.errorMsg = statusDescriptionOrCode(e);
            return result;
        }
    }

    public CompletableFuture<TranscribeResult> transcribeVideoAsync(
            String taskId, String videoPath, String language, int timeoutSec) {
        return CompletableFuture.supplyAsync(
            () -> transcribeVideo(taskId, videoPath, language, timeoutSec)
        );
    }
    
    // ========== Step 3: Stage1 Processing ==========
    
    public static class Stage1Result {
        public boolean success;
        public String step2JsonPath;
        public String step6JsonPath;
        public String sentenceTimestampsPath;
        public String errorMsg;
    }
    
    public Stage1Result processStage1(
            String taskId, String videoPath, String subtitlePath,
            String outputDir, int maxStep, int timeoutSec) {
        try {
            logger.info("[{}] Calling ProcessStage1 (max_step={})", taskId, maxStep);

            Stage1Request request = Stage1Request.newBuilder()
                .setTaskId(taskId)
                .setVideoPath(videoPath)
                .setSubtitlePath(subtitlePath)
                .setOutputDir(outputDir)
                .setMaxStep(maxStep)
                .build();

            Stage1Response response = blockingStub
                .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                .processStage1(request);

            Stage1Result result = new Stage1Result();
            result.success = response.getSuccess();
            result.step2JsonPath = response.getStep2JsonPath();
            result.step6JsonPath = response.getStep6JsonPath();
            result.sentenceTimestampsPath = response.getSentenceTimestampsPath();
            result.errorMsg = response.getErrorMsg();
            return result;
        } catch (StatusRuntimeException e) {
            logger.error("[{}] ProcessStage1 failed: {}", taskId, e.getStatus());
            Stage1Result result = new Stage1Result();
            result.success = false;
            result.errorMsg = statusDescriptionOrCode(e);
            return result;
        }
    }

    public CompletableFuture<Stage1Result> processStage1Async(
            String taskId, String videoPath, String subtitlePath,
            String outputDir, int maxStep, int timeoutSec) {
        return CompletableFuture.supplyAsync(
            () -> processStage1(taskId, videoPath, subtitlePath, outputDir, maxStep, timeoutSec)
        );
    }

    public static class RecoverRuntimeContextResult {
        public boolean success;
        public String resolvedStartStage;
        public boolean downloadReady;
        public String videoPath;
        public double videoDurationSec;
        public String videoTitle;
        public String resolvedUrl;
        public String sourcePlatform;
        public String canonicalId;
        public String contentType;
        public boolean transcribeReady;
        public String subtitlePath;
        public boolean stage1Ready;
        public String step2JsonPath;
        public String step6JsonPath;
        public String sentenceTimestampsPath;
        public boolean phase2aReady;
        public String semanticUnitsPath;
        public boolean phase2bReady;
        public String markdownPath;
        public String jsonPath;
        public int reusedLlmCallCount;
        public int reusedChunkCount;
        public String decisionReason;
        public String errorMsg;
    }

    public RecoverRuntimeContextResult recoverRuntimeContext(
            String taskId,
            String outputDir,
            String requestedStartStage,
            String semanticUnitsPath,
            String requestedVideoPath,
            String requestedSubtitlePath,
            int timeoutSec
    ) {
        try {
            logger.info("[{}] Calling RecoverRuntimeContext (requestedStartStage={})", taskId, requestedStartStage);

            RecoverRuntimeContextRequest request = RecoverRuntimeContextRequest.newBuilder()
                .setTaskId(taskId != null ? taskId : "")
                .setOutputDir(outputDir != null ? outputDir : "")
                .setRequestedStartStage(requestedStartStage != null ? requestedStartStage : "")
                .setSemanticUnitsPath(semanticUnitsPath != null ? semanticUnitsPath : "")
                .setRequestedVideoPath(requestedVideoPath != null ? requestedVideoPath : "")
                .setRequestedSubtitlePath(requestedSubtitlePath != null ? requestedSubtitlePath : "")
                .build();

            RecoverRuntimeContextResponse response = blockingStub
                .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                .recoverRuntimeContext(request);

            RecoverRuntimeContextResult result = new RecoverRuntimeContextResult();
            result.success = response.getSuccess();
            result.resolvedStartStage = response.getResolvedStartStage();
            result.downloadReady = response.getDownloadReady();
            result.videoPath = response.getVideoPath();
            result.videoDurationSec = response.getVideoDurationSec();
            result.videoTitle = response.getVideoTitle();
            result.resolvedUrl = response.getResolvedUrl();
            result.sourcePlatform = response.getSourcePlatform();
            result.canonicalId = response.getCanonicalId();
            result.contentType = response.getContentType();
            result.transcribeReady = response.getTranscribeReady();
            result.subtitlePath = response.getSubtitlePath();
            result.stage1Ready = response.getStage1Ready();
            result.step2JsonPath = response.getStep2JsonPath();
            result.step6JsonPath = response.getStep6JsonPath();
            result.sentenceTimestampsPath = response.getSentenceTimestampsPath();
            result.phase2aReady = response.getPhase2AReady();
            result.semanticUnitsPath = response.getSemanticUnitsPath();
            result.phase2bReady = response.getPhase2BReady();
            result.markdownPath = response.getMarkdownPath();
            result.jsonPath = response.getJsonPath();
            result.reusedLlmCallCount = response.getReusedLlmCallCount();
            result.reusedChunkCount = response.getReusedChunkCount();
            result.decisionReason = response.getDecisionReason();
            result.errorMsg = response.getErrorMsg();
            return result;
        } catch (StatusRuntimeException e) {
            logger.error("[{}] RecoverRuntimeContext failed: {}", taskId, e.getStatus());
            RecoverRuntimeContextResult result = new RecoverRuntimeContextResult();
            result.success = false;
            result.errorMsg = statusDescriptionOrCode(e);
            return result;
        }
    }

    public static class WatchdogSignalProgress {
        public String schema;
        public String source;
        public String taskId;
        public String stage;
        public String status;
        public String checkpoint;
        public int completed;
        public int pending;
        public long seq;
        public long streamSeq;
        public long updatedAtMs;
        public String signalType;
    }

    public static class WatchdogSignalStreamResult {
        public boolean success;
        public boolean unsupported;
        public boolean deadlineExceeded;
        public boolean cancelled;
        public boolean stageTerminal;
        public long lastStreamSeq;
        public String errorMsg;
    }

    public WatchdogSignalStreamResult streamTaskWatchdogSignalsBlocking(
            String taskId,
            String stage,
            long fromStreamSeq,
            int idleTimeoutSec,
            int callTimeoutSec,
            java.util.function.Consumer<WatchdogSignalProgress> signalConsumer) {
        WatchdogSignalStreamResult result = new WatchdogSignalStreamResult();
        result.success = false;
        result.unsupported = false;
        result.deadlineExceeded = false;
        result.cancelled = false;
        result.stageTerminal = false;
        result.lastStreamSeq = Math.max(0L, fromStreamSeq);
        result.errorMsg = "";

        if (taskId == null || taskId.isBlank()) {
            result.errorMsg = "taskId is blank";
            return result;
        }

        int safeIdleTimeoutSec = Math.max(5, idleTimeoutSec);
        int safeCallTimeoutSec = Math.max(2, callTimeoutSec);
        String safeStage = stage == null ? "" : stage.trim();

        try {
            WatchdogSignalStreamRequest request = WatchdogSignalStreamRequest.newBuilder()
                .setTaskId(taskId)
                .setStage(safeStage)
                .setFromStreamSeq(Math.max(0L, fromStreamSeq))
                .setIdleTimeoutSec(safeIdleTimeoutSec)
                .build();

            java.util.Iterator<WatchdogSignalEvent> iterator = blockingStub
                .withDeadlineAfter(safeCallTimeoutSec, TimeUnit.SECONDS)
                .streamTaskWatchdogSignals(request);

            while (iterator.hasNext()) {
                WatchdogSignalEvent event = iterator.next();
                WatchdogSignalProgress progress = new WatchdogSignalProgress();
                progress.schema = event.getSchema();
                progress.source = event.getSource();
                progress.taskId = event.getTaskId();
                progress.stage = event.getStage();
                progress.status = event.getStatus();
                progress.checkpoint = event.getCheckpoint();
                progress.completed = event.getCompleted();
                progress.pending = event.getPending();
                progress.seq = event.getSeq();
                progress.streamSeq = event.getStreamSeq();
                progress.updatedAtMs = event.getUpdatedAtMs();
                progress.signalType = event.getSignalType();

                if (progress.streamSeq > result.lastStreamSeq) {
                    result.lastStreamSeq = progress.streamSeq;
                }
                if (signalConsumer != null) {
                    signalConsumer.accept(progress);
                }

                String normalizedStatus = progress.status == null
                    ? ""
                    : progress.status.trim().toLowerCase(Locale.ROOT);
                if (!safeStage.isBlank() && ("completed".equals(normalizedStatus) || "failed".equals(normalizedStatus))) {
                    result.stageTerminal = true;
                }
            }

            result.success = true;
            return result;
        } catch (StatusRuntimeException statusError) {
            Status.Code code = statusError.getStatus() != null
                ? statusError.getStatus().getCode()
                : Status.Code.UNKNOWN;
            result.errorMsg = statusDescriptionOrCode(statusError);
            result.unsupported = code == Status.Code.UNIMPLEMENTED;
            result.deadlineExceeded = code == Status.Code.DEADLINE_EXCEEDED;
            result.cancelled = code == Status.Code.CANCELLED;
            return result;
        } catch (Exception e) {
            result.errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            return result;
        }
    }
    
    // ========== Step 4: Phase2A Semantic Analysis ==========
    
    public static class ScreenshotRequest {
        public String screenshotId;
        public double timestampSec;
        public String label;
        public String semanticUnitId;
        public String frameReason;
    }

    public static class ClipSegment {
        public double startSec;
        public double endSec;
    }
    
    public static class ClipRequest {
        public String clipId;
        public double startSec;
        public double endSec;
        public String knowledgeType;
        public String semanticUnitId;
        public List<ClipSegment> segments = new ArrayList<>();
    }

    public static class SemanticUnitsRefDTO {
        public String refId;
        public String taskId;
        public String outputDir;
        public int unitCount;
        public String schemaVersion;
        public String fingerprint;
    }

    public static class SemanticUnitsInlineDTO {
        public byte[] payload;
        public String codec;
        public int unitCount;
        public String sha256;
    }
    
    public static class AnalyzeResult {
        public boolean success;
        public List<ScreenshotRequest> screenshotRequests = new ArrayList<>();
        public List<ClipRequest> clipRequests = new ArrayList<>();
        public SemanticUnitsRefDTO semanticUnitsRef;
        public SemanticUnitsInlineDTO semanticUnitsInline;
        public String errorMsg;
    }
    
    public AnalyzeResult analyzeSemanticUnits(
            String taskId, String videoPath, String step2JsonPath,
            String step6JsonPath, String sentenceTimestampsPath,
            String outputDir, int timeoutSec) {
        AnalyzeRequest request = AnalyzeRequest.newBuilder()
            .setTaskId(taskId)
            .setVideoPath(videoPath)
            .setStep2JsonPath(step2JsonPath)
            .setStep6JsonPath(step6JsonPath)
            .setSentenceTimestampsPath(sentenceTimestampsPath)
            .setOutputDir(outputDir)
            .build();
        try {
            logger.info("[{}] Calling AnalyzeSemanticUnits (Phase2A)", taskId);
            AnalyzeResponse response = blockingStub
                .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                .analyzeSemanticUnits(request);
            return toAnalyzeResult(response);
        } catch (StatusRuntimeException e) {
            if (isAnalyzeResponseReadFailure(e)) {
                logger.warn(
                    "[{}] AnalyzeSemanticUnits decode failed, retry once: status={}, cause={}",
                    taskId,
                    e.getStatus(),
                    rootCauseSummary(e)
                );
                try {
                    AnalyzeResponse retryResponse = blockingStub
                        .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                        .analyzeSemanticUnits(request);
                    return toAnalyzeResult(retryResponse);
                } catch (StatusRuntimeException retryError) {
                    logger.error("[{}] AnalyzeSemanticUnits retry failed: {}", taskId, retryError.getStatus());
                    AnalyzeResult result = new AnalyzeResult();
                    result.success = false;
                    result.errorMsg = statusDescriptionOrCode(retryError);
                    return result;
                }
            }
            logger.error("[{}] AnalyzeSemanticUnits failed: {}", taskId, e.getStatus());
            AnalyzeResult result = new AnalyzeResult();
            result.success = false;
            result.errorMsg = statusDescriptionOrCode(e);
            return result;
        }
    }

    private AnalyzeResult toAnalyzeResult(AnalyzeResponse response) {
        AnalyzeResult result = new AnalyzeResult();
        result.success = response.getSuccess();
        if (response.hasSemanticUnitsRef()) {
            com.mvp.videoprocessing.grpc.SemanticUnitsRef ref = response.getSemanticUnitsRef();
            SemanticUnitsRefDTO dto = new SemanticUnitsRefDTO();
            dto.refId = ref.getRefId();
            dto.taskId = ref.getTaskId();
            dto.outputDir = ref.getOutputDir();
            dto.unitCount = ref.getUnitCount();
            dto.schemaVersion = ref.getSchemaVersion();
            dto.fingerprint = ref.getFingerprint();
            result.semanticUnitsRef = dto;
        }
        if (response.hasSemanticUnitsInline()) {
            com.mvp.videoprocessing.grpc.SemanticUnitsInline inline = response.getSemanticUnitsInline();
            SemanticUnitsInlineDTO dto = new SemanticUnitsInlineDTO();
            dto.payload = inline.getPayload().toByteArray();
            dto.codec = inline.getCodec();
            dto.unitCount = inline.getUnitCount();
            dto.sha256 = inline.getSha256();
            result.semanticUnitsInline = dto;
        }
        result.errorMsg = response.getErrorMsg();

        for (com.mvp.videoprocessing.grpc.ScreenshotRequest req : response.getScreenshotRequestsList()) {
            ScreenshotRequest r = new ScreenshotRequest();
            r.screenshotId = req.getScreenshotId();
            r.timestampSec = req.getTimestampSec();
            r.label = req.getLabel();
            r.semanticUnitId = req.getSemanticUnitId();
            r.frameReason = req.getFrameReason();
            result.screenshotRequests.add(r);
        }

        for (com.mvp.videoprocessing.grpc.ClipRequest req : response.getClipRequestsList()) {
            ClipRequest r = new ClipRequest();
            r.clipId = req.getClipId();
            r.startSec = req.getStartSec();
            r.endSec = req.getEndSec();
            r.knowledgeType = req.getKnowledgeType();
            r.semanticUnitId = req.getSemanticUnitId();
            r.segments = buildClipSegments(req.getSegmentsList());
            result.clipRequests.add(r);
        }
        return result;
    }

    public CompletableFuture<AnalyzeResult> analyzeSemanticUnitsAsync(
            String taskId, String videoPath, String step2JsonPath,
            String step6JsonPath, String sentenceTimestampsPath,
            String outputDir, int timeoutSec) {
        return CompletableFuture.supplyAsync(
            () -> analyzeSemanticUnits(
                taskId,
                videoPath,
                step2JsonPath,
                step6JsonPath,
                sentenceTimestampsPath,
                outputDir,
                timeoutSec
            )
        );
    }
    
    // ========== Step 6: Phase2B Rich Text Assembly ==========
    
    public static class AssembleResult {
        public boolean success;
        public String markdownPath;
        public String jsonPath;
        public int totalSections;
        public int videoClipsCount;
        public int screenshotsCount;
        public String errorMsg;
    }
    
    public AssembleResult assembleRichText(
            String taskId, String videoPath, AnalyzeResult analyzeResult,
            String screenshotsDir, String clipsDir, String outputDir,
            String title, int timeoutSec) {
        SemanticUnitsRefDTO ref = analyzeResult != null ? analyzeResult.semanticUnitsRef : null;
        SemanticUnitsInlineDTO inline = analyzeResult != null ? analyzeResult.semanticUnitsInline : null;
        return assembleRichText(
            taskId,
            videoPath,
            ref,
            inline,
            screenshotsDir,
            clipsDir,
            outputDir,
            title,
            timeoutSec
        );
    }

    public CompletableFuture<AssembleResult> assembleRichTextAsync(
            String taskId, String videoPath, AnalyzeResult analyzeResult,
            String screenshotsDir, String clipsDir, String outputDir,
            String title, int timeoutSec) {
        return CompletableFuture.supplyAsync(
            () -> assembleRichText(
                taskId,
                videoPath,
                analyzeResult,
                screenshotsDir,
                clipsDir,
                outputDir,
                title,
                timeoutSec
            )
        );
    }

    private AssembleResult assembleRichText(
            String taskId, String videoPath,
            SemanticUnitsRefDTO semanticUnitsRef,
            SemanticUnitsInlineDTO semanticUnitsInline,
            String screenshotsDir, String clipsDir, String outputDir,
            String title, int timeoutSec) {
        try {
            logger.info("[{}] Calling AssembleRichText (Phase2B)", taskId);

            AssembleRequest request = AssembleRequest.newBuilder()
                .setTaskId(taskId)
                .setVideoPath(videoPath)
                .setScreenshotsDir(screenshotsDir)
                .setClipsDir(clipsDir)
                .setOutputDir(outputDir)
                .setTitle(title)
                .build();
            AssembleRequest.Builder requestBuilder = request.toBuilder();
            if (semanticUnitsInline != null
                && semanticUnitsInline.payload != null
                && semanticUnitsInline.payload.length > 0) {
                requestBuilder.setSemanticUnitsInline(
                    com.mvp.videoprocessing.grpc.SemanticUnitsInline.newBuilder()
                        .setPayload(ByteString.copyFrom(semanticUnitsInline.payload))
                        .setCodec(semanticUnitsInline.codec != null ? semanticUnitsInline.codec : "")
                        .setUnitCount(semanticUnitsInline.unitCount)
                        .setSha256(semanticUnitsInline.sha256 != null ? semanticUnitsInline.sha256 : "")
                        .build()
                );
            } else if (semanticUnitsRef != null
                && semanticUnitsRef.refId != null
                && !semanticUnitsRef.refId.isBlank()) {
                requestBuilder.setSemanticUnitsRef(
                    com.mvp.videoprocessing.grpc.SemanticUnitsRef.newBuilder()
                        .setRefId(semanticUnitsRef.refId)
                        .setTaskId(semanticUnitsRef.taskId != null ? semanticUnitsRef.taskId : "")
                        .setOutputDir(semanticUnitsRef.outputDir != null ? semanticUnitsRef.outputDir : "")
                        .setUnitCount(semanticUnitsRef.unitCount)
                        .setSchemaVersion(semanticUnitsRef.schemaVersion != null ? semanticUnitsRef.schemaVersion : "")
                        .setFingerprint(semanticUnitsRef.fingerprint != null ? semanticUnitsRef.fingerprint : "")
                        .build()
                );
            }

            AssembleResponse response = blockingStub
                .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                .assembleRichText(requestBuilder.build());

            AssembleResult result = new AssembleResult();
            result.success = response.getSuccess();
            result.markdownPath = response.getMarkdownPath();
            result.jsonPath = response.getJsonPath();
            result.errorMsg = response.getErrorMsg();

            if (response.hasStats()) {
                AssembleStats stats = response.getStats();
                result.totalSections = stats.getTotalSections();
                result.videoClipsCount = stats.getVideoClipsCount();
                result.screenshotsCount = stats.getScreenshotsCount();
            }

            return result;
        } catch (StatusRuntimeException e) {
            logger.error("[{}] AssembleRichText failed: {}", taskId, e.getStatus());
            AssembleResult result = new AssembleResult();
            result.success = false;
            result.errorMsg = statusDescriptionOrCode(e);
            return result;
        }
    }
    
    // ========== 鍋ュ悍妫€鏌?==========
    
    public static class ExtractBookPdfResult {
        public boolean success;
        public String markdown;
        public String markdownPath;
        public String extractor;
        public int imageCount;
        public int tableCount;
        public int codeBlockCount;
        public int formulaBlockCount;
        public String errorMsg;
        public List<String> imagePaths = new ArrayList<>();
    }

    public ExtractBookPdfResult extractBookPdf(
            String taskId,
            String pdfPath,
            String outputDir,
            int startPage,
            int endPage,
            String imageDir,
            String outputRoot,
            String sectionId,
            boolean preferMineru,
            int timeoutSec
    ) {
        try {
            logger.info(
                    "[{}] Calling ExtractBookPdf, pages={}~{}, sectionId={}, preferMineru={}",
                    taskId,
                    startPage,
                    endPage,
                    sectionId,
                    preferMineru
            );
            ExtractBookPdfRequest request = ExtractBookPdfRequest.newBuilder()
                    .setTaskId(taskId != null ? taskId : "")
                    .setPdfPath(pdfPath != null ? pdfPath : "")
                    .setOutputDir(outputDir != null ? outputDir : "")
                    .setStartPage(Math.max(1, startPage))
                    .setEndPage(Math.max(1, endPage))
                    .setImageDir(imageDir != null ? imageDir : "")
                    .setOutputRoot(outputRoot != null ? outputRoot : "")
                    .setSectionId(sectionId != null ? sectionId : "")
                    .setPreferMineru(preferMineru)
                    .build();

            ExtractBookPdfResponse response = blockingStub
                    .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                    .extractBookPdf(request);

            ExtractBookPdfResult result = new ExtractBookPdfResult();
            result.success = response.getSuccess();
            result.markdown = response.getMarkdown();
            result.markdownPath = response.getMarkdownPath();
            result.extractor = response.getExtractor();
            result.imageCount = response.getImageCount();
            result.tableCount = response.getTableCount();
            result.codeBlockCount = response.getCodeBlockCount();
            result.formulaBlockCount = response.getFormulaBlockCount();
            result.errorMsg = response.getErrorMsg();
            result.imagePaths = new ArrayList<>(response.getImagePathsList());
            return result;
        } catch (StatusRuntimeException e) {
            logger.error("[{}] ExtractBookPdf failed: {}", taskId, e.getStatus());
            ExtractBookPdfResult result = new ExtractBookPdfResult();
            result.success = false;
            result.errorMsg = statusDescriptionOrCode(e);
            return result;
        }
    }

    public boolean healthCheck() {
        try {
            if (blockingStub == null) return false;
            
            HealthCheckResponse response = blockingStub
                .withDeadlineAfter(300, TimeUnit.SECONDS)
                .healthCheck(HealthCheckRequest.newBuilder().build());
            
            return response.getHealthy();
        } catch (Exception e) {
            logger.warn("Health check failed: {}", e.getMessage());
            return false;
        }
    }
    
    // ========== V3: Batch CV Validation ==========
    
    public static class StableIslandResult {
        public double startSec;
        public double endSec;
        public double midSec;
        public double durationSec;
    }
    
    public static class ActionSegmentResult {
        public int id;
        public double startSec;
        public double endSec;
        public String actionType;
        public List<StableIslandResult> internalStableIslands = new ArrayList<>();
    }
    
    public static class CVValidationUnitResult {
        public String unitId;
        public List<StableIslandResult> stableIslands = new ArrayList<>();
        public List<ActionSegmentResult> actionSegments = new ArrayList<>();
    }
    
    public static class CVBatchResult {
        public boolean success;
        public List<CVValidationUnitResult> results = new ArrayList<>();
        public String errorMsg;
    }
    
    public static class SemanticUnitInput {
        public String unitId;
        public double startSec;
        public double endSec;
        public String knowledgeType;
        public String title;
        public String text;
    }
    
    /**
     * Batch CV validation with streaming callbacks.
     *
     * @param taskId task id
     * @param videoPath video path
     * @param units semantic unit inputs
     * @param timeoutSec timeout in seconds
     * @param resultConsumer callback invoked after each unit finishes
     * @return whether the streaming task started successfully
     */
    public boolean validateCVBatchStreamingBlocking(
            String taskId, String videoPath, List<SemanticUnitInput> units, int timeoutSec,
            java.util.function.Consumer<CVValidationUnitResult> resultConsumer) {

        try {
            logger.info("[{}] ValidateCVBatch Streaming: {} units", taskId, units.size());

            CVValidationRequest.Builder requestBuilder = CVValidationRequest.newBuilder()
                .setTaskId(taskId)
                .setVideoPath(videoPath);

            for (SemanticUnitInput unit : units) {
                requestBuilder.addSemanticUnits(
                    SemanticUnitForCV.newBuilder()
                        .setUnitId(unit.unitId)
                        .setStartSec(unit.startSec)
                        .setEndSec(unit.endSec)
                        .setKnowledgeType(unit.knowledgeType != null ? unit.knowledgeType : "")
                        .build()
                );
            }

            java.util.Iterator<CVValidationResponse> responseIterator = blockingStub
                .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                .validateCVBatch(requestBuilder.build());

            while (responseIterator.hasNext()) {
                CVValidationResponse response = responseIterator.next();
                if (!response.getSuccess()) {
                    logger.error("[{}] Streaming response error: {}", taskId, response.getErrorMsg());
                    continue;
                }

                for (com.mvp.videoprocessing.grpc.CVValidationResult pbResult : response.getResultsList()) {
                    CVValidationUnitResult unitResult = new CVValidationUnitResult();
                    unitResult.unitId = pbResult.getUnitId();

                    for (com.mvp.videoprocessing.grpc.StableIsland si : pbResult.getStableIslandsList()) {
                        StableIslandResult islandResult = new StableIslandResult();
                        islandResult.startSec = si.getStartSec();
                        islandResult.endSec = si.getEndSec();
                        islandResult.midSec = si.getMidSec();
                        islandResult.durationSec = si.getDurationSec();
                        unitResult.stableIslands.add(islandResult);
                    }

                    for (com.mvp.videoprocessing.grpc.ActionSegment as : pbResult.getActionSegmentsList()) {
                        ActionSegmentResult segResult = new ActionSegmentResult();
                        segResult.startSec = as.getStartSec();
                        segResult.endSec = as.getEndSec();
                        segResult.actionType = as.getActionType();

                        for (com.mvp.videoprocessing.grpc.StableIsland internalSi : as.getInternalStableIslandsList()) {
                            StableIslandResult internalIsland = new StableIslandResult();
                            internalIsland.startSec = internalSi.getStartSec();
                            internalIsland.endSec = internalSi.getEndSec();
                            internalIsland.midSec = internalSi.getMidSec();
                            internalIsland.durationSec = internalSi.getDurationSec();
                            segResult.internalStableIslands.add(internalIsland);
                        }
                        unitResult.actionSegments.add(segResult);
                    }

                    if (resultConsumer != null) {
                        resultConsumer.accept(unitResult);
                    }
                }
            }

            logger.info("[{}] ValidateCVBatch Streaming completed", taskId);
            return true;
        } catch (LinkageError linkageError) {
            logger.error(
                "[{}] ValidateCVBatch Streaming linkage failed: {}",
                taskId,
                rootCauseSummary(linkageError),
                linkageError
            );
            return false;
        } catch (Exception e) {
            logger.error("[{}] ValidateCVBatch Streaming failed: {}", taskId, e.getMessage());
            return false;
        }
    }

    public CompletableFuture<Boolean> validateCVBatchStreaming(
            String taskId, String videoPath, List<SemanticUnitInput> units, int timeoutSec,
            java.util.function.Consumer<CVValidationUnitResult> resultConsumer) {
        return CompletableFuture.supplyAsync(
            () -> validateCVBatchStreamingBlocking(taskId, videoPath, units, timeoutSec, resultConsumer)
        );
    }

    // ========== 馃殌 V3: Phase2A - Step 2: Knowledge Classification ==========
    
    public static class ClassificationInput {
        public String unitId;
        public String title;
        public String text;
        public List<ActionSegmentResult> actionUnits = new ArrayList<>(); // Reusing ActionSegmentResult logic for ID and times
        public List<SubtitleItem> subtitles = new ArrayList<>();
    }
    
    public static class SubtitleItem {
        public double startSec;
        public double endSec;
        public String text;
    }

    public static class KnowledgeResultItem {
        public String unitId;
        public int actionId;
        public String knowledgeType;
        public double confidence;
        public String keyEvidence;
        public String reasoning;
    }
    
    public static class ClassificationBatchResult {
         public boolean success;
         public List<KnowledgeResultItem> results = new ArrayList<>();
         public String errorMsg;
    }
    
    public ClassificationBatchResult classifyKnowledgeBatch(
            String taskId, List<ClassificationInput> units, String step2Path, int timeoutSec) {
        try {
            logger.info("[{}] ClassifyKnowledgeBatch: {} units", taskId, units.size());

            KnowledgeClassificationRequest.Builder requestBuilder = KnowledgeClassificationRequest.newBuilder()
                .setTaskId(taskId)
                .setStep2Path(step2Path != null ? step2Path : "");

            for (ClassificationInput unit : units) {
                SemanticUnitForClassification.Builder unitBuilder = SemanticUnitForClassification.newBuilder()
                    .setUnitId(unit.unitId)
                    .setTitle(unit.title != null ? unit.title : "")
                    .setText(unit.text != null ? unit.text : "");

                for (ActionSegmentResult as : unit.actionUnits) {
                    unitBuilder.addActionUnits(
                        ActionUnitForClassification.newBuilder()
                            .setId(as.id)
                            .setStartSec(as.startSec)
                            .setEndSec(as.endSec)
                            .build()
                    );
                }

                requestBuilder.addUnits(unitBuilder.build());
            }

            KnowledgeClassificationResponse response = blockingStub
                .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                .classifyKnowledgeBatch(requestBuilder.build());

            ClassificationBatchResult result = new ClassificationBatchResult();
            result.success = response.getSuccess();
            result.errorMsg = response.getErrorMsg();

            for (KnowledgeClassificationResult r : response.getResultsList()) {
                KnowledgeResultItem item = new KnowledgeResultItem();
                item.unitId = r.getUnitId();
                item.actionId = r.getActionId();
                item.knowledgeType = r.getKnowledgeType();
                item.confidence = r.getConfidence();
                item.keyEvidence = r.getKeyEvidence();
                item.reasoning = r.getReasoning();
                result.results.add(item);
            }

            return result;
        } catch (StatusRuntimeException e) {
            logger.error("[{}] ClassifyKnowledgeBatch failed: {}", taskId, e.getStatus());
            ClassificationBatchResult result = new ClassificationBatchResult();
            result.success = false;
            result.errorMsg = statusDescriptionOrCode(e);
            return result;
        }
    }

    public CompletableFuture<ClassificationBatchResult> classifyKnowledgeBatchAsync(
            String taskId, List<ClassificationInput> units, String step2Path, int timeoutSec) {
        return CompletableFuture.supplyAsync(
            () -> classifyKnowledgeBatch(taskId, units, step2Path, timeoutSec)
        );
    }

    // ========== 馃殌 V3: Phase2A - Step 3: Material Request Generation ==========

    public static class MaterialGenerationInput {
        public String unitId;
        public String knowledgeType;
        public double startSec;
        public double endSec;
        public String fullText;
        public List<ActionSegmentResult> actionUnits = new ArrayList<>();
        public List<StableIslandResult> stableIslands = new ArrayList<>(); // Stable islands extracted from CV results.
    }
    
    public static class ScreenshotRequestDTO {
        public String screenshotId;
        public double timestampSec;
        public String label;
        public String semanticUnitId;
        public String frameReason;
    }

    public static class ClipRequestDTO {
        public String clipId;
        public double startSec;
        public double endSec;
        public String knowledgeType;
        public String semanticUnitId;
        public List<ClipSegment> segments = new ArrayList<>();
    }

    public static class MaterialGenerationResult {
        public boolean success;
        public List<ScreenshotRequestDTO> screenshotRequests = new ArrayList<>();
        public List<ClipRequestDTO> clipRequests = new ArrayList<>();
        public String errorMsg;
    }

    public MaterialGenerationResult generateMaterialRequests(
            String taskId, List<MaterialGenerationInput> units, String videoPath, int timeoutSec) {
        return generateMaterialRequests(taskId, units, videoPath, "", timeoutSec);
    }

    public MaterialGenerationResult generateMaterialRequests(
            String taskId, List<MaterialGenerationInput> units, String videoPath, String outputDir, int timeoutSec) {
         try {
            logger.info("[{}] GenerateMaterialRequests: {} units", taskId, units.size());

            GenerateMaterialRequestsRequest.Builder requestBuilder = GenerateMaterialRequestsRequest.newBuilder()
                .setTaskId(taskId)
                .setVideoPath(videoPath)
                .setOutputDir(outputDir != null ? outputDir : "");

            for (MaterialGenerationInput unit : units) {
                SemanticUnitForMaterialGeneration.Builder unitBuilder = SemanticUnitForMaterialGeneration.newBuilder()
                        .setUnitId(unit.unitId)
                        .setKnowledgeType(unit.knowledgeType != null ? unit.knowledgeType : "")
                        .setStartSec(unit.startSec)
                        .setEndSec(unit.endSec)
                        .setFullText(unit.fullText != null ? unit.fullText : "");

                if (unit.actionUnits != null) {
                    final Set<String> coarseUnitTypes = new HashSet<>(Arrays.asList(
                        "abstract", "process", "concrete", "configuration", "deduction", "practical", "scan", "scanning"
                    ));
                    final String unitKt = unit.knowledgeType != null ? unit.knowledgeType.trim() : "";
                    int missingCnt = 0;
                    int cvLikeCnt = 0;
                    int defaultLikeCnt = 0;
                    String example = "";

                    for (ActionSegmentResult as : unit.actionUnits) {
                        final String kt = as.actionType != null ? as.actionType.trim() : "";
                        final String ktLower = kt.toLowerCase(Locale.ROOT);
                        final boolean isMissing = kt.isEmpty()
                            || "unknown".equals(ktLower) || "knowledge".equals(ktLower)
                            || "none".equals(ktLower) || "null".equals(ktLower);
                        final boolean isCvLike = ktLower.matches("^k\\d+_.*")
                            || ktLower.contains("operation") || ktLower.contains("click")
                            || ktLower.contains("drag") || ktLower.contains("scroll")
                            || ktLower.contains("mouse") || ktLower.contains("keyboard");
                        final boolean isDefaultLike = !unitKt.isEmpty()
                            && kt.equals(unitKt)
                            && coarseUnitTypes.contains(unitKt.toLowerCase(Locale.ROOT));

                        if (isMissing) {
                            missingCnt++;
                        } else if (isCvLike) {
                            cvLikeCnt++;
                        } else if (isDefaultLike) {
                            defaultLikeCnt++;
                        }

                        if ((isMissing || isCvLike || isDefaultLike) && example.isEmpty()) {
                            example = String.format(
                                "action_id=%d, kt=%s, start=%.2f, end=%.2f",
                                as.id, kt, as.startSec, as.endSec
                            );
                        }
                        unitBuilder.addActionUnits(
                            ActionUnitForMaterialGeneration.newBuilder()
                                .setId(as.id)
                                .setStartSec(as.startSec)
                                .setEndSec(as.endSec)
                                .setKnowledgeType(kt)
                                .build()
                        );
                    }

                    if (missingCnt > 0 || cvLikeCnt > 0 || defaultLikeCnt > 0) {
                        logger.warn(
                            "[{}] 涓婃父 knowledge_type 缂哄け/鐤戜技 CV actionType: unit={}, actions={}, missing={}, cv_like={}, default_like={}, unit_kt={}, example=({})",
                            taskId, unit.unitId, unit.actionUnits.size(), missingCnt, cvLikeCnt, defaultLikeCnt, unitKt, example
                        );
                    }
                }

                if (unit.stableIslands != null) {
                    for (StableIslandResult si : unit.stableIslands) {
                        unitBuilder.addStableIslands(
                            com.mvp.videoprocessing.grpc.StableIsland.newBuilder()
                                .setStartSec(si.startSec)
                                .setEndSec(si.endSec)
                                .setMidSec(si.midSec)
                                .setDurationSec(si.durationSec)
                                .build()
                        );
                    }
                }

                requestBuilder.addUnits(unitBuilder.build());
            }

            GenerateMaterialRequestsResponse response = blockingStub
                .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                .generateMaterialRequests(requestBuilder.build());

            MaterialGenerationResult result = new MaterialGenerationResult();
            result.success = response.getSuccess();
            result.errorMsg = response.getErrorMsg();

             for (com.mvp.videoprocessing.grpc.ScreenshotRequest req : response.getScreenshotRequestsList()) {
                ScreenshotRequestDTO r = new ScreenshotRequestDTO();
                r.screenshotId = req.getScreenshotId();
                r.timestampSec = req.getTimestampSec();
                r.label = req.getLabel();
                r.semanticUnitId = req.getSemanticUnitId();
                r.frameReason = req.getFrameReason();
                result.screenshotRequests.add(r);
            }

            for (com.mvp.videoprocessing.grpc.ClipRequest req : response.getClipRequestsList()) {
                ClipRequestDTO r = new ClipRequestDTO();
                r.clipId = req.getClipId();
                r.startSec = req.getStartSec();
                r.endSec = req.getEndSec();
                r.knowledgeType = req.getKnowledgeType();
                r.semanticUnitId = req.getSemanticUnitId();
                r.segments = buildClipSegments(req.getSegmentsList());
                result.clipRequests.add(r);
            }

            return result;
         } catch (StatusRuntimeException e) {
            logger.error("[{}] GenerateMaterialRequests failed: {}", taskId, e.getStatus());
            MaterialGenerationResult result = new MaterialGenerationResult();
            result.success = false;
            result.errorMsg = statusDescriptionOrCode(e);
            return result;
        }
    }

    public CompletableFuture<MaterialGenerationResult> generateMaterialRequestsAsync(
            String taskId, List<MaterialGenerationInput> units, String videoPath, int timeoutSec) {
        return generateMaterialRequestsAsync(taskId, units, videoPath, "", timeoutSec);
    }

    public CompletableFuture<MaterialGenerationResult> generateMaterialRequestsAsync(
            String taskId, List<MaterialGenerationInput> units, String videoPath, String outputDir, int timeoutSec) {
        return CompletableFuture.supplyAsync(
            () -> generateMaterialRequests(taskId, units, videoPath, outputDir, timeoutSec)
        );
    }


    // ========== 馃敟 V7: VL-Based Analysis ==========
    
    public static class VLAnalysisResult {
        public boolean success;
        public boolean vlEnabled;
        public boolean usedFallback;
        public boolean interrupted;
        public List<ScreenshotRequestDTO> screenshotRequests = new ArrayList<>();
        public List<ClipRequestDTO> clipRequests = new ArrayList<>();
        public int unitsAnalyzed;
        public int vlClipsGenerated;
        public int vlScreenshotsGenerated;
        public String errorMsg;
    }
    
    /**
     * V7: VL-based analysis using a multimodal model directly on the video.
     *
     * This path bypasses the legacy CV/LLM chain and consumes AnalyzeSemanticUnits output directly.
     *
     * @param taskId task id
     * @param videoPath video path
     * @param analyzeResult AnalyzeSemanticUnits result
     * @param outputDir output directory
     * @param timeoutSec timeout in seconds
     * @return CompletableFuture<VLAnalysisResult>
     */
    public VLAnalysisResult analyzeWithVL(
            String taskId, String videoPath, AnalyzeResult analyzeResult,
            String outputDir, int timeoutSec) {
        try {
            logger.info("[{}] Calling AnalyzeWithVL: {}", taskId, videoPath);

            VLAnalysisRequest.Builder requestBuilder = VLAnalysisRequest.newBuilder()
                .setTaskId(taskId)
                .setVideoPath(videoPath)
                .setOutputDir(outputDir);
            if (analyzeResult != null
                && analyzeResult.semanticUnitsInline != null
                && analyzeResult.semanticUnitsInline.payload != null
                && analyzeResult.semanticUnitsInline.payload.length > 0) {
                SemanticUnitsInlineDTO inline = analyzeResult.semanticUnitsInline;
                requestBuilder.setSemanticUnitsInline(
                    com.mvp.videoprocessing.grpc.SemanticUnitsInline.newBuilder()
                        .setPayload(ByteString.copyFrom(inline.payload))
                        .setCodec(inline.codec != null ? inline.codec : "")
                        .setUnitCount(inline.unitCount)
                        .setSha256(inline.sha256 != null ? inline.sha256 : "")
                        .build()
                );
            } else if (analyzeResult != null
                && analyzeResult.semanticUnitsRef != null
                && analyzeResult.semanticUnitsRef.refId != null
                && !analyzeResult.semanticUnitsRef.refId.isBlank()) {
                SemanticUnitsRefDTO ref = analyzeResult.semanticUnitsRef;
                requestBuilder.setSemanticUnitsRef(
                    com.mvp.videoprocessing.grpc.SemanticUnitsRef.newBuilder()
                        .setRefId(ref.refId)
                        .setTaskId(ref.taskId != null ? ref.taskId : "")
                        .setOutputDir(ref.outputDir != null ? ref.outputDir : "")
                        .setUnitCount(ref.unitCount)
                        .setSchemaVersion(ref.schemaVersion != null ? ref.schemaVersion : "")
                        .setFingerprint(ref.fingerprint != null ? ref.fingerprint : "")
                        .build()
                );
            }

            VLAnalysisResponse response = blockingStub
                .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                .analyzeWithVL(requestBuilder.build());

            VLAnalysisResult result = new VLAnalysisResult();
            result.success = response.getSuccess();
            result.vlEnabled = response.getVlEnabled();
            result.usedFallback = response.getUsedFallback();
            result.interrupted = false;
            result.unitsAnalyzed = response.getUnitsAnalyzed();
            result.vlClipsGenerated = response.getVlClipsGenerated();
            result.vlScreenshotsGenerated = response.getVlScreenshotsGenerated();
            result.errorMsg = response.getErrorMsg();

            for (com.mvp.videoprocessing.grpc.ScreenshotRequest req : response.getScreenshotRequestsList()) {
                ScreenshotRequestDTO r = new ScreenshotRequestDTO();
                r.screenshotId = req.getScreenshotId();
                r.timestampSec = req.getTimestampSec();
                r.label = req.getLabel();
                r.semanticUnitId = req.getSemanticUnitId();
                r.frameReason = req.getFrameReason();
                result.screenshotRequests.add(r);
            }

            for (com.mvp.videoprocessing.grpc.ClipRequest req : response.getClipRequestsList()) {
                ClipRequestDTO r = new ClipRequestDTO();
                r.clipId = req.getClipId();
                r.startSec = req.getStartSec();
                r.endSec = req.getEndSec();
                r.knowledgeType = req.getKnowledgeType();
                r.semanticUnitId = req.getSemanticUnitId();
                r.segments = buildClipSegments(req.getSegmentsList());
                result.clipRequests.add(r);
            }

            logger.info("[{}] AnalyzeWithVL completed: vlEnabled={}, usedFallback={}, screenshots={}, clips={}",
                taskId, result.vlEnabled, result.usedFallback,
                result.screenshotRequests.size(), result.clipRequests.size());

            return result;
        } catch (StatusRuntimeException e) {
            boolean interrupted = isInterruptedCancellation(e);
            boolean protoClasspathFailure = isGrpcProtoClasspathFailure(e);
            if (interrupted) {
                Thread.currentThread().interrupt();
                logger.warn("[{}] AnalyzeWithVL interrupted: {}", taskId, e.getStatus());
            } else if (protoClasspathFailure) {
                logger.error("[{}] AnalyzeWithVL failed due to protobuf classpath mismatch: {}", taskId, rootCauseSummary(e), e);
            } else {
                logger.error("[{}] AnalyzeWithVL failed: {}", taskId, e.getStatus());
            }
            VLAnalysisResult result = new VLAnalysisResult();
            result.success = false;
            result.interrupted = interrupted;
            if (protoClasspathFailure) {
                result.errorMsg = buildGrpcProtoClasspathFixHint(e);
            } else {
                result.errorMsg = statusDescriptionOrCode(e);
            }
            return result;
        } catch (LinkageError linkageError) {
            logger.error("[{}] AnalyzeWithVL linkage failed: {}", taskId, rootCauseSummary(linkageError), linkageError);
            VLAnalysisResult result = new VLAnalysisResult();
            result.success = false;
            result.interrupted = false;
            result.errorMsg = buildGrpcProtoClasspathFixHint(linkageError);
            return result;
        }
    }

    public CompletableFuture<VLAnalysisResult> analyzeWithVLAsync(
            String taskId, String videoPath, AnalyzeResult analyzeResult,
            String outputDir, int timeoutSec) {
        return CompletableFuture.supplyAsync(
            () -> analyzeWithVL(taskId, videoPath, analyzeResult, outputDir, timeoutSec)
        );
    }

    // ========== 馃殌 V6: 璧勬簮閲婃斁 ==========
    
    public static class ReleaseResourcesResult {
        public boolean success;
        public String message;
        public int freedWorkersCount;
        public float freedMemoryMb;
    }

    public ReleaseResourcesResult releaseCVResources(String taskId) {
        try {
            logger.info("[{}] Calling ReleaseCVResources", taskId);

            ReleaseResourcesRequest request = ReleaseResourcesRequest.newBuilder()
                .setTaskId(taskId != null ? taskId : "")
                .build();

            ReleaseResourcesResponse response = blockingStub
                .withDeadlineAfter(5, TimeUnit.SECONDS)
                .releaseCVResources(request);

            ReleaseResourcesResult result = new ReleaseResourcesResult();
            result.success = response.getSuccess();
            result.message = response.getMessage();
            result.freedWorkersCount = response.getFreedWorkersCount();
            result.freedMemoryMb = response.getFreedMemoryMb();
            logger.info("[{}] ReleaseCVResources result: {}", taskId, result.message);
            return result;

        } catch (StatusRuntimeException e) {
            logger.error("[{}] ReleaseCVResources failed: {}", taskId, e.getStatus());
            ReleaseResourcesResult result = new ReleaseResourcesResult();
            result.success = false;
            result.message = statusDescriptionOrCode(e);
            return result;
        }
    }
    
    public CompletableFuture<ReleaseResourcesResult> releaseCVResourcesAsync(String taskId) {
        return CompletableFuture.supplyAsync(() -> releaseCVResources(taskId));
    }

    private String statusDescriptionOrCode(StatusRuntimeException e) {
        if (e == null || e.getStatus() == null) {
            return "gRPC call failed with unknown status";
        }
        String description = e.getStatus().getDescription();
        if (description != null && !description.isBlank()) {
            return description;
        }
        return "gRPC status=" + e.getStatus().getCode().name();
    }

    private boolean isInterruptedCancellation(StatusRuntimeException error) {
        if (error == null || error.getStatus() == null) {
            return false;
        }
        if (error.getStatus().getCode() != Status.Code.CANCELLED) {
            return false;
        }
        Throwable cause = error.getCause();
        while (cause != null) {
            if (cause instanceof InterruptedException) {
                return true;
            }
            cause = cause.getCause();
        }
        String description = error.getStatus().getDescription();
        if (description == null || description.isBlank()) {
            return false;
        }
        String normalized = description.toLowerCase(Locale.ROOT);
        return normalized.contains("interrupted");
    }

    private boolean isAnalyzeResponseReadFailure(StatusRuntimeException error) {
        if (error == null || error.getStatus() == null) {
            return false;
        }
        if (error.getStatus().getCode() != Status.Code.CANCELLED) {
            return false;
        }
        String description = error.getStatus().getDescription();
        if (description == null || description.isBlank()) {
            return false;
        }
        String normalized = description.toLowerCase(Locale.ROOT);
        if (!normalized.contains("failed to read message")) {
            return false;
        }
        return isGrpcProtoClasspathFailure(error);
    }

    private boolean isGrpcProtoClasspathFailure(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            if (cause instanceof NoClassDefFoundError || cause instanceof ClassNotFoundException || cause instanceof LinkageError) {
                String message = String.valueOf(cause.getMessage());
                if (message.contains("com/mvp/videoprocessing/grpc") || message.contains("com.mvp.videoprocessing.grpc")) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }

    private String buildGrpcProtoClasspathFixHint(Throwable error) {
        return "gRPC protobuf classpath mismatch: "
            + rootCauseSummary(error)
            + ". Please run grpc rebuild script and compile java orchestrator, then restart service.";
    }

    private String rootCauseSummary(Throwable error) {
        if (error == null) {
            return "none";
        }
        Throwable cause = error;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        if (message == null || message.isBlank()) {
            return cause.getClass().getSimpleName();
        }
        return cause.getClass().getSimpleName() + ": " + message;
    }

    private List<ClipSegment> buildClipSegments(List<com.mvp.videoprocessing.grpc.ClipSegment> segments) {
        List<ClipSegment> results = new ArrayList<>();
        if (segments == null || segments.isEmpty()) {
            return results;
        }
        for (com.mvp.videoprocessing.grpc.ClipSegment seg : segments) {
            ClipSegment out = new ClipSegment();
            out.startSec = seg.getStartSec();
            out.endSec = seg.getEndSec();
            results.add(out);
        }
        return results;
    }

}
