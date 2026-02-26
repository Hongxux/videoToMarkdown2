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
 * Python gRPC 客户端
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
    
    @Value("${grpc.python.host:localhost}")
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
    
    private ManagedChannel channel;
    private VideoProcessingServiceGrpc.VideoProcessingServiceBlockingStub blockingStub;
    
    @PostConstruct
    public void init() {
        logger.info("Initializing Python gRPC client: {}:{}", pythonHost, pythonPort);

        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(pythonHost, pythonPort)
            .usePlaintext()  // 开发环境使用明文
            .maxInboundMessageSize(100 * 1024 * 1024);  // 100MB
        if (grpcKeepaliveEnabled) {
            // 做什么：按配置启用 gRPC keepalive。
            // 为什么：部分网络拓扑（跨网段/NAT）可能需要保活，但默认关闭以避免触发服务端 too_many_pings。
            // 权衡：开启后若频率过高会被服务端限流，因此默认值设置为保守档位（600s）。
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
        
        logger.info("Python gRPC client initialized");
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
    
    // ========== 步骤1: 下载视频 ==========
    
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
    
    // ========== 步骤2: Whisper转录 ==========
    
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
    
    // ========== 步骤3: Stage1处理 ==========
    
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
    
    // ========== 步骤4: Phase2A 语义分析 ==========
    
    public static class ScreenshotRequest {
        public String screenshotId;
        public double timestampSec;
        public String label;
        public String semanticUnitId;
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
        try {
            logger.info("[{}] Calling AnalyzeSemanticUnits (Phase2A)", taskId);

            AnalyzeRequest request = AnalyzeRequest.newBuilder()
                .setTaskId(taskId)
                .setVideoPath(videoPath)
                .setStep2JsonPath(step2JsonPath)
                .setStep6JsonPath(step6JsonPath)
                .setSentenceTimestampsPath(sentenceTimestampsPath)
                .setOutputDir(outputDir)
                .build();

            AnalyzeResponse response = blockingStub
                .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                .analyzeSemanticUnits(request);

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
        } catch (StatusRuntimeException e) {
            logger.error("[{}] AnalyzeSemanticUnits failed: {}", taskId, e.getStatus());
            AnalyzeResult result = new AnalyzeResult();
            result.success = false;
            result.errorMsg = statusDescriptionOrCode(e);
            return result;
        }
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
    
    // ========== 步骤6: Phase2B 富文本组装 ==========
    
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
    
    // ========== 健康检查 ==========
    
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
    
    // ========== 🚀 V3: CV验证批量并行处理 ==========
    
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
     * 🚀 批量CV验证 (异步)
     * 
     * @param taskId 任务ID
     * @param videoPath 视频路径
     * @param units 语义单元列表
     * @param timeoutSec 超时秒数
     * @return CompletableFuture<CVBatchResult>
     */
    /**
     * 🚀 批量CV验证 (流式返回)
     * 
     * @param taskId 任务ID
     * @param videoPath 视频路径
     * @param units 语义单元列表
     * @param timeoutSec 超时秒数
     * @param resultConsumer 结果回调 (每完成一个 Unit 回调一次)
     * @return CompletableFuture<Boolean> 任务是否启动成功
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

    // ========== 🚀 V3: Phase2A - Step 2: Knowledge Classification ==========
    
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

    // ========== 🚀 V3: Phase2A - Step 3: Material Request Generation ==========

    public static class MaterialGenerationInput {
        public String unitId;
        public String knowledgeType;
        public double startSec;
        public double endSec;
        public String fullText;
        public List<ActionSegmentResult> actionUnits = new ArrayList<>();
        public List<StableIslandResult> stableIslands = new ArrayList<>(); // 🚀 CV结果中的稳定岛
    }
    
    public static class ScreenshotRequestDTO {
        public String screenshotId;
        public double timestampSec;
        public String label;
        public String semanticUnitId;
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
         try {
            logger.info("[{}] GenerateMaterialRequests: {} units", taskId, units.size());

            GenerateMaterialRequestsRequest.Builder requestBuilder = GenerateMaterialRequestsRequest.newBuilder()
                .setTaskId(taskId)
                .setVideoPath(videoPath);

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
                            "[{}] 上游 knowledge_type 缺失/疑似 CV actionType: unit={}, actions={}, missing={}, cv_like={}, default_like={}, unit_kt={}, example=({})",
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
        return CompletableFuture.supplyAsync(
            () -> generateMaterialRequests(taskId, units, videoPath, timeoutSec)
        );
    }


    // ========== 🔥 V7: VL-Based Analysis ==========
    
    public static class VLAnalysisResult {
        public boolean success;
        public boolean vlEnabled;
        public boolean usedFallback;
        public List<ScreenshotRequestDTO> screenshotRequests = new ArrayList<>();
        public List<ClipRequestDTO> clipRequests = new ArrayList<>();
        public int unitsAnalyzed;
        public int vlClipsGenerated;
        public int vlScreenshotsGenerated;
        public String errorMsg;
    }
    
    /**
     * 🔥 V7: VL-Based Analysis - 使用 Qwen3-VL-Plus 直接分析视频
     * 
     * 完全跳过 CV/LLM 流程，直接使用视觉语言模型分析视频片段。
     * 
     * @param taskId 任务ID
     * @param videoPath 视频路径
     * @param analyzeResult AnalyzeSemanticUnits 的返回结果（含 ref/inline）
     * @param outputDir 输出目录
     * @param timeoutSec 超时秒数
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
            logger.error("[{}] AnalyzeWithVL failed: {}", taskId, e.getStatus());
            VLAnalysisResult result = new VLAnalysisResult();
            result.success = false;
            result.errorMsg = statusDescriptionOrCode(e);
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

    // ========== 🚀 V6: 资源释放 ==========
    
    public static class ReleaseResourcesResult {
        public boolean success;
        public String message;
        public int freedWorkersCount;
        public float freedMemoryMb;
    }
    
    public CompletableFuture<ReleaseResourcesResult> releaseCVResourcesAsync(String taskId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.info("[{}] Calling ReleaseCVResources", taskId);
                
                ReleaseResourcesRequest request = ReleaseResourcesRequest.newBuilder()
                    .setTaskId(taskId)
                    .build();
                
                // Short timeout (5s) as this should be fast
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
        });
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
