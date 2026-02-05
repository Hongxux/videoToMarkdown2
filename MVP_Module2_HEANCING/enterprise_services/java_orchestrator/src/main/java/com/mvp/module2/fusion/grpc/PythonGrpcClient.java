package com.mvp.module2.fusion.grpc;

import com.mvp.videoprocessing.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
    
    private ManagedChannel channel;
    private VideoProcessingServiceGrpc.VideoProcessingServiceBlockingStub blockingStub;
    
    @PostConstruct
    public void init() {
        logger.info("Initializing Python gRPC client: {}:{}", pythonHost, pythonPort);
        
        channel = ManagedChannelBuilder.forAddress(pythonHost, pythonPort)
            .usePlaintext()  // 开发环境使用明文
            .maxInboundMessageSize(100 * 1024 * 1024)  // 100MB
            .keepAliveTime(30, TimeUnit.SECONDS)
            .keepAliveTimeout(10, TimeUnit.SECONDS)
            .build();
        
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
                
                return result;
                
            } catch (StatusRuntimeException e) {
                logger.error("[{}] DownloadVideo failed: {}", taskId, e.getStatus());
                DownloadResult result = new DownloadResult();
                result.success = false;
                result.errorMsg = e.getStatus().getDescription();
                return result;
            }
        });
    }
    
    // ========== 步骤2: Whisper转录 ==========
    
    public static class TranscribeResult {
        public boolean success;
        public String subtitlePath;
        public String subtitleText;
        public String errorMsg;
    }
    
    public CompletableFuture<TranscribeResult> transcribeVideoAsync(
            String taskId, String videoPath, String language, int timeoutSec) {
        return CompletableFuture.supplyAsync(() -> {
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
                result.errorMsg = e.getStatus().getDescription();
                return result;
            }
        });
    }
    
    // ========== 步骤3: Stage1处理 ==========
    
    public static class Stage1Result {
        public boolean success;
        public String step2JsonPath;
        public String step6JsonPath;
        public String sentenceTimestampsPath;
        public String errorMsg;
    }
    
    public CompletableFuture<Stage1Result> processStage1Async(
            String taskId, String videoPath, String subtitlePath, 
            String outputDir, int maxStep, int timeoutSec) {
        return CompletableFuture.supplyAsync(() -> {
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
                result.errorMsg = e.getStatus().getDescription();
                return result;
            }
        });
    }
    
    // ========== 步骤4: Phase2A 语义分析 ==========
    
    public static class ScreenshotRequest {
        public String screenshotId;
        public double timestampSec;
        public String label;
        public String semanticUnitId;
    }
    
    public static class ClipRequest {
        public String clipId;
        public double startSec;
        public double endSec;
        public String knowledgeType;
        public String semanticUnitId;
    }
    
    public static class AnalyzeResult {
        public boolean success;
        public List<ScreenshotRequest> screenshotRequests = new ArrayList<>();
        public List<ClipRequest> clipRequests = new ArrayList<>();
        public String semanticUnitsJsonPath;
        public String errorMsg;
    }
    
    public CompletableFuture<AnalyzeResult> analyzeSemanticUnitsAsync(
            String taskId, String videoPath, String step2JsonPath,
            String step6JsonPath, String sentenceTimestampsPath,
            String outputDir, int timeoutSec) {
        return CompletableFuture.supplyAsync(() -> {
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
                result.semanticUnitsJsonPath = response.getSemanticUnitsJsonPath();
                result.errorMsg = response.getErrorMsg();
                
                // 转换列表
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
                    result.clipRequests.add(r);
                }
                
                return result;
                
            } catch (StatusRuntimeException e) {
                logger.error("[{}] AnalyzeSemanticUnits failed: {}", taskId, e.getStatus());
                AnalyzeResult result = new AnalyzeResult();
                result.success = false;
                result.errorMsg = e.getStatus().getDescription();
                return result;
            }
        });
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
    
    public CompletableFuture<AssembleResult> assembleRichTextAsync(
            String taskId, String videoPath, String semanticUnitsJsonPath,
            String screenshotsDir, String clipsDir, String outputDir,
            String title, int timeoutSec) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.info("[{}] Calling AssembleRichText (Phase2B)", taskId);
                
                AssembleRequest request = AssembleRequest.newBuilder()
                    .setTaskId(taskId)
                    .setVideoPath(videoPath)
                    .setSemanticUnitsJsonPath(semanticUnitsJsonPath)
                    .setScreenshotsDir(screenshotsDir)
                    .setClipsDir(clipsDir)
                    .setOutputDir(outputDir)
                    .setTitle(title)
                    .build();
                
                AssembleResponse response = blockingStub
                    .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                    .assembleRichText(request);
                
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
                result.errorMsg = e.getStatus().getDescription();
                return result;
            }
        });
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
    public CompletableFuture<Boolean> validateCVBatchStreaming(
            String taskId, String videoPath, List<SemanticUnitInput> units, int timeoutSec, 
            java.util.function.Consumer<CVValidationUnitResult> resultConsumer) {
        
        return CompletableFuture.supplyAsync(() -> {
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

                // 🚀 调用流式 gRPC (Blocking Stub 返回 Iterator)
                java.util.Iterator<CVValidationResponse> responseIterator = blockingStub
                    .withDeadlineAfter(timeoutSec, TimeUnit.SECONDS)
                    .validateCVBatch(requestBuilder.build());

                // 🚀 迭代流式结果
                while (responseIterator.hasNext()) {
                    CVValidationResponse response = responseIterator.next();
                    if (!response.getSuccess()) {
                        logger.error("[{}] Streaming response error: {}", taskId, response.getErrorMsg());
                        continue;
                    }

                    for (com.mvp.videoprocessing.grpc.CVValidationResult pbResult : response.getResultsList()) {
                        CVValidationUnitResult unitResult = new CVValidationUnitResult();
                        unitResult.unitId = pbResult.getUnitId();
                        
                        // 转换 stable islands
                        for (com.mvp.videoprocessing.grpc.StableIsland si : pbResult.getStableIslandsList()) {
                            StableIslandResult islandResult = new StableIslandResult();
                            islandResult.startSec = si.getStartSec();
                            islandResult.endSec = si.getEndSec();
                            islandResult.midSec = si.getMidSec();
                            islandResult.durationSec = si.getDurationSec();
                            unitResult.stableIslands.add(islandResult);
                        }
                        
                        // 转换 action segments
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
                        
                        // 🚀 立即通知 Java
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
        });
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
    
    public CompletableFuture<ClassificationBatchResult> classifyKnowledgeBatchAsync(
            String taskId, List<ClassificationInput> units, String step2Path, int timeoutSec) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.info("[{}] ClassifyKnowledgeBatch: {} units", taskId, units.size());
                
                KnowledgeClassificationRequest.Builder requestBuilder = KnowledgeClassificationRequest.newBuilder()
                    .setTaskId(taskId)
                    .setStep2Path(step2Path != null ? step2Path : "");  // 🔑 传递 Step 2 路径
                    
                for (ClassificationInput unit : units) {
                    SemanticUnitForClassification.Builder unitBuilder = SemanticUnitForClassification.newBuilder()
                        .setUnitId(unit.unitId)
                        .setTitle(unit.title != null ? unit.title : "")
                        .setText(unit.text != null ? unit.text : "");
                        
                    for (ActionSegmentResult as : unit.actionUnits) {
                        // We map ActionSegmentResult to ActionUnitForClassification
                        // Note: ActionSegmentResult doesn't have ID, but we need ID for matching. 
                        // Assuming inputs come with ID or we rely on index?
                        // Proto ActionUnitForClassification has 'id'. 
                        // In ValidateCV, we received ActionSegments without explicit ID field in Java DTO, 
                        // but Proto ActionSegment didn't have ID either. 
                        // Wait, ValidateCV returns ActionSegment which has no ID. logic relies on structure.
                        // But KnowledgeClassification needs ActionID to map back.
                        // We should probably add 'id' to ActionSegmentResult or assume index matches.
                        // Let's use a hash or temporary ID if needed, or assume caller provides it.
                        // Actually, CV results are "ActionSegments". We can assign them IDs (1, 2, 3...) when creating this input.
                        unitBuilder.addActionUnits(
                            ActionUnitForClassification.newBuilder()
                                .setId(as.id) // TODO: Caller should set this if needed. For now 0.
                                .setStartSec(as.startSec)
                                .setEndSec(as.endSec)
                                .build()
                        );
                    }
                    
                    // ❌ Removed: Subtitle transmission - Classifier reads directly from step2_path
                    
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
                result.errorMsg = e.getStatus().getDescription();
                return result;
            }
        });
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
    }

    public static class MaterialGenerationResult {
        public boolean success;
        public List<ScreenshotRequestDTO> screenshotRequests = new ArrayList<>();
        public List<ClipRequestDTO> clipRequests = new ArrayList<>();
        public String errorMsg;
    }

    public CompletableFuture<MaterialGenerationResult> generateMaterialRequestsAsync(
            String taskId, List<MaterialGenerationInput> units, String videoPath, int timeoutSec) {
        return CompletableFuture.supplyAsync(() -> {
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
                        // 💥 断链探针：上游 knowledge_type 缺失/疑似 CV actionType（用于定位默认值/字段错用）
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
                    
                    // 🚀 添加稳定岛数据
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
                    result.clipRequests.add(r);
                }
                
                return result;
             } catch (StatusRuntimeException e) {
                logger.error("[{}] GenerateMaterialRequests failed: {}", taskId, e.getStatus());
                MaterialGenerationResult result = new MaterialGenerationResult();
                result.success = false;
                result.errorMsg = e.getStatus().getDescription();
                return result;
            }
        });
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
                result.message = e.getStatus().getDescription();
                return result;
            }
        });
    }

}
