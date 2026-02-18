package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.grpc.PythonGrpcClient.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.Locale;
import java.time.Instant;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * ??????? (V3 Parallel)
 * 
 * ?????????
 * 1. ?????????????
 * 2. Stage1??????????
 * 3. Phase2A???????????
 * 4. ??????? VL????? Legacy(CV + LLM)?
 * 5. ???????????????
 * 6. Phase2B??????????
 * 
 * ????????????????????????????
 * 
 * 
 */
@Service
public class VideoProcessingOrchestrator {
    
    private static final Logger logger = LoggerFactory.getLogger(VideoProcessingOrchestrator.class);

    // ????????????
    private static class ExtractionRequests {
        List<JavaCVFFmpegService.ScreenshotRequest> screenshotRequests;
        List<JavaCVFFmpegService.ClipRequest> clipRequests;
        CompletableFuture<JavaCVFFmpegService.ExtractionResult> extractionFuture;
        
        public ExtractionRequests(List<JavaCVFFmpegService.ScreenshotRequest> ss, List<JavaCVFFmpegService.ClipRequest> clips) {
            this.screenshotRequests = ss != null ? ss : new ArrayList<>();
            this.clipRequests = clips != null ? clips : new ArrayList<>();
            this.extractionFuture = null;
        }

        public ExtractionRequests(
                List<JavaCVFFmpegService.ScreenshotRequest> ss,
                List<JavaCVFFmpegService.ClipRequest> clips,
                CompletableFuture<JavaCVFFmpegService.ExtractionResult> extractionFuture
        ) {
            this.screenshotRequests = ss != null ? ss : new ArrayList<>();
            this.clipRequests = clips != null ? clips : new ArrayList<>();
            this.extractionFuture = extractionFuture;
        }
    }

    // ????CV ?????????
    private static class AnalysisResults {
        Map<String, CVValidationUnitResult> cvResults;
        List<KnowledgeResultItem> classResults;
        
        public AnalysisResults(Map<String, CVValidationUnitResult> cv, List<KnowledgeResultItem> cls) {
            this.cvResults = cv != null ? cv : new ConcurrentHashMap<>();
            this.classResults = cls != null ? cls : new ArrayList<>();
        }
    }
    
    @Autowired
    private PythonGrpcClient grpcClient;
    
    @Autowired
    private JavaCVFFmpegService ffmpegService;  // ?? JNI ??????????
    
    @Autowired
    private DynamicTimeoutCalculator timeoutCalculator;
    
    @Autowired
    private CVValidationOrchestrator cvOrchestrator;
    
    @Autowired
    private KnowledgeClassificationOrchestrator knowledgeOrchestrator;
    
    @Autowired
    private ModuleConfigService configService;
    
    // ????
    private final ConcurrentHashMap<String, TaskContext> activeTasks = new ConcurrentHashMap<>();
    private final AtomicInteger taskCounter = new AtomicInteger(0);
    private final ObjectMapper objectMapper = new ObjectMapper();

    // LLM pricing (USD / 1M tokens)
    private static final double QWEN3_VL_PLUS_INPUT_PER_M = 1.50d;
    private static final double QWEN3_VL_PLUS_OUTPUT_PER_M = 4.50d;
    private static final double ERNIE_45_TURBO_VL_INPUT_MIN_PER_M = 0.80d;
    private static final double ERNIE_45_TURBO_VL_INPUT_MAX_PER_M = 1.50d;
    private static final double ERNIE_45_TURBO_VL_OUTPUT_MIN_PER_M = 3.20d;
    private static final double ERNIE_45_TURBO_VL_OUTPUT_MAX_PER_M = 4.50d;
    private static final double DEEPSEEK_CHAT_INPUT_UNCACHED_PER_M = 2.00d;
    private static final double DEEPSEEK_CHAT_INPUT_CACHED_PER_M = 0.50d;
    private static final double DEEPSEEK_CHAT_OUTPUT_PER_M = 8.00d;
    
    // ???? (Functional Interface)
    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(String taskId, double progress, String message);
    }
    private ProgressCallback progressCallback;
    public void setProgressCallback(ProgressCallback callback) { this.progressCallback = callback; }
    
    // --- Context Classes ---
    public static class ProcessingResult {
        public boolean success;
        public String taskId;
        public String markdownPath;
        public String jsonPath;
        public String errorMessage;
        public long processingTimeMs;
    }
    
    /**
     * ?????
     */
    public static class TaskContext {
        public String taskId;
        public String videoUrl;
        public String outputDir;
        public double videoDuration;
        public long startTime;
    }

    /**
     * ?????????
     */
    public ProcessingResult processVideo(String taskId, String videoUrl, String outputDir) {
        ProcessingResult result = new ProcessingResult();
        result.taskId = taskId;
        long startTime = System.currentTimeMillis();
        String metricsOutputDir = outputDir;
        String metricsVideoPath = videoUrl;
        String metricsInputVideoUrl = videoUrl;
        Map<String, Long> stageTimingsMs = new LinkedHashMap<>();
        Map<String, Object> flowFlags = new LinkedHashMap<>();

        try {
            String videoPath = videoUrl;
            double videoDuration = 60;
            boolean downloadedFromUrl = false;
            boolean usedVLFlow = false;
            boolean usedLegacyFlow = false;

            long localPrepareStart = System.currentTimeMillis();
            if (!isHttpUrl(videoUrl)) {
                videoPath = normalizeLocalVideoPath(videoUrl);
                assertLocalVideoExists(videoUrl, videoPath);
                outputDir = resolveOutputDirForLocalVideo(videoPath);
                new File(outputDir).mkdirs();
                logger.info("[{}] 统一本地任务输出目录 -> {}", taskId, outputDir);

                videoPath = ensureLocalVideoInStorage(videoPath, outputDir);
                videoDuration = resolveVideoDurationSec(taskId, videoPath, videoDuration);
            }
            stageTimingsMs.put("prepare_local_video", System.currentTimeMillis() - localPrepareStart);
            metricsVideoPath = videoPath;
            metricsOutputDir = outputDir;

            long downloadStart = System.currentTimeMillis();
            if (isHttpUrl(videoUrl)) {
                updateProgress(taskId, 0.05, "下载视频中...");
                DownloadResult dl = grpcClient.downloadVideoAsync(taskId, videoUrl, outputDir, 300).get(5, TimeUnit.MINUTES);
                if (!dl.success) {
                    throw new RuntimeException("Download failed: " + dl.errorMsg);
                }
                downloadedFromUrl = true;
                videoPath = dl.videoPath;
                videoDuration = dl.durationSec;
                outputDir = new File(videoPath).getParentFile().getAbsolutePath();
                new File(outputDir).mkdirs();
                metricsVideoPath = videoPath;
                metricsOutputDir = outputDir;
            }
            stageTimingsMs.put("download_video", System.currentTimeMillis() - downloadStart);

            if (videoDuration <= 0) {
                videoDuration = resolveVideoDurationSec(taskId, videoPath, videoDuration);
            }

            DynamicTimeoutCalculator.TimeoutConfig timeouts = timeoutCalculator.calculateTimeouts(videoDuration);

            updateProgress(taskId, 0.15, "语音转录中...");
            long transcribeStart = System.currentTimeMillis();
            TranscribeResult tr = grpcClient.transcribeVideoAsync(taskId, videoPath, "auto", timeouts.getTranscribeTimeoutSec())
                .get(timeouts.getTranscribeTimeoutSec() + 60, TimeUnit.SECONDS);
            if (!tr.success) {
                throw new RuntimeException("Transcribe failed: " + tr.errorMsg);
            }
            stageTimingsMs.put("transcribe", System.currentTimeMillis() - transcribeStart);

            updateProgress(taskId, 0.25, "Stage1 文本结构化...");
            long stage1Start = System.currentTimeMillis();
            Stage1Result s1 = grpcClient.processStage1Async(taskId, videoPath, tr.subtitlePath, outputDir, 6, timeouts.getStage1TimeoutSec())
                .get(timeouts.getStage1TimeoutSec() + 60, TimeUnit.SECONDS);
            if (!s1.success) {
                throw new RuntimeException("Stage1 failed: " + s1.errorMsg);
            }
            stageTimingsMs.put("stage1", System.currentTimeMillis() - stage1Start);

            updateProgress(taskId, 0.35, "语义分割...");
            long phase2aStart = System.currentTimeMillis();
            AnalyzeResult ar = grpcClient.analyzeSemanticUnitsAsync(
                    taskId,
                    videoPath,
                    s1.step2JsonPath,
                    s1.step6JsonPath,
                    s1.sentenceTimestampsPath,
                    outputDir,
                    timeouts.getPhase2aTimeoutSec())
                .get(timeouts.getPhase2aTimeoutSec() + 60, TimeUnit.SECONDS);
            if (!ar.success) {
                throw new RuntimeException("Phase2A failed: " + ar.errorMsg);
            }
            stageTimingsMs.put("phase2a_segmentation", System.currentTimeMillis() - phase2aStart);

            updateProgress(taskId, 0.40, "语义分割LLM调用完成...");

            ExtractionRequests materialRequests = null;
            JavaCVFFmpegService.ExtractionResult extractRes;
            long analysisTotalStart = System.currentTimeMillis();

            boolean vlEnabled = configService.isVLEnabled();
            flowFlags.put("vl_enabled", vlEnabled);
            long vlAnalysisStart = System.currentTimeMillis();
            if (vlEnabled) {
                materialRequests = tryVLAnalysis(taskId, videoPath, ar, outputDir, timeouts);
                if (materialRequests == null) {
                    logger.warn("[{}] Proceeding to Legacy Flow (Fallback or VL failed).", taskId);
                } else {
                    usedVLFlow = true;
                }
            } else {
                logger.info("[{}] VL disabled in config.", taskId);
            }
            stageTimingsMs.put("analysis_vl", System.currentTimeMillis() - vlAnalysisStart);

            long legacyAnalysisStart = System.currentTimeMillis();
            if (materialRequests == null) {
                updateProgress(taskId, 0.45, "执行级联并行分析 (CV/LLM Legacy)...");
                materialRequests = runLegacyAnalysis(taskId, videoPath, ar, s1, outputDir, timeouts);
                usedLegacyFlow = true;
            }
            stageTimingsMs.put("analysis_legacy", System.currentTimeMillis() - legacyAnalysisStart);
            stageTimingsMs.put("analysis_total", System.currentTimeMillis() - analysisTotalStart);

            updateProgress(taskId, 0.80, "执行素材提取...");
            long extractionStart = System.currentTimeMillis();
            int ffmpegTimeoutSec = calculateFfmpegTimeoutSec(taskId, videoDuration, materialRequests, timeouts);
            if (materialRequests.extractionFuture == null) {
                materialRequests = startExtractionPipeline(
                    taskId,
                    videoPath,
                    outputDir,
                    materialRequests.screenshotRequests,
                    materialRequests.clipRequests,
                    ffmpegTimeoutSec
                );
            }
            if (materialRequests.extractionFuture != null) {
                logger.info("[{}] Reusing in-flight extraction future (producer-consumer path)", taskId);
                extractRes = materialRequests.extractionFuture
                    .orTimeout(ffmpegTimeoutSec, TimeUnit.SECONDS)
                    .join();
            } else {
                extractRes = ffmpegService.extractAllSync(
                    videoPath,
                    outputDir,
                    materialRequests.screenshotRequests,
                    materialRequests.clipRequests,
                    ffmpegTimeoutSec
                );
            }
            stageTimingsMs.put("extract_assets", System.currentTimeMillis() - extractionStart);

            updateProgress(taskId, 0.90, "生成最终文档...");
            long assembleStart = System.currentTimeMillis();
            String title = new File(videoPath).getName().replace(".mp4", "");
            AssembleResult assembleRes = grpcClient.assembleRichTextAsync(
                    taskId,
                    videoPath,
                    ar,
                    outputDir + "/assets",
                    outputDir + "/assets",
                    outputDir,
                    title,
                    timeouts.getPhase2bTimeoutSec())
                .get(timeouts.getPhase2bTimeoutSec() + 60, TimeUnit.SECONDS);

            if (!assembleRes.success) {
                throw new RuntimeException("Assemble failed: " + assembleRes.errorMsg);
            }
            stageTimingsMs.put("phase2b_assemble", System.currentTimeMillis() - assembleStart);

            result.success = true;
            result.markdownPath = assembleRes.markdownPath;
            result.jsonPath = assembleRes.jsonPath;
            logger.info("✅ Pipeline Complete: {}", taskId);

            flowFlags.put("downloaded_from_url", downloadedFromUrl);
            flowFlags.put("used_vl_flow", usedVLFlow);
            flowFlags.put("used_legacy_flow", usedLegacyFlow);

        } catch (Exception e) {
            logger.error("❌ Pipeline Failed: {} - {}", taskId, e.getMessage());
            result.success = false;
            result.errorMessage = e.getMessage();

            flowFlags.putIfAbsent("downloaded_from_url", false);
            flowFlags.putIfAbsent("used_vl_flow", false);
            flowFlags.putIfAbsent("used_legacy_flow", false);
        } finally {
            result.processingTimeMs = System.currentTimeMillis() - startTime;
            stageTimingsMs.put("total_pipeline", result.processingTimeMs);
            writeTaskMetricsReport(
                    taskId,
                    metricsOutputDir,
                    metricsVideoPath,
                    metricsInputVideoUrl,
                    result,
                    stageTimingsMs,
                    flowFlags
            );
        }
        return result;
    }
    private static class VLTokenUsage {
        long inputTokens;
        long outputTokens;
        long totalTokens;
        String sourcePath = "";
    }

    private static class DeepSeekUsage {
        long inputTokensUncached;
        long inputTokensCached;
        long outputTokens;
        long totalCalls;
        long cachedCalls;
        String sourcePath = "";
        boolean hasData() {
            return inputTokensUncached > 0 || inputTokensCached > 0 || outputTokens > 0 || totalCalls > 0;
        }
    }

    private void writeTaskMetricsReport(
            String taskId,
            String outputDir,
            String videoPath,
            String inputVideoUrl,
            ProcessingResult result,
            Map<String, Long> stageTimingsMs,
            Map<String, Object> flowFlags
    ) {
        if (outputDir == null || outputDir.isBlank()) {
            logger.warn("[{}] Skip task metrics report: outputDir is empty", taskId);
            return;
        }
        try {
            Path reportDir = Paths.get(outputDir, "intermediates");
            Files.createDirectories(reportDir);

            String vlModel = configService != null ? configService.getVLModelName() : "";
            VLTokenUsage vlUsage = loadVLTokenUsage(outputDir);
            DeepSeekUsage deepSeekUsage = loadDeepSeekUsage(outputDir);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("version", "1.0");
            payload.put("generated_at", Instant.now().toString());
            payload.put("task_id", taskId);
            payload.put("success", result != null && result.success);
            payload.put("error_message", result != null ? (result.errorMessage != null ? result.errorMessage : "") : "");
            payload.put("input_video_url", inputVideoUrl != null ? inputVideoUrl : "");
            payload.put("video_path", videoPath != null ? videoPath : "");
            payload.put("output_dir", outputDir);
            payload.put("result_markdown_path", result != null ? (result.markdownPath != null ? result.markdownPath : "") : "");
            payload.put("result_json_path", result != null ? (result.jsonPath != null ? result.jsonPath : "") : "");
            payload.put("stage_timings_ms", new LinkedHashMap<>(stageTimingsMs));
            payload.put("flow_flags", new LinkedHashMap<>(flowFlags));
            payload.put("llm_cost", buildLLMCostPayload(vlModel, vlUsage, deepSeekUsage));

            String reportFileName = (taskId != null && !taskId.isBlank())
                    ? ("task_metrics_" + taskId + ".json")
                    : "task_metrics_unknown.json";
            Path reportPath = reportDir.resolve(reportFileName);
            Path latestPath = reportDir.resolve("task_metrics_latest.json");

            objectMapper.writerWithDefaultPrettyPrinter().writeValue(reportPath.toFile(), payload);
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(latestPath.toFile(), payload);
            logger.info("[{}] Task metrics report saved: {}", taskId, reportPath.toAbsolutePath());
        } catch (Exception e) {
            logger.warn("[{}] Failed to write task metrics report: {}", taskId, e.getMessage());
        }
    }

    private Map<String, Object> buildLLMCostPayload(String vlModel, VLTokenUsage vlUsage, DeepSeekUsage deepSeekUsage) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("currency", "USD");
        payload.put("pricing_basis", "per_1m_tokens");

        long vlInput = Math.max(0L, vlUsage.inputTokens);
        long vlOutput = Math.max(0L, vlUsage.outputTokens);
        long vlTotal = Math.max(0L, vlUsage.totalTokens > 0 ? vlUsage.totalTokens : (vlInput + vlOutput));

        double qwenInputCost = tokenCostUsd(vlInput, QWEN3_VL_PLUS_INPUT_PER_M);
        double qwenOutputCost = tokenCostUsd(vlOutput, QWEN3_VL_PLUS_OUTPUT_PER_M);
        double qwenTotalCost = qwenInputCost + qwenOutputCost;

        double ernieMinInputCost = tokenCostUsd(vlInput, ERNIE_45_TURBO_VL_INPUT_MIN_PER_M);
        double ernieMaxInputCost = tokenCostUsd(vlInput, ERNIE_45_TURBO_VL_INPUT_MAX_PER_M);
        double ernieMinOutputCost = tokenCostUsd(vlOutput, ERNIE_45_TURBO_VL_OUTPUT_MIN_PER_M);
        double ernieMaxOutputCost = tokenCostUsd(vlOutput, ERNIE_45_TURBO_VL_OUTPUT_MAX_PER_M);
        double ernieMinTotalCost = ernieMinInputCost + ernieMinOutputCost;
        double ernieMaxTotalCost = ernieMaxInputCost + ernieMaxOutputCost;

        Map<String, Object> vlSection = new LinkedHashMap<>();
        vlSection.put("model", vlModel != null ? vlModel : "");
        vlSection.put("input_tokens", vlInput);
        vlSection.put("output_tokens", vlOutput);
        vlSection.put("total_tokens", vlTotal);
        vlSection.put("token_source", vlUsage.sourcePath);

        Map<String, Object> qwenCost = new LinkedHashMap<>();
        qwenCost.put("input_cost_usd", roundCost(qwenInputCost));
        qwenCost.put("output_cost_usd", roundCost(qwenOutputCost));
        qwenCost.put("total_cost_usd", roundCost(qwenTotalCost));
        vlSection.put("qwen3_vl_plus_cost", qwenCost);

        Map<String, Object> ernieCostRange = new LinkedHashMap<>();
        ernieCostRange.put("input_cost_usd_min", roundCost(ernieMinInputCost));
        ernieCostRange.put("input_cost_usd_max", roundCost(ernieMaxInputCost));
        ernieCostRange.put("output_cost_usd_min", roundCost(ernieMinOutputCost));
        ernieCostRange.put("output_cost_usd_max", roundCost(ernieMaxOutputCost));
        ernieCostRange.put("total_cost_usd_min", roundCost(ernieMinTotalCost));
        ernieCostRange.put("total_cost_usd_max", roundCost(ernieMaxTotalCost));
        vlSection.put("ernie_4_5_turbo_vl_cost_range", ernieCostRange);

        double vlSelectedMin;
        double vlSelectedMax;
        String normalizedVLModel = (vlModel != null ? vlModel : "").toLowerCase(Locale.ROOT);
        if (normalizedVLModel.contains("qwen3-vl-plus")) {
            vlSelectedMin = qwenTotalCost;
            vlSelectedMax = qwenTotalCost;
            vlSection.put("selected_pricing_model", "qwen3-vl-plus");
        } else if (normalizedVLModel.contains("ernie-4.5-turbo-vl") || normalizedVLModel.contains("ernie")) {
            vlSelectedMin = ernieMinTotalCost;
            vlSelectedMax = ernieMaxTotalCost;
            vlSection.put("selected_pricing_model", "ernie-4.5-turbo-vl");
        } else {
            vlSelectedMin = Math.min(qwenTotalCost, ernieMinTotalCost);
            vlSelectedMax = Math.max(qwenTotalCost, ernieMaxTotalCost);
            vlSection.put("selected_pricing_model", "unknown");
        }
        vlSection.put("selected_cost_usd_min", roundCost(vlSelectedMin));
        vlSection.put("selected_cost_usd_max", roundCost(vlSelectedMax));
        payload.put("vl", vlSection);

        long deepInputUncached = Math.max(0L, deepSeekUsage.inputTokensUncached);
        long deepInputCached = Math.max(0L, deepSeekUsage.inputTokensCached);
        long deepOutput = Math.max(0L, deepSeekUsage.outputTokens);
        double deepInputUncachedCost = tokenCostUsd(deepInputUncached, DEEPSEEK_CHAT_INPUT_UNCACHED_PER_M);
        double deepInputCachedCost = tokenCostUsd(deepInputCached, DEEPSEEK_CHAT_INPUT_CACHED_PER_M);
        double deepOutputCost = tokenCostUsd(deepOutput, DEEPSEEK_CHAT_OUTPUT_PER_M);
        double deepTotalCost = deepInputUncachedCost + deepInputCachedCost + deepOutputCost;

        Map<String, Object> deepSection = new LinkedHashMap<>();
        deepSection.put("model", "deepseek-chat");
        deepSection.put("input_tokens_uncached", deepInputUncached);
        deepSection.put("input_tokens_cached", deepInputCached);
        deepSection.put("output_tokens", deepOutput);
        deepSection.put("total_calls", deepSeekUsage.totalCalls);
        deepSection.put("cached_calls", deepSeekUsage.cachedCalls);
        deepSection.put("token_source", deepSeekUsage.sourcePath);
        deepSection.put("input_uncached_cost_usd", roundCost(deepInputUncachedCost));
        deepSection.put("input_cached_cost_usd", roundCost(deepInputCachedCost));
        deepSection.put("output_cost_usd", roundCost(deepOutputCost));
        deepSection.put("total_cost_usd", roundCost(deepTotalCost));
        payload.put("deepseek_chat", deepSection);

        double totalMin = vlSelectedMin + deepTotalCost;
        double totalMax = vlSelectedMax + deepTotalCost;
        payload.put("total_cost_usd_min", roundCost(totalMin));
        payload.put("total_cost_usd_max", roundCost(totalMax));
        if (Math.abs(totalMax - totalMin) < 1e-12) {
            payload.put("total_cost_usd", roundCost(totalMin));
        }
        payload.put(
                "coverage_note",
                "DeepSeek cost is computed from persisted traces (phase2b_llm_trace/deepseek_audit) only."
        );
        return payload;
    }

    private VLTokenUsage loadVLTokenUsage(String outputDir) {
        VLTokenUsage usage = new VLTokenUsage();
        Path reportPath = Paths.get(outputDir, "intermediates", "vl_token_report_latest.json");
        if (!Files.exists(reportPath)) {
            return usage;
        }
        try {
            JsonNode root = objectMapper.readTree(reportPath.toFile());
            JsonNode tokenStats = root.path("token_stats");
            usage.inputTokens = firstLong(tokenStats, "prompt_tokens_actual", "prompt_tokens");
            usage.outputTokens = firstLong(tokenStats, "completion_tokens_actual", "completion_tokens");
            usage.totalTokens = firstLong(tokenStats, "total_tokens_actual", "total_tokens");
            if (usage.totalTokens <= 0L) {
                usage.totalTokens = usage.inputTokens + usage.outputTokens;
            }
            usage.sourcePath = reportPath.toAbsolutePath().toString();
        } catch (Exception e) {
            logger.warn("Failed to parse VL token report: {}", e.getMessage());
        }
        return usage;
    }

    private DeepSeekUsage loadDeepSeekUsage(String outputDir) {
        Path tracePath = Paths.get(outputDir, "intermediates", "phase2b_llm_trace.jsonl");
        DeepSeekUsage traceUsage = loadDeepSeekUsageFromTrace(tracePath);
        if (traceUsage.hasData()) {
            return traceUsage;
        }
        Path auditPath = Paths.get(outputDir, "intermediates", "phase2b_deepseek_call_audit.json");
        return loadDeepSeekUsageFromAudit(auditPath);
    }

    private DeepSeekUsage loadDeepSeekUsageFromTrace(Path tracePath) {
        DeepSeekUsage usage = new DeepSeekUsage();
        if (tracePath == null || !Files.exists(tracePath)) {
            return usage;
        }
        usage.sourcePath = tracePath.toAbsolutePath().toString();
        try (BufferedReader reader = Files.newBufferedReader(tracePath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line == null) continue;
                String trimmed = line.trim();
                if (trimmed.isEmpty()) continue;
                JsonNode node;
                try {
                    node = objectMapper.readTree(trimmed);
                } catch (Exception ignored) {
                    continue;
                }
                if (!node.path("success").asBoolean(true)) {
                    continue;
                }
                String model = node.path("model").asText("");
                if (!model.toLowerCase(Locale.ROOT).contains("deepseek")) {
                    continue;
                }
                long promptTokens = Math.max(0L, node.path("prompt_tokens").asLong(0L));
                long completionTokens = Math.max(0L, node.path("completion_tokens").asLong(0L));
                boolean cacheHit = node.path("cache_hit").asBoolean(false);
                if (cacheHit) {
                    usage.inputTokensCached += promptTokens;
                    usage.cachedCalls += 1L;
                } else {
                    usage.inputTokensUncached += promptTokens;
                }
                usage.outputTokens += completionTokens;
                usage.totalCalls += 1L;
            }
        } catch (Exception e) {
            logger.warn("Failed to read phase2b_llm_trace: {}", e.getMessage());
        }
        return usage;
    }

    private DeepSeekUsage loadDeepSeekUsageFromAudit(Path auditPath) {
        DeepSeekUsage usage = new DeepSeekUsage();
        if (auditPath == null || !Files.exists(auditPath)) {
            return usage;
        }
        usage.sourcePath = auditPath.toAbsolutePath().toString();
        try {
            JsonNode root = objectMapper.readTree(auditPath.toFile());
            JsonNode records = root.path("records");
            if (!records.isArray()) {
                return usage;
            }
            for (JsonNode record : records) {
                JsonNode outputNode = record.path("output");
                if (!outputNode.path("success").asBoolean(true)) {
                    continue;
                }
                JsonNode meta = outputNode.path("metadata");
                String model = meta.path("model").asText(record.path("input").path("model").asText(""));
                if (!model.toLowerCase(Locale.ROOT).contains("deepseek")) {
                    continue;
                }
                long promptTokens = Math.max(0L, meta.path("prompt_tokens").asLong(0L));
                long completionTokens = Math.max(0L, meta.path("completion_tokens").asLong(0L));
                boolean cacheHit = meta.path("cache_hit").asBoolean(false);
                if (cacheHit) {
                    usage.inputTokensCached += promptTokens;
                    usage.cachedCalls += 1L;
                } else {
                    usage.inputTokensUncached += promptTokens;
                }
                usage.outputTokens += completionTokens;
                usage.totalCalls += 1L;
            }
        } catch (Exception e) {
            logger.warn("Failed to parse deepseek audit report: {}", e.getMessage());
        }
        return usage;
    }

    private long firstLong(JsonNode node, String... fieldNames) {
        if (node == null || fieldNames == null) {
            return 0L;
        }
        for (String fieldName : fieldNames) {
            if (fieldName == null || fieldName.isBlank()) {
                continue;
            }
            JsonNode valueNode = node.get(fieldName);
            if (valueNode == null || valueNode.isMissingNode() || valueNode.isNull()) {
                continue;
            }
            if (valueNode.isIntegralNumber() || valueNode.isFloatingPointNumber()) {
                return Math.max(0L, valueNode.asLong(0L));
            }
            String text = valueNode.asText("").trim();
            if (text.isEmpty()) {
                continue;
            }
            try {
                return Math.max(0L, Long.parseLong(text));
            } catch (Exception ignored) {
                // Ignore parse errors and try next field
            }
        }
        return 0L;
    }

    private double tokenCostUsd(long tokens, double ratePerMillion) {
        if (tokens <= 0L || ratePerMillion <= 0d) {
            return 0d;
        }
        return ((double) tokens / 1_000_000d) * ratePerMillion;
    }

    private double roundCost(double cost) {
        return Math.round(cost * 1_000_000d) / 1_000_000d;
    }

    // --- OutputDir ???? ---
    private int calculateFfmpegTimeoutSec(
            String taskId,
            double videoDurationSec,
            ExtractionRequests requests,
            DynamicTimeoutCalculator.TimeoutConfig timeouts
    ) {
        int baseTimeoutSec = timeouts != null ? timeouts.getFfmpegTimeoutSec() : 0;

        int screenshotCount = (requests != null && requests.screenshotRequests != null) ? requests.screenshotRequests.size() : 0;
        int clipCount = (requests != null && requests.clipRequests != null) ? requests.clipRequests.size() : 0;

        double totalClipDurationSec = 0.0;
        if (requests != null && requests.clipRequests != null) {
            for (JavaCVFFmpegService.ClipRequest clip : requests.clipRequests) {
                if (clip == null) continue;
                totalClipDurationSec += Math.max(0.0, clip.endSec - clip.startSec);
            }
        }

        // ????????????? seek/??/??????????????????
        double seekAndImageCostSec = screenshotCount * 0.8;
        double clipInitCostSec = clipCount * 2.0;
        double clipEncodeCostSec = totalClipDurationSec * 1.6;
        double bufferSec = 120.0 + Math.max(0.0, videoDurationSec * 0.05);

        int computedTimeoutSec = (int) Math.ceil((seekAndImageCostSec + clipInitCostSec + clipEncodeCostSec + bufferSec) * 1.2);
        int mergedTimeoutSec = Math.max(baseTimeoutSec, computedTimeoutSec);

        double scale = configService != null ? configService.getFfmpegTimeoutMultiplier() : 1.0;
        if (scale <= 0) scale = 1.0;
        int minTimeoutSec = configService != null ? configService.getFfmpegTimeoutMinSec() : 0;
        int maxTimeoutSec = configService != null ? configService.getFfmpegTimeoutMaxSec() : 0;

        int scaledTimeoutSec = (int) Math.ceil(mergedTimeoutSec * scale);
        if (minTimeoutSec > 0) scaledTimeoutSec = Math.max(scaledTimeoutSec, minTimeoutSec);
        if (maxTimeoutSec > 0) scaledTimeoutSec = Math.min(scaledTimeoutSec, maxTimeoutSec);

        logger.info(
            "[{}] FFmpeg timeout computed: {}s -> {}s (base={}s, scale={}, min={}, max={}, screenshots={}, clips={}, clipDur={}s)",
            taskId,
            mergedTimeoutSec,
            scaledTimeoutSec,
            baseTimeoutSec,
            String.format(Locale.ROOT, "%.2f", scale),
            minTimeoutSec,
            maxTimeoutSec,
            screenshotCount,
            clipCount,
            String.format(Locale.ROOT, "%.1f", totalClipDurationSec)
        );

        return scaledTimeoutSec;
    }

    private double resolveVideoDurationSec(String taskId, String videoPath, double fallbackSec) {
        double probed = ffmpegService.probeVideoDurationSec(videoPath);
        if (probed > 0) {
            logger.info("[{}] Probed video duration: {}s", taskId, String.format(Locale.ROOT, "%.1f", probed));
            return probed;
        }
        if (fallbackSec > 0) {
            logger.warn("[{}] Failed to probe video duration, fallback={}s", taskId, String.format(Locale.ROOT, "%.1f", fallbackSec));
        } else {
            logger.warn("[{}] Failed to probe video duration, fallback=0", taskId);
        }
        return fallbackSec;
    }

    private boolean isHttpUrl(String value) {
        if (value == null) return false;
        String lower = value.toLowerCase(Locale.ROOT);
        return lower.startsWith("http://") || lower.startsWith("https://");
    }

    private String normalizeLocalVideoPath(String videoUrl) {
        // ???? file:// ???????????????? hash ??
        try {
            if (videoUrl != null && videoUrl.toLowerCase(Locale.ROOT).startsWith("file://")) {
                return Paths.get(URI.create(videoUrl)).toAbsolutePath().normalize().toString();
            }
        } catch (Exception e) {
            logger.warn("Failed to parse file URI, fallback to raw path: {}", videoUrl);
        }
        return new File(videoUrl).getAbsolutePath();
    }

    private String resolveOutputDirForLocalVideo(String videoPath) {
        Path storageRoot = resolveStorageRoot();
        try {
            Path absVideoPath = Paths.get(videoPath).toAbsolutePath().normalize();
            if (isUnderPath(absVideoPath, storageRoot)) {
                return absVideoPath.getParent().toString();
            }
        } catch (Exception e) {
            // ??????? hash ??????????
        }

        String normalized = normalizePathForHash(videoPath);
        String hash = md5Hex(normalized);
        return storageRoot.resolve(hash).toString();
    }

    private String ensureLocalVideoInStorage(String videoPath, String outputDir) {
        // ???????/???? storage/{hash}????????????
        try {
            Path source = Paths.get(videoPath).toAbsolutePath().normalize();
            if (!Files.exists(source) || !Files.isRegularFile(source)) {
                throw new IllegalArgumentException("Local video not found: " + source);
            }
            Path storageRoot = resolveStorageRoot();
            if (isUnderPath(source, storageRoot)) {
                return source.toString();
            }

            Path targetDir = Paths.get(outputDir).toAbsolutePath().normalize();
            Files.createDirectories(targetDir);
            String fileName = source.getFileName().toString();
            Path target = targetDir.resolve(fileName);

            if (Files.exists(target)) {
                return target.toString();
            }

            try {
                Files.createLink(target, source);
                logger.info("Linked local video into storage: {}", target);
                return target.toString();
            } catch (Exception linkError) {
                // ????????????????????????
            }

            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
            logger.info("Copied local video into storage: {}", target);
            return target.toString();
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to place local video in storage: " + videoPath, e);
        }
    }

    /**
     * 在进入 FFmpeg / Python 阶段前做本地文件硬校验，避免“伪路径”穿透到后续链路。
     */
    private void assertLocalVideoExists(String rawInput, String normalizedPath) {
        if (normalizedPath == null || normalizedPath.isBlank()) {
            throw new IllegalArgumentException("Local video path is empty");
        }
        final Path resolved;
        try {
            resolved = Paths.get(normalizedPath).toAbsolutePath().normalize();
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Invalid local video path: " + rawInput);
        }
        if (!Files.exists(resolved) || !Files.isRegularFile(resolved)) {
            throw new IllegalArgumentException(
                "Local video not found: " + rawInput + " (resolved: " + resolved + ")"
            );
        }
    }

    private Path resolveStorageRoot() {
        // ????????? storage??? Java/Python ??????
        Path repoRoot = resolveRepoRoot();
        Path storageRoot = repoRoot.resolve("storage").toAbsolutePath().normalize();
        try {
            Files.createDirectories(storageRoot);
        } catch (IOException e) {
            logger.warn("Failed to create storage root: {}", storageRoot);
        }
        return storageRoot;
    }

    private Path resolveRepoRoot() {
        // ????????????????????????

        Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        for (int i = 0; i < 6; i++) {
            if (Files.exists(current.resolve("apps").resolve("grpc-server").resolve("main.py"))
                || Files.isDirectory(current.resolve("contracts"))) {
                return current;
            }
            Path parent = current.getParent();
            if (parent == null) break;
            current = parent;
        }
        return Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
    }

    private boolean isUnderPath(Path path, Path root) {
        try {
            return path.toAbsolutePath().normalize().startsWith(root.toAbsolutePath().normalize());
        } catch (Exception e) {
            return false;
        }
    }

    private String normalizePathForHash(String path) {
        // ?? hash ??????????? Java/Python ??????
        String abs = new File(path).getAbsolutePath();
        String normalized = abs.replace('/', File.separatorChar);
        if (File.separatorChar == '\\') {
            normalized = normalized.toLowerCase(Locale.ROOT);
        }
        return normalized;
    }

    private String md5Hex(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("MD5 hash failed", e);
        }
    }

    // --- 绱犳潗璇锋眰鍚堝苟 ---
    private List<JavaCVFFmpegService.ScreenshotRequest> mergeScreenshotRequests(
            List<PythonGrpcClient.ScreenshotRequest> phase2aRequests,
            List<PythonGrpcClient.ScreenshotRequestDTO> generatedRequests) {
        // ??????????? Phase2A ??????????????
        // ?????generatedRequests ?????????????/ID ?????????
        // ???? ID ???? generated ???????????
        Map<String, JavaCVFFmpegService.ScreenshotRequest> merged = new LinkedHashMap<>();
        appendScreenshotRequestsFromDtoPreferNew(merged, generatedRequests);
        appendScreenshotRequestsIfAbsent(merged, phase2aRequests);
        return new ArrayList<>(merged.values());
    }

    private void appendScreenshotRequestsIfAbsent(
            Map<String, JavaCVFFmpegService.ScreenshotRequest> merged,
            List<PythonGrpcClient.ScreenshotRequest> requests) {
        if (requests == null) return;
        for (PythonGrpcClient.ScreenshotRequest req : requests) {
            if (req == null) continue;
            String key = buildScreenshotKey(req.screenshotId, req.semanticUnitId, req.timestampSec);
            JavaCVFFmpegService.ScreenshotRequest existing = merged.get(key);
            if (existing != null) {
                if (!nearlyEqual(existing.timestampSec, req.timestampSec)
                    || !safeEq(existing.semanticUnitId, req.semanticUnitId)) {
                    logger.warn(
                        "[ScreenshotMerge] Skip Phase2A due to generated present: id={}, old_ts={}, new_ts={}, old_su={}, new_su={}",
                        req.screenshotId, existing.timestampSec, req.timestampSec, existing.semanticUnitId, req.semanticUnitId
                    );
                }
                continue;
            }
            merged.put(
                key,
                new JavaCVFFmpegService.ScreenshotRequest(req.screenshotId, req.timestampSec, req.label, req.semanticUnitId)
            );
        }
    }

    private void appendScreenshotRequestsFromDtoPreferNew(
            Map<String, JavaCVFFmpegService.ScreenshotRequest> merged,
            List<PythonGrpcClient.ScreenshotRequestDTO> requests) {
        if (requests == null) return;
        for (PythonGrpcClient.ScreenshotRequestDTO req : requests) {
            if (req == null) continue;
            String key = buildScreenshotKey(req.screenshotId, req.semanticUnitId, req.timestampSec);
            JavaCVFFmpegService.ScreenshotRequest existing = merged.get(key);
            if (existing != null) {
                if (!nearlyEqual(existing.timestampSec, req.timestampSec)
                    || !safeEq(existing.semanticUnitId, req.semanticUnitId)) {
                    logger.warn(
                        "[ScreenshotMerge] Override by generated: id={}, old_ts={}, new_ts={}, old_su={}, new_su={}",
                        req.screenshotId, existing.timestampSec, req.timestampSec, existing.semanticUnitId, req.semanticUnitId
                    );
                }
            }
            merged.put(
                key,
                new JavaCVFFmpegService.ScreenshotRequest(req.screenshotId, req.timestampSec, req.label, req.semanticUnitId)
            );
        }
    }

    private String buildScreenshotKey(String screenshotId, String semanticUnitId, double timestampSec) {
        String id = screenshotId != null ? screenshotId.trim() : "";
        if (!id.isEmpty()) {
            return "id:" + id;
        }
        String unit = semanticUnitId != null ? semanticUnitId.trim() : "";
        return "ts:" + unit + "|" + Double.toString(timestampSec);
    }

    private List<JavaCVFFmpegService.ClipRequest> mergeClipRequests(
            List<PythonGrpcClient.ClipRequest> phase2aRequests,
            List<PythonGrpcClient.ClipRequestDTO> generatedRequests) {
        // ??????????? Phase2A ????????????
        // ?????generatedRequests ????????????????????
        // ???? clipId ???? generated ?????????? FFmpeg ??
        Map<String, JavaCVFFmpegService.ClipRequest> merged = new LinkedHashMap<>();
        appendClipRequestsFromDtoPreferNew(merged, generatedRequests);
        appendClipRequestsIfAbsent(merged, phase2aRequests);
        return new ArrayList<>(merged.values());
    }

    private void appendClipRequestsIfAbsent(
            Map<String, JavaCVFFmpegService.ClipRequest> merged,
            List<PythonGrpcClient.ClipRequest> requests) {
        if (requests == null) return;
        for (PythonGrpcClient.ClipRequest req : requests) {
            if (req == null) continue;
            String key = buildClipKey(req.clipId, req.semanticUnitId, req.startSec, req.endSec);
            JavaCVFFmpegService.ClipRequest existing = merged.get(key);
            if (existing != null) {
                if (!nearlyEqual(existing.startSec, req.startSec)
                    || !nearlyEqual(existing.endSec, req.endSec)
                    || !safeEq(existing.semanticUnitId, req.semanticUnitId)) {
                    logger.warn(
                        "[ClipMerge] Skip Phase2A due to generated present: id={}, old=[{}-{}], new=[{}-{}], old_su={}, new_su={}",
                        req.clipId, existing.startSec, existing.endSec, req.startSec, req.endSec, existing.semanticUnitId, req.semanticUnitId
                    );
                }
                continue;
            }
            merged.put(
                key,
                new JavaCVFFmpegService.ClipRequest(
                    req.clipId,
                    req.startSec,
                    req.endSec,
                    req.knowledgeType,
                    req.semanticUnitId,
                    convertClipSegments(req.segments)
                )
            );
        }
    }

    private void appendClipRequestsFromDtoPreferNew(
            Map<String, JavaCVFFmpegService.ClipRequest> merged,
            List<PythonGrpcClient.ClipRequestDTO> requests) {
        if (requests == null) return;
        for (PythonGrpcClient.ClipRequestDTO req : requests) {
            if (req == null) continue;
            String key = buildClipKey(req.clipId, req.semanticUnitId, req.startSec, req.endSec);
            JavaCVFFmpegService.ClipRequest existing = merged.get(key);
            if (existing != null) {
                if (!nearlyEqual(existing.startSec, req.startSec)
                    || !nearlyEqual(existing.endSec, req.endSec)
                    || !safeEq(existing.semanticUnitId, req.semanticUnitId)) {
                    logger.warn(
                        "[ClipMerge] Override by generated: id={}, old=[{}-{}], new=[{}-{}], old_su={}, new_su={}",
                        req.clipId, existing.startSec, existing.endSec, req.startSec, req.endSec, existing.semanticUnitId, req.semanticUnitId
                    );
                }
            }
            merged.put(
                key,
                new JavaCVFFmpegService.ClipRequest(
                    req.clipId,
                    req.startSec,
                    req.endSec,
                    req.knowledgeType,
                    req.semanticUnitId,
                    convertClipSegments(req.segments)
                )
            );
        }
    }

    private String buildClipKey(String clipId, String semanticUnitId, double startSec, double endSec) {
        String id = clipId != null ? clipId.trim() : "";
        if (!id.isEmpty()) {
            return "id:" + id;
        }
        String unit = semanticUnitId != null ? semanticUnitId.trim() : "";
        return "range:" + unit + "|" + Double.toString(startSec) + "-" + Double.toString(endSec);
    }

    private List<JavaCVFFmpegService.ClipSegment> convertClipSegments(List<PythonGrpcClient.ClipSegment> segments) {
        List<JavaCVFFmpegService.ClipSegment> results = new ArrayList<>();
        if (segments == null || segments.isEmpty()) {
            return results;
        }
        for (PythonGrpcClient.ClipSegment seg : segments) {
            if (seg == null) {
                continue;
            }
            results.add(new JavaCVFFmpegService.ClipSegment(seg.startSec, seg.endSec));
        }
        return results;
    }

    private boolean nearlyEqual(double a, double b) {
        return Math.abs(a - b) <= 1e-3;
    }

    private boolean safeEq(String a, String b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.equals(b);
    }

    private void ensureActionIds(List<ActionSegmentResult> actions) {
        // ?? action_id???/??????????????
        if (actions == null || actions.isEmpty()) return;
        Set<Integer> used = new HashSet<>();
        int nextId = 1;
        for (ActionSegmentResult as : actions) {
            if (as == null) continue;
            if (as.id > 0 && !used.contains(as.id)) {
                used.add(as.id);
                continue;
            }
            while (used.contains(nextId)) {
                nextId++;
            }
            as.id = nextId;
            used.add(nextId);
            nextId++;
        }
    }
    
    // --- Helpers ---
    private List<SemanticUnitInput> convertToCVInputs(List<Map<String, Object>> units) {
        return units.stream().map(u -> {
            SemanticUnitInput in = new SemanticUnitInput();
            in.unitId = (String) u.get("unit_id");
            in.title = (String) u.getOrDefault("full_text", u.getOrDefault("knowledge_topic", ""));
            in.text = (String) u.getOrDefault("full_text", u.getOrDefault("text", ""));
            in.startSec = parseDouble(u.getOrDefault("start_sec", u.get("timestamp_start")), 0.0);
            in.endSec = parseDouble(u.getOrDefault("end_sec", u.get("timestamp_end")), 0.0);
            in.knowledgeType = (String) u.getOrDefault("knowledge_type", "");
            return in;
        }).collect(Collectors.toList());
    }
    
    private boolean isCVTask(String kType) {
        if (kType == null) return false;
        String t = kType.toLowerCase();
        // CV Types: Process, Practical (heavy CV)
        // CF Types: Explanation, Abstract, Configuration, Deduction (light CF)
        return t.contains("process") || t.contains("practical") || t.contains("过程") || t.contains("实操");
    }
    
    private List<ClassificationInput> convertToClassInputs(List<Map<String, Object>> units, Map<String, CVValidationUnitResult> cvResults) {
         return units.stream().map(u -> {
            ClassificationInput in = new ClassificationInput();
            String uid = (String) u.get("unit_id");
            in.unitId = uid;
            in.title = (String) u.get("title");
            in.text = (String) u.get("text");
            
            // First, load action units from Stage 1/Phase 2A (if any)
            List<Map<String, Object>> aus = (List<Map<String, Object>>) u.get("action_units");
            if (aus != null) {
                for (Map<String, Object> au : aus) {
                    ActionSegmentResult as = new ActionSegmentResult();
                    as.startSec = parseDouble(au.get("start_sec"), 0.0);
                    as.endSec = parseDouble(au.get("end_sec"), 0.0);
                    in.actionUnits.add(as);
                }
            }
            
            // SECOND, merge results from Parallel CV (which may have updated or added actions)
            if (cvResults.containsKey(uid)) {
                CVValidationUnitResult cvRes = cvResults.get(uid);
                if (cvRes.actionSegments != null) {
                    // If CV detected new segments, they take priority for classification
                    // For now, let's append them if actionUnits is empty, or merge carefully
                    if (in.actionUnits.isEmpty()) {
                        in.actionUnits.addAll(cvRes.actionSegments);
                    } else {
                        // TODO: Implement more complex merging if needed
                        // For now, we prefer the CV results as they are more accurate
                        in.actionUnits.clear();
                        in.actionUnits.addAll(cvRes.actionSegments);
                    }
                }
            }
            
            // ?? action_id???? action ???????????????
            ensureActionIds(in.actionUnits);
            // Removed: Subtitle mapping - Classifier reads directly from Step 2
            
            return in;
        }).collect(Collectors.toList());
    }
    
    private List<MaterialGenerationInput> convertToMatInputs(List<Map<String, Object>> units, Map<String, CVValidationUnitResult> cvResults) {
        return units.stream().map(u -> {
            MaterialGenerationInput in = new MaterialGenerationInput();
            String uid = (String) u.get("unit_id");
            in.unitId = uid;
            in.startSec = parseDouble(u.getOrDefault("start_sec", u.get("timestamp_start")), 0.0);
            in.endSec = parseDouble(u.getOrDefault("end_sec", u.get("timestamp_end")), 0.0);
            in.knowledgeType = (String) u.getOrDefault("knowledge_type", "");
            in.fullText = (String) u.getOrDefault("full_text", u.getOrDefault("text", ""));
            
            // ???? action_units ??????????????????????
            List<Map<String, Object>> unitActions = (List<Map<String, Object>>) u.get("action_units");
            if (unitActions != null && !unitActions.isEmpty()) {
                // 鐭棩蹇楋細瀹氫綅 JSON -> Java 鏄惁鎷垮埌 action_units.knowledge_type
                Object firstKt = unitActions.get(0).get("knowledge_type");
                logger.info("[{}] MatInputs from semantic_units: unit={}, actions={}, first_kt={}",
                    "MaterialGen", uid, unitActions.size(), firstKt);
                for (Map<String, Object> au : unitActions) {
                    ActionSegmentResult as = new ActionSegmentResult();
                    as.id = parseInt(au.get("id"), 0);
                    as.startSec = parseDouble(au.get("start_sec"), 0.0);
                    as.endSec = parseDouble(au.get("end_sec"), 0.0);
                    String kt = au.get("knowledge_type") != null ? au.get("knowledge_type").toString() : "";
                    String fallback = in.knowledgeType != null ? in.knowledgeType : "";
                    // ???? action_type ???????knowledge????????
                    as.actionType = !kt.isEmpty() ? kt : fallback;
                    in.actionUnits.add(as);
                }
            } else if (cvResults.containsKey(uid)) {
                logger.info("[{}] MatInputs fallback to CV actionSegments: unit={}, actions=0",
                    "MaterialGen", uid);
                // ????? action_units ?????? CV ????????????
                CVValidationUnitResult cvRes = cvResults.get(uid);
                if (cvRes.actionSegments != null) {
                    for (ActionSegmentResult as : cvRes.actionSegments) {
                        if (in.knowledgeType != null && !in.knowledgeType.isEmpty()) {
                            as.actionType = in.knowledgeType;
                        }
                        in.actionUnits.add(as);
                    }
                }
            }

            // ????????????????????
            if (cvResults.containsKey(uid)) {
                CVValidationUnitResult cvRes = cvResults.get(uid);
                if (cvRes.stableIslands != null) {
                    in.stableIslands.addAll(cvRes.stableIslands);
                }
            }
            // ?? action_id????????????????
            ensureActionIds(in.actionUnits);
            return in;
        }).collect(Collectors.toList());
    }
    
    private void updateSemanticUnits(List<Map<String, Object>> units, 
                                     Map<String, CVValidationUnitResult> cvResults,
                                     List<KnowledgeResultItem> classResults) {
        // Map for fast lookup: unitId -> actionId -> result
        Map<String, Map<Integer, KnowledgeResultItem>> classMap = classResults.stream()
            .collect(Collectors.groupingBy(k -> k.unitId,
                Collectors.toMap(k -> k.actionId, k -> k, (k1, k2) -> k1)));
            
        for (Map<String, Object> unit : units) {
            String uid = (String) unit.get("unit_id");
            
            // V7.6: Always update top-level knowledge_type first
            // This ensures Phase 2B Python pipeline sees the correct classification 
            // even if CV modality is screenshot (no actions) or other edge cases.
            // V7.8: Do NOT overwrite Unit-Level knowledge_type with Action-Level classification.
            // The LLM results are specific to individual actions (e.g. "Explainer" action within "Process" unit).
            // We should trust the Unit Type from Stage 1 (Segmentation) or explicit Unit classification (if added later).
            // if (classMap.containsKey(uid)) { ... } // REMOVED
            
            // 1. Update CV results (Sync structure with Python expectation)
            if (cvResults.containsKey(uid)) {
                CVValidationUnitResult cvRes = cvResults.get(uid);
                unit.put("cv_validated", true);

                
                List<Map<String, Object>> actionsOut = new ArrayList<>();
                if (cvRes.actionSegments != null) {
                    for (ActionSegmentResult as : cvRes.actionSegments) {
                        Map<String, Object> actionMap = new java.util.HashMap<>();
                        actionMap.put("start_sec", as.startSec);
                        actionMap.put("end_sec", as.endSec);
                        actionMap.put("action_type", as.actionType);
                        actionMap.put("id", as.id);
                        
                        // 2. Apply Knowledge Type to this specific action
                        // ???? knowledge_type ???????????????
                        String unitKt = unit.get("knowledge_type") != null ? unit.get("knowledge_type").toString() : "";
                        actionMap.put("knowledge_type", unitKt);

                        if (classMap.containsKey(uid) && classMap.get(uid).containsKey(as.id)) {
                            KnowledgeResultItem kri = classMap.get(uid).get(as.id);
                            actionMap.put("knowledge_type", kri.knowledgeType);
                            actionMap.put("reasoning", kri.reasoning);
                            actionMap.put("confidence", kri.confidence);
                        } else {
                            logger.warn(
                                "[SemanticWriteBack] Missing action-level knowledge_type: unit={}, action_id={}, range=[{}-{}], actionType={}, unit_kt={}",
                                uid, as.id, as.startSec, as.endSec, as.actionType, unitKt
                            );
                        }
                        actionsOut.add(actionMap);
                    }
                }
                unit.put("action_units", actionsOut);
            }
        }
    }
    
    private double parseDouble(Object val, double defaultVal) {
        if (val == null) return defaultVal;
        if (val instanceof Number) return ((Number) val).doubleValue();
        try {
            return Double.parseDouble(val.toString());
        } catch (Exception e) {
            return defaultVal;
        }
    }

    private int parseInt(Object val, int defaultVal) {
        if (val == null) return defaultVal;
        if (val instanceof Number) return ((Number) val).intValue();
        try {
            return Integer.parseInt(val.toString());
        } catch (Exception e) {
            return defaultVal;
        }
    }

    private void saveUpdatedSemantics(File file, Object root) {
        try {
            // Pretty Print for better readability
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file, root);
        } catch(IOException e) {
            logger.error("Failed to save updated semantics", e);
        }
    }

    private JsonNode loadSemanticUnitsRoot(AnalyzeResult analyzeResult) throws IOException {
        if (analyzeResult != null && analyzeResult.semanticUnitsInline != null) {
            SemanticUnitsInlineDTO inline = analyzeResult.semanticUnitsInline;
            if (inline.payload != null && inline.payload.length > 0) {
                byte[] decoded = decodeSemanticUnitsInlinePayload(inline);
                return objectMapper.readTree(decoded);
            }
        }
        throw new IOException("semantic units source missing: inline payload unavailable");
    }

    private byte[] decodeSemanticUnitsInlinePayload(SemanticUnitsInlineDTO inline) throws IOException {
        String codec = inline.codec != null ? inline.codec.trim().toLowerCase(Locale.ROOT) : "";
        if ("json-utf8-gzip".equals(codec) || "gzip".equals(codec)) {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(inline.payload);
                 GZIPInputStream gis = new GZIPInputStream(bais);
                 ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                gis.transferTo(baos);
                return baos.toByteArray();
            }
        }
        return inline.payload;
    }

    private SemanticUnitsInlineDTO buildSemanticUnitsInlineDTO(Object root, int unitCount) {
        try {
            byte[] raw = objectMapper.writeValueAsBytes(root);
            byte[] compressed = gzip(raw);
            byte[] payload;
            String codec;
            if (compressed.length < raw.length) {
                payload = compressed;
                codec = "json-utf8-gzip";
            } else {
                payload = raw;
                codec = "json-utf8";
            }
            SemanticUnitsInlineDTO inline = new SemanticUnitsInlineDTO();
            inline.payload = payload;
            inline.codec = codec;
            inline.unitCount = Math.max(unitCount, 0);
            inline.sha256 = sha256Hex(payload);
            return inline;
        } catch (IOException error) {
            logger.warn("Failed to build semantic_units_inline payload", error);
            return null;
        }
    }

    private byte[] gzip(byte[] raw) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gos = new GZIPOutputStream(baos)) {
            gos.write(raw);
            gos.finish();
            return baos.toByteArray();
        }
    }

    private String sha256Hex(byte[] payload) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(payload);
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception error) {
            logger.warn("Failed to calculate SHA-256", error);
            return "";
        }
    }

    private void updateAnalyzeResultInlinePayload(AnalyzeResult analyzeResult, Object updatedRoot, int unitCount) {
        if (analyzeResult == null) {
            return;
        }
        SemanticUnitsInlineDTO inline = buildSemanticUnitsInlineDTO(updatedRoot, unitCount);
        if (inline != null) {
            analyzeResult.semanticUnitsInline = inline;
        }
        // 语义内容已被 Java 侧更新，旧 ref 指向的是旧版本缓存，需主动失效避免误用。
        analyzeResult.semanticUnitsRef = null;
    }
    
    // Removed: enrichUnitsWithSubtitles method
    // Classifier now reads subtitles directly from step2_path
    
    private void updateProgress(String taskId, double progress, String message) {
        if (progressCallback != null) progressCallback.onProgress(taskId, progress, message);
        logger.info("[{}] {} ({}%)", taskId, message, (int)(progress * 100));
    }
    
    public CompletableFuture<ProcessingResult> submitTaskAsync(String videoUrl, String outputDir) {
        String taskId = "task_" + taskCounter.incrementAndGet() + "_" + System.currentTimeMillis();
        return CompletableFuture.supplyAsync(() -> processVideo(taskId, videoUrl, outputDir));
    }
    
    public Map<String, TaskContext> getActiveTasks() { return new ConcurrentHashMap<>(activeTasks); }

    // --- Helper Methods ---
    
    private List<JavaCVFFmpegService.ScreenshotRequest> convertScreenshotRequests(List<ScreenshotRequestDTO> dtos) {
        List<JavaCVFFmpegService.ScreenshotRequest> list = new ArrayList<>();
        if (dtos == null) return list;
        for (ScreenshotRequestDTO dto : dtos) {
            list.add(new JavaCVFFmpegService.ScreenshotRequest(
                dto.screenshotId, dto.timestampSec, dto.label, dto.semanticUnitId
            ));
        }
        return list;
    }

    private List<JavaCVFFmpegService.ClipRequest> convertClipRequests(List<ClipRequestDTO> dtos) {
        List<JavaCVFFmpegService.ClipRequest> list = new ArrayList<>();
        if (dtos == null) return list;
        for (ClipRequestDTO dto : dtos) {
            list.add(new JavaCVFFmpegService.ClipRequest(
                dto.clipId,
                dto.startSec,
                dto.endSec,
                dto.knowledgeType,
                dto.semanticUnitId,
                convertClipSegments(dto.segments)
            ));
        }
        return list;
    }

    // --- Extracted Methods ---

    /**
     * ???? VL ??????????????????? null ????
     */
    private ExtractionRequests tryVLAnalysis(String taskId, String videoPath, AnalyzeResult ar, String outputDir, DynamicTimeoutCalculator.TimeoutConfig timeouts) {
        updateProgress(taskId, 0.40, "执行 VL 视觉语言模型分析...");
        try {
            VLAnalysisResult vlResult = grpcClient.analyzeWithVLAsync(
                taskId, videoPath, ar, outputDir, 
                timeouts.getPhase2aTimeoutSec())
                .get(timeouts.getPhase2aTimeoutSec() + 60, TimeUnit.SECONDS);

            if (vlResult.success && vlResult.vlEnabled && !vlResult.usedFallback) {
                logger.info("[{}] VL Analysis Success! Skipping legacy flow.", taskId);
                List<JavaCVFFmpegService.ScreenshotRequest> screenshots = convertScreenshotRequests(vlResult.screenshotRequests);
                List<JavaCVFFmpegService.ClipRequest> clips = convertClipRequests(vlResult.clipRequests);
                return startExtractionPipeline(taskId, videoPath, outputDir, screenshots, clips, 0);
            } else {
                logger.warn("[{}] VL Analysis fallback reason: {}", taskId, vlResult.errorMsg);
            }
        } catch (Exception e) {
            logger.error("[{}] VL Analysis failed with exception", taskId, e);
        }
        return null;
    }

    /**
     * ?????????CV/LLM ?? -> ???? -> ??????
     */
    private ExtractionRequests runLegacyAnalysis(String taskId, String videoPath, AnalyzeResult ar, Stage1Result s1, String outputDir, DynamicTimeoutCalculator.TimeoutConfig timeouts) throws Exception {
        // Load Semantic Units：仅使用 AnalyzeResponse 内联载荷（协议已移除路径字段）
        JsonNode rootNode = loadSemanticUnitsRoot(ar);
        
        final boolean originallyArray = rootNode.isArray();
        final Map<String, Object> unitsMap;
        final List<Map<String, Object>> unitsList;
        
        if (originallyArray) {
            unitsList = objectMapper.convertValue(rootNode, new TypeReference<List<Map<String, Object>>>() {});
            unitsMap = new HashMap<>(); 
        } else {
            unitsMap = objectMapper.convertValue(rootNode, new TypeReference<Map<String, Object>>() {});
            List<Map<String, Object>> list = (List<Map<String, Object>>) unitsMap.get("semantic_units");
            unitsList = list != null ? list : new ArrayList<>();
        }

        // 1. Execute Core Analysis (CV Validation + Knowledge Classification)
        // ?????????????
        AnalysisResults analysisResults = executeHybridAnalysis(taskId, videoPath, unitsList, s1.step2JsonPath, outputDir);

        // 2. Merge & Update
        updateSemanticUnits(unitsList, analysisResults.cvResults, analysisResults.classResults);
        Object updatedRoot = originallyArray ? unitsList : unitsMap;
        updateAnalyzeResultInlinePayload(ar, updatedRoot, unitsList.size());

        // 3. Generate Material Requests
        updateProgress(taskId, 0.70, "生成素材清单...");
        List<MaterialGenerationInput> matInputs = convertToMatInputs(unitsList, analysisResults.cvResults);
        MaterialGenerationResult matRes = grpcClient.generateMaterialRequestsAsync(taskId, matInputs, videoPath, 600).get(10, TimeUnit.MINUTES);
        if (!matRes.success) throw new RuntimeException("Material Gen failed: " + matRes.errorMsg);

        // 4. Merge Requests
        List<JavaCVFFmpegService.ScreenshotRequest> screenshots =
            mergeScreenshotRequests(ar.screenshotRequests, matRes.screenshotRequests);
        List<JavaCVFFmpegService.ClipRequest> clips =
            mergeClipRequests(ar.clipRequests, matRes.clipRequests);
        return startExtractionPipeline(taskId, videoPath, outputDir, screenshots, clips, 0);
    }

    private ExtractionRequests startExtractionPipeline(
            String taskId,
            String videoPath,
            String outputDir,
            List<JavaCVFFmpegService.ScreenshotRequest> screenshots,
            List<JavaCVFFmpegService.ClipRequest> clips,
            int timeoutSecHint
    ) {
        List<JavaCVFFmpegService.ScreenshotRequest> ss = screenshots != null ? screenshots : new ArrayList<>();
        List<JavaCVFFmpegService.ClipRequest> cp = clips != null ? clips : new ArrayList<>();

        logger.info(
            "[{}] Start extraction producer-consumer pipeline: screenshots={}, clips={}",
            taskId,
            ss.size(),
            cp.size()
        );

        int extractionTimeout = timeoutSecHint > 0 ? timeoutSecHint : 3600;
        CompletableFuture<JavaCVFFmpegService.ExtractionResult> extractionFuture =
            ffmpegService.extractAllAsync(videoPath, outputDir, ss, cp, extractionTimeout);

        return new ExtractionRequests(ss, cp, extractionFuture);
    }

    /**
     * ???????????????????????
     */
    private AnalysisResults executeHybridAnalysis(String taskId, String videoPath, List<Map<String, Object>> unitsList, String step2JsonPath, String outputDir) {
        updateProgress(taskId, 0.45, "执行级联并行分析 (CV/CF 混合调度)...");
        
        Map<String, CVValidationUnitResult> cvResults = new ConcurrentHashMap<>();
        List<KnowledgeResultItem> classResults = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<?>> allFutures = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Boolean>> cvFuturesList = new ArrayList<>();

        // A. Convert ALL units to CV Inputs & Sort
        List<SemanticUnitInput> allInputs = convertToCVInputs(unitsList);

        // Cache Check
        Map<String, CVValidationUnitResult> cachedCv = cvOrchestrator.tryLoadCachedResults(taskId, videoPath, allInputs, outputDir);
        boolean cvCacheHit = cachedCv != null && !cachedCv.isEmpty();
        boolean classCacheHit = false;
        List<ClassificationInput> cachedClassInputs = null;

        if (cvCacheHit) {
            cvResults.putAll(cachedCv);
            cachedClassInputs = convertToClassInputs(unitsList, cvResults);
            List<KnowledgeResultItem> cachedClass = knowledgeOrchestrator.tryLoadCachedResults(taskId, cachedClassInputs, step2JsonPath, outputDir);
            if (cachedClass != null && !cachedClass.isEmpty()) {
                classResults.addAll(cachedClass);
                classCacheHit = true;
            }
        }

        if (!cvCacheHit) {
            // Weighted LPT Scheduling
            Collections.sort(allInputs, (o1, o2) -> {
                double w1 = isCVTask(o1.knowledgeType) ? 10.0 : 1.0;
                double w2 = isCVTask(o2.knowledgeType) ? 10.0 : 1.0;
                double score1 = (o1.endSec - o1.startSec) * w1;
                double score2 = (o2.endSec - o2.startSec) * w2;
                return Double.compare(score2, score1);
            });

            if (!allInputs.isEmpty()) {
                List<CompletableFuture<Boolean>> cvFutures = cvOrchestrator.validateBatchesAsync(taskId, videoPath, allInputs, outputDir, unitResult -> {
                    cvResults.put(unitResult.unitId, unitResult);
                    
                    // Immediate Classification Chain
                    List<Map<String, Object>> unitToClassify = unitsList.stream()
                        .filter(u -> unitResult.unitId.equals((String)u.get("unit_id")))
                        .collect(Collectors.toList());

                    if (!unitToClassify.isEmpty()) {
                        List<ClassificationInput> classInputs = convertToClassInputs(unitToClassify, cvResults);
                        CompletableFuture<Void> classFuture = knowledgeOrchestrator.classifyBatchAsync(taskId, classInputs, step2JsonPath)
                            .thenAccept(classBatchRes -> {
                                if (classBatchRes.success && classBatchRes.results != null) {
                                    classResults.addAll(classBatchRes.results);
                                    logger.info("[{}] Incremental classification done for: {}", taskId, unitResult.unitId);
                                }
                            });
                        allFutures.add(classFuture);
                    }
                });
                if (cvFutures != null) cvFuturesList.addAll(cvFutures);
            }

            if (!cvFuturesList.isEmpty()) {
                CompletableFuture.allOf(cvFuturesList.toArray(new CompletableFuture[0])).join();
            }

            // Wait for all classification tasks
            while (true) {
                CompletableFuture<?>[] pending;
                synchronized(allFutures) {
                    pending = allFutures.stream().filter(f -> !f.isDone()).toArray(CompletableFuture[]::new);
                }
                if (pending.length == 0) break;
                CompletableFuture.allOf(pending).join();
            }
        } else if (!classCacheHit) {
            // CV Hit but Class Miss
            List<ClassificationInput> classInputs = cachedClassInputs != null ? cachedClassInputs : convertToClassInputs(unitsList, cvResults);
            List<KnowledgeResultItem> batchRes = knowledgeOrchestrator.classifyParallel(taskId, classInputs, step2JsonPath, outputDir);
            if (batchRes != null) classResults.addAll(batchRes);
        } else {
            logger.info("[{}] Reusing CV + LLM caches.", taskId);
        }

        // Cache Save
        if (!cvResults.isEmpty()) cvOrchestrator.saveCache(taskId, videoPath, allInputs, outputDir, cvResults);
        if (!classResults.isEmpty()) {
            List<ClassificationInput> classInputsForCache = cachedClassInputs != null ? cachedClassInputs : convertToClassInputs(unitsList, cvResults);
            knowledgeOrchestrator.saveCache(taskId, classResults, classInputsForCache, step2JsonPath, outputDir);
        }
        
        logger.info("✅ Staged Analysis done. CV: {}, Class: {}", cvResults.size(), classResults.size());
        return new AnalysisResults(cvResults, classResults);
    }
}

