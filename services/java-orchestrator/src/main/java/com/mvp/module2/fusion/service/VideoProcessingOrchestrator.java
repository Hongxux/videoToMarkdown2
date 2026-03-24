package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.grpc.PythonGrpcClient.*;
import com.mvp.module2.fusion.service.watchdog.TaskProgressWatchdogBridge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URLDecoder;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Locale;
import java.time.Instant;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 视频处理编排器（V3 并行版）。
 *
 * 主流程：
 * 1. 下载或准备输入视频。
 * 2. 执行 Stage1，生成基础结构化数据。
 * 3. 执行 Phase2A，产出语义单元分析结果。
 * 4. 优先尝试 VL；若失败或回退则走 Legacy（CV + LLM）链路。
 * 5. 提取截图与片段素材。
 * 6. 执行 Phase2B，组装富文本与 Markdown。
 *
 * 该类负责跨阶段编排、超时控制、缓存复用和进度上报。
 */
@Service
public class VideoProcessingOrchestrator {
    
    private static final Logger logger = LoggerFactory.getLogger(VideoProcessingOrchestrator.class);

    // 素材提取请求容器：同时承载请求列表与可复用的异步提取任务。
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

    // 核心分析结果容器：包含 CV 验证结果与知识分类结果。
    private static class AnalysisResults {
        Map<String, CVValidationUnitResult> cvResults;
        List<KnowledgeResultItem> classResults;
        
        public AnalysisResults(Map<String, CVValidationUnitResult> cv, List<KnowledgeResultItem> cls) {
            this.cvResults = cv != null ? cv : new ConcurrentHashMap<>();
            this.classResults = cls != null ? cls : new ArrayList<>();
        }
    }

    private static class AssetExtractRuntimeWave {
        final String substageName;
        final String waveId;
        final String inputFingerprint;
        final String substageScopeId;
        final String chunkScopeId;
        final String substageScopeRef;
        final String chunkScopeRef;

        private AssetExtractRuntimeWave(
                String substageName,
                String waveId,
                String inputFingerprint,
                String substageScopeId,
                String chunkScopeId,
                String substageScopeRef,
                String chunkScopeRef
        ) {
            this.substageName = substageName;
            this.waveId = waveId;
            this.inputFingerprint = inputFingerprint;
            this.substageScopeId = substageScopeId;
            this.chunkScopeId = chunkScopeId;
            this.substageScopeRef = substageScopeRef;
            this.chunkScopeRef = chunkScopeRef;
        }
    }

    private static class ArticleBookSource {
        String requestedUrl;
        String finalUrl;
        String siteType;
        String title;
        String markdownPath;
        String outputDir;
        int imageCount;
        int copiedImageCount;
    }
    
    @Autowired
    private PythonGrpcClient grpcClient;
    
    // 通过 JNI/进程方式调度 FFmpeg 能力，负责截图与切片提取。
    @Autowired
    private JavaCVFFmpegService ffmpegService;
    
    @Autowired
    private DynamicTimeoutCalculator timeoutCalculator;
    
    @Autowired
    private CVValidationOrchestrator cvOrchestrator;
    
    @Autowired
    private KnowledgeClassificationOrchestrator knowledgeOrchestrator;
    
    @Autowired
    private ModuleConfigService configService;

    @Autowired
    private TaskProgressWatchdogBridge taskProgressWatchdogBridge;

    @Autowired
    private BookMarkdownService bookMarkdownService;

    @Autowired(required = false)
    private BookEnhancedPipelineService bookEnhancedPipelineService;

    @Autowired(required = false)
    private Phase2bArticleLinkService phase2bArticleLinkService;

    @Autowired(required = false)
    private StorageTaskCategoryService storageTaskCategoryService;

    @Autowired(required = false)
    private TaskRuntimeStageStore taskRuntimeStageStore;

    private final AssetExtractRuntimeRepositoryAdapter assetExtractRuntimeRepositoryAdapter =
            new AssetExtractRuntimeRepositoryAdapter();

    private VideoMetaService videoMetaService = new VideoMetaService();
    
    // 运行时任务上下文缓存。
    private final ConcurrentHashMap<String, TaskContext> activeTasks = new ConcurrentHashMap<>();
    private final AtomicInteger taskCounter = new AtomicInteger(0);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired(required = false)
    public void setVideoMetaService(VideoMetaService videoMetaService) {
        if (videoMetaService != null) {
            this.videoMetaService = videoMetaService;
        }
    }

    // LLM pricing fallback (CNY / 1M tokens)
    private static final double QWEN3_VL_PLUS_INPUT_PER_M = 1.50d;
    private static final double QWEN3_VL_PLUS_OUTPUT_PER_M = 4.50d;
    private static final double ERNIE_45_TURBO_VL_INPUT_MIN_PER_M = 0.80d;
    private static final double ERNIE_45_TURBO_VL_INPUT_MAX_PER_M = 1.50d;
    private static final double ERNIE_45_TURBO_VL_OUTPUT_MIN_PER_M = 3.20d;
    private static final double ERNIE_45_TURBO_VL_OUTPUT_MAX_PER_M = 4.50d;
    private static final double DEEPSEEK_CHAT_INPUT_UNCACHED_PER_M = 2.00d;
    private static final double DEEPSEEK_CHAT_INPUT_CACHED_PER_M = 0.20d;
    private static final double DEEPSEEK_CHAT_OUTPUT_PER_M = 3.00d;
    private static final double QWEN_PLUS_INPUT_PER_M = 0.80d;
    private static final double QWEN_PLUS_OUTPUT_PER_M = 2.00d;
    private static final Pattern BILIBILI_BV_PATTERN =
        Pattern.compile("BV[0-9A-Za-z]{10}", Pattern.CASE_INSENSITIVE);
    private static final Pattern BILIBILI_AV_PATTERN =
        Pattern.compile("(?:^|[^0-9A-Za-z])av(\\d{1,20})(?:$|[^0-9A-Za-z])", Pattern.CASE_INSENSITIVE);
    private static final Set<String> BOOK_FILE_EXTENSIONS = Set.of(".txt", ".md", ".pdf", ".epub");
    private static final double DEFAULT_VL_PROCESS_DURATION_THRESHOLD_SEC = 20.0d;
    private static final double VL_ANALYZE_WORKLOAD_TIMEOUT_MULTIPLIER = 3.0d;
    private static final int MIN_VL_ANALYZE_TIMEOUT_SEC = 120;

    @Value("${video.download.grpc-deadline-seconds:1800}")
    private int downloadGrpcDeadlineSec;

    @Value("${video.download.hard-timeout-seconds:1800}")
    private int downloadHardTimeoutSec;

    @Value("${video.download.idle-timeout-seconds:120}")
    private int downloadIdleTimeoutSec;

    @Value("${video.download.poll-interval-seconds:5}")
    private int downloadPollIntervalSec;

    @Value("${video.download.watchdog-scan-depth:2}")
    private int downloadWatchdogScanDepth;

    @Value("${video.download.watchdog-min-rescan-ms:1000}")
    private long downloadWatchdogMinRescanMs;
    
    // 进度回调接口（函数式接口）。
    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(String taskId, double progress, String message);
    }
    private volatile ProgressCallback progressCallback;
    private final ConcurrentHashMap<String, ProgressCallback> taskProgressCallbacks = new ConcurrentHashMap<>();

    public void setProgressCallback(ProgressCallback callback) {
        this.progressCallback = callback;
    }

    public void setProgressCallback(String taskId, ProgressCallback callback) {
        if (taskId == null || taskId.isBlank()) {
            return;
        }
        if (callback == null) {
            taskProgressCallbacks.remove(taskId);
            return;
        }
        taskProgressCallbacks.put(taskId, callback);
    }

    public void clearProgressCallback(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return;
        }
        taskProgressCallbacks.remove(taskId);
    }
    
    // --- Context Classes ---
    public static class ProcessingResult {
        public boolean success;
        public String taskId;
        public String markdownPath;
        public String jsonPath;
        public String cleanupSourcePath;
        public String errorMessage;
        public long processingTimeMs;
    }

    public static class IOPhaseResult {
        public String taskId;
        public String videoUrl;
        public String videoPath;
        public String outputDir;
        public double videoDuration;
        public boolean downloadedFromUrl;
        public String cleanupSourcePath;
        public DownloadResult downloadResult;
        public String subtitlePath;
        public Stage1Result stage1Result;
        public DynamicTimeoutCalculator.TimeoutConfig timeouts;
        public long pipelineStartTimeMs;
        public String metricsOutputDir;
        public String metricsVideoPath;
        public String metricsInputVideoUrl;
        public String metricsVideoTitle;
        public Map<String, Long> stageTimingsMs = new LinkedHashMap<>();
        public Map<String, Object> flowFlags = new LinkedHashMap<>();
        public String recoveryStartStage;
        public String phase2aSemanticUnitsPath;
        public AnalyzeResult phase2aAnalyzeResult;
        public String phase2bMarkdownPath;
        public String phase2bJsonPath;
    }
    
    /**
     * 任务上下文。
     */
    public static class TaskContext {
        public String taskId;
        public String videoUrl;
        public String outputDir;
        public double videoDuration;
        public long startTime;
    }

    public static class BookProcessingOptions {
        public String chapterSelector;
        public String sectionSelector;
        public Boolean splitByChapter;
        public Boolean splitBySection;
        public Integer pageOffset;
        public String bookTitle;
        public String leafTitle;
        public String leafOutlineIndex;
        public String storageKey;
    }

    /**
     * 视频处理入口。
     */
    public ProcessingResult processVideo(String taskId, String videoUrl, String outputDir) {
        return processVideo(taskId, videoUrl, outputDir, null);
    }

    public ProcessingResult processVideo(
            String taskId,
            String videoUrl,
            String outputDir,
            BookProcessingOptions bookOptions
    ) {
        if (shouldProcessAsBook(videoUrl, bookOptions)) {
            return processBook(taskId, videoUrl, outputDir, bookOptions);
        }
        return processVideoInternal(taskId, videoUrl, outputDir);
    }

    private ProcessingResult processVideoInternal(String taskId, String videoUrl, String outputDir) {
        try {
            IOPhaseResult ioResult = processVideoDownloadPhase(taskId, videoUrl, outputDir);
            ioResult = processVideoTranscribePhase(taskId, ioResult);
            ioResult = processVideoStage1Phase(taskId, ioResult);
            return processVideoLLMPhase(taskId, ioResult);
        } catch (Exception error) {
            ProcessingResult failed = new ProcessingResult();
            failed.taskId = taskId;
            failed.success = false;
            failed.errorMessage = normalizeThrowableMessage(error, "Video pipeline failed with unknown throwable");
            logger.error("Pipeline Failed: {} - {}", taskId, failed.errorMessage, error);
            return failed;
        }
        /*
        ProcessingResult result = new ProcessingResult();
        result.taskId = taskId;
        long startTime = System.currentTimeMillis();
        String metricsOutputDir = outputDir;
        String metricsVideoPath = videoUrl;
        String metricsInputVideoUrl = videoUrl;
        String metricsVideoTitle = "";
        Map<String, Long> stageTimingsMs = new LinkedHashMap<>();
        Map<String, Object> flowFlags = new LinkedHashMap<>();

        try {
            String videoPath = videoUrl;
            double videoDuration = 60;
            boolean downloadedFromUrl = false;
            String cleanupSourcePath = null;
            DownloadResult downloadResult = null;
            boolean usedVLFlow = false;
            boolean usedLegacyFlow = false;

            long localPrepareStart = System.currentTimeMillis();
            if (!isHttpUrl(videoUrl)) {
                videoPath = normalizeLocalVideoPath(videoUrl);
                assertLocalVideoExists(videoUrl, videoPath);
                outputDir = resolveOutputDirForLocalVideo(videoPath);
                new File(outputDir).mkdirs();
                logger.info("[{}] 本地视频路径校验通过，输出目录已准备 -> {}", taskId, outputDir);

                videoPath = ensureLocalVideoInStorage(videoPath, outputDir);
                videoDuration = resolveVideoDurationSec(taskId, videoPath, videoDuration);
            }
            stageTimingsMs.put("prepare_local_video", System.currentTimeMillis() - localPrepareStart);
            metricsVideoPath = videoPath;
            metricsOutputDir = outputDir;

            long downloadStart = System.currentTimeMillis();
            try {
                if (isHttpUrl(videoUrl)) {
                    int downloadTimeoutSec = normalizePositive(downloadGrpcDeadlineSec, 1800);
                    int hardTimeoutSec = Math.max(downloadTimeoutSec, normalizePositive(downloadHardTimeoutSec, 1800));
                    int idleTimeoutSec = normalizePositive(downloadIdleTimeoutSec, 120);
                    int pollIntervalSec = Math.min(
                        normalizePositive(downloadPollIntervalSec, 5),
                        Math.max(1, idleTimeoutSec)
                    );
                    String predictedDownloadDir = resolvePredictedDownloadWatchDir(videoUrl, outputDir);
                    logger.info(
                        "[{}] Download watchdog targets: request_output_dir={}, predicted_storage_dir={}",
                        taskId,
                        firstNonBlank(outputDir, "(empty)"),
                        firstNonBlank(predictedDownloadDir, "(empty)")
                    );
                    updateProgress(taskId, 0.05, "Downloading video...");
                    DownloadResult dl = waitForDownloadWithLease(
                        taskId,
                        videoUrl,
                        outputDir,
                        predictedDownloadDir,
                        downloadTimeoutSec,
                        hardTimeoutSec,
                        idleTimeoutSec,
                        pollIntervalSec
                    );
                    if (!dl.success) {
                        throw new RuntimeException(
                            "Download failed: " + firstNonBlank(
                                dl.errorMsg,
                                "python worker returned unsuccessful download response without details"
                            )
                        );
                    }
                    downloadedFromUrl = true;
                    downloadResult = dl;
                    if (dl.contentType != null && !dl.contentType.isBlank() && !"video".equalsIgnoreCase(dl.contentType)) {
                        logger.warn(
                            "[{}] Download content_type={} detected, pipeline keeps video-first path",
                            taskId,
                            dl.contentType
                        );
                    }
                    videoPath = dl.videoPath;
                    cleanupSourcePath = dl.videoPath;
                    videoDuration = dl.durationSec;
                    outputDir = new File(videoPath).getParentFile().getAbsolutePath();
                    new File(outputDir).mkdirs();
                    metricsVideoPath = videoPath;
                    metricsOutputDir = outputDir;
                }
            } finally {
                stageTimingsMs.put("download_video", System.currentTimeMillis() - downloadStart);
            }

            if (videoDuration <= 0) {
                videoDuration = resolveVideoDurationSec(taskId, videoPath, videoDuration);
            }

            DynamicTimeoutCalculator.TimeoutConfig timeouts = timeoutCalculator.calculateTimeouts(videoDuration);
            TaskProgressWatchdogBridge.SignalEmitter taskSignalEmitter =
                (progress, message) -> updateProgress(taskId, progress, message);


            updateProgress(taskId, 0.15, "正在进行语音转写...");
            long transcribeStart = System.currentTimeMillis();
            taskProgressWatchdogBridge.resetTask(taskId);
            TaskProgressWatchdogBridge.MonitorHandle transcribeMonitor =
                taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "transcribe", taskSignalEmitter);
            TranscribeResult tr;
            try {
                tr = grpcClient.transcribeVideo(
                    taskId,
                    videoPath,
                    "auto",
                    timeouts.getTranscribeTimeoutSec()
                );
            } finally {
                taskProgressWatchdogBridge.stopMonitor(taskId, transcribeMonitor, taskSignalEmitter);
            }
            if (!tr.success) {
                throw new RuntimeException("Transcribe failed: " + tr.errorMsg);
            }
            stageTimingsMs.put("transcribe", System.currentTimeMillis() - transcribeStart);

            updateProgress(taskId, 0.25, "正在执行阶段一处理...");
            long stage1Start = System.currentTimeMillis();
            taskProgressWatchdogBridge.resetTask(taskId);
            TaskProgressWatchdogBridge.MonitorHandle stage1Monitor =
                taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "stage1", taskSignalEmitter);
            Stage1Result s1;
            try {
                s1 = grpcClient.processStage1(
                    taskId,
                    videoPath,
                    tr.subtitlePath,
                    outputDir,
                    6,
                    timeouts.getStage1TimeoutSec()
                );
            } finally {
                taskProgressWatchdogBridge.stopMonitor(taskId, stage1Monitor, taskSignalEmitter);
            }
            if (!s1.success) {
                throw new RuntimeException("Stage1 failed: " + s1.errorMsg);
            }
            stageTimingsMs.put("stage1", System.currentTimeMillis() - stage1Start);

            updateProgress(taskId, 0.35, "正在进行语义单元分析...");
            if (!skipPhase2a) {
            AnalyzeResult ar = ioResult.phase2aAnalyzeResult;
            updateProgress(taskId, 0.35, "正在进行语义单元分析...");
                long phase2aStart = System.currentTimeMillis();
            taskProgressWatchdogBridge.resetTask(taskId);
            TaskProgressWatchdogBridge.MonitorHandle phase2aMonitor =
                taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "phase2a", taskSignalEmitter);
            try {
                ar = grpcClient.analyzeSemanticUnits(
                    taskId,
                    videoPath,
                    s1.step2JsonPath,
                    s1.step6JsonPath,
                    s1.sentenceTimestampsPath,
                    outputDir,
                    timeouts.getPhase2aTimeoutSec()
                );
            } finally {
                taskProgressWatchdogBridge.stopMonitor(taskId, phase2aMonitor, taskSignalEmitter);
            }
            if (!ar.success) {
                throw new RuntimeException("Phase2A failed: " + ar.errorMsg);
            }
            stageTimingsMs.put("phase2a_segmentation", System.currentTimeMillis() - phase2aStart);
            ioResult.phase2aAnalyzeResult = ar;
            ioResult.phase2aSemanticUnitsPath = firstNonBlank(
                    ioResult.phase2aSemanticUnitsPath,
                    resolvePhase2aSemanticUnitsPath(outputDir)
            );
            } else {
                ar = restorePhase2aAnalyzeResult(taskId, ioResult);
                ioResult.phase2aAnalyzeResult = ar;
                ioResult.phase2aSemanticUnitsPath = firstNonBlank(
                        ioResult.phase2aSemanticUnitsPath,
                        resolvePhase2aSemanticUnitsPath(outputDir)
                );
            }
            ioResult.phase2aAnalyzeResult = ar;
            ioResult.phase2aSemanticUnitsPath = firstNonBlank(
                    ioResult.phase2aSemanticUnitsPath,
                    resolvePhase2aSemanticUnitsPath(outputDir)
            );
            AssetExtractStageResult assetExtractStageResult =
                    executeAssetExtractStage(taskId, ioResult, ar, timeouts, flowFlags);
            ar = assetExtractStageResult.analyzeResult;
            usedVLFlow = assetExtractStageResult.usedVLFlow;
            usedLegacyFlow = assetExtractStageResult.usedLegacyFlow;
            if (false) {

            updateProgress(taskId, 0.40, "正在规划素材提取方案...");

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
                updateProgress(taskId, 0.45, "正在执行回退分析流程...");
                materialRequests = runLegacyAnalysis(taskId, videoPath, ar, s1, outputDir, timeouts);
                usedLegacyFlow = true;
            }
            stageTimingsMs.put("analysis_legacy", System.currentTimeMillis() - legacyAnalysisStart);
            stageTimingsMs.put("analysis_total", System.currentTimeMillis() - analysisTotalStart);

            updateProgress(taskId, 0.80, "正在提取截图与片段素材...");
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

            updateProgress(taskId, 0.90, "正在组装富文本与 Markdown...");
            long assembleStart = System.currentTimeMillis();
            String title = resolveDocumentTitle(downloadResult, outputDir, videoPath);
            metricsVideoTitle = title;
            taskProgressWatchdogBridge.resetTask(taskId);
            TaskProgressWatchdogBridge.MonitorHandle phase2bMonitor =
                taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "phase2b", taskSignalEmitter);
            AssembleResult assembleRes;
            try {
                assembleRes = grpcClient.assembleRichText(
                    taskId,
                    videoPath,
                    ar,
                    outputDir + "/assets",
                    outputDir + "/assets",
                    outputDir,
                    title,
                    timeouts.getPhase2bTimeoutSec()
                );
            } finally {
                taskProgressWatchdogBridge.stopMonitor(taskId, phase2bMonitor, taskSignalEmitter);
            }

            if (!assembleRes.success) {
                throw new RuntimeException("Assemble failed: " + assembleRes.errorMsg);
            }
            stageTimingsMs.put("phase2b_assemble", System.currentTimeMillis() - assembleStart);

            result.success = true;
            result.markdownPath = assembleRes.markdownPath;
            result.jsonPath = assembleRes.jsonPath;
            result.cleanupSourcePath = cleanupSourcePath;
            persistTaskTocMetadata(outputDir, "video", List.of());
            logger.info("Pipeline Complete: {}", taskId);

            flowFlags.put("downloaded_from_url", downloadedFromUrl);
            if (downloadResult != null) {
                flowFlags.put("download_content_type", firstNonBlank(downloadResult.contentType, "unknown"));
                flowFlags.put("download_source_platform", firstNonBlank(downloadResult.sourcePlatform, "unknown"));
            }
            flowFlags.put("used_vl_flow", usedVLFlow);
            flowFlags.put("used_legacy_flow", usedLegacyFlow);

        } catch (Throwable e) {
            String normalizedError = normalizeThrowableMessage(e, "Video pipeline failed with unknown throwable");
            logger.error("Pipeline Failed: {} - {}", taskId, normalizedError, e);
            result.success = false;
            result.errorMessage = normalizedError;

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
                    metricsVideoTitle,
                    result,
                    stageTimingsMs,
                    flowFlags
            );
            if (result.success
                    && storageTaskCategoryService != null
                    && StringUtils.hasText(metricsOutputDir)) {
                storageTaskCategoryService.classifyBookTaskIfNeeded(metricsOutputDir);
            }
        }
        */
    }

    public IOPhaseResult processVideoIOPhase(String taskId, String videoUrl, String outputDir) throws Exception {
        IOPhaseResult ioResult = processVideoDownloadPhase(taskId, videoUrl, outputDir);
        return processVideoTranscribeStage1Phase(taskId, ioResult);
    }

    public IOPhaseResult processVideoDownloadPhase(String taskId, String videoUrl, String outputDir) throws Exception {
        if (shouldProcessAsBook(videoUrl, null)) {
            throw new IllegalArgumentException("processVideoDownloadPhase does not support book/article sources");
        }

        IOPhaseResult ioResult = new IOPhaseResult();
        ioResult.taskId = taskId;
        ioResult.videoUrl = videoUrl;
        ioResult.videoPath = videoUrl;
        ioResult.outputDir = outputDir;
        ioResult.videoDuration = 60;
        ioResult.downloadedFromUrl = false;
        ioResult.cleanupSourcePath = null;
        ioResult.downloadResult = null;
        ioResult.pipelineStartTimeMs = System.currentTimeMillis();
        ioResult.metricsOutputDir = outputDir;
        ioResult.metricsVideoPath = videoUrl;
        ioResult.metricsInputVideoUrl = videoUrl;
        ioResult.metricsVideoTitle = "";

        try {
            String videoPath = videoUrl;
            double videoDuration = 60;
            boolean downloadedFromUrl = false;
            String cleanupSourcePath = null;
            DownloadResult downloadResult = null;

            long localPrepareStart = System.currentTimeMillis();
            if (!isHttpUrl(videoUrl)) {
                videoPath = normalizeLocalVideoPath(videoUrl);
                assertLocalVideoExists(videoUrl, videoPath);
                outputDir = resolveOutputDirForLocalVideo(videoPath);
                new File(outputDir).mkdirs();
                logger.info("[{}] 本地视频路径校验通过，输出目录已准备 -> {}", taskId, outputDir);

                videoPath = ensureLocalVideoInStorage(videoPath, outputDir);
                videoDuration = resolveVideoDurationSec(taskId, videoPath, videoDuration);
            }
            ioResult.stageTimingsMs.put("prepare_local_video", System.currentTimeMillis() - localPrepareStart);
            ioResult.metricsVideoPath = videoPath;
            ioResult.metricsOutputDir = outputDir;

            long downloadStart = System.currentTimeMillis();
            try {
                if (isHttpUrl(videoUrl)) {
                    int downloadTimeoutSec = normalizePositive(downloadGrpcDeadlineSec, 1800);
                    int hardTimeoutSec = Math.max(downloadTimeoutSec, normalizePositive(downloadHardTimeoutSec, 1800));
                    int idleTimeoutSec = normalizePositive(downloadIdleTimeoutSec, 120);
                    int pollIntervalSec = Math.min(
                        normalizePositive(downloadPollIntervalSec, 5),
                        Math.max(1, idleTimeoutSec)
                    );
                    String predictedDownloadDir = resolvePredictedDownloadWatchDir(videoUrl, outputDir);
                    logger.info(
                        "[{}] Download watchdog targets: request_output_dir={}, predicted_storage_dir={}",
                        taskId,
                        firstNonBlank(outputDir, "(empty)"),
                        firstNonBlank(predictedDownloadDir, "(empty)")
                    );
                    updateProgress(taskId, 0.05, "Downloading video...");
                    DownloadResult dl = waitForDownloadWithLease(
                        taskId,
                        videoUrl,
                        outputDir,
                        predictedDownloadDir,
                        downloadTimeoutSec,
                        hardTimeoutSec,
                        idleTimeoutSec,
                        pollIntervalSec
                    );
                    if (!dl.success) {
                        throw new RuntimeException(
                            "Download failed: " + firstNonBlank(
                                dl.errorMsg,
                                "python worker returned unsuccessful download response without details"
                            )
                        );
                    }
                    downloadedFromUrl = true;
                    downloadResult = dl;
                    if (dl.contentType != null && !dl.contentType.isBlank() && !"video".equalsIgnoreCase(dl.contentType)) {
                        logger.warn(
                            "[{}] Download content_type={} detected, pipeline keeps video-first path",
                            taskId,
                            dl.contentType
                        );
                    }
                    videoPath = dl.videoPath;
                    cleanupSourcePath = dl.videoPath;
                    videoDuration = dl.durationSec;
                    outputDir = new File(videoPath).getParentFile().getAbsolutePath();
                    new File(outputDir).mkdirs();
                    ioResult.metricsVideoPath = videoPath;
                    ioResult.metricsOutputDir = outputDir;
                }
            } finally {
                ioResult.stageTimingsMs.put("download_video", System.currentTimeMillis() - downloadStart);
            }

            if (videoDuration <= 0) {
                videoDuration = resolveVideoDurationSec(taskId, videoPath, videoDuration);
            }

            ioResult.videoPath = videoPath;
            ioResult.outputDir = outputDir;
            ioResult.videoDuration = videoDuration;
            ioResult.downloadedFromUrl = downloadedFromUrl;
            ioResult.cleanupSourcePath = cleanupSourcePath;
            ioResult.downloadResult = downloadResult;
            ioResult.timeouts = timeoutCalculator.calculateTimeouts(videoDuration);
            return ioResult;
        } catch (Throwable e) {
            handleVideoIOPhaseFailure(taskId, ioResult, e);
            return ioResult;
        }
    }

    public IOPhaseResult processVideoTranscribeStage1Phase(String taskId, IOPhaseResult ioResult) throws Exception {
        IOPhaseResult transcribed = processVideoTranscribePhase(taskId, ioResult);
        return processVideoStage1Phase(taskId, transcribed);
    }

    public IOPhaseResult processVideoTranscribePhase(String taskId, IOPhaseResult ioResult) throws Exception {
        if (ioResult == null) {
            throw new IllegalArgumentException("ioResult is required for transcribe phase");
        }
        if (ioResult.videoPath == null || ioResult.videoPath.isBlank()) {
            throw new IllegalArgumentException("videoPath is required for transcribe phase");
        }

        try {
            String outputDir = ioResult.outputDir;
            String videoPath = ioResult.videoPath;
            DynamicTimeoutCalculator.TimeoutConfig timeouts = ioResult.timeouts;
            if (timeouts == null) {
                timeouts = timeoutCalculator.calculateTimeouts(Math.max(1.0d, ioResult.videoDuration));
                ioResult.timeouts = timeouts;
            }
            TaskProgressWatchdogBridge.SignalEmitter taskSignalEmitter =
                (progress, message) -> updateProgress(taskId, progress, message);

            updateProgress(taskId, 0.15, "正在进行语音转写...");
            long transcribeStart = System.currentTimeMillis();
            taskProgressWatchdogBridge.resetTask(taskId);
            TaskProgressWatchdogBridge.MonitorHandle transcribeMonitor =
                taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "transcribe", taskSignalEmitter);
            TranscribeResult tr;
            try {
                tr = grpcClient.transcribeVideo(
                    taskId,
                    videoPath,
                    "auto",
                    timeouts.getTranscribeTimeoutSec()
                );
            } finally {
                taskProgressWatchdogBridge.stopMonitor(taskId, transcribeMonitor, taskSignalEmitter);
            }
            if (!tr.success) {
                throw new RuntimeException("Transcribe failed: " + tr.errorMsg);
            }
            ioResult.stageTimingsMs.put("transcribe", System.currentTimeMillis() - transcribeStart);
            ioResult.subtitlePath = tr.subtitlePath;
            return ioResult;
        } catch (Throwable e) {
            handleVideoIOPhaseFailure(taskId, ioResult, e);
            return ioResult;
        }
    }

    public IOPhaseResult processVideoStage1Phase(String taskId, IOPhaseResult ioResult) throws Exception {
        if (ioResult == null) {
            throw new IllegalArgumentException("ioResult is required for stage1 phase");
        }
        if (ioResult.videoPath == null || ioResult.videoPath.isBlank()) {
            throw new IllegalArgumentException("videoPath is required for stage1 phase");
        }
        if (ioResult.subtitlePath == null || ioResult.subtitlePath.isBlank()) {
            throw new IllegalArgumentException("subtitlePath is required for stage1 phase");
        }
        try {
            String outputDir = ioResult.outputDir;
            String videoPath = ioResult.videoPath;
            DynamicTimeoutCalculator.TimeoutConfig timeouts = ioResult.timeouts;
            if (timeouts == null) {
                timeouts = timeoutCalculator.calculateTimeouts(Math.max(1.0d, ioResult.videoDuration));
                ioResult.timeouts = timeouts;
            }
            TaskProgressWatchdogBridge.SignalEmitter taskSignalEmitter =
                (progress, message) -> updateProgress(taskId, progress, message);

            updateProgress(taskId, 0.25, "正在执行阶段一处理...");
            long stage1Start = System.currentTimeMillis();
            taskProgressWatchdogBridge.resetTask(taskId);
            TaskProgressWatchdogBridge.MonitorHandle stage1Monitor =
                taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "stage1", taskSignalEmitter);
            Stage1Result s1;
            try {
                s1 = grpcClient.processStage1(
                    taskId,
                    videoPath,
                    ioResult.subtitlePath,
                    outputDir,
                    6,
                    timeouts.getStage1TimeoutSec()
                );
            } finally {
                taskProgressWatchdogBridge.stopMonitor(taskId, stage1Monitor, taskSignalEmitter);
            }
            if (!s1.success) {
                throw new RuntimeException("Stage1 failed: " + s1.errorMsg);
            }
            ioResult.stageTimingsMs.put("stage1", System.currentTimeMillis() - stage1Start);
            ioResult.stage1Result = s1;
            return ioResult;
        } catch (Throwable e) {
            handleVideoIOPhaseFailure(taskId, ioResult, e);
            return ioResult;
        }
    }

    private void handleVideoIOPhaseFailure(String taskId, IOPhaseResult ioResult, Throwable error) throws Exception {
        String normalizedError = normalizeThrowableMessage(error, "Video IO phase failed with unknown throwable");
        logger.error("Video IO Phase Failed: {} - {}", taskId, normalizedError, error);

        ProcessingResult failed = new ProcessingResult();
        failed.taskId = taskId;
        failed.success = false;
        failed.errorMessage = normalizedError;
        long startedAt = ioResult != null && ioResult.pipelineStartTimeMs > 0
                ? ioResult.pipelineStartTimeMs
                : System.currentTimeMillis();
        failed.processingTimeMs = Math.max(0L, System.currentTimeMillis() - startedAt);

        if (ioResult != null) {
            ioResult.stageTimingsMs.put("total_pipeline", failed.processingTimeMs);
            ioResult.flowFlags.putIfAbsent("downloaded_from_url", false);
            ioResult.flowFlags.putIfAbsent("used_vl_flow", false);
            ioResult.flowFlags.putIfAbsent("used_legacy_flow", false);
            writeTaskMetricsReport(
                    taskId,
                    firstNonBlank(ioResult.metricsOutputDir, ioResult.outputDir),
                    firstNonBlank(ioResult.metricsVideoPath, ioResult.videoPath),
                    firstNonBlank(ioResult.metricsInputVideoUrl, ioResult.videoUrl),
                    firstNonBlank(ioResult.metricsVideoTitle, ""),
                    failed,
                    ioResult.stageTimingsMs,
                    ioResult.flowFlags
            );
        }

        if (error instanceof Exception exception) {
            throw exception;
        }
        throw new RuntimeException(normalizedError, error);
    }

    public ProcessingResult processVideoLLMPhase(String taskId, IOPhaseResult ioResult) {
        String startStage = ioResult != null ? firstNonBlank(ioResult.recoveryStartStage, "phase2a") : "phase2a";
        return processVideoPhase2Resume(taskId, ioResult, startStage);
    }

    public ProcessingResult processVideoFromAssetExtractStage(String taskId, IOPhaseResult ioResult) {
        return processVideoPhase2Resume(taskId, ioResult, "asset_extract_java");
    }

    public ProcessingResult processVideoFromPhase2BStage(String taskId, IOPhaseResult ioResult) {
        return processVideoPhase2Resume(taskId, ioResult, "phase2b");
    }

    private ProcessingResult processVideoLLMPhaseLegacy(String taskId, IOPhaseResult ioResult) {
        return processVideoPhase2Resume(taskId, ioResult, "phase2a");
        /*
        ProcessingResult result = new ProcessingResult();
        result.taskId = taskId;
        if (ioResult == null) {
            result.success = false;
            result.errorMessage = "IO phase result is required before LLM phase";
            return result;
        }

        Map<String, Long> stageTimingsMs = ioResult.stageTimingsMs != null ? ioResult.stageTimingsMs : new LinkedHashMap<>();
        Map<String, Object> flowFlags = ioResult.flowFlags != null ? ioResult.flowFlags : new LinkedHashMap<>();
        long startTime = ioResult.pipelineStartTimeMs > 0 ? ioResult.pipelineStartTimeMs : System.currentTimeMillis();
        String metricsOutputDir = firstNonBlank(ioResult.metricsOutputDir, ioResult.outputDir);
        String metricsVideoPath = firstNonBlank(ioResult.metricsVideoPath, ioResult.videoPath);
        String metricsInputVideoUrl = firstNonBlank(ioResult.metricsInputVideoUrl, ioResult.videoUrl);
        String metricsVideoTitle = firstNonBlank(ioResult.metricsVideoTitle, "");

        boolean usedVLFlow = false;
        boolean usedLegacyFlow = false;
        try {
            String recoveryStartStage = firstNonBlank(ioResult.recoveryStartStage, "phase2a").toLowerCase(Locale.ROOT);
            boolean skipPhase2a = "asset_extract_java".equals(recoveryStartStage) || "phase2b".equals(recoveryStartStage);
            boolean skipAssetExtract = "phase2b".equals(recoveryStartStage);
            if (!skipAssetExtract && (ioResult.stage1Result == null || !ioResult.stage1Result.success)) {
                throw new IllegalStateException("Stage1 result is invalid for LLM phase");
            }

            String videoPath = ioResult.videoPath;
            String outputDir = ioResult.outputDir;
            double videoDuration = ioResult.videoDuration;
            Stage1Result s1 = ioResult.stage1Result;
            DownloadResult downloadResult = ioResult.downloadResult;
            DynamicTimeoutCalculator.TimeoutConfig timeouts = ioResult.timeouts;
            if (timeouts == null) {
                timeouts = timeoutCalculator.calculateTimeouts(Math.max(videoDuration, 1.0d));
            }

            TaskProgressWatchdogBridge.SignalEmitter taskSignalEmitter =
                (progress, message) -> updateProgress(taskId, progress, message);

            updateProgress(taskId, 0.35, "正在进行语义单元分析...");
            if (!skipPhase2a) {
                long phase2aStart = System.currentTimeMillis();
            taskProgressWatchdogBridge.resetTask(taskId);
            TaskProgressWatchdogBridge.MonitorHandle phase2aMonitor =
                taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "phase2a", taskSignalEmitter);
            AnalyzeResult ar = ioResult.phase2aAnalyzeResult;
            try {
                ar = grpcClient.analyzeSemanticUnits(
                    taskId,
                    videoPath,
                    s1.step2JsonPath,
                    s1.step6JsonPath,
                    s1.sentenceTimestampsPath,
                    outputDir,
                    timeouts.getPhase2aTimeoutSec()
                );
            } finally {
                taskProgressWatchdogBridge.stopMonitor(taskId, phase2aMonitor, taskSignalEmitter);
            }
            if (!ar.success) {
                throw new RuntimeException("Phase2A failed: " + ar.errorMsg);
            }
            stageTimingsMs.put("phase2a_segmentation", System.currentTimeMillis() - phase2aStart);

            updateProgress(taskId, 0.40, "正在规划素材提取方案...");

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
                updateProgress(taskId, 0.45, "正在执行回退分析流程...");
                materialRequests = runLegacyAnalysis(taskId, videoPath, ar, s1, outputDir, timeouts);
                usedLegacyFlow = true;
            }
            stageTimingsMs.put("analysis_legacy", System.currentTimeMillis() - legacyAnalysisStart);
            stageTimingsMs.put("analysis_total", System.currentTimeMillis() - analysisTotalStart);

            updateProgress(taskId, 0.80, "正在提取截图与片段素材...");
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

            updateProgress(taskId, 0.90, "正在组装富文本与 Markdown...");
            long assembleStart = System.currentTimeMillis();
            String title = resolveDocumentTitle(downloadResult, outputDir, videoPath);
            metricsVideoTitle = title;
            taskProgressWatchdogBridge.resetTask(taskId);
            TaskProgressWatchdogBridge.MonitorHandle phase2bMonitor =
                taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "phase2b", taskSignalEmitter);
            AssembleResult assembleRes;
            try {
                assembleRes = grpcClient.assembleRichText(
                    taskId,
                    videoPath,
                    ar,
                    outputDir + "/assets",
                    outputDir + "/assets",
                    outputDir,
                    title,
                    timeouts.getPhase2bTimeoutSec()
                );
            } finally {
                taskProgressWatchdogBridge.stopMonitor(taskId, phase2bMonitor, taskSignalEmitter);
            }

            if (!assembleRes.success) {
                throw new RuntimeException("Assemble failed: " + assembleRes.errorMsg);
            }
            stageTimingsMs.put("phase2b_assemble", System.currentTimeMillis() - assembleStart);

            result.success = true;
            result.markdownPath = assembleRes.markdownPath;
            result.jsonPath = assembleRes.jsonPath;
            result.cleanupSourcePath = ioResult.cleanupSourcePath;
            persistTaskTocMetadata(outputDir, "video", List.of());
            logger.info("Pipeline Complete: {}", taskId);

            flowFlags.put("downloaded_from_url", ioResult.downloadedFromUrl);
            if (downloadResult != null) {
                flowFlags.put("download_content_type", firstNonBlank(downloadResult.contentType, "unknown"));
                flowFlags.put("download_source_platform", firstNonBlank(downloadResult.sourcePlatform, "unknown"));
            }
            flowFlags.put("used_vl_flow", usedVLFlow);
            flowFlags.put("used_legacy_flow", usedLegacyFlow);

        } catch (Throwable e) {
            String normalizedError = normalizeThrowableMessage(e, "Video LLM phase failed with unknown throwable");
            logger.error("Pipeline LLM Phase Failed: {} - {}", taskId, normalizedError, e);
            result.success = false;
            result.errorMessage = normalizedError;

            flowFlags.putIfAbsent("downloaded_from_url", ioResult.downloadedFromUrl);
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
                    metricsVideoTitle,
                    result,
                    stageTimingsMs,
                    flowFlags
            );
        }
        return result;
        */
    }

    private ProcessingResult processVideoPhase2Resume(String taskId, IOPhaseResult ioResult, String startStage) {
        ProcessingResult result = new ProcessingResult();
        result.taskId = taskId;
        if (ioResult == null) {
            result.success = false;
            result.errorMessage = "IO phase result is required before resumed phase2 stage";
            return result;
        }

        Map<String, Long> stageTimingsMs = ioResult.stageTimingsMs != null ? ioResult.stageTimingsMs : new LinkedHashMap<>();
        Map<String, Object> flowFlags = ioResult.flowFlags != null ? ioResult.flowFlags : new LinkedHashMap<>();
        long startTime = ioResult.pipelineStartTimeMs > 0 ? ioResult.pipelineStartTimeMs : System.currentTimeMillis();
        String metricsOutputDir = firstNonBlank(ioResult.metricsOutputDir, ioResult.outputDir);
        String metricsVideoPath = firstNonBlank(ioResult.metricsVideoPath, ioResult.videoPath);
        String metricsInputVideoUrl = firstNonBlank(ioResult.metricsInputVideoUrl, ioResult.videoUrl);
        String metricsVideoTitle = firstNonBlank(ioResult.metricsVideoTitle, "");

        boolean usedVLFlow = false;
        boolean usedLegacyFlow = false;
        try {
            String effectiveStartStage = firstNonBlank(startStage, "phase2a");
            String videoPath = ioResult.videoPath;
            String outputDir = ioResult.outputDir;
            double videoDuration = ioResult.videoDuration;
            DownloadResult downloadResult = ioResult.downloadResult;
            DynamicTimeoutCalculator.TimeoutConfig timeouts = ioResult.timeouts;
            if (timeouts == null) {
                timeouts = timeoutCalculator.calculateTimeouts(Math.max(videoDuration, 1.0d));
            }
            effectiveStartStage = reconcileRecoveredPhase2Context(taskId, ioResult, effectiveStartStage, timeouts);
            if ("stage1".equalsIgnoreCase(effectiveStartStage)) {
                ioResult = processVideoStage1Phase(taskId, ioResult);
                effectiveStartStage = "phase2a";
            }

            AnalyzeResult analyzeResult;
            if ("phase2a".equalsIgnoreCase(effectiveStartStage)) {
                if (ioResult.stage1Result == null || !ioResult.stage1Result.success) {
                    throw new IllegalStateException("Stage1 result is invalid for phase2a resume");
                }
                analyzeResult = executePhase2aStage(
                        taskId,
                        ioResult,
                        timeouts,
                        (progress, message) -> updateProgress(taskId, progress, message)
                );
                ioResult.phase2aAnalyzeResult = analyzeResult;
                ioResult.phase2aSemanticUnitsPath = firstNonBlank(
                        ioResult.phase2aSemanticUnitsPath,
                        resolvePhase2aSemanticUnitsPath(outputDir)
                );
            } else {
                analyzeResult = ensurePhase2aAnalyzeResult(taskId, ioResult);
            }
            if (!"phase2b".equalsIgnoreCase(effectiveStartStage)) {
                AssetExtractStageResult assetExtractStageResult =
                        executeAssetExtractStage(taskId, ioResult, analyzeResult, timeouts, flowFlags);
                analyzeResult = assetExtractStageResult.analyzeResult;
                usedVLFlow = assetExtractStageResult.usedVLFlow;
                usedLegacyFlow = assetExtractStageResult.usedLegacyFlow;
            }
            updateProgress(taskId, 0.90, "正在组装富文本与 Markdown...");
            long assembleStart = System.currentTimeMillis();
            String title = resolveDocumentTitle(downloadResult, outputDir, videoPath);
            metricsVideoTitle = title;
            TaskProgressWatchdogBridge.SignalEmitter taskSignalEmitter =
                    (progress, message) -> updateProgress(taskId, progress, message);
            taskProgressWatchdogBridge.resetTask(taskId);
            TaskProgressWatchdogBridge.MonitorHandle phase2bMonitor =
                    taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "phase2b", taskSignalEmitter);
            AssembleResult assembleRes;
            try {
                assembleRes = grpcClient.assembleRichText(
                        taskId,
                        videoPath,
                        analyzeResult,
                        outputDir + "/assets",
                        outputDir + "/assets",
                        outputDir,
                        title,
                        timeouts.getPhase2bTimeoutSec()
                );
            } finally {
                taskProgressWatchdogBridge.stopMonitor(taskId, phase2bMonitor, taskSignalEmitter);
            }

            if (!assembleRes.success) {
                throw new RuntimeException("Assemble failed: " + assembleRes.errorMsg);
            }
            stageTimingsMs.put("phase2b_assemble", System.currentTimeMillis() - assembleStart);

            result.success = true;
            result.markdownPath = assembleRes.markdownPath;
            result.jsonPath = assembleRes.jsonPath;
            result.cleanupSourcePath = ioResult.cleanupSourcePath;
            persistTaskTocMetadata(outputDir, "video", List.of());

            flowFlags.put("downloaded_from_url", ioResult.downloadedFromUrl);
            if (downloadResult != null) {
                flowFlags.put("download_content_type", firstNonBlank(downloadResult.contentType, "unknown"));
                flowFlags.put("download_source_platform", firstNonBlank(downloadResult.sourcePlatform, "unknown"));
            }
            flowFlags.put("used_vl_flow", usedVLFlow);
            flowFlags.put("used_legacy_flow", usedLegacyFlow);
        } catch (Throwable e) {
            String normalizedError = normalizeThrowableMessage(e, "Resumed video phase2 failed with unknown throwable");
            logger.error("Pipeline resumed phase2 failed: {} - {}", taskId, normalizedError, e);
            result.success = false;
            result.errorMessage = normalizedError;
            flowFlags.putIfAbsent("downloaded_from_url", ioResult.downloadedFromUrl);
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
                    metricsVideoTitle,
                    result,
                    stageTimingsMs,
                    flowFlags
            );
        }
        return result;
    }

    public String reconcileRecoveredRuntimeContext(
            String taskId,
            IOPhaseResult ioResult,
            String requestedStartStage,
            DynamicTimeoutCalculator.TimeoutConfig timeouts
    ) {
        String safeRequestedStage = firstNonBlank(requestedStartStage, "download");
        if (ioResult == null || grpcClient == null) {
            return safeRequestedStage;
        }
        String outputDir = firstNonBlank(ioResult.outputDir, "");
        if (outputDir.isBlank()) {
            return safeRequestedStage;
        }

        int timeoutSec = 30;
        if (timeouts != null) {
            timeoutSec = Math.max(15, Math.min(60, timeouts.getPhase2aTimeoutSec()));
        }

        PythonGrpcClient.RecoverRuntimeContextResult recovered =
                grpcClient.recoverRuntimeContext(
                        taskId,
                        outputDir,
                        safeRequestedStage,
                        firstNonBlank(ioResult.phase2aSemanticUnitsPath, ""),
                        firstNonBlank(ioResult.videoPath, ioResult.videoUrl),
                        firstNonBlank(ioResult.subtitlePath, ""),
                        timeoutSec
                );
        if (recovered == null || !recovered.success) {
            if (recovered != null && StringUtils.hasText(recovered.errorMsg)) {
                logger.warn("[{}] Python runtime recovery skipped: {}", taskId, recovered.errorMsg);
            }
            return safeRequestedStage;
        }

        if (recovered.downloadReady && StringUtils.hasText(recovered.videoPath)) {
            String recoveredVideoPath = recovered.videoPath;
            if (!StringUtils.hasText(ioResult.videoPath) || isHttpUrl(ioResult.videoPath)) {
                ioResult.videoPath = recoveredVideoPath;
            }
            if (recovered.videoDurationSec > 0) {
                ioResult.videoDuration = recovered.videoDurationSec;
            }
            ioResult.metricsVideoPath = firstNonBlank(ioResult.metricsVideoPath, ioResult.videoPath);
            ioResult.metricsVideoTitle = firstNonBlank(ioResult.metricsVideoTitle, recovered.videoTitle);
            DownloadResult downloadResult = ioResult.downloadResult;
            if (downloadResult == null) {
                downloadResult = new DownloadResult();
                ioResult.downloadResult = downloadResult;
            }
            downloadResult.success = true;
            if (!StringUtils.hasText(downloadResult.videoPath) || isHttpUrl(downloadResult.videoPath)) {
                downloadResult.videoPath = ioResult.videoPath;
            }
            downloadResult.durationSec = recovered.videoDurationSec > 0 ? recovered.videoDurationSec : downloadResult.durationSec;
            downloadResult.videoTitle = firstNonBlank(downloadResult.videoTitle, recovered.videoTitle);
            downloadResult.resolvedUrl = firstNonBlank(downloadResult.resolvedUrl, recovered.resolvedUrl);
            downloadResult.sourcePlatform = firstNonBlank(downloadResult.sourcePlatform, recovered.sourcePlatform);
            downloadResult.canonicalId = firstNonBlank(downloadResult.canonicalId, recovered.canonicalId);
            downloadResult.contentType = firstNonBlank(downloadResult.contentType, recovered.contentType);
            if (!StringUtils.hasText(ioResult.cleanupSourcePath) || isHttpUrl(ioResult.cleanupSourcePath)) {
                ioResult.cleanupSourcePath = ioResult.videoPath;
            }
            if ((ioResult.timeouts == null || ioResult.videoDuration <= 1.0d) && timeoutCalculator != null) {
                ioResult.timeouts = timeoutCalculator.calculateTimeouts(Math.max(ioResult.videoDuration, 1.0d));
            }
        }
        if (recovered.transcribeReady && StringUtils.hasText(recovered.subtitlePath)) {
            ioResult.subtitlePath = firstNonBlank(ioResult.subtitlePath, recovered.subtitlePath);
        }
        if (recovered.stage1Ready) {
            Stage1Result stage1Result = ioResult.stage1Result;
            if (stage1Result == null) {
                stage1Result = new Stage1Result();
                ioResult.stage1Result = stage1Result;
            }
            stage1Result.success = true;
            stage1Result.step2JsonPath = firstNonBlank(stage1Result.step2JsonPath, recovered.step2JsonPath);
            stage1Result.step6JsonPath = firstNonBlank(stage1Result.step6JsonPath, recovered.step6JsonPath);
            stage1Result.sentenceTimestampsPath = firstNonBlank(
                    stage1Result.sentenceTimestampsPath,
                    firstNonBlank(recovered.sentenceTimestampsPath, "")
            );
        }
        if (recovered.phase2aReady) {
            ioResult.phase2aSemanticUnitsPath = firstNonBlank(
                    ioResult.phase2aSemanticUnitsPath,
                    firstNonBlank(recovered.semanticUnitsPath, resolvePhase2aSemanticUnitsPath(outputDir))
            );
        }
        if (recovered.phase2bReady) {
            ioResult.phase2bMarkdownPath = firstNonBlank(ioResult.phase2bMarkdownPath, recovered.markdownPath);
            ioResult.phase2bJsonPath = firstNonBlank(ioResult.phase2bJsonPath, recovered.jsonPath);
        }

        String effectiveStartStage = firstNonBlank(recovered.resolvedStartStage, safeRequestedStage);
        ioResult.recoveryStartStage = effectiveStartStage;
        logger.info(
                "[{}] Python runtime recovery resolved start stage: requested={} resolved={} downloadReady={} transcribeReady={} stage1Ready={} phase2aReady={} phase2bReady={} reusedLlmCalls={} reusedChunks={} reason={}",
                taskId,
                safeRequestedStage,
                effectiveStartStage,
                recovered.downloadReady,
                recovered.transcribeReady,
                recovered.stage1Ready,
                recovered.phase2aReady,
                recovered.phase2bReady,
                recovered.reusedLlmCallCount,
                recovered.reusedChunkCount,
                firstNonBlank(recovered.decisionReason, "none")
        );
        return effectiveStartStage;
    }

    String reconcileRecoveredPhase2Context(
            String taskId,
            IOPhaseResult ioResult,
            String requestedStartStage,
            DynamicTimeoutCalculator.TimeoutConfig timeouts
    ) {
        return reconcileRecoveredRuntimeContext(taskId, ioResult, requestedStartStage, timeouts);
    }

    public ProcessingResult processVideoFromRecoveredOutputs(String taskId, IOPhaseResult ioResult) {
        ProcessingResult result = new ProcessingResult();
        result.taskId = taskId;
        if (ioResult == null) {
            result.success = false;
            result.errorMessage = "IO phase result is required before recovered completion";
            return result;
        }

        String markdownPath = firstNonBlank(ioResult.phase2bMarkdownPath, "");
        if (markdownPath.isBlank()) {
            result.success = false;
            result.errorMessage = "Recovered markdown path is required before completed resume";
            return result;
        }

        Map<String, Long> stageTimingsMs = ioResult.stageTimingsMs != null ? ioResult.stageTimingsMs : new LinkedHashMap<>();
        Map<String, Object> flowFlags = ioResult.flowFlags != null ? ioResult.flowFlags : new LinkedHashMap<>();
        long startTime = ioResult.pipelineStartTimeMs > 0 ? ioResult.pipelineStartTimeMs : System.currentTimeMillis();
        String metricsOutputDir = firstNonBlank(ioResult.metricsOutputDir, ioResult.outputDir);
        String metricsVideoPath = firstNonBlank(ioResult.metricsVideoPath, ioResult.videoPath);
        String metricsInputVideoUrl = firstNonBlank(ioResult.metricsInputVideoUrl, ioResult.videoUrl);
        String metricsVideoTitle = firstNonBlank(
                ioResult.metricsVideoTitle,
                ioResult.downloadResult != null ? ioResult.downloadResult.videoTitle : ""
        );

        try {
            result.success = true;
            result.markdownPath = markdownPath;
            result.jsonPath = firstNonBlank(ioResult.phase2bJsonPath, "");
            result.cleanupSourcePath = ioResult.cleanupSourcePath;
            persistTaskTocMetadata(firstNonBlank(ioResult.outputDir, ""), "video", List.of());

            flowFlags.putIfAbsent("downloaded_from_url", ioResult.downloadedFromUrl);
            if (ioResult.downloadResult != null) {
                flowFlags.put("download_content_type", firstNonBlank(ioResult.downloadResult.contentType, "unknown"));
                flowFlags.put("download_source_platform", firstNonBlank(ioResult.downloadResult.sourcePlatform, "unknown"));
            }
            flowFlags.put("used_vl_flow", false);
            flowFlags.put("used_legacy_flow", false);
            flowFlags.put("reused_completed_outputs", true);
            stageTimingsMs.putIfAbsent("phase2b_assemble", 0L);
        } catch (Throwable error) {
            String normalizedError = normalizeThrowableMessage(error, "Recovered completed outputs handling failed");
            logger.error("[{}] Recovered completed outputs handling failed: {}", taskId, normalizedError, error);
            result.success = false;
            result.errorMessage = normalizedError;
        } finally {
            result.processingTimeMs = System.currentTimeMillis() - startTime;
            stageTimingsMs.put("total_pipeline", result.processingTimeMs);
            writeTaskMetricsReport(
                    taskId,
                    metricsOutputDir,
                    metricsVideoPath,
                    metricsInputVideoUrl,
                    metricsVideoTitle,
                    result,
                    stageTimingsMs,
                    flowFlags
            );
        }
        return result;
    }

    private AnalyzeResult executePhase2aStage(
            String taskId,
            IOPhaseResult ioResult,
            DynamicTimeoutCalculator.TimeoutConfig timeouts,
            TaskProgressWatchdogBridge.SignalEmitter taskSignalEmitter
    ) throws Exception {
        String outputDir = ioResult.outputDir;
        Stage1Result stage1Result = ioResult.stage1Result;
        updateProgress(taskId, 0.35, "正在进行语义单元分析...");
        long phase2aStart = System.currentTimeMillis();
        taskProgressWatchdogBridge.resetTask(taskId);
        TaskProgressWatchdogBridge.MonitorHandle phase2aMonitor =
                taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "phase2a", taskSignalEmitter);
        AnalyzeResult analyzeResult;
        try {
            analyzeResult = grpcClient.analyzeSemanticUnits(
                    taskId,
                    ioResult.videoPath,
                    stage1Result.step2JsonPath,
                    stage1Result.step6JsonPath,
                    stage1Result.sentenceTimestampsPath,
                    outputDir,
                    timeouts.getPhase2aTimeoutSec()
            );
        } finally {
            taskProgressWatchdogBridge.stopMonitor(taskId, phase2aMonitor, taskSignalEmitter);
        }
        if (!analyzeResult.success) {
            throw new RuntimeException("Phase2A failed: " + analyzeResult.errorMsg);
        }
        ioResult.stageTimingsMs.put("phase2a_segmentation", System.currentTimeMillis() - phase2aStart);
        return analyzeResult;
    }

    private AnalyzeResult ensurePhase2aAnalyzeResult(String taskId, IOPhaseResult ioResult) throws Exception {
        if (ioResult != null && ioResult.phase2aAnalyzeResult != null) {
            return ioResult.phase2aAnalyzeResult;
        }
        String outputDir = ioResult != null ? firstNonBlank(ioResult.outputDir, "") : "";
        String semanticUnitsPath = "";
        if (ioResult != null) {
            semanticUnitsPath = firstNonBlank(
                    ioResult.phase2aSemanticUnitsPath,
                    resolvePhase2aSemanticUnitsPath(outputDir)
            );
        }
        if (semanticUnitsPath.isBlank()) {
            throw new IllegalStateException("phase2a semantic units path is required before asset extract / phase2b resume");
        }
        JsonNode rootNode = loadPhase2aSemanticUnitsRootNode(outputDir, semanticUnitsPath);
        AnalyzeResult analyzeResult = new AnalyzeResult();
        analyzeResult.success = true;
        analyzeResult.errorMsg = "";
        analyzeResult.semanticUnitsInline = buildSemanticUnitsInlineDTO(rootNode, extractSemanticUnitNodes(rootNode).size());
        if (analyzeResult.semanticUnitsInline == null) {
            throw new IllegalStateException("failed to rebuild semantic_units_inline from " + semanticUnitsPath);
        }
        if (ioResult != null) {
            ioResult.phase2aAnalyzeResult = analyzeResult;
            ioResult.phase2aSemanticUnitsPath = semanticUnitsPath;
        }
        logger.info("[{}] Restored Phase2A semantic units for resume: {}", taskId, semanticUnitsPath);
        return analyzeResult;
    }

    private JsonNode loadPhase2aSemanticUnitsRootNode(String outputDir, String semanticUnitsPath) throws Exception {
        if (StringUtils.hasText(semanticUnitsPath)) {
            try {
                Path normalizedPath = Paths.get(semanticUnitsPath).toAbsolutePath().normalize();
                if (Files.isRegularFile(normalizedPath)) {
                    return objectMapper.readTree(Files.readAllBytes(normalizedPath));
                }
            } catch (InvalidPathException ignored) {
                // 路径可能是虚拟恢复路径，此时继续回退到任务内 SQLite。
            }
        }
        if (taskRuntimeStageStore == null || !StringUtils.hasText(outputDir)) {
            throw new IllegalStateException("phase2a semantic units artifact unavailable: " + semanticUnitsPath);
        }
        Map<String, Object> artifactPayload = taskRuntimeStageStore.loadProjectionPayload(outputDir, "phase2a", "semantic_units");
        if (artifactPayload.isEmpty()) {
            throw new IllegalStateException("phase2a semantic units artifact unavailable: " + semanticUnitsPath);
        }
        return objectMapper.valueToTree(artifactPayload);
    }

    private String resolvePhase2aSemanticUnitsPath(String outputDir) {
        if (outputDir == null || outputDir.isBlank()) {
            return "";
        }
        List<Path> candidates = List.of(
                Paths.get(outputDir, "intermediates", "stages", "phase2a", "outputs", "semantic_units.json"),
                Paths.get(outputDir, "semantic_units_phase2a.json"),
                Paths.get(outputDir, "intermediates", "semantic_units_phase2a.json")
        );
        for (Path candidate : candidates) {
            try {
                Path normalized = candidate.toAbsolutePath().normalize();
                if (Files.isRegularFile(normalized)) {
                    return normalized.toString();
                }
            } catch (Exception ignored) {
                // noop
            }
        }
        if (taskRuntimeStageStore != null && taskRuntimeStageStore.hasProjectionPayload(outputDir, "phase2a", "semantic_units")) {
            return Paths.get(outputDir, "intermediates", "stages", "phase2a", "outputs", "semantic_units.json")
                    .toAbsolutePath()
                    .normalize()
                    .toString();
        }
        return "";
    }

    private AssetExtractStageResult executeAssetExtractStage(
            String taskId,
            IOPhaseResult ioResult,
            AnalyzeResult analyzeResult,
            DynamicTimeoutCalculator.TimeoutConfig timeouts,
            Map<String, Object> flowFlags
    ) throws Exception {
        String videoPath = ioResult.videoPath;
        String outputDir = ioResult.outputDir;
        double videoDuration = ioResult.videoDuration;
        Stage1Result stage1Result = ioResult.stage1Result;
        if (stage1Result == null || !stage1Result.success) {
            throw new IllegalStateException("Stage1 result is invalid for asset_extract_java stage");
        }
        if (taskRuntimeStageStore != null) {
            taskRuntimeStageStore.resetRunningScopesToPlanned(outputDir, "asset_extract_java", "substage", "runtime_context_lost");
            taskRuntimeStageStore.resetRunningScopesToPlanned(outputDir, "asset_extract_java", "chunk", "runtime_context_lost");
        }

        Map<String, Object> basePayload = new LinkedHashMap<>();
        basePayload.put("video_path", videoPath);
        basePayload.put("output_dir", outputDir);
        basePayload.put("phase2a_semantic_units_path", firstNonBlank(ioResult.phase2aSemanticUnitsPath, ""));
        writeAssetExtractStageCheckpoint(outputDir, taskId, "RUNNING", "asset_extract_prepare", 0, 3, basePayload);

        updateProgress(taskId, 0.40, "正在规划素材提取方案...");
        ExtractionRequests materialRequests = null;
        long analysisTotalStart = System.currentTimeMillis();
        boolean usedVLFlow = false;
        boolean usedLegacyFlow = false;
        int stageCompleted = 0;
        int stagePending = 3;
        Integer currentTimeoutSec = null;
        AssetExtractRuntimeWave materialPlanWave = null;
        AssetExtractRuntimeWave extractionWave = null;
        AssetExtractRuntimeWave outputsFinalizeWave = null;
        Map<String, Object> materialPlanContext = new LinkedHashMap<>();
        int materialPlanAttempt = 0;
        int extractionAttempt = 0;
        int outputsFinalizeAttempt = 0;
        Map<String, Object> restoredMaterialPlanPayload = new LinkedHashMap<>();
        Map<String, Object> restoredExtractionPayload = new LinkedHashMap<>();
        Map<String, Object> restoredOutputsPayload = new LinkedHashMap<>();

        assetExtractRuntimeRepositoryAdapter.rebuildFromStore(
                outputDir,
                taskId,
                videoPath,
                firstNonBlank(ioResult.phase2aSemanticUnitsPath, ""),
                taskRuntimeStageStore
        );

        boolean vlEnabled = configService.isVLEnabled();
        flowFlags.put("vl_enabled", vlEnabled);
        materialPlanContext.put("video_path", videoPath);
        materialPlanContext.put("phase2a_semantic_units_path", firstNonBlank(ioResult.phase2aSemanticUnitsPath, ""));
        materialPlanContext.put("vl_enabled", vlEnabled);
        materialPlanWave = planAssetExtractRuntimeWave(
                outputDir,
                "material_request_plan",
                "wave_0001",
                buildStablePayloadHash(materialPlanContext),
                List.of(),
                materialPlanContext
        );
        if (taskRuntimeStageStore != null) {
            restoredMaterialPlanPayload = taskRuntimeStageStore.loadCommittedChunkPayload(
                    outputDir,
                    "asset_extract_java",
                    materialPlanWave.chunkScopeId,
                    materialPlanWave.inputFingerprint
            );
        }
        if (!restoredMaterialPlanPayload.isEmpty()) {
            materialRequests = restoreExtractionRequests(restoredMaterialPlanPayload);
            if (materialRequests == null) {
                restoredMaterialPlanPayload = new LinkedHashMap<>();
            }
        }
        if (!restoredMaterialPlanPayload.isEmpty()) {
            String restoredMaterialSource = String.valueOf(restoredMaterialPlanPayload.getOrDefault("material_source", ""));
            usedVLFlow = "vl".equalsIgnoreCase(restoredMaterialSource);
            usedLegacyFlow = "legacy".equalsIgnoreCase(restoredMaterialSource);
            ioResult.stageTimingsMs.put("analysis_vl", 0L);
            ioResult.stageTimingsMs.put("analysis_legacy", 0L);
            ioResult.stageTimingsMs.put("analysis_total", 0L);
            assetExtractRuntimeRepositoryAdapter.markMaterialRequestsReady(
                    outputDir,
                    restoredMaterialSource,
                    safeInt(restoredMaterialPlanPayload.get("screenshot_count"), materialRequests != null ? materialRequests.screenshotRequests.size() : 0),
                    safeInt(restoredMaterialPlanPayload.get("clip_count"), materialRequests != null ? materialRequests.clipRequests.size() : 0),
                    false
            );
        } else if (taskRuntimeStageStore != null) {
            materialPlanAttempt = taskRuntimeStageStore.loadLatestChunkAttempt(
                    outputDir,
                    "asset_extract_java",
                    materialPlanWave.chunkScopeId
            ) + 1;
            taskRuntimeStageStore.recordChunkState(
                    outputDir,
                    "asset_extract_java",
                    materialPlanWave.chunkScopeId,
                    materialPlanWave.inputFingerprint,
                    materialPlanAttempt,
                    Map.of(
                            "status", "RUNNING",
                            "stage_step", materialPlanWave.substageName,
                            "updated_at_ms", System.currentTimeMillis()
                    )
            );
        }
        if (restoredMaterialPlanPayload.isEmpty()) {
        transitionAssetExtractRuntimeWave(
                outputDir,
                materialPlanWave,
                "RUNNING",
                materialPlanContext,
                null,
                Map.of("attempt_count", materialPlanAttempt)
        );
        try {
            long vlAnalysisStart = System.currentTimeMillis();
        if (vlEnabled) {
            materialRequests = tryVLAnalysis(taskId, videoPath, analyzeResult, outputDir, timeouts);
            if (materialRequests != null) {
                usedVLFlow = true;
            } else {
                logger.warn("[{}] Proceeding to Legacy Flow (Fallback or VL failed).", taskId);
            }
        } else {
            logger.info("[{}] VL disabled in config.", taskId);
        }
        ioResult.stageTimingsMs.put("analysis_vl", System.currentTimeMillis() - vlAnalysisStart);

        long legacyAnalysisStart = System.currentTimeMillis();
        if (materialRequests == null) {
            updateProgress(taskId, 0.45, "正在执行回退分析流程...");
            materialRequests = runLegacyAnalysis(taskId, videoPath, analyzeResult, stage1Result, outputDir, timeouts);
            usedLegacyFlow = true;
        }
        ioResult.stageTimingsMs.put("analysis_legacy", System.currentTimeMillis() - legacyAnalysisStart);
            ioResult.stageTimingsMs.put("analysis_total", System.currentTimeMillis() - analysisTotalStart);
        } catch (Exception error) {
            String scopeFailureStatus = classifyAssetExtractScopeFailureStatus(error);
            Map<String, Object> failureResourceSnapshot = buildAssetExtractResourceSnapshot(
                    flowFlags,
                    materialPlanWave != null ? materialPlanWave.substageName : "material_request_plan",
                    null,
                    Map.of("video_duration_sec", videoDuration)
            );
            Map<String, Object> failureScopePayload = buildAssetExtractFailureScopePayload(
                    error,
                    materialPlanContext,
                    failureResourceSnapshot,
                    scopeFailureStatus
            );
            transitionAssetExtractRuntimeWave(
                    outputDir,
                    materialPlanWave,
                    scopeFailureStatus,
                    materialPlanContext,
                    failureResourceSnapshot,
                    mergeAttemptPayload(failureScopePayload, materialPlanAttempt)
            );
            if (taskRuntimeStageStore != null && materialPlanWave != null && materialPlanAttempt > 0) {
                taskRuntimeStageStore.failChunkPayload(
                        outputDir,
                        "asset_extract_java",
                        materialPlanWave.chunkScopeId,
                        materialPlanWave.inputFingerprint,
                        materialPlanAttempt,
                        mergeAttemptPayload(
                                failureScopePayload,
                                materialPlanAttempt,
                                Map.of(
                                        "status", scopeFailureStatus,
                                        "stage_step", materialPlanWave.substageName,
                                        "updated_at_ms", System.currentTimeMillis()
                                )
                        )
                );
            }
            assetExtractRuntimeRepositoryAdapter.markFailed(
                    outputDir,
                    materialPlanWave != null ? materialPlanWave.substageName : "material_request_plan",
                    scopeFailureStatus,
                    String.valueOf(failureScopePayload.getOrDefault("error_message", "asset extract failed"))
            );
            Map<String, Object> failedPayload = new LinkedHashMap<>(basePayload);
            failedPayload.put("failed_substage", materialPlanWave != null ? materialPlanWave.substageName : "material_request_plan");
            failedPayload.put("error_class", failureScopePayload.getOrDefault("error_class", ""));
            failedPayload.put("error_code", failureScopePayload.getOrDefault("error_code", ""));
            failedPayload.put("error_message", failureScopePayload.getOrDefault("error_message", normalizeThrowableMessage(error, "asset extract failed")));
            failedPayload.put("required_action", failureScopePayload.getOrDefault("required_action", ""));
            failedPayload.put("retry_mode", failureScopePayload.getOrDefault("retry_mode", ""));
            failedPayload.put("retry_entry_point", failureScopePayload.getOrDefault("retry_entry_point", ""));
            writeAssetExtractStageCheckpoint(outputDir, taskId, "MANUAL_NEEDED", "asset_extract_failed", stageCompleted, stagePending, failedPayload);
            throw error;
        }
        } else {
            materialRequests = restoreExtractionRequests(restoredMaterialPlanPayload);
        }

        Map<String, Object> materialResultPayload = new LinkedHashMap<>();
        materialResultPayload.put("screenshot_count", materialRequests.screenshotRequests.size());
        materialResultPayload.put("clip_count", materialRequests.clipRequests.size());
        materialResultPayload.put("material_source", usedVLFlow ? "vl" : "legacy");
        materialResultPayload.put("has_inflight_extraction", materialRequests.extractionFuture != null);
        materialResultPayload.put("screenshot_requests", buildScreenshotRequestPayload(materialRequests.screenshotRequests));
        materialResultPayload.put("clip_requests", buildClipRequestPayload(materialRequests.clipRequests));
        transitionAssetExtractRuntimeWave(
                outputDir,
                materialPlanWave,
                "SUCCESS",
                materialPlanContext,
                null,
                Map.of(
                        "result_hash", buildStablePayloadHash(materialResultPayload),
                        "attempt_count", materialPlanAttempt
                )
        );
        if (taskRuntimeStageStore != null && materialPlanWave != null && materialPlanAttempt > 0) {
            taskRuntimeStageStore.commitChunkPayload(
                    outputDir,
                    "asset_extract_java",
                    materialPlanWave.chunkScopeId,
                    materialPlanWave.inputFingerprint,
                    materialPlanAttempt,
                    Map.of(
                            "status", "SUCCESS",
                            "stage_step", materialPlanWave.substageName,
                            "result_hash", buildStablePayloadHash(materialResultPayload),
                            "updated_at_ms", System.currentTimeMillis(),
                            "committed_at_ms", System.currentTimeMillis()
                    ),
                    materialResultPayload
            );
        }
        assetExtractRuntimeRepositoryAdapter.markMaterialRequestsReady(
                outputDir,
                usedVLFlow ? "vl" : "legacy",
                materialRequests.screenshotRequests.size(),
                materialRequests.clipRequests.size(),
                materialRequests.extractionFuture != null
        );
        Map<String, Object> readyPayload = new LinkedHashMap<>(basePayload);
        readyPayload.putAll(materialResultPayload);
        stageCompleted = 1;
        stagePending = 2;
        writeAssetExtractStageCheckpoint(outputDir, taskId, "RUNNING", "material_requests_ready", stageCompleted, stagePending, readyPayload);

        updateProgress(taskId, 0.80, "正在提取截图与片段素材...");
        long extractionStart = System.currentTimeMillis();
        int ffmpegTimeoutSec = calculateFfmpegTimeoutSec(taskId, videoDuration, materialRequests, timeouts);
        currentTimeoutSec = ffmpegTimeoutSec;
        Map<String, Object> extractionPlanContext = new LinkedHashMap<>(materialResultPayload);
        extractionPlanContext.put("video_path", videoPath);
        extractionPlanContext.put("ffmpeg_timeout_sec", ffmpegTimeoutSec);
        extractionPlanContext.put("has_inflight_extraction", materialRequests.extractionFuture != null);
        extractionWave = planAssetExtractRuntimeWave(
                outputDir,
                "asset_extraction",
                "wave_0001",
                buildStablePayloadHash(extractionPlanContext),
                List.of(materialPlanWave.substageScopeRef),
                extractionPlanContext
        );
        if (taskRuntimeStageStore != null) {
            restoredExtractionPayload = taskRuntimeStageStore.loadCommittedChunkPayload(
                    outputDir,
                    "asset_extract_java",
                    extractionWave.chunkScopeId,
                    extractionWave.inputFingerprint
            );
        }
        if (!restoredExtractionPayload.isEmpty()) {
            JavaCVFFmpegService.ExtractionResult restoredExtractionResult =
                    restoreExtractionResult(restoredExtractionPayload, outputDir);
            if (restoredExtractionResult == null) {
                restoredExtractionPayload = new LinkedHashMap<>();
            } else {
                assetExtractRuntimeRepositoryAdapter.markExtractionResult(
                        outputDir,
                        restoredExtractionResult
                );
            }
        } else if (taskRuntimeStageStore != null) {
            extractionAttempt = taskRuntimeStageStore.loadLatestChunkAttempt(
                    outputDir,
                    "asset_extract_java",
                    extractionWave.chunkScopeId
            ) + 1;
            taskRuntimeStageStore.recordChunkState(
                    outputDir,
                    "asset_extract_java",
                    extractionWave.chunkScopeId,
                    extractionWave.inputFingerprint,
                    extractionAttempt,
                    Map.of(
                            "status", "RUNNING",
                            "stage_step", extractionWave.substageName,
                            "updated_at_ms", System.currentTimeMillis()
                    )
            );
        }
        JavaCVFFmpegService.ExtractionResult extractRes;
        if (restoredExtractionPayload.isEmpty()) {
            transitionAssetExtractRuntimeWave(
                    outputDir,
                    extractionWave,
                    "RUNNING",
                    extractionPlanContext,
                    null,
                    Map.of("attempt_count", extractionAttempt)
            );
            assetExtractRuntimeRepositoryAdapter.markExtractionRunning(
                    outputDir,
                    extractionAttempt,
                    ffmpegTimeoutSec
            );
            Map<String, Object> extractionCheckpointPayload = new LinkedHashMap<>(readyPayload);
            extractionCheckpointPayload.put("ffmpeg_timeout_sec", ffmpegTimeoutSec);
            writeAssetExtractStageCheckpoint(
                    outputDir,
                    taskId,
                    "RUNNING",
                    "asset_extraction_running",
                    stageCompleted,
                    stagePending,
                    extractionCheckpointPayload
            );
            try {
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
            } catch (Exception error) {
                String scopeFailureStatus = classifyAssetExtractScopeFailureStatus(error);
                Map<String, Object> failureResourceSnapshot = buildAssetExtractResourceSnapshot(
                        flowFlags,
                        extractionWave != null ? extractionWave.substageName : "asset_extraction",
                        currentTimeoutSec,
                        Map.of("video_duration_sec", videoDuration)
                );
                Map<String, Object> failureScopePayload = buildAssetExtractFailureScopePayload(
                        error,
                        extractionPlanContext,
                        failureResourceSnapshot,
                        scopeFailureStatus
                );
                transitionAssetExtractRuntimeWave(
                        outputDir,
                        extractionWave,
                        scopeFailureStatus,
                        extractionPlanContext,
                        failureResourceSnapshot,
                        mergeAttemptPayload(failureScopePayload, extractionAttempt)
                );
                if (taskRuntimeStageStore != null && extractionWave != null && extractionAttempt > 0) {
                    taskRuntimeStageStore.failChunkPayload(
                            outputDir,
                            "asset_extract_java",
                            extractionWave.chunkScopeId,
                            extractionWave.inputFingerprint,
                            extractionAttempt,
                            mergeAttemptPayload(
                                    failureScopePayload,
                                    extractionAttempt,
                                    Map.of(
                                            "status", scopeFailureStatus,
                                            "stage_step", extractionWave.substageName,
                                            "updated_at_ms", System.currentTimeMillis()
                                    )
                            )
                    );
                }
                assetExtractRuntimeRepositoryAdapter.markFailed(
                        outputDir,
                        extractionWave != null ? extractionWave.substageName : "asset_extraction",
                        scopeFailureStatus,
                        String.valueOf(failureScopePayload.getOrDefault("error_message", "asset extract failed"))
                );
                Map<String, Object> failedPayload = new LinkedHashMap<>(readyPayload);
                failedPayload.put("failed_substage", extractionWave != null ? extractionWave.substageName : "asset_extraction");
                failedPayload.put("error_class", failureScopePayload.getOrDefault("error_class", ""));
                failedPayload.put("error_code", failureScopePayload.getOrDefault("error_code", ""));
                failedPayload.put("error_message", failureScopePayload.getOrDefault("error_message", normalizeThrowableMessage(error, "asset extract failed")));
                failedPayload.put("required_action", failureScopePayload.getOrDefault("required_action", ""));
                failedPayload.put("retry_mode", failureScopePayload.getOrDefault("retry_mode", ""));
                failedPayload.put("retry_entry_point", failureScopePayload.getOrDefault("retry_entry_point", ""));
                if (currentTimeoutSec != null) {
                    failedPayload.put("ffmpeg_timeout_sec", currentTimeoutSec);
                }
                writeAssetExtractStageCheckpoint(outputDir, taskId, "MANUAL_NEEDED", "asset_extract_failed", stageCompleted, stagePending, failedPayload);
                throw error;
            }
            ioResult.stageTimingsMs.put("extract_assets", System.currentTimeMillis() - extractionStart);
        } else {
            extractRes = restoreExtractionResult(restoredExtractionPayload, outputDir);
            ioResult.stageTimingsMs.put("extract_assets", 0L);
        }
        Map<String, Object> extractionResultPayload = new LinkedHashMap<>();
        extractionResultPayload.put("assets_dir", firstNonBlank(extractRes.screenshotsDir, outputDir + "/assets"));
        extractionResultPayload.put("screenshots_dir", firstNonBlank(extractRes.screenshotsDir, outputDir + "/assets"));
        extractionResultPayload.put("clips_dir", firstNonBlank(extractRes.clipsDir, outputDir + "/assets"));
        extractionResultPayload.put("successful_screenshots", extractRes.successfulScreenshots);
        extractionResultPayload.put("successful_clips", extractRes.successfulClips);
        extractionResultPayload.put("error_count", extractRes.errors != null ? extractRes.errors.size() : 0);
        extractionResultPayload.put("errors", extractRes.errors != null ? extractRes.errors : List.of());
        extractionResultPayload.put("elapsed_ms", extractRes.elapsedMs);
        transitionAssetExtractRuntimeWave(
                outputDir,
                extractionWave,
                "SUCCESS",
                extractionPlanContext,
                null,
                Map.of(
                        "result_hash", buildStablePayloadHash(extractionResultPayload),
                        "attempt_count", extractionAttempt
                )
        );
        if (taskRuntimeStageStore != null && extractionWave != null && extractionAttempt > 0) {
            taskRuntimeStageStore.commitChunkPayload(
                    outputDir,
                    "asset_extract_java",
                    extractionWave.chunkScopeId,
                    extractionWave.inputFingerprint,
                    extractionAttempt,
                    Map.of(
                            "status", "SUCCESS",
                            "stage_step", extractionWave.substageName,
                            "result_hash", buildStablePayloadHash(extractionResultPayload),
                            "updated_at_ms", System.currentTimeMillis(),
                            "committed_at_ms", System.currentTimeMillis()
                    ),
                    extractionResultPayload
            );
        }
        assetExtractRuntimeRepositoryAdapter.markExtractionResult(outputDir, extractRes);
        stageCompleted = 2;
        stagePending = 1;

        try {
            Map<String, Object> outputsManifestPayload = new LinkedHashMap<>(readyPayload);
            outputsManifestPayload.putAll(extractionResultPayload);
            outputsManifestPayload.put("errors", extractRes.errors != null ? extractRes.errors : List.of());
            Map<String, Object> outputsFinalizePlanContext = new LinkedHashMap<>(extractionResultPayload);
            outputsFinalizePlanContext.put("material_source", usedVLFlow ? "vl" : "legacy");
            outputsFinalizeWave = planAssetExtractRuntimeWave(
                    outputDir,
                    "outputs_finalize",
                    "wave_0001",
                    buildStablePayloadHash(outputsFinalizePlanContext),
                    List.of(extractionWave.substageScopeRef),
                    outputsFinalizePlanContext
            );
            if (taskRuntimeStageStore != null) {
                restoredOutputsPayload = taskRuntimeStageStore.loadCommittedChunkPayload(
                        outputDir,
                        "asset_extract_java",
                        outputsFinalizeWave.chunkScopeId,
                        outputsFinalizeWave.inputFingerprint
                );
            }
            if (!restoredOutputsPayload.isEmpty()) {
                assetExtractRuntimeRepositoryAdapter.markOutputsReady(
                        outputDir,
                        restoreExtractionResult(restoredOutputsPayload, outputDir)
                );
                writeAssetExtractStageCheckpoint(outputDir, taskId, "SUCCESS", "outputs_ready", 3, 0, restoredOutputsPayload);
            } else if (taskRuntimeStageStore != null) {
                outputsFinalizeAttempt = taskRuntimeStageStore.loadLatestChunkAttempt(
                        outputDir,
                        "asset_extract_java",
                        outputsFinalizeWave.chunkScopeId
                ) + 1;
                taskRuntimeStageStore.recordChunkState(
                        outputDir,
                        "asset_extract_java",
                        outputsFinalizeWave.chunkScopeId,
                        outputsFinalizeWave.inputFingerprint,
                        outputsFinalizeAttempt,
                        Map.of(
                                "status", "RUNNING",
                                "stage_step", outputsFinalizeWave.substageName,
                                "updated_at_ms", System.currentTimeMillis()
                        )
                );
            }
            if (restoredOutputsPayload.isEmpty()) {
                transitionAssetExtractRuntimeWave(
                        outputDir,
                        outputsFinalizeWave,
                        "RUNNING",
                        outputsFinalizePlanContext,
                        null,
                        Map.of("attempt_count", outputsFinalizeAttempt)
                );
                writeAssetExtractStageCheckpoint(outputDir, taskId, "RUNNING", "outputs_finalizing", stageCompleted, stagePending, outputsManifestPayload);
                Map<String, Object> completedPayload = new LinkedHashMap<>(outputsManifestPayload);
                transitionAssetExtractRuntimeWave(
                        outputDir,
                        outputsFinalizeWave,
                        "SUCCESS",
                        outputsFinalizePlanContext,
                        null,
                        Map.of(
                                "result_hash", buildStablePayloadHash(completedPayload),
                                "attempt_count", outputsFinalizeAttempt
                        )
                );
                if (taskRuntimeStageStore != null && outputsFinalizeWave != null && outputsFinalizeAttempt > 0) {
                    taskRuntimeStageStore.commitChunkPayload(
                            outputDir,
                            "asset_extract_java",
                            outputsFinalizeWave.chunkScopeId,
                            outputsFinalizeWave.inputFingerprint,
                            outputsFinalizeAttempt,
                            Map.of(
                                    "status", "SUCCESS",
                                    "stage_step", outputsFinalizeWave.substageName,
                                    "result_hash", buildStablePayloadHash(completedPayload),
                                    "updated_at_ms", System.currentTimeMillis(),
                                    "committed_at_ms", System.currentTimeMillis()
                            ),
                            completedPayload
                    );
                }
                assetExtractRuntimeRepositoryAdapter.markOutputsReady(outputDir, extractRes);
                writeAssetExtractStageCheckpoint(outputDir, taskId, "SUCCESS", "outputs_ready", 3, 0, completedPayload);
            }
        } catch (Exception error) {
            String scopeFailureStatus = classifyAssetExtractScopeFailureStatus(error);
            Map<String, Object> failureResourceSnapshot = buildAssetExtractResourceSnapshot(
                    flowFlags,
                    outputsFinalizeWave != null ? outputsFinalizeWave.substageName : "outputs_finalize",
                    currentTimeoutSec,
                    Map.of("video_duration_sec", videoDuration)
            );
            Map<String, Object> failureScopePayload = buildAssetExtractFailureScopePayload(
                    error,
                    extractionResultPayload,
                    failureResourceSnapshot,
                    scopeFailureStatus
            );
            transitionAssetExtractRuntimeWave(
                    outputDir,
                    outputsFinalizeWave,
                    scopeFailureStatus,
                    extractionResultPayload,
                    failureResourceSnapshot,
                    mergeAttemptPayload(failureScopePayload, outputsFinalizeAttempt)
            );
            if (taskRuntimeStageStore != null && outputsFinalizeWave != null && outputsFinalizeAttempt > 0) {
                taskRuntimeStageStore.failChunkPayload(
                        outputDir,
                        "asset_extract_java",
                        outputsFinalizeWave.chunkScopeId,
                        outputsFinalizeWave.inputFingerprint,
                        outputsFinalizeAttempt,
                        mergeAttemptPayload(
                                failureScopePayload,
                                outputsFinalizeAttempt,
                                Map.of(
                                        "status", scopeFailureStatus,
                                        "stage_step", outputsFinalizeWave.substageName,
                                        "updated_at_ms", System.currentTimeMillis()
                                )
                        )
                );
            }
            assetExtractRuntimeRepositoryAdapter.markFailed(
                    outputDir,
                    outputsFinalizeWave != null ? outputsFinalizeWave.substageName : "outputs_finalize",
                    scopeFailureStatus,
                    String.valueOf(failureScopePayload.getOrDefault("error_message", "asset extract failed"))
            );
            Map<String, Object> failedPayload = new LinkedHashMap<>(basePayload);
            failedPayload.put("failed_substage", outputsFinalizeWave != null ? outputsFinalizeWave.substageName : "outputs_finalize");
            failedPayload.put("error_class", failureScopePayload.getOrDefault("error_class", ""));
            failedPayload.put("error_code", failureScopePayload.getOrDefault("error_code", ""));
            failedPayload.put("error_message", failureScopePayload.getOrDefault("error_message", normalizeThrowableMessage(error, "asset extract failed")));
            failedPayload.put("required_action", failureScopePayload.getOrDefault("required_action", ""));
            failedPayload.put("retry_mode", failureScopePayload.getOrDefault("retry_mode", ""));
            failedPayload.put("retry_entry_point", failureScopePayload.getOrDefault("retry_entry_point", ""));
            if (currentTimeoutSec != null) {
                failedPayload.put("ffmpeg_timeout_sec", currentTimeoutSec);
            }
            writeAssetExtractStageCheckpoint(outputDir, taskId, "MANUAL_NEEDED", "asset_extract_failed", stageCompleted, stagePending, failedPayload);
            throw error;
        }

        return new AssetExtractStageResult(analyzeResult, usedVLFlow, usedLegacyFlow);
    }

    private AssetExtractRuntimeWave planAssetExtractRuntimeWave(
            String outputDir,
            String substageName,
            String waveId,
            String inputFingerprint,
            List<String> dependsOnScopeRefs,
            Map<String, Object> planContext
    ) {
        String normalizedWaveId = firstNonBlank(waveId, "wave_0001");
        if (taskRuntimeStageStore == null) {
            String scopeId = substageName + "." + normalizedWaveId;
            String scopeRef = "asset_extract_java/substage/" + scopeId;
            String chunkRef = "asset_extract_java/chunk/" + scopeId;
            return new AssetExtractRuntimeWave(substageName, normalizedWaveId, inputFingerprint, scopeId, scopeId, scopeRef, chunkRef);
        }
        String substageScopeId = taskRuntimeStageStore.buildSubstageScopeId(substageName, normalizedWaveId);
        String chunkScopeId = substageScopeId;
        String substageScopeRef = taskRuntimeStageStore.buildSubstageScopeRef("asset_extract_java", substageName, normalizedWaveId);
        String chunkScopeRef = taskRuntimeStageStore.buildScopeRef("asset_extract_java", "chunk", chunkScopeId);
        Map<String, Object> substagePayload = new LinkedHashMap<>();
        substagePayload.put("stage_step", substageName);
        substagePayload.put("plan_context", new LinkedHashMap<>(planContext != null ? planContext : Map.of()));
        substagePayload.put("attempt_count", 0);
        taskRuntimeStageStore.planSubstageScope(
                outputDir,
                "asset_extract_java",
                substageName,
                normalizedWaveId,
                inputFingerprint,
                dependsOnScopeRefs,
                substagePayload
        );
        Map<String, Object> chunkPayload = new LinkedHashMap<>();
        chunkPayload.put("stage_step", substageName);
        chunkPayload.put("chunk_id", chunkScopeId);
        chunkPayload.put("plan_context", new LinkedHashMap<>(planContext != null ? planContext : Map.of()));
        chunkPayload.put("attempt_count", 0);
        taskRuntimeStageStore.planScopeNode(
                outputDir,
                "asset_extract_java",
                "chunk",
                chunkScopeId,
                "",
                inputFingerprint,
                dependsOnScopeRefs,
                chunkPayload
        );
        return new AssetExtractRuntimeWave(
                substageName,
                normalizedWaveId,
                inputFingerprint,
                substageScopeId,
                chunkScopeId,
                substageScopeRef,
                chunkScopeRef
        );
    }

    private void transitionAssetExtractRuntimeWave(
            String outputDir,
            AssetExtractRuntimeWave wave,
            String status,
            Map<String, Object> planContext,
            Map<String, Object> resourceSnapshot,
            Map<String, Object> extraPayload
    ) {
        if (taskRuntimeStageStore == null || wave == null) {
            return;
        }
        int attemptCount = 0;
        if (extraPayload != null && extraPayload.get("attempt_count") != null) {
            try {
                attemptCount = Math.max(0, Integer.parseInt(String.valueOf(extraPayload.get("attempt_count"))));
            } catch (Exception ignored) {
                attemptCount = 0;
            }
        } else if ("RUNNING".equalsIgnoreCase(status)) {
            attemptCount = 1;
        }
        Map<String, Object> substagePayload = new LinkedHashMap<>(extraPayload != null ? extraPayload : Map.of());
        substagePayload.put("stage_step", wave.substageName);
        substagePayload.put("attempt_count", attemptCount);
        substagePayload.put("plan_context", new LinkedHashMap<>(planContext != null ? planContext : Map.of()));
        if (resourceSnapshot != null && !resourceSnapshot.isEmpty()) {
            substagePayload.put("resource_snapshot", new LinkedHashMap<>(resourceSnapshot));
        }
        taskRuntimeStageStore.transitionScopeNode(
                outputDir,
                "asset_extract_java",
                "substage",
                wave.substageScopeId,
                "",
                status,
                wave.inputFingerprint,
                substagePayload
        );

        Map<String, Object> chunkPayload = new LinkedHashMap<>(extraPayload != null ? extraPayload : Map.of());
        chunkPayload.put("stage_step", wave.substageName);
        chunkPayload.put("chunk_id", wave.chunkScopeId);
        chunkPayload.put("attempt_count", attemptCount);
        chunkPayload.put("plan_context", new LinkedHashMap<>(planContext != null ? planContext : Map.of()));
        if (resourceSnapshot != null && !resourceSnapshot.isEmpty()) {
            chunkPayload.put("resource_snapshot", new LinkedHashMap<>(resourceSnapshot));
        }
        taskRuntimeStageStore.transitionScopeNode(
                outputDir,
                "asset_extract_java",
                "chunk",
                wave.chunkScopeId,
                "",
                status,
                wave.inputFingerprint,
                chunkPayload
        );
    }

    private Map<String, Object> buildAssetExtractFailureScopePayload(
            Throwable error,
            Map<String, Object> planContext,
            Map<String, Object> resourceSnapshot,
            String status
    ) {
        Throwable root = rootCause(error);
        String normalizedStatus = firstNonBlank(status, "MANUAL_NEEDED").toUpperCase(Locale.ROOT);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("plan_context", new LinkedHashMap<>(planContext != null ? planContext : Map.of()));
        payload.put("resource_snapshot", new LinkedHashMap<>(resourceSnapshot != null ? resourceSnapshot : Map.of()));
        payload.put("error_class", root.getClass().getSimpleName());
        payload.put("error_code", root.getClass().getSimpleName());
        payload.put("error_message", normalizeThrowableMessage(error, "asset extract failed"));
        payload.put("retry_mode", "manual");
        payload.put("retry_entry_point", "from_last_checkpoint");
        if ("ERROR".equals(normalizedStatus)) {
            payload.put("required_action", "需要先检查底层依赖、资源状态或超时配置，再从当前子阶段/当前计算单元重试。");
        } else {
            payload.put("required_action", "需要人工检查错误上下文后，从当前子阶段/当前计算单元重试。");
        }
        return payload;
    }

    private Map<String, Object> mergeAttemptPayload(
            Map<String, Object> basePayload,
            int attemptCount
    ) {
        return mergeAttemptPayload(basePayload, attemptCount, Map.of());
    }

    private Map<String, Object> mergeAttemptPayload(
            Map<String, Object> basePayload,
            int attemptCount,
            Map<String, Object> extraPayload
    ) {
        Map<String, Object> payload = new LinkedHashMap<>(basePayload != null ? basePayload : Map.of());
        payload.put("attempt_count", Math.max(0, attemptCount));
        if (extraPayload != null && !extraPayload.isEmpty()) {
            payload.putAll(extraPayload);
        }
        return payload;
    }

    private Map<String, Object> buildAssetExtractResourceSnapshot(
            Map<String, Object> flowFlags,
            String substageName,
            Integer ffmpegTimeoutSec,
            Map<String, Object> extraSnapshot
    ) {
        Runtime runtime = Runtime.getRuntime();
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("substage_name", firstNonBlank(substageName, "unknown"));
        snapshot.put("available_processors", runtime.availableProcessors());
        snapshot.put("max_memory_bytes", runtime.maxMemory());
        snapshot.put("total_memory_bytes", runtime.totalMemory());
        snapshot.put("free_memory_bytes", runtime.freeMemory());
        if (ffmpegTimeoutSec != null) {
            snapshot.put("ffmpeg_timeout_sec", ffmpegTimeoutSec);
        }
        if (flowFlags != null && !flowFlags.isEmpty()) {
            snapshot.put("flow_flags", new LinkedHashMap<>(flowFlags));
        }
        if (extraSnapshot != null && !extraSnapshot.isEmpty()) {
            snapshot.putAll(extraSnapshot);
        }
        return snapshot;
    }

    private String classifyAssetExtractScopeFailureStatus(Throwable error) {
        Throwable root = rootCause(error);
        String message = normalizeThrowableMessage(root, "").toLowerCase(Locale.ROOT);
        if (root instanceof OutOfMemoryError
                || root instanceof TimeoutException
                || root instanceof RejectedExecutionException
                || root instanceof IOException
                || message.contains("outofmemory")
                || message.contains("oom")) {
            return "ERROR";
        }
        if (root instanceof IllegalArgumentException || root instanceof IllegalStateException) {
            return "FAILED";
        }
        return "MANUAL_NEEDED";
    }

    private Throwable rootCause(Throwable error) {
        Throwable cursor = error;
        int depth = 0;
        while (cursor != null && cursor.getCause() != null && depth < 16) {
            cursor = cursor.getCause();
            depth++;
        }
        return cursor != null ? cursor : error;
    }

    private String buildStablePayloadHash(Object payload) {
        if (payload == null) {
            return "";
        }
        try {
            return sha256Hex(objectMapper.writeValueAsBytes(payload));
        } catch (Exception error) {
            logger.warn("Failed to hash asset extract payload", error);
            return "";
        }
    }

    private void writeAssetExtractStageCheckpoint(
            String outputDir,
            String taskId,
            String status,
            String checkpoint,
            int completed,
            int pending,
            Map<String, Object> payload
    ) {
        if (taskRuntimeStageStore == null) {
            return;
        }
        taskRuntimeStageStore.writeStageCheckpoint(
                outputDir,
                taskId,
                "asset_extract_java",
                status,
                checkpoint,
                completed,
                pending,
                payload
        );
    }

    private void appendAssetExtractJournal(String outputDir, Map<String, Object> payload) {
    }

    private List<Map<String, Object>> buildScreenshotRequestPayload(List<JavaCVFFmpegService.ScreenshotRequest> requests) {
        List<Map<String, Object>> payload = new ArrayList<>();
        for (JavaCVFFmpegService.ScreenshotRequest request : requests != null ? requests : List.<JavaCVFFmpegService.ScreenshotRequest>of()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("screenshot_id", request.screenshotId);
            item.put("timestamp_sec", request.timestampSec);
            item.put("label", request.label);
            item.put("semantic_unit_id", request.semanticUnitId);
            item.put("frame_reason", request.frameReason);
            payload.add(item);
        }
        return payload;
    }

    private List<Map<String, Object>> buildClipRequestPayload(List<JavaCVFFmpegService.ClipRequest> requests) {
        List<Map<String, Object>> payload = new ArrayList<>();
        for (JavaCVFFmpegService.ClipRequest request : requests != null ? requests : List.<JavaCVFFmpegService.ClipRequest>of()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("clip_id", request.clipId);
            item.put("start_sec", request.startSec);
            item.put("end_sec", request.endSec);
            item.put("knowledge_type", request.knowledgeType);
            item.put("semantic_unit_id", request.semanticUnitId);
            List<Map<String, Object>> segments = new ArrayList<>();
            for (JavaCVFFmpegService.ClipSegment segment : request.segments != null ? request.segments : List.<JavaCVFFmpegService.ClipSegment>of()) {
                Map<String, Object> segmentPayload = new LinkedHashMap<>();
                segmentPayload.put("start_sec", segment.startSec);
                segmentPayload.put("end_sec", segment.endSec);
                segments.add(segmentPayload);
            }
            item.put("segments", segments);
            payload.add(item);
        }
        return payload;
    }

    private ExtractionRequests restoreExtractionRequests(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            return null;
        }
        List<JavaCVFFmpegService.ScreenshotRequest> screenshotRequests =
                restoreScreenshotRequests(payload.get("screenshot_requests"));
        List<JavaCVFFmpegService.ClipRequest> clipRequests =
                restoreClipRequests(payload.get("clip_requests"));
        if (screenshotRequests.isEmpty() && clipRequests.isEmpty()) {
            return null;
        }
        return new ExtractionRequests(screenshotRequests, clipRequests);
    }

    private List<JavaCVFFmpegService.ScreenshotRequest> restoreScreenshotRequests(Object payload) {
        List<JavaCVFFmpegService.ScreenshotRequest> requests = new ArrayList<>();
        if (!(payload instanceof List<?> rawList)) {
            return requests;
        }
        for (Object item : rawList) {
            if (!(item instanceof Map<?, ?> rawMap)) {
                continue;
            }
            String screenshotId = String.valueOf(mapValue(rawMap, "screenshot_id", "")).trim();
            if (screenshotId.isBlank()) {
                continue;
            }
            requests.add(new JavaCVFFmpegService.ScreenshotRequest(
                    screenshotId,
                    safeDouble(rawMap.get("timestamp_sec"), 0.0),
                    String.valueOf(mapValue(rawMap, "label", "")),
                    String.valueOf(mapValue(rawMap, "semantic_unit_id", "")),
                    String.valueOf(mapValue(rawMap, "frame_reason", ""))
            ));
        }
        return requests;
    }

    private List<JavaCVFFmpegService.ClipRequest> restoreClipRequests(Object payload) {
        List<JavaCVFFmpegService.ClipRequest> requests = new ArrayList<>();
        if (!(payload instanceof List<?> rawList)) {
            return requests;
        }
        for (Object item : rawList) {
            if (!(item instanceof Map<?, ?> rawMap)) {
                continue;
            }
            String clipId = String.valueOf(mapValue(rawMap, "clip_id", "")).trim();
            if (clipId.isBlank()) {
                continue;
            }
            requests.add(new JavaCVFFmpegService.ClipRequest(
                    clipId,
                    safeDouble(rawMap.get("start_sec"), 0.0),
                    safeDouble(rawMap.get("end_sec"), 0.0),
                    String.valueOf(mapValue(rawMap, "knowledge_type", "")),
                    String.valueOf(mapValue(rawMap, "semantic_unit_id", "")),
                    restoreClipSegments(rawMap.get("segments"))
            ));
        }
        return requests;
    }

    private List<JavaCVFFmpegService.ClipSegment> restoreClipSegments(Object payload) {
        List<JavaCVFFmpegService.ClipSegment> segments = new ArrayList<>();
        if (!(payload instanceof List<?> rawList)) {
            return segments;
        }
        for (Object item : rawList) {
            if (!(item instanceof Map<?, ?> rawMap)) {
                continue;
            }
            segments.add(new JavaCVFFmpegService.ClipSegment(
                    safeDouble(rawMap.get("start_sec"), 0.0),
                    safeDouble(rawMap.get("end_sec"), 0.0)
            ));
        }
        return segments;
    }

    private Object mapValue(Map<?, ?> rawMap, String key, Object defaultValue) {
        return rawMap.containsKey(key) ? rawMap.get(key) : defaultValue;
    }

    private JavaCVFFmpegService.ExtractionResult restoreExtractionResult(Map<String, Object> payload, String outputDir) {
        if (payload == null || payload.isEmpty()) {
            return null;
        }
        JavaCVFFmpegService.ExtractionResult result = new JavaCVFFmpegService.ExtractionResult();
        result.screenshotsDir = firstNonBlank(String.valueOf(payload.getOrDefault("screenshots_dir", "")), outputDir + "/assets");
        result.clipsDir = firstNonBlank(String.valueOf(payload.getOrDefault("clips_dir", "")), outputDir + "/assets");
        result.successfulScreenshots = safeInt(payload.get("successful_screenshots"), 0);
        result.successfulClips = safeInt(payload.get("successful_clips"), 0);
        result.elapsedMs = safeLong(payload.get("elapsed_ms"), 0L);
        Object errors = payload.get("errors");
        if (errors instanceof List<?> rawErrors) {
            for (Object item : rawErrors) {
                result.errors.add(String.valueOf(item));
            }
        }
        return result;
    }

    private double safeDouble(Object value, double fallback) {
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private int safeInt(Object value, int fallback) {
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private long safeLong(Object value, long fallback) {
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private static class AssetExtractStageResult {
        private final AnalyzeResult analyzeResult;
        private final boolean usedVLFlow;
        private final boolean usedLegacyFlow;

        private AssetExtractStageResult(AnalyzeResult analyzeResult, boolean usedVLFlow, boolean usedLegacyFlow) {
            this.analyzeResult = analyzeResult;
            this.usedVLFlow = usedVLFlow;
            this.usedLegacyFlow = usedLegacyFlow;
        }
    }

    private ProcessingResult processBook(
            String taskId,
            String sourceUrl,
            String outputDir,
            BookProcessingOptions bookOptions
    ) {
        ProcessingResult result = new ProcessingResult();
        result.taskId = taskId;
        long startTime = System.currentTimeMillis();
        String metricsOutputDir = outputDir;
        String metricsVideoPath = sourceUrl;
        String metricsInputVideoUrl = sourceUrl;
        String metricsVideoTitle = "";
        Map<String, Long> stageTimingsMs = new LinkedHashMap<>();
        Map<String, Object> flowFlags = new LinkedHashMap<>();

        try {
            String sourcePath = sourceUrl;
            boolean downloadedFromUrl = false;
            String cleanupSourcePath = null;
            DownloadResult downloadResult = null;
            ArticleBookSource articleSource = null;

            long articleExtractStart = System.currentTimeMillis();
            if (isHttpUrl(sourceUrl)) {
                articleSource = prepareBookSourceFromArticleLink(taskId, sourceUrl);
                if (articleSource != null) {
                    sourcePath = articleSource.markdownPath;
                    outputDir = articleSource.outputDir;
                    downloadedFromUrl = true;
                    metricsVideoPath = sourcePath;
                    metricsOutputDir = outputDir;
                    if (StringUtils.hasText(articleSource.title)) {
                        metricsVideoTitle = articleSource.title;
                    }
                    flowFlags.put("article_link_enabled", true);
                    flowFlags.put("article_link_requested_url", firstNonBlank(articleSource.requestedUrl, sourceUrl));
                    flowFlags.put("article_link_final_url", firstNonBlank(articleSource.finalUrl, articleSource.requestedUrl));
                    flowFlags.put("article_link_site", firstNonBlank(articleSource.siteType, "unknown"));
                    flowFlags.put("article_link_image_count", articleSource.imageCount);
                    flowFlags.put("article_link_copied_image_count", articleSource.copiedImageCount);
                }
            }
            if (articleSource != null) {
                stageTimingsMs.put("extract_article_link", System.currentTimeMillis() - articleExtractStart);
                stageTimingsMs.put("prepare_local_video", 0L);
                stageTimingsMs.put("download_video", 0L);
            } else {
                flowFlags.put("article_link_enabled", false);
                long localPrepareStart = System.currentTimeMillis();
                if (!isHttpUrl(sourceUrl)) {
                    sourcePath = normalizeLocalVideoPath(sourceUrl);
                    assertLocalVideoExists(sourceUrl, sourcePath);
                    outputDir = resolveBookTaskOutputDir(outputDir, sourcePath, bookOptions);
                    new File(outputDir).mkdirs();
                    sourcePath = ensureLocalVideoInStorage(sourcePath, outputDir);
                }
                stageTimingsMs.put("prepare_local_video", System.currentTimeMillis() - localPrepareStart);
                metricsVideoPath = sourcePath;
                metricsOutputDir = outputDir;

                long downloadStart = System.currentTimeMillis();
                try {
                    if (isHttpUrl(sourceUrl)) {
                        int downloadTimeoutSec = normalizePositive(downloadGrpcDeadlineSec, 1800);
                        int hardTimeoutSec = Math.max(downloadTimeoutSec, normalizePositive(downloadHardTimeoutSec, 1800));
                        int idleTimeoutSec = normalizePositive(downloadIdleTimeoutSec, 120);
                        int pollIntervalSec = Math.min(
                                normalizePositive(downloadPollIntervalSec, 5),
                                Math.max(1, idleTimeoutSec)
                        );
                        String predictedDownloadDir = resolvePredictedDownloadWatchDir(sourceUrl, outputDir);
                        updateProgress(taskId, 0.05, "Downloading source file...");
                        DownloadResult dl = waitForDownloadWithLease(
                                taskId,
                                sourceUrl,
                                outputDir,
                                predictedDownloadDir,
                                downloadTimeoutSec,
                                hardTimeoutSec,
                                idleTimeoutSec,
                                pollIntervalSec
                        );
                        if (!dl.success) {
                            throw new RuntimeException(
                                    "Download failed: " + firstNonBlank(
                                            dl.errorMsg,
                                            "python worker returned unsuccessful download response without details"
                                    )
                            );
                        }
                        downloadedFromUrl = true;
                        downloadResult = dl;
                        sourcePath = dl.videoPath;
                        cleanupSourcePath = dl.videoPath;
                        if (hasExplicitBookLeafOutputDir(outputDir, bookOptions)) {
                            outputDir = Paths.get(outputDir).toAbsolutePath().normalize().toString();
                            new File(outputDir).mkdirs();
                            sourcePath = ensureLocalVideoInStorage(sourcePath, outputDir);
                        } else {
                            outputDir = new File(sourcePath).getParentFile().getAbsolutePath();
                            new File(outputDir).mkdirs();
                        }
                        metricsVideoPath = sourcePath;
                        metricsOutputDir = outputDir;
                    }
                } finally {
                    stageTimingsMs.put("download_video", System.currentTimeMillis() - downloadStart);
                }
            }

            if (!isBookPath(sourcePath)) {
                throw new IllegalArgumentException("Book format not supported for source: " + sourcePath);
            }

            TaskProgressWatchdogBridge.SignalEmitter bookSignalEmitter =
                (progress, message) -> updateProgress(taskId, progress, message);
            updateProgress(taskId, 0.20, "Extracting book content...");
            long bookFlowStart = System.currentTimeMillis();
            boolean pdfBookSource = firstNonBlank(sourcePath, "").toLowerCase(Locale.ROOT).endsWith(".pdf");
            BookMarkdownService.BookProcessingResult bookResult;
            if (pdfBookSource) {
                taskProgressWatchdogBridge.resetTask(taskId);
                TaskProgressWatchdogBridge.MonitorHandle bookExtractMonitor =
                    taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "book_pdf_extract", bookSignalEmitter);
                try {
                    bookResult = bookMarkdownService.processBook(
                            taskId,
                            sourcePath,
                            outputDir,
                            toBookServiceOptions(bookOptions)
                    );
                } finally {
                    taskProgressWatchdogBridge.stopMonitor(taskId, bookExtractMonitor, bookSignalEmitter);
                }
            } else {
                bookResult = bookMarkdownService.processBook(
                        taskId,
                        sourcePath,
                        outputDir,
                        toBookServiceOptions(bookOptions)
                );
            }
            stageTimingsMs.put("book_extract_markdown", System.currentTimeMillis() - bookFlowStart);
            if (!bookResult.success) {
                throw new RuntimeException(
                        "Book processing failed: " + firstNonBlank(
                                bookResult.errorMessage,
                                "unknown error from BookMarkdownService"
                        )
                );
            }

            BookEnhancedPipelineService.EnhancedResult enhancedResult = null;
            boolean enhancedApplied = false;
            boolean enhancedEnabled = bookEnhancedPipelineService != null && bookEnhancedPipelineService.isEnabled();
            flowFlags.put("book_enhanced_enabled", enhancedEnabled);
            if (enhancedEnabled) {
                updateProgress(taskId, 0.60, "Enhancing book content...");
                long enhancedStart = System.currentTimeMillis();
                enhancedResult = bookEnhancedPipelineService.enhanceBook(
                        taskId,
                        sourcePath,
                        outputDir,
                        bookResult
                );
                stageTimingsMs.put("book_enhanced_pipeline", System.currentTimeMillis() - enhancedStart);
                if (enhancedResult != null && enhancedResult.stageTimingsMs != null) {
                    for (Map.Entry<String, Long> stageEntry : enhancedResult.stageTimingsMs.entrySet()) {
                        String key = stageEntry.getKey();
                        Long value = stageEntry.getValue();
                        if (key == null || key.isBlank() || value == null) {
                            continue;
                        }
                        stageTimingsMs.put("book_enhanced_" + key, value);
                    }
                }
                if (enhancedResult != null) {
                    flowFlags.put("book_enhanced_translation_attempted", enhancedResult.translationAttempted);
                    flowFlags.put("book_enhanced_translation_applied", enhancedResult.translationApplied);
                    flowFlags.put("book_enhanced_translated_blocks", enhancedResult.translatedBlockCount);
                    flowFlags.put("book_enhanced_protected_blocks", enhancedResult.protectedBlockCount);
                    flowFlags.put("book_enhanced_unit_count", enhancedResult.phase2SemanticUnitCount);
                }
                if (enhancedResult != null
                        && enhancedResult.success
                        && enhancedResult.enhancementApplied
                        && enhancedResult.markdownPath != null
                        && !enhancedResult.markdownPath.isBlank()) {
                    enhancedApplied = true;
                    result.markdownPath = enhancedResult.markdownPath;
                    result.jsonPath = firstNonBlank(enhancedResult.jsonPath, bookResult.metadataPath);
                } else {
                    flowFlags.put("book_enhanced_fallback_reason", firstNonBlank(
                            enhancedResult != null ? enhancedResult.errorMessage : null,
                            "enhanced_result_unavailable"
                    ));
                }
            }

            updateProgress(taskId, 0.95, "Finalizing markdown...");
            result.success = true;
            result.cleanupSourcePath = cleanupSourcePath;
            if (!enhancedApplied) {
                result.markdownPath = bookResult.markdownPath;
                result.jsonPath = bookResult.metadataPath;
            }
            persistTaskTocMetadata(
                    outputDir,
                    firstNonBlank(bookResult.contentType, "book"),
                    bookResult.bookSectionTree != null ? bookResult.bookSectionTree : List.of()
            );
            if (!StringUtils.hasText(metricsVideoTitle)) {
                metricsVideoTitle = firstNonBlank(
                        bookResult.leafTitle,
                        bookOptions != null ? bookOptions.leafTitle : null
                );
            }
            if (!StringUtils.hasText(metricsVideoTitle)) {
                metricsVideoTitle = stripExtensionSafe(new File(sourcePath).getName());
            }

            flowFlags.put("downloaded_from_url", downloadedFromUrl);
            if (downloadResult != null) {
                flowFlags.put("download_content_type", firstNonBlank(downloadResult.contentType, "unknown"));
                flowFlags.put("download_source_platform", firstNonBlank(downloadResult.sourcePlatform, "unknown"));
            }
            flowFlags.put("used_vl_flow", false);
            flowFlags.put("used_legacy_flow", false);
            flowFlags.put("used_book_flow", true);
            flowFlags.put("book_enhanced_applied", enhancedApplied);
            flowFlags.put("book_split_by_chapter", bookOptions == null || bookOptions.splitByChapter == null || bookOptions.splitByChapter);
            flowFlags.put("book_split_by_section", bookOptions != null && Boolean.TRUE.equals(bookOptions.splitBySection));
            if (bookOptions != null && bookOptions.bookTitle != null && !bookOptions.bookTitle.isBlank()) {
                flowFlags.put("book_title", bookOptions.bookTitle.trim());
            } else if (bookResult.bookTitle != null && !bookResult.bookTitle.isBlank()) {
                flowFlags.put("book_title", bookResult.bookTitle.trim());
            }
            if (bookOptions != null && bookOptions.leafTitle != null && !bookOptions.leafTitle.isBlank()) {
                flowFlags.put("book_leaf_title", bookOptions.leafTitle.trim());
            } else if (bookResult.leafTitle != null && !bookResult.leafTitle.isBlank()) {
                flowFlags.put("book_leaf_title", bookResult.leafTitle.trim());
            }
            if (bookOptions != null && bookOptions.leafOutlineIndex != null && !bookOptions.leafOutlineIndex.isBlank()) {
                flowFlags.put("book_leaf_outline_index", bookOptions.leafOutlineIndex.trim());
            } else if (bookResult.leafOutlineIndex != null && !bookResult.leafOutlineIndex.isBlank()) {
                flowFlags.put("book_leaf_outline_index", bookResult.leafOutlineIndex.trim());
            }
            if (bookOptions != null && bookOptions.storageKey != null && !bookOptions.storageKey.isBlank()) {
                flowFlags.put("book_leaf_storage_key", bookOptions.storageKey.trim());
            }
            if (bookOptions != null && bookOptions.chapterSelector != null && !bookOptions.chapterSelector.isBlank()) {
                flowFlags.put("book_chapter_selector", bookOptions.chapterSelector.trim());
            }
            if (bookOptions != null && bookOptions.sectionSelector != null && !bookOptions.sectionSelector.isBlank()) {
                flowFlags.put("book_section_selector", bookOptions.sectionSelector.trim());
            }
            if (bookOptions != null && bookOptions.pageOffset != null) {
                flowFlags.put("book_page_offset", bookOptions.pageOffset);
            }
        } catch (Throwable error) {
            String normalizedError = normalizeThrowableMessage(error, "Book pipeline failed with unknown throwable");
            logger.error("[{}] Book pipeline failed: {}", taskId, normalizedError, error);
            result.success = false;
            result.errorMessage = normalizedError;

            flowFlags.putIfAbsent("downloaded_from_url", false);
            flowFlags.putIfAbsent("used_vl_flow", false);
            flowFlags.putIfAbsent("used_legacy_flow", false);
            flowFlags.putIfAbsent("used_book_flow", true);
        } finally {
            result.processingTimeMs = System.currentTimeMillis() - startTime;
            stageTimingsMs.put("total_pipeline", result.processingTimeMs);
            writeTaskMetricsReport(
                    taskId,
                    metricsOutputDir,
                    metricsVideoPath,
                    metricsInputVideoUrl,
                    metricsVideoTitle,
                    result,
                    stageTimingsMs,
                    flowFlags
            );
        }
        return result;
    }

    private BookMarkdownService.BookProcessingOptions toBookServiceOptions(BookProcessingOptions raw) {
        if (raw == null) {
            return null;
        }
        BookMarkdownService.BookProcessingOptions options = new BookMarkdownService.BookProcessingOptions();
        options.chapterSelector = raw.chapterSelector;
        options.sectionSelector = raw.sectionSelector;
        options.splitByChapter = raw.splitByChapter;
        options.splitBySection = raw.splitBySection;
        options.pageOffset = raw.pageOffset;
        options.bookTitle = raw.bookTitle;
        options.leafTitle = raw.leafTitle;
        options.leafOutlineIndex = raw.leafOutlineIndex;
        options.storageKey = raw.storageKey;
        if ((options.chapterSelector == null || options.chapterSelector.isBlank())
                && (options.sectionSelector == null || options.sectionSelector.isBlank())
                && options.splitByChapter == null
                && options.splitBySection == null
                && options.pageOffset == null
                && (options.bookTitle == null || options.bookTitle.isBlank())
                && (options.leafTitle == null || options.leafTitle.isBlank())
                && (options.leafOutlineIndex == null || options.leafOutlineIndex.isBlank())
                && (options.storageKey == null || options.storageKey.isBlank())) {
            return null;
        }
        return options;
    }

    public boolean shouldRunBookPipeline(String source, BookProcessingOptions options) {
        return shouldProcessAsBook(source, options);
    }

    private boolean shouldProcessAsBook(String source, BookProcessingOptions options) {
        if (options != null
                && ((options.chapterSelector != null && !options.chapterSelector.isBlank())
                || (options.sectionSelector != null && !options.sectionSelector.isBlank())
                || options.splitByChapter != null
                || options.splitBySection != null
                || options.pageOffset != null
                || (options.bookTitle != null && !options.bookTitle.isBlank())
                || (options.leafTitle != null && !options.leafTitle.isBlank())
                || (options.leafOutlineIndex != null && !options.leafOutlineIndex.isBlank())
                || (options.storageKey != null && !options.storageKey.isBlank()))) {
            return true;
        }
        if (source == null || source.isBlank()) {
            return false;
        }
        if (isHttpUrl(source)) {
            return isBookPathFromUrl(source) || isSupportedArticleLink(source);
        }
        return isBookPath(normalizeLocalVideoPath(source));
    }

    private boolean isBookPathFromUrl(String sourceUrl) {
        try {
            URI uri = URI.create(sourceUrl);
            return isBookPath(uri.getPath());
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean isBookPath(String pathLike) {
        if (pathLike == null || pathLike.isBlank()) {
            return false;
        }
        String lower = pathLike.toLowerCase(Locale.ROOT);
        for (String extension : BOOK_FILE_EXTENSIONS) {
            if (lower.endsWith(extension)) {
                return true;
            }
        }
        return false;
    }

    private boolean isSupportedArticleLink(String sourceUrl) {
        if (phase2bArticleLinkService == null || !isHttpUrl(sourceUrl)) {
            return false;
        }
        try {
            List<String> normalizedLinks = phase2bArticleLinkService.normalizeSupportedLinks(List.of(sourceUrl));
            return normalizedLinks != null && !normalizedLinks.isEmpty();
        } catch (Exception error) {
            logger.debug("normalize article link failed: source={} err={}", sourceUrl, error.getMessage());
            return false;
        }
    }

    private ArticleBookSource prepareBookSourceFromArticleLink(
            String taskId,
            String sourceUrl
    ) throws IOException {
        if (!isSupportedArticleLink(sourceUrl) || phase2bArticleLinkService == null) {
            return null;
        }
        List<String> normalizedLinks = phase2bArticleLinkService.normalizeSupportedLinks(List.of(sourceUrl));
        if (normalizedLinks == null || normalizedLinks.isEmpty()) {
            return null;
        }
        String normalizedLink = normalizedLinks.get(0);
        updateProgress(taskId, 0.05, "Extracting article content...");
        Phase2bArticleLinkService.LinkBatchExtractionResult extraction =
                phase2bArticleLinkService.extractArticlesForBook(List.of(normalizedLink));
        if (extraction == null || extraction.articles == null || extraction.articles.isEmpty()) {
            String failureDetail = extraction == null || extraction.failures == null
                    ? "empty extraction result"
                    : String.join(" | ", extraction.failures);
            throw new IllegalStateException("article extraction failed: " + firstNonBlank(failureDetail, "empty result"));
        }
        Phase2bArticleLinkService.ExtractedLinkArticle article = extraction.articles.get(0);
        if (article == null || !StringUtils.hasText(article.markdown)) {
            throw new IllegalStateException("article markdown is empty");
        }
        String safeTaskId = StringUtils.hasText(taskId) ? taskId.trim() : ("article_" + System.currentTimeMillis());
        Path taskSourceRoot = resolveStorageRoot().resolve("article_link").resolve(safeTaskId).toAbsolutePath().normalize();
        Files.createDirectories(taskSourceRoot);
        Path taskAssetsRoot = taskSourceRoot.resolve("assets").toAbsolutePath().normalize();
        Files.createDirectories(taskAssetsRoot);

        Path articleOutputDir = resolveArticleOutputDir(article.pageOutputDir);
        if (StringUtils.hasText(article.pageOutputDir) && articleOutputDir == null) {
            throw new IllegalStateException("article extractor output dir not found: " + article.pageOutputDir);
        }
        List<String> imageCandidates = collectArticleImageCandidates(article);
        int copiedCount = copyArticleAssets(articleOutputDir, taskAssetsRoot, imageCandidates);
        if (!imageCandidates.isEmpty() && copiedCount <= 0) {
            logger.warn(
                    "[{}] article images detected but none copied, outputDir={} imageCandidates={}",
                    safeTaskId,
                    firstNonBlank(article.pageOutputDir, "(empty)"),
                    imageCandidates.size()
            );
        }

        String markdownForBook = rewriteArticleMarkdownAssetPaths(article.markdown, imageCandidates, "assets");
        Path markdownPath = taskSourceRoot.resolve("article_source.md");
        Files.writeString(markdownPath, markdownForBook, StandardCharsets.UTF_8);
        if (!Files.isRegularFile(markdownPath) || !Files.isReadable(markdownPath)) {
            throw new IllegalStateException("persisted article markdown is not readable: " + markdownPath);
        }
        String persistedMarkdown = Files.readString(markdownPath, StandardCharsets.UTF_8);
        if (!StringUtils.hasText(persistedMarkdown)) {
            throw new IllegalStateException("persisted article markdown is empty: " + markdownPath);
        }

        ArticleBookSource prepared = new ArticleBookSource();
        prepared.requestedUrl = firstNonBlank(article.requestedUrl, normalizedLink);
        prepared.finalUrl = firstNonBlank(article.finalUrl, prepared.requestedUrl);
        prepared.siteType = firstNonBlank(article.siteType, "unknown");
        prepared.title = firstNonBlank(article.title, stripExtensionSafe(markdownPath.getFileName().toString()));
        prepared.markdownPath = markdownPath.toString();
        prepared.outputDir = taskSourceRoot.toString();
        prepared.imageCount = imageCandidates.size();
        prepared.copiedImageCount = copiedCount;
        logger.info(
                "[{}] Prepared article link source: requested={} final={} site={} images={}/{} output={}",
                safeTaskId,
                prepared.requestedUrl,
                prepared.finalUrl,
                prepared.siteType,
                copiedCount,
                imageCandidates.size(),
                taskSourceRoot
        );
        return prepared;
    }

    private Path resolveArticleOutputDir(String rawOutputDir) {
        if (!StringUtils.hasText(rawOutputDir)) {
            return null;
        }
        try {
            Path candidate = Paths.get(rawOutputDir.trim());
            Path absolute = candidate.toAbsolutePath().normalize();
            if (!Files.isDirectory(absolute)) {
                return null;
            }
            return absolute;
        } catch (Exception error) {
            logger.warn("resolve article output dir failed: path={} err={}", rawOutputDir, error.getMessage());
            return null;
        }
    }

    private List<String> collectArticleImageCandidates(Phase2bArticleLinkService.ExtractedLinkArticle article) {
        if (article == null) {
            return List.of();
        }
        LinkedHashSet<String> dedup = new LinkedHashSet<>();
        if (article.imageRelativePaths != null) {
            for (String rawPath : article.imageRelativePaths) {
                String normalized = normalizeArticleAssetRelativePath(rawPath);
                if (StringUtils.hasText(normalized)) {
                    dedup.add(normalized);
                }
            }
        }
        Pattern markdownImagePattern = Pattern.compile("!\\[[^\\]]*\\]\\(([^)]+)\\)");
        Matcher matcher = markdownImagePattern.matcher(String.valueOf(article.markdown == null ? "" : article.markdown));
        while (matcher.find()) {
            String rawRef = String.valueOf(matcher.group(1) == null ? "" : matcher.group(1)).trim();
            if (rawRef.startsWith("<") && rawRef.endsWith(">") && rawRef.length() > 2) {
                rawRef = rawRef.substring(1, rawRef.length() - 1).trim();
            }
            int spaceAt = rawRef.indexOf(' ');
            if (spaceAt > 0) {
                rawRef = rawRef.substring(0, spaceAt);
            }
            String normalized = normalizeArticleAssetRelativePath(rawRef);
            if (StringUtils.hasText(normalized)) {
                dedup.add(normalized);
            }
        }
        return new ArrayList<>(dedup);
    }

    private String normalizeArticleAssetRelativePath(String rawPath) {
        String normalized = firstNonBlank(rawPath, "")
                .replace('\\', '/')
                .trim();
        while (normalized.startsWith("./")) {
            normalized = normalized.substring(2);
        }
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        normalized = normalized.replaceAll("/+", "/");
        if (!StringUtils.hasText(normalized)) {
            return "";
        }
        if (normalized.startsWith("http://") || normalized.startsWith("https://")) {
            return "";
        }
        if (normalized.startsWith("data:")) {
            return "";
        }
        return normalized;
    }

    private String rewriteArticleMarkdownAssetPaths(
            String markdown,
            List<String> relativePaths,
            String targetPrefix
    ) {
        String rewritten = String.valueOf(markdown == null ? "" : markdown);
        if (!StringUtils.hasText(rewritten) || relativePaths == null || relativePaths.isEmpty()) {
            return rewritten;
        }
        String normalizedPrefix = normalizeArticleAssetRelativePath(firstNonBlank(targetPrefix, ""));
        if (!StringUtils.hasText(normalizedPrefix)) {
            return rewritten;
        }
        for (String rawPath : relativePaths) {
            String normalizedPath = normalizeArticleAssetRelativePath(rawPath);
            if (!StringUtils.hasText(normalizedPath)) {
                continue;
            }
            String targetPath = normalizeArticleAssetRelativePath(normalizedPrefix + "/" + normalizedPath);
            if (!StringUtils.hasText(targetPath)) {
                continue;
            }
            rewritten = rewritten.replace("(" + normalizedPath + ")", "(" + targetPath + ")");
            rewritten = rewritten.replace("(./" + normalizedPath + ")", "(" + targetPath + ")");
            rewritten = rewritten.replace("=\"" + normalizedPath + "\"", "=\"" + targetPath + "\"");
            rewritten = rewritten.replace("=\"./" + normalizedPath + "\"", "=\"" + targetPath + "\"");
            rewritten = rewritten.replace("='" + normalizedPath + "'", "='" + targetPath + "'");
            rewritten = rewritten.replace("='./" + normalizedPath + "'", "='" + targetPath + "'");
        }
        return rewritten;
    }

    private int copyArticleAssets(Path sourceRoot, Path targetRoot, List<String> relativePaths) {
        if (sourceRoot == null || targetRoot == null || relativePaths == null || relativePaths.isEmpty()) {
            return 0;
        }
        int copiedCount = 0;
        for (String relativePathRaw : relativePaths) {
            String normalizedRel = normalizeArticleAssetRelativePath(relativePathRaw);
            if (!StringUtils.hasText(normalizedRel)) {
                continue;
            }
            final Path relPath;
            try {
                relPath = Paths.get(normalizedRel).normalize();
            } catch (Exception ignored) {
                continue;
            }
            if (relPath.isAbsolute() || relPath.startsWith("..")) {
                logger.warn("skip unsafe article asset path: {}", normalizedRel);
                continue;
            }
            Path sourcePath = sourceRoot.resolve(relPath).toAbsolutePath().normalize();
            if (!sourcePath.startsWith(sourceRoot)) {
                logger.warn("skip escaped article source asset path: {}", sourcePath);
                continue;
            }
            if (!Files.isRegularFile(sourcePath)) {
                logger.debug("article asset missing on extractor output: {}", sourcePath);
                continue;
            }
            Path targetPath = targetRoot.resolve(relPath).toAbsolutePath().normalize();
            if (!targetPath.startsWith(targetRoot)) {
                logger.warn("skip escaped article target asset path: {}", targetPath);
                continue;
            }
            try {
                Path parent = targetPath.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                copiedCount += 1;
            } catch (Exception error) {
                logger.warn(
                        "copy article asset failed: source={} target={} err={}",
                        sourcePath,
                        targetPath,
                        error.getMessage()
                );
            }
        }
        return copiedCount;
    }

    private String stripExtensionSafe(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        int dotAt = value.lastIndexOf('.');
        if (dotAt <= 0) {
            return value;
        }
        return value.substring(0, dotAt);
    }

    private static class VLTokenUsage {
        long inputTokens;
        long outputTokens;
        long totalTokens;
        String sourcePath = "";
        String currency = "";
        String pricingStatus = "";
        String costSource = "";
        Double totalCost = null;
    }

    private static class AuditUsage {
        long promptTokens;
        long completionTokens;
        long totalTokens;
        long inputTokens;
        long outputTokens;
        long cachedPromptTokens;
        long uncachedPromptTokens;
        long textInputTokens;
        long imageInputTokens;
        long audioInputTokens;
        long videoInputTokens;
        long mediaInputTokens;
        long totalCalls;
        String sourcePath = "";
        String currency = "";
        String pricingStatus = "";
        String costSource = "";
        Double totalCost = null;
        List<Map<String, Object>> byModel = new ArrayList<>();
        boolean hasData() {
            return promptTokens > 0
                || completionTokens > 0
                || totalTokens > 0
                || inputTokens > 0
                || outputTokens > 0
                || cachedPromptTokens > 0
                || uncachedPromptTokens > 0
                || textInputTokens > 0
                || imageInputTokens > 0
                || audioInputTokens > 0
                || videoInputTokens > 0
                || mediaInputTokens > 0
                || !byModel.isEmpty()
                || outputTokens > 0
                || totalCalls > 0
                || totalCost != null;
        }
    }

    private static class ResolvedCost {
        final String currency;
        final Double totalCost;

        private ResolvedCost(String currency, Double totalCost) {
            this.currency = currency != null ? currency : "";
            this.totalCost = totalCost;
        }
    }

    private void writeTaskMetricsReport(
            String taskId,
            String outputDir,
            String videoPath,
            String inputVideoUrl,
            String videoTitle,
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
            AuditUsage visionAiUsage = loadVisionAiUsage(outputDir);
            AuditUsage textLlmUsage = loadTextLlmUsage(outputDir);

            boolean success = result != null && result.success;
            String normalizedErrorMessage = "";
            if (!success) {
                String rawErrorMessage = result != null ? result.errorMessage : "";
                normalizedErrorMessage = ensureNonEmptyErrorMessage(
                    rawErrorMessage,
                    "unknown error (error message missing)"
                );
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("version", "1.0");
            payload.put("generated_at", Instant.now().toString());
            payload.put("task_id", taskId);
            payload.put("success", success);
            payload.put("error_message", normalizedErrorMessage);
            payload.put("input_video_url", inputVideoUrl != null ? inputVideoUrl : "");
            payload.put("video_title", videoTitle != null ? videoTitle : "");
            payload.put("video_path", videoPath != null ? videoPath : "");
            payload.put("output_dir", outputDir);
            payload.put("result_markdown_path", result != null ? (result.markdownPath != null ? result.markdownPath : "") : "");
            payload.put("result_json_path", result != null ? (result.jsonPath != null ? result.jsonPath : "") : "");
            payload.put("stage_timings_ms", new LinkedHashMap<>(stageTimingsMs));
            payload.put("flow_flags", new LinkedHashMap<>(flowFlags));
            payload.put("llm_cost", buildLLMCostPayload(vlModel, vlUsage, visionAiUsage, textLlmUsage));

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

    private Map<String, Object> buildLLMCostPayload(
            String vlModel,
            VLTokenUsage vlUsage,
            AuditUsage visionAiUsage,
            AuditUsage textLlmUsage
    ) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("pricing_basis", "provider_usage_and_audit");

        long vlInput = Math.max(0L, vlUsage.inputTokens);
        long vlOutput = Math.max(0L, vlUsage.outputTokens);
        long vlTotal = Math.max(0L, vlUsage.totalTokens > 0 ? vlUsage.totalTokens : (vlInput + vlOutput));

        Map<String, Object> vlSection = new LinkedHashMap<>();
        vlSection.put("model", vlModel != null ? vlModel : "");
        vlSection.put("input_tokens", vlInput);
        vlSection.put("output_tokens", vlOutput);
        vlSection.put("total_tokens", vlTotal);
        vlSection.put("token_source", vlUsage.sourcePath);
        if (vlUsage.currency != null && !vlUsage.currency.isBlank()) {
            vlSection.put("currency", vlUsage.currency);
        }
        if (vlUsage.pricingStatus != null && !vlUsage.pricingStatus.isBlank()) {
            vlSection.put("pricing_status", vlUsage.pricingStatus);
        }
        if (vlUsage.costSource != null && !vlUsage.costSource.isBlank()) {
            vlSection.put("cost_source", vlUsage.costSource);
        }
        if (vlUsage.totalCost != null) {
            vlSection.put("total_cost", roundCost(vlUsage.totalCost.doubleValue()));
        }
        payload.put("vl", vlSection);

        Map<String, Object> visionSection = buildAuditUsageSection(visionAiUsage);
        if (!visionSection.isEmpty()) {
            payload.put("vision_ai", visionSection);
        }

        Map<String, Object> textSection = buildAuditUsageSection(textLlmUsage);
        if (!textSection.isEmpty()) {
            payload.put("text_llm", textSection);
        }

        AuditUsage deepSeekOnly = filterAuditUsageByModel(textLlmUsage, "deepseek");
        Map<String, Object> deepSeekSection = buildAuditUsageSection(deepSeekOnly);
        if (!deepSeekSection.isEmpty()) {
            payload.put("deepseek_chat", deepSeekSection);
        }

        AuditUsage qwenOnly = filterAuditUsageByModel(textLlmUsage, "qwen");
        Map<String, Object> qwenSection = buildAuditUsageSection(qwenOnly);
        if (!qwenSection.isEmpty()) {
            payload.put("qwen_fallback", qwenSection);
        }

        ResolvedCost totalCost = resolveAggregateCost(vlUsage, visionAiUsage, textLlmUsage);
        if (totalCost.totalCost != null) {
            if (totalCost.currency != null && !totalCost.currency.isBlank()) {
                payload.put("currency", totalCost.currency);
            }
            payload.put("total_cost", roundCost(totalCost.totalCost.doubleValue()));
        } else {
            String fallbackCurrency = firstNonBlank(
                    vlUsage.currency,
                    firstNonBlank(visionAiUsage.currency, textLlmUsage.currency)
            );
            if (fallbackCurrency != null && !fallbackCurrency.isBlank()) {
                payload.put("currency", fallbackCurrency);
            }
        }
        payload.put(
                "coverage_note",
                "VL uses persisted vl_token_report pricing; Vision AI and text LLM use task audit summaries, including DeepSeek-to-Qwen fallback when it happens."
        );
        return payload;
    }

    private Map<String, Object> buildAuditUsageSection(AuditUsage usage) {
        if (usage == null || !usage.hasData()) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> section = new LinkedHashMap<>();
        section.put("model", resolvePrimaryModel(usage));
        section.put("prompt_tokens", Math.max(0L, usage.promptTokens));
        section.put("completion_tokens", Math.max(0L, usage.completionTokens));
        section.put("total_tokens", Math.max(0L, usage.totalTokens > 0L ? usage.totalTokens : (usage.promptTokens + usage.completionTokens)));
        section.put("input_tokens", Math.max(0L, usage.inputTokens > 0L ? usage.inputTokens : usage.promptTokens));
        section.put("output_tokens", Math.max(0L, usage.outputTokens > 0L ? usage.outputTokens : usage.completionTokens));
        section.put("cached_prompt_tokens", Math.max(0L, usage.cachedPromptTokens));
        section.put("uncached_prompt_tokens", Math.max(0L, usage.uncachedPromptTokens));
        section.put("text_input_tokens", Math.max(0L, usage.textInputTokens));
        section.put("image_input_tokens", Math.max(0L, usage.imageInputTokens));
        section.put("audio_input_tokens", Math.max(0L, usage.audioInputTokens));
        section.put("video_input_tokens", Math.max(0L, usage.videoInputTokens));
        section.put("media_input_tokens", Math.max(0L, usage.mediaInputTokens));
        section.put("total_calls", Math.max(0L, usage.totalCalls));
        section.put("token_source", usage.sourcePath);
        if (usage.currency != null && !usage.currency.isBlank()) {
            section.put("currency", usage.currency);
        }
        if (usage.pricingStatus != null && !usage.pricingStatus.isBlank()) {
            section.put("pricing_status", usage.pricingStatus);
        }
        if (usage.costSource != null && !usage.costSource.isBlank()) {
            section.put("cost_source", usage.costSource);
        }
        if (usage.totalCost != null) {
            section.put("total_cost", roundCost(usage.totalCost.doubleValue()));
        }
        if (!usage.byModel.isEmpty()) {
            section.put("by_model", usage.byModel);
        }
        return section;
    }

    private String resolvePrimaryModel(AuditUsage usage) {
        if (usage == null || usage.byModel == null || usage.byModel.isEmpty()) {
            return "";
        }
        Object model = usage.byModel.get(0).get("model");
        return model != null ? String.valueOf(model) : "";
    }

    private AuditUsage filterAuditUsageByModel(AuditUsage source, String keyword) {
        AuditUsage filtered = new AuditUsage();
        if (source == null || source.byModel == null || keyword == null || keyword.isBlank()) {
            return filtered;
        }
        String loweredKeyword = keyword.toLowerCase(Locale.ROOT);
        for (Map<String, Object> entry : source.byModel) {
            String model = String.valueOf(entry.getOrDefault("model", "")).toLowerCase(Locale.ROOT);
            if (!model.contains(loweredKeyword)) {
                continue;
            }
            filtered.byModel.add(new LinkedHashMap<>(entry));
            filtered.promptTokens += readLongValue(entry.get("prompt_tokens"));
            filtered.completionTokens += readLongValue(entry.get("completion_tokens"));
            filtered.totalTokens += readLongValue(entry.get("total_tokens"));
            filtered.inputTokens += readLongValue(entry.get("input_tokens"));
            filtered.outputTokens += readLongValue(entry.get("output_tokens"));
            filtered.cachedPromptTokens += readLongValue(entry.get("cached_prompt_tokens"));
            filtered.uncachedPromptTokens += readLongValue(entry.get("uncached_prompt_tokens"));
            filtered.textInputTokens += readLongValue(entry.get("text_input_tokens"));
            filtered.imageInputTokens += readLongValue(entry.get("image_input_tokens"));
            filtered.audioInputTokens += readLongValue(entry.get("audio_input_tokens"));
            filtered.videoInputTokens += readLongValue(entry.get("video_input_tokens"));
            filtered.mediaInputTokens += readLongValue(entry.get("media_input_tokens"));
            filtered.totalCalls += readLongValue(entry.get("records"));
            if (filtered.totalCost == null) {
                filtered.totalCost = 0d;
            }
            filtered.totalCost += readDoubleValue(entry.get("estimated_total_cost"));
            if (filtered.currency == null || filtered.currency.isBlank()) {
                filtered.currency = String.valueOf(entry.getOrDefault("currency", source.currency));
            }
            if (filtered.pricingStatus == null || filtered.pricingStatus.isBlank()) {
                filtered.pricingStatus = source.pricingStatus;
            }
            if (filtered.costSource == null || filtered.costSource.isBlank()) {
                filtered.costSource = source.costSource;
            }
            if (filtered.sourcePath == null || filtered.sourcePath.isBlank()) {
                filtered.sourcePath = source.sourcePath;
            }
        }
        if (filtered.totalCost != null) {
            filtered.totalCost = roundCost(filtered.totalCost.doubleValue());
        }
        return filtered;
    }

    private ResolvedCost resolveAggregateCost(VLTokenUsage vlUsage, AuditUsage... usages) {
        Map<String, Double> totalsByCurrency = new LinkedHashMap<>();
        appendCostByCurrency(totalsByCurrency, vlUsage != null ? vlUsage.currency : "", vlUsage != null ? vlUsage.totalCost : null);
        if (usages != null) {
            for (AuditUsage usage : usages) {
                appendCostByCurrency(totalsByCurrency, usage != null ? usage.currency : "", usage != null ? usage.totalCost : null);
            }
        }
        if (totalsByCurrency.size() == 1) {
            Map.Entry<String, Double> entry = totalsByCurrency.entrySet().iterator().next();
            return new ResolvedCost(entry.getKey(), roundCost(entry.getValue()));
        }
        return new ResolvedCost("", null);
    }

    private void appendCostByCurrency(Map<String, Double> totalsByCurrency, String currency, Double totalCost) {
        if (totalsByCurrency == null || totalCost == null) {
            return;
        }
        String normalizedCurrency = currency == null ? "" : currency.trim().toUpperCase(Locale.ROOT);
        if (normalizedCurrency.isBlank()) {
            return;
        }
        totalsByCurrency.put(
                normalizedCurrency,
                roundCost(totalsByCurrency.getOrDefault(normalizedCurrency, 0d) + Math.max(0d, totalCost.doubleValue()))
        );
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
            JsonNode tokenUsage = root.path("token_usage");
            usage.inputTokens = firstLong(tokenStats, "input_tokens_actual", "prompt_tokens_actual", "input_tokens", "prompt_tokens");
            if (usage.inputTokens <= 0L) {
                usage.inputTokens = firstLong(tokenUsage, "input_tokens", "prompt_tokens");
            }
            usage.outputTokens = firstLong(
                tokenStats,
                "output_tokens_actual",
                "completion_tokens_actual",
                "output_tokens",
                "completion_tokens"
            );
            if (usage.outputTokens <= 0L) {
                usage.outputTokens = firstLong(tokenUsage, "output_tokens", "completion_tokens");
            }
            usage.totalTokens = firstLong(tokenStats, "total_tokens_actual", "total_tokens");
            if (usage.totalTokens <= 0L) {
                usage.totalTokens = firstLong(tokenUsage, "total_tokens");
            }
            if (usage.totalTokens <= 0L) {
                usage.totalTokens = usage.inputTokens + usage.outputTokens;
            }
            JsonNode pricing = root.path("pricing");
            usage.currency = pricing.path("currency").asText("");
            usage.pricingStatus = pricing.path("status").asText("");
            usage.totalCost = firstDouble(pricing, "total_cost");
            usage.costSource = reportPath.toAbsolutePath().toString();
            usage.sourcePath = reportPath.toAbsolutePath().toString();
        } catch (Exception e) {
            logger.warn("Failed to parse VL token report: {}", e.getMessage());
        }
        return usage;
    }

    private AuditUsage loadVisionAiUsage(String outputDir) {
        Path auditPath = Paths.get(outputDir, "intermediates", "vision_ai_call_audit.json");
        return loadAuditUsage(auditPath);
    }

    private AuditUsage loadTextLlmUsage(String outputDir) {
        Path auditPath = Paths.get(outputDir, "intermediates", "phase2b_deepseek_call_audit.json");
        AuditUsage auditUsage = loadAuditUsage(auditPath);
        if (auditUsage.hasData()) {
            return auditUsage;
        }
        Path tracePath = Paths.get(outputDir, "intermediates", "phase2b_llm_trace.jsonl");
        return loadTextLlmUsageFromTrace(tracePath);
    }

    private AuditUsage loadAuditUsage(Path auditPath) {
        AuditUsage usage = new AuditUsage();
        if (auditPath == null || !Files.exists(auditPath)) {
            return usage;
        }
        usage.sourcePath = auditPath.toAbsolutePath().toString();
        usage.costSource = usage.sourcePath;
        try {
            JsonNode root = objectMapper.readTree(auditPath.toFile());
            JsonNode summary = root.path("summary");
            usage.promptTokens = firstLong(summary, "total_prompt_tokens");
            usage.completionTokens = firstLong(summary, "total_completion_tokens");
            usage.totalTokens = firstLong(summary, "total_tokens");
            usage.inputTokens = firstLong(summary, "total_input_tokens");
            usage.outputTokens = firstLong(summary, "total_output_tokens");
            usage.cachedPromptTokens = firstLong(summary, "total_cached_prompt_tokens");
            usage.uncachedPromptTokens = firstLong(summary, "total_uncached_prompt_tokens");
            usage.textInputTokens = firstLong(summary, "total_text_input_tokens");
            usage.imageInputTokens = firstLong(summary, "total_image_input_tokens");
            usage.audioInputTokens = firstLong(summary, "total_audio_input_tokens");
            usage.videoInputTokens = firstLong(summary, "total_video_input_tokens");
            usage.mediaInputTokens = firstLong(summary, "total_media_input_tokens");
            usage.totalCalls = firstLong(summary, "total_records");

            JsonNode estimatedByCurrency = summary.path("estimated_cost_by_currency");
            if (estimatedByCurrency.isObject()) {
                if (estimatedByCurrency.has("CNY")) {
                    usage.currency = "CNY";
                    usage.totalCost = Math.max(0d, estimatedByCurrency.path("CNY").asDouble(0d));
                } else {
                    Iterator<String> names = estimatedByCurrency.fieldNames();
                    if (names.hasNext()) {
                        String currency = names.next();
                        usage.currency = currency != null ? currency : "";
                        usage.totalCost = Math.max(0d, estimatedByCurrency.path(currency).asDouble(0d));
                    }
                }
            }
            usage.pricingStatus = usage.totalCost != null ? "ok" : "";

            JsonNode byModel = summary.path("by_model");
            if (byModel.isArray()) {
                for (JsonNode item : byModel) {
                    if (!item.isObject()) {
                        continue;
                    }
                    Map<String, Object> modelUsage = new LinkedHashMap<>();
                    modelUsage.put("model", item.path("model").asText(""));
                    modelUsage.put("provider", item.path("provider").asText(""));
                    modelUsage.put("currency", item.path("currency").asText(""));
                    modelUsage.put("records", item.path("records").asLong(0L));
                    modelUsage.put("prompt_tokens", item.path("prompt_tokens").asLong(0L));
                    modelUsage.put("completion_tokens", item.path("completion_tokens").asLong(0L));
                    modelUsage.put("total_tokens", item.path("total_tokens").asLong(0L));
                    modelUsage.put("input_tokens", item.path("input_tokens").asLong(0L));
                    modelUsage.put("output_tokens", item.path("output_tokens").asLong(0L));
                    modelUsage.put("cached_prompt_tokens", item.path("cached_prompt_tokens").asLong(0L));
                    modelUsage.put("uncached_prompt_tokens", item.path("uncached_prompt_tokens").asLong(0L));
                    modelUsage.put("text_input_tokens", item.path("text_input_tokens").asLong(0L));
                    modelUsage.put("image_input_tokens", item.path("image_input_tokens").asLong(0L));
                    modelUsage.put("audio_input_tokens", item.path("audio_input_tokens").asLong(0L));
                    modelUsage.put("video_input_tokens", item.path("video_input_tokens").asLong(0L));
                    modelUsage.put("media_input_tokens", item.path("media_input_tokens").asLong(0L));
                    modelUsage.put("estimated_total_cost", roundCost(item.path("estimated_total_cost").asDouble(0d)));
                    usage.byModel.add(modelUsage);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to parse task audit report {}: {}", auditPath, e.getMessage());
        }
        return usage;
    }

    private AuditUsage loadTextLlmUsageFromTrace(Path tracePath) {
        AuditUsage usage = new AuditUsage();
        if (tracePath == null || !Files.exists(tracePath)) {
            return usage;
        }
        usage.sourcePath = tracePath.toAbsolutePath().toString();
        usage.costSource = usage.sourcePath;
        Map<String, Map<String, Object>> byModel = new LinkedHashMap<>();
        Map<String, Double> costByCurrency = new LinkedHashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(tracePath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line == null ? "" : line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
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
                long promptTokens = Math.max(0L, node.path("prompt_tokens").asLong(0L));
                long completionTokens = Math.max(0L, node.path("completion_tokens").asLong(0L));
                long totalTokens = Math.max(0L, node.path("total_tokens").asLong(promptTokens + completionTokens));
                boolean cacheHit = node.path("cache_hit").asBoolean(false);

                usage.promptTokens += promptTokens;
                usage.completionTokens += completionTokens;
                usage.totalTokens += totalTokens;
                usage.inputTokens += promptTokens;
                usage.outputTokens += completionTokens;
                if (cacheHit) {
                    usage.cachedPromptTokens += promptTokens;
                } else {
                    usage.uncachedPromptTokens += promptTokens;
                }
                usage.totalCalls += 1L;

                String currency = resolveTextModelCurrency(model);
                double estimatedCost = estimateTextModelCost(model, promptTokens, completionTokens, cacheHit);
                if (!currency.isBlank() && estimatedCost > 0d) {
                    costByCurrency.put(currency, roundCost(costByCurrency.getOrDefault(currency, 0d) + estimatedCost));
                }

                Map<String, Object> bucket = byModel.computeIfAbsent(model, key -> {
                    Map<String, Object> created = new LinkedHashMap<>();
                    created.put("model", key);
                    created.put("provider", resolveTextModelProvider(key));
                    created.put("currency", currency);
                    created.put("records", 0L);
                    created.put("prompt_tokens", 0L);
                    created.put("completion_tokens", 0L);
                    created.put("total_tokens", 0L);
                    created.put("input_tokens", 0L);
                    created.put("output_tokens", 0L);
                    created.put("cached_prompt_tokens", 0L);
                    created.put("uncached_prompt_tokens", 0L);
                    created.put("text_input_tokens", 0L);
                    created.put("image_input_tokens", 0L);
                    created.put("audio_input_tokens", 0L);
                    created.put("video_input_tokens", 0L);
                    created.put("media_input_tokens", 0L);
                    created.put("estimated_total_cost", 0d);
                    return created;
                });
                bucket.put("records", readLongValue(bucket.get("records")) + 1L);
                bucket.put("prompt_tokens", readLongValue(bucket.get("prompt_tokens")) + promptTokens);
                bucket.put("completion_tokens", readLongValue(bucket.get("completion_tokens")) + completionTokens);
                bucket.put("total_tokens", readLongValue(bucket.get("total_tokens")) + totalTokens);
                bucket.put("input_tokens", readLongValue(bucket.get("input_tokens")) + promptTokens);
                bucket.put("output_tokens", readLongValue(bucket.get("output_tokens")) + completionTokens);
                if (cacheHit) {
                    bucket.put("cached_prompt_tokens", readLongValue(bucket.get("cached_prompt_tokens")) + promptTokens);
                } else {
                    bucket.put("uncached_prompt_tokens", readLongValue(bucket.get("uncached_prompt_tokens")) + promptTokens);
                }
                bucket.put(
                        "estimated_total_cost",
                        roundCost(readDoubleValue(bucket.get("estimated_total_cost")) + estimatedCost)
                );
            }
        } catch (Exception e) {
            logger.warn("Failed to read phase2b_llm_trace: {}", e.getMessage());
        }
        usage.byModel.addAll(byModel.values());
        if (costByCurrency.size() == 1) {
            Map.Entry<String, Double> entry = costByCurrency.entrySet().iterator().next();
            usage.currency = entry.getKey();
            usage.totalCost = roundCost(entry.getValue());
            usage.pricingStatus = usage.totalCost != null ? "estimated_from_trace" : "";
        }
        return usage;
    }

    private String resolveTextModelProvider(String model) {
        String normalized = model == null ? "" : model.trim().toLowerCase(Locale.ROOT);
        if (normalized.contains("deepseek")) {
            return "deepseek";
        }
        if (normalized.contains("qwen")) {
            return "dashscope";
        }
        return "";
    }

    private String resolveTextModelCurrency(String model) {
        String provider = resolveTextModelProvider(model);
        if ("deepseek".equals(provider) || "dashscope".equals(provider)) {
            return "CNY";
        }
        return "";
    }

    private double estimateTextModelCost(String model, long promptTokens, long completionTokens, boolean cacheHit) {
        String normalized = model == null ? "" : model.trim().toLowerCase(Locale.ROOT);
        if (normalized.contains("deepseek")) {
            double inputRate = cacheHit ? DEEPSEEK_CHAT_INPUT_CACHED_PER_M : DEEPSEEK_CHAT_INPUT_UNCACHED_PER_M;
            return tokenCostAmount(promptTokens, inputRate) + tokenCostAmount(completionTokens, DEEPSEEK_CHAT_OUTPUT_PER_M);
        }
        if (normalized.contains("qwen-plus")) {
            return tokenCostAmount(promptTokens, QWEN_PLUS_INPUT_PER_M) + tokenCostAmount(completionTokens, QWEN_PLUS_OUTPUT_PER_M);
        }
        return 0d;
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

    private Double firstDouble(JsonNode node, String... fieldNames) {
        if (node == null || fieldNames == null) {
            return null;
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
                return Math.max(0d, valueNode.asDouble(0d));
            }
            String text = valueNode.asText("").trim();
            if (text.isEmpty()) {
                continue;
            }
            try {
                return Math.max(0d, Double.parseDouble(text));
            } catch (Exception ignored) {
                // Ignore parse errors and try next field
            }
        }
        return null;
    }

    private long readLongValue(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number number) {
            return Math.max(0L, number.longValue());
        }
        try {
            return Math.max(0L, Long.parseLong(String.valueOf(value).trim()));
        } catch (Exception ignored) {
            return 0L;
        }
    }

    private double readDoubleValue(Object value) {
        if (value == null) {
            return 0d;
        }
        if (value instanceof Number number) {
            return Math.max(0d, number.doubleValue());
        }
        try {
            return Math.max(0d, Double.parseDouble(String.valueOf(value).trim()));
        } catch (Exception ignored) {
            return 0d;
        }
    }

    private double tokenCostAmount(long tokens, double ratePerMillion) {
        if (tokens <= 0L || ratePerMillion <= 0d) {
            return 0d;
        }
        return ((double) tokens / 1_000_000d) * ratePerMillion;
    }

    private double roundCost(double cost) {
        return Math.round(cost * 1_000_000d) / 1_000_000d;
    }

    // --- OutputDir 相关策略 ---
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

        // 经验估算：截图 seek/解码 与片段切分/编码是主要成本来源。
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
        // 支持 file:// 输入，统一转为绝对路径，确保后续 hash 稳定。
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
            // 路径解析失败时，回退到基于规范化路径的 hash 目录策略。
        }

        String normalized = normalizePathForHash(videoPath);
        String hash = md5Hex(normalized);
        return storageRoot.resolve(hash).toString();
    }

    private String resolveBookTaskOutputDir(String requestedOutputDir, String sourcePath, BookProcessingOptions bookOptions) {
        if (hasExplicitBookLeafOutputDir(requestedOutputDir, bookOptions)) {
            return Paths.get(requestedOutputDir).toAbsolutePath().normalize().toString();
        }
        return resolveOutputDirForLocalVideo(sourcePath);
    }

    private boolean hasExplicitBookLeafOutputDir(String requestedOutputDir, BookProcessingOptions bookOptions) {
        return StringUtils.hasText(requestedOutputDir)
                && bookOptions != null
                && StringUtils.hasText(bookOptions.storageKey)
                && StringUtils.hasText(bookOptions.leafTitle)
                && StringUtils.hasText(bookOptions.leafOutlineIndex);
    }

    private String ensureLocalVideoInStorage(String videoPath, String outputDir) {
        // 将本地视频放入 storage/{hash} 目录，确保后续链路有稳定输入位置。
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
                // 硬链接失败时回退为复制，兼容跨磁盘与权限受限场景。
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
     * 校验本地视频路径是否真实存在且为文件。
     * 这样可以在进入 FFmpeg/Python 阶段之前提前失败，避免在下游链路才暴露路径问题。
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
        // 统一 storage 根目录，保证 Java/Python 使用同一份素材空间。
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
        // 从当前工作目录逐级上探仓库根目录，兼容服务子目录启动。

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
        // 统一路径归一化规则，确保 Java/Python 生成一致的 hash。
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

    // --- 截图请求合并：优先保留生成阶段的新请求，并补齐 Phase2A 的缺失项 ---
    private List<JavaCVFFmpegService.ScreenshotRequest> mergeScreenshotRequests(
            List<PythonGrpcClient.ScreenshotRequest> phase2aRequests,
            List<PythonGrpcClient.ScreenshotRequestDTO> generatedRequests) {
        // 合并截图请求时保留 Phase2A 的兜底能力。
        // 优先采用 generatedRequests 的时间窗和标识，避免覆盖更精细结果。
        // 若 key 已存在则以 generated 为准，仅补齐缺失项。
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
                new JavaCVFFmpegService.ScreenshotRequest(req.screenshotId, req.timestampSec, req.label, req.semanticUnitId, req.frameReason)
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
                new JavaCVFFmpegService.ScreenshotRequest(req.screenshotId, req.timestampSec, req.label, req.semanticUnitId, req.frameReason)
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
        // 合并切片请求时保留 Phase2A 的兜底能力。
        // 优先采用 generatedRequests，尽量保留更精确的切片区间。
        // 若 clipId 冲突，默认保留 generated 结果以保证 FFmpeg 输入一致。
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
        // 统一 action_id：保证每个 action 都有唯一且稳定的编号。
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
        return t.contains("process")
                || t.contains("practical")
                || t.contains("\u8fc7\u7a0b")
                || t.contains("\u5b9e\u64cd");
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
            
            // 统一 action_id，保证后续分类映射与写回键值稳定。
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
            
            // 优先读取语义单元内的 action_units 作为素材生成输入。
            List<Map<String, Object>> unitActions = (List<Map<String, Object>>) u.get("action_units");
            if (unitActions != null && !unitActions.isEmpty()) {
                // JSON -> Java 的 Map 反序列化后，action_units.knowledge_type 可能为 null，需显式兜底。
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
                    // 若 action_type 缺失，则回退到单元层 knowledge_type。
                    as.actionType = !kt.isEmpty() ? kt : fallback;
                    in.actionUnits.add(as);
                }
            } else if (cvResults.containsKey(uid)) {
                logger.info("[{}] MatInputs fallback to CV actionSegments: unit={}, actions=0",
                    "MaterialGen", uid);
                // 当 action_units 缺失时，回退到 CV 识别出的 action segments。
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

            // 继承 CV 输出的稳定区间信息，供后续素材阶段使用。
            if (cvResults.containsKey(uid)) {
                CVValidationUnitResult cvRes = cvResults.get(uid);
                if (cvRes.stableIslands != null) {
                    in.stableIslands.addAll(cvRes.stableIslands);
                }
            }
            // 最终再次归一化 action_id，避免合并后出现重复或缺号。
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
                        // 默认先写入单元级 knowledge_type，再按 action 级结果覆盖。
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
        // inline 载荷更新后主动清空 ref，避免序列化时出现双源数据不一致。
        analyzeResult.semanticUnitsRef = null;
    }
    
    // Removed: enrichUnitsWithSubtitles method
    // Classifier now reads subtitles directly from step2_path
    
    private void updateProgress(String taskId, double progress, String message) {
        String normalizedMessage = normalizeProgressMessage(progress, message);
        ProgressCallback callback = resolveProgressCallback(taskId);
        if (callback != null) {
            callback.onProgress(taskId, progress, normalizedMessage);
        }
        logger.info("[{}] {} ({}%)", taskId, normalizedMessage, (int)(progress * 100));
    }

    private ProgressCallback resolveProgressCallback(String taskId) {
        if (taskId != null && !taskId.isBlank()) {
            ProgressCallback callback = taskProgressCallbacks.get(taskId);
            if (callback != null) {
                return callback;
            }
        }
        return progressCallback;
    }

    /**
     * 规范化进度文案：优先使用上游传入的非通用消息。
     * 当上游为空或只给出通用状态时，按进度区间回落到可读的默认提示。
     */
    private String normalizeProgressMessage(double progress, String rawMessage) {
        String message = rawMessage != null ? rawMessage.trim() : "";
        if (!message.isEmpty() && !isGenericProgressMessage(message)) {
            return message;
        }
        if (progress <= 0.10) {
            return "正在初始化任务...";
        }
        if (progress <= 0.28) {
            return "正在处理转写与基础结构...";
        }
        if (progress <= 0.82) {
            return "正在执行 AI 分析与素材生成...";
        }
        if (progress < 0.99) {
            return "正在组装 Markdown 文档...";
        }
        return "处理即将完成...";
    }

    private boolean isGenericProgressMessage(String message) {
        String lower = message.toLowerCase(Locale.ROOT);
        return lower.equals("processing")
                || lower.equals("running")
                || lower.equals("queued")
                || lower.equals("pending")
                || message.equals("\u5904\u7406\u4e2d")
                || message.equals("\u6392\u961f\u4e2d");
    }

    private DownloadResult waitForDownloadWithLease(
            String taskId,
            String videoUrl,
            String outputDir,
            String predictedDownloadDir,
            int grpcDeadlineSec,
            int hardTimeoutSec,
            int idleTimeoutSec,
            int pollIntervalSec) {
        String heartbeatOutputDir = firstNonBlank(predictedDownloadDir, outputDir);
        TaskProgressWatchdogBridge.SignalEmitter signalEmitter =
            (progress, message) -> updateProgress(taskId, progress, message);
        taskProgressWatchdogBridge.resetTask(taskId);
        TaskProgressWatchdogBridge.MonitorHandle downloadMonitor =
            taskProgressWatchdogBridge.startMonitor(taskId, heartbeatOutputDir, "download", signalEmitter);
        CompletableFuture<DownloadResult> downloadFuture = grpcClient.downloadVideoAsync(
            taskId,
            videoUrl,
            outputDir,
            grpcDeadlineSec
        );
        long startAt = System.currentTimeMillis();
        long hardDeadlineAt = startAt + TimeUnit.SECONDS.toMillis(hardTimeoutSec);
        long idleDeadlineAt = startAt + TimeUnit.SECONDS.toMillis(idleTimeoutSec);
        List<String> watchDirs = buildDownloadWatchDirs(outputDir, predictedDownloadDir);
        DownloadActivitySnapshot lastActivity = observeDownloadActivity(watchDirs, null, startAt);
        long nextLeaseLogAt = 0L;
        try {
            while (true) {
                try {
                    return downloadFuture.get(pollIntervalSec, TimeUnit.SECONDS);
                } catch (TimeoutException ignored) {
                    long now = System.currentTimeMillis();
                    DownloadActivitySnapshot observedActivity = observeDownloadActivity(watchDirs, lastActivity, now);
                    if (observedActivity.isChangedFrom(lastActivity)) {
                        lastActivity = observedActivity;
                        idleDeadlineAt = now + TimeUnit.SECONDS.toMillis(idleTimeoutSec);
                        if (now >= nextLeaseLogAt) {
                            logger.info(
                                "[{}] Download lease renewed: bytes={}, files={}, latest_mtime={}, watch_dirs={}, hard_timeout={}s, idle_timeout={}s",
                                taskId,
                                observedActivity.totalBytes,
                                observedActivity.fileCount,
                                observedActivity.latestModifiedMs,
                                String.join(" | ", watchDirs),
                                hardTimeoutSec,
                                idleTimeoutSec
                            );
                            nextLeaseLogAt = now + TimeUnit.SECONDS.toMillis(30);
                        }
                    }
                    if (now >= hardDeadlineAt) {
                        downloadFuture.cancel(true);
                        throw new RuntimeException(
                            String.format("Download hard timeout exceeded (%ds)", hardTimeoutSec)
                        );
                    }
                    if (now >= idleDeadlineAt) {
                        downloadFuture.cancel(true);
                        throw new RuntimeException(
                            String.format(
                                "Download idle timeout exceeded (%ds without file activity in dirs: %s; bytes=%d, files=%d, latest_mtime=%d)",
                                idleTimeoutSec,
                                watchDirs.isEmpty() ? "(none)" : String.join(" | ", watchDirs),
                                lastActivity.totalBytes,
                                lastActivity.fileCount,
                                lastActivity.latestModifiedMs
                            )
                        );
                    }
                } catch (InterruptedException interruptedError) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(
                        "Download stage interrupted while waiting for Python worker response",
                        interruptedError
                    );
                } catch (ExecutionException executionError) {
                    throw new RuntimeException(
                        "Download stage execution failed: " + extractThrowableMessage(executionError),
                        executionError
                    );
                }
            }
        } finally {
            taskProgressWatchdogBridge.stopMonitor(taskId, downloadMonitor, signalEmitter);
        }
    }

    private String resolvePredictedDownloadWatchDir(String videoUrl, String fallbackOutputDir) {
        if (!isHttpUrl(videoUrl)) {
            return fallbackOutputDir;
        }
        Path storageRoot = resolvePythonPrimaryStorageRoot();
        String taskDirSource = buildDownloadTaskDirSource(videoUrl);
        String taskHash = md5Hex(taskDirSource);
        return storageRoot.resolve(taskHash).toString();
    }

    private Path resolvePythonPrimaryStorageRoot() {
        String envRoot = System.getenv("V2M_STORAGE_ROOT");
        if (envRoot != null && !envRoot.isBlank()) {
            return Paths.get(envRoot).toAbsolutePath().normalize();
        }
        return resolveRepoRoot()
            .resolve("var")
            .resolve("storage")
            .resolve("storage")
            .toAbsolutePath()
            .normalize();
    }

    private String buildDownloadTaskDirSource(String videoUrl) {
        String bilibiliVideoId = extractBilibiliVideoId(videoUrl);
        if (bilibiliVideoId != null && !bilibiliVideoId.isBlank()) {
            Integer bilibiliEpisodeIndex = extractBilibiliEpisodeIndex(videoUrl);
            if (bilibiliEpisodeIndex != null && bilibiliEpisodeIndex > 0) {
                return bilibiliVideoId + "_" + bilibiliEpisodeIndex;
            }
            return bilibiliVideoId;
        }
        return videoUrl == null ? "" : videoUrl;
    }

    private String extractBilibiliVideoId(String videoUrl) {
        if (videoUrl == null || videoUrl.isBlank()) {
            return null;
        }
        try {
            URI parsed = URI.create(videoUrl);
            if (!isBilibiliHost(parsed.getHost())) {
                return null;
            }

            Map<String, String> query = parseQueryParams(parsed.getRawQuery());
            String bvid = query.getOrDefault("bvid", "");
            if (!bvid.isBlank()) {
                Matcher bvidMatcher = BILIBILI_BV_PATTERN.matcher(bvid);
                if (bvidMatcher.find()) {
                    return bvidMatcher.group();
                }
            }

            String aid = query.getOrDefault("aid", "");
            if (!aid.isBlank() && aid.chars().allMatch(Character::isDigit)) {
                return "AV" + aid;
            }

            String searchSpace = String.join(
                " ",
                firstNonBlank(parsed.getRawPath(), ""),
                firstNonBlank(parsed.getRawQuery(), ""),
                firstNonBlank(parsed.getRawFragment(), "")
            );
            Matcher bvMatcher = BILIBILI_BV_PATTERN.matcher(searchSpace);
            if (bvMatcher.find()) {
                return bvMatcher.group();
            }
            Matcher avMatcher = BILIBILI_AV_PATTERN.matcher(searchSpace);
            if (avMatcher.find()) {
                return "AV" + avMatcher.group(1);
            }
            return null;
        } catch (Exception ignored) {
            return null;
        }
    }

    private boolean isBilibiliHost(String host) {
        if (host == null || host.isBlank()) {
            return false;
        }
        String normalized = host.toLowerCase(Locale.ROOT).split(":", 2)[0];
        return normalized.equals("bilibili.com")
            || normalized.endsWith(".bilibili.com")
            || normalized.equals("b23.tv")
            || normalized.endsWith(".b23.tv");
    }

    private Integer extractBilibiliEpisodeIndex(String videoUrl) {
        if (videoUrl == null || videoUrl.isBlank()) {
            return null;
        }
        try {
            URI parsed = URI.create(videoUrl);
            if (!isBilibiliHost(parsed.getHost())) {
                return null;
            }
            Map<String, String> query = parseQueryParams(parsed.getRawQuery());
            String rawEpisode = query.getOrDefault("p", "");
            if (rawEpisode.isBlank()) {
                rawEpisode = query.getOrDefault("P", "");
            }
            if (rawEpisode.isBlank()) {
                return null;
            }
            int value = Integer.parseInt(rawEpisode);
            if (value > 0) {
                return value;
            }
            return null;
        } catch (Exception ignored) {
            return null;
        }
    }

    private Map<String, String> parseQueryParams(String rawQuery) {
        Map<String, String> result = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return result;
        }
        String[] parts = rawQuery.split("&");
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            int equalsAt = part.indexOf('=');
            String keyRaw = equalsAt >= 0 ? part.substring(0, equalsAt) : part;
            String valueRaw = equalsAt >= 0 ? part.substring(equalsAt + 1) : "";
            String key = decodeUrlComponent(keyRaw);
            String value = decodeUrlComponent(valueRaw);
            if (!key.isBlank() && !result.containsKey(key)) {
                result.put(key, value);
            }
        }
        return result;
    }

    private String resolveDocumentTitle(DownloadResult downloadResult, String outputDir, String videoPath) {
        String fallbackTitle = new File(videoPath).getName().replaceFirst("\\.[^.]+$", "");
        String grpcTitle = "";
        if (downloadResult != null && downloadResult.videoTitle != null) {
            grpcTitle = downloadResult.videoTitle.trim();
        }
        if (!grpcTitle.isBlank()) {
            logger.info("Using grpc download title for markdown: {}", grpcTitle);
            return grpcTitle;
        }

        String metaTitle = readTitleFromVideoMeta(outputDir);
        if (metaTitle != null && !metaTitle.isBlank()) {
            logger.info("Using video_meta title for markdown: {}", metaTitle);
            return metaTitle;
        }

        return fallbackTitle;
    }

    private String readTitleFromVideoMeta(String outputDir) {
        if (outputDir == null || outputDir.isBlank()) {
            return "";
        }
        try {
            String title = videoMetaService.readTitle(Paths.get(outputDir));
            return title == null ? "" : title;
        } catch (Exception e) {
            logger.warn("Failed to read video_meta.json title: {}", e.getMessage());
            return "";
        }
    }

    private void persistTaskTocMetadata(String outputDir, String contentType, List<Map<String, Object>> bookSectionTree) {
        if (outputDir == null || outputDir.isBlank()) {
            return;
        }
        try {
            Path taskRoot = Paths.get(outputDir).toAbsolutePath().normalize();
            List<Map<String, Object>> safeTree = bookSectionTree != null ? bookSectionTree : List.of();
            boolean updated = videoMetaService.writeTocMetadata(taskRoot, contentType, safeTree);
            if (!updated) {
                logger.warn("Skip writing toc metadata because task root is invalid: outputDir={}", outputDir);
            }
        } catch (Exception ex) {
            logger.warn("Persist task toc metadata failed: outputDir={} err={}", outputDir, ex.getMessage());
        }
    }

    private String decodeUrlComponent(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (Exception ignored) {
            return value;
        }
    }

    private List<String> buildDownloadWatchDirs(String requestedOutputDir, String predictedDownloadDir) {
        LinkedHashSet<String> watchDirs = new LinkedHashSet<>();
        if (requestedOutputDir != null && !requestedOutputDir.isBlank()) {
            try {
                watchDirs.add(Paths.get(requestedOutputDir).toAbsolutePath().normalize().toString());
            } catch (Exception ignored) {
                watchDirs.add(requestedOutputDir);
            }
        }
        if (predictedDownloadDir != null && !predictedDownloadDir.isBlank()) {
            try {
                watchDirs.add(Paths.get(predictedDownloadDir).toAbsolutePath().normalize().toString());
            } catch (Exception ignored) {
                watchDirs.add(predictedDownloadDir);
            }
        }
        return new ArrayList<>(watchDirs);
    }

    private DownloadActivitySnapshot observeDownloadActivity(
            List<String> watchDirs,
            DownloadActivitySnapshot previous,
            long nowMs) {
        if (watchDirs == null || watchDirs.isEmpty()) {
            return new DownloadActivitySnapshot(0L, 0L, 0L, new LinkedHashMap<>());
        }
        Map<String, DownloadDirActivity> perDir = new LinkedHashMap<>();
        long totalBytes = 0L;
        long fileCount = 0L;
        long latestModifiedMs = 0L;
        int scanDepth = Math.max(1, downloadWatchdogScanDepth);
        long minRescanMs = Math.max(200L, downloadWatchdogMinRescanMs);

        for (String dirPath : watchDirs) {
            if (dirPath == null || dirPath.isBlank()) {
                continue;
            }
            try {
                Path dir = Paths.get(dirPath).toAbsolutePath().normalize();
                if (!Files.isDirectory(dir)) {
                    continue;
                }
                String dirKey = dir.toString();
                DownloadDirActivity oldDir = previous != null ? previous.perDir.get(dirKey) : null;
                long dirMtime = safeReadLastModifiedMillis(dir);

                if (oldDir != null) {
                    boolean withinRescanWindow = nowMs - oldDir.scannedAtMs < minRescanMs;
                    boolean directoryUnchanged = dirMtime > 0L
                        && dirMtime == oldDir.directoryModifiedMs;
                    if (withinRescanWindow || directoryUnchanged) {
                        DownloadDirActivity reused = oldDir.withScannedAt(nowMs);
                        perDir.put(dirKey, reused);
                        totalBytes += reused.totalBytes;
                        fileCount += reused.fileCount;
                        latestModifiedMs = Math.max(latestModifiedMs, reused.latestModifiedMs);
                        continue;
                    }
                }

                long dirBytes = 0L;
                long dirFiles = 0L;
                long dirLatestModifiedMs = Math.max(0L, dirMtime);
                try (Stream<Path> stream = Files.walk(dir, scanDepth)) {
                    Iterator<Path> iterator = stream.iterator();
                    while (iterator.hasNext()) {
                        Path file = iterator.next();
                        if (!Files.isRegularFile(file)) {
                            continue;
                        }
                        dirFiles += 1L;
                        try {
                            dirBytes += Math.max(0L, Files.size(file));
                        } catch (Exception ignored) {
                            // 单文件读取失败不影响整体统计，继续扫描其余文件。
                        }
                        long fileMtime = safeReadLastModifiedMillis(file);
                        dirLatestModifiedMs = Math.max(dirLatestModifiedMs, fileMtime);
                    }
                }

                DownloadDirActivity current = new DownloadDirActivity(
                    dirBytes,
                    dirFiles,
                    dirLatestModifiedMs,
                    Math.max(0L, dirMtime),
                    nowMs
                );
                perDir.put(dirKey, current);
                totalBytes += current.totalBytes;
                fileCount += current.fileCount;
                latestModifiedMs = Math.max(latestModifiedMs, current.latestModifiedMs);
            } catch (Exception ignored) {
                // 目录不可读或遍历失败时跳过，避免阻断下载活跃度探测。
            }
        }
        return new DownloadActivitySnapshot(totalBytes, fileCount, latestModifiedMs, perDir);
    }

    private long safeReadLastModifiedMillis(Path path) {
        if (path == null) {
            return 0L;
        }
        try {
            return Math.max(0L, Files.getLastModifiedTime(path).toMillis());
        } catch (Exception ignored) {
            return 0L;
        }
    }

    private static final class DownloadDirActivity {
        private final long totalBytes;
        private final long fileCount;
        private final long latestModifiedMs;
        private final long directoryModifiedMs;
        private final long scannedAtMs;

        private DownloadDirActivity(
                long totalBytes,
                long fileCount,
                long latestModifiedMs,
                long directoryModifiedMs,
                long scannedAtMs) {
            this.totalBytes = totalBytes;
            this.fileCount = fileCount;
            this.latestModifiedMs = latestModifiedMs;
            this.directoryModifiedMs = directoryModifiedMs;
            this.scannedAtMs = scannedAtMs;
        }

        private DownloadDirActivity withScannedAt(long scannedAtMs) {
            return new DownloadDirActivity(
                totalBytes,
                fileCount,
                latestModifiedMs,
                directoryModifiedMs,
                scannedAtMs
            );
        }
    }

    private static final class DownloadActivitySnapshot {
        private final long totalBytes;
        private final long fileCount;
        private final long latestModifiedMs;
        private final Map<String, DownloadDirActivity> perDir;

        private DownloadActivitySnapshot(
                long totalBytes,
                long fileCount,
                long latestModifiedMs,
                Map<String, DownloadDirActivity> perDir) {
            this.totalBytes = totalBytes;
            this.fileCount = fileCount;
            this.latestModifiedMs = latestModifiedMs;
            this.perDir = perDir != null ? perDir : Collections.emptyMap();
        }

        private boolean isChangedFrom(DownloadActivitySnapshot previous) {
            if (previous == null) {
                return true;
            }
            return totalBytes != previous.totalBytes
                || fileCount != previous.fileCount
                || latestModifiedMs != previous.latestModifiedMs;
        }
    }

    private int normalizePositive(int value, int fallback) {
        return value > 0 ? value : fallback;
    }
    
    private String firstNonBlank(String value, String fallback) {
        if (value != null && !value.isBlank()) {
            return value;
        }
        return fallback;
    }

    private boolean isInterruptedVLResult(VLAnalysisResult vlResult) {
        if (vlResult == null) {
            return false;
        }
        if (vlResult.interrupted) {
            return true;
        }
        String error = vlResult.errorMsg;
        if (error == null || error.isBlank()) {
            return false;
        }
        String normalized = error.toLowerCase(Locale.ROOT);
        return normalized.contains("thread interrupted") || normalized.contains("interrupted");
    }

    private boolean isInterruptedFailure(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        if (throwable instanceof InterruptedException || throwable instanceof CancellationException) {
            return true;
        }
        if (Thread.currentThread().isInterrupted()) {
            return true;
        }
        String message = extractThrowableMessage(throwable);
        if (message == null || message.isBlank()) {
            return false;
        }
        String normalized = message.toLowerCase(Locale.ROOT);
        return normalized.contains("thread interrupted") || normalized.contains("interrupted");
    }

    private String extractThrowableMessage(Throwable throwable) {
        if (throwable == null) {
            return "Pipeline failed with unknown error";
        }
        Throwable cursor = throwable;
        String fallbackType = throwable.getClass().getSimpleName();
        int depth = 0;
        while (cursor != null && depth < 8) {
            String message = cursor.getMessage();
            if (message != null && !message.isBlank()) {
                if (depth == 0) {
                    return message;
                }
                return cursor.getClass().getSimpleName() + ": " + message;
            }
            fallbackType = cursor.getClass().getSimpleName();
            cursor = cursor.getCause();
            depth++;
        }
        return fallbackType + " (message unavailable)";
    }

    private String normalizeThrowableMessage(Throwable throwable, String fallback) {
        return ensureNonEmptyErrorMessage(extractThrowableMessage(throwable), fallback);
    }

    private String ensureNonEmptyErrorMessage(String message, String fallback) {
        String normalizedMessage = message != null ? message.trim() : "";
        if (!normalizedMessage.isBlank()) {
            return normalizedMessage;
        }
        String normalizedFallback = fallback != null ? fallback.trim() : "";
        if (!normalizedFallback.isBlank()) {
            return normalizedFallback;
        }
        return "unknown error (empty message)";
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
                dto.screenshotId, dto.timestampSec, dto.label, dto.semanticUnitId, dto.frameReason
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

    private int resolveVLAnalyzeTimeoutSec(
            String taskId,
            AnalyzeResult analyzeResult,
            DynamicTimeoutCalculator.TimeoutConfig timeouts
    ) {
        int baseTimeoutSec = Math.max(
            MIN_VL_ANALYZE_TIMEOUT_SEC,
            timeouts != null ? timeouts.getPhase2aTimeoutSec() : 0
        );
        double processThresholdSec = DEFAULT_VL_PROCESS_DURATION_THRESHOLD_SEC;
        if (configService != null) {
            processThresholdSec = Math.max(0.0d, configService.getVLProcessDurationThresholdSec());
        }

        double estimatedVLSegmentDurationSec = estimateVLSegmentDurationSec(analyzeResult, processThresholdSec);
        int workloadTimeoutSec = (int) Math.ceil(
            Math.max(0.0d, estimatedVLSegmentDurationSec) * VL_ANALYZE_WORKLOAD_TIMEOUT_MULTIPLIER
        );
        int resolvedTimeoutSec = Math.max(baseTimeoutSec, workloadTimeoutSec);

        logger.info(
            "[{}] VL timeout resolved: base={}s, workload={}s (sum_vl_segment={}s, threshold={}s, multiplier={}x), final={}s",
            taskId,
            baseTimeoutSec,
            workloadTimeoutSec,
            String.format(Locale.ROOT, "%.2f", estimatedVLSegmentDurationSec),
            String.format(Locale.ROOT, "%.2f", processThresholdSec),
            String.format(Locale.ROOT, "%.2f", VL_ANALYZE_WORKLOAD_TIMEOUT_MULTIPLIER),
            resolvedTimeoutSec
        );
        return resolvedTimeoutSec;
    }

    private double estimateVLSegmentDurationSec(AnalyzeResult analyzeResult, double processThresholdSec) {
        if (analyzeResult == null) {
            return 0.0d;
        }
        try {
            JsonNode rootNode = loadSemanticUnitsRoot(analyzeResult);
            List<JsonNode> unitNodes = extractSemanticUnitNodes(rootNode);
            if (unitNodes.isEmpty()) {
                return 0.0d;
            }
            double totalDurationSec = 0.0d;
            for (JsonNode unitNode : unitNodes) {
                if (unitNode == null || !unitNode.isObject()) {
                    continue;
                }
                String knowledgeType = unitNode.path("knowledge_type").asText("");
                if (!isProcessKnowledgeTypeForVL(knowledgeType)) {
                    continue;
                }
                double durationSec = readDurationSecFromUnitNode(unitNode);
                if (durationSec > processThresholdSec) {
                    totalDurationSec += durationSec;
                }
            }
            return totalDurationSec;
        } catch (Exception estimateError) {
            logger.warn("Failed to estimate VL segment duration from semantic units: {}", estimateError.getMessage());
            return 0.0d;
        }
    }

    private List<JsonNode> extractSemanticUnitNodes(JsonNode rootNode) {
        if (rootNode == null || rootNode.isNull()) {
            return Collections.emptyList();
        }
        List<JsonNode> nodes = new ArrayList<>();
        if (rootNode.isArray()) {
            rootNode.forEach(nodes::add);
            return nodes;
        }
        JsonNode semanticUnitsNode = rootNode.path("semantic_units");
        if (semanticUnitsNode.isArray()) {
            semanticUnitsNode.forEach(nodes::add);
        }
        return nodes;
    }

    private boolean isProcessKnowledgeTypeForVL(String knowledgeType) {
        String normalized = knowledgeType == null ? "" : knowledgeType.trim().toLowerCase(Locale.ROOT);
        if (normalized.isBlank()) {
            return true;
        }
        return normalized.contains("process")
            || normalized.contains("practical")
            || normalized.contains("过程")
            || normalized.contains("流程")
            || normalized.contains("操作")
            || normalized.contains("实操");
    }

    private double readDurationSecFromUnitNode(JsonNode unitNode) {
        double startSec = parseDouble(unitNode.path("start_sec").asText(null), 0.0d);
        double endSec = parseDouble(unitNode.path("end_sec").asText(null), startSec);
        if (endSec <= startSec) {
            double fallbackStart = parseDouble(unitNode.path("timestamp_start").asText(null), startSec);
            double fallbackEnd = parseDouble(unitNode.path("timestamp_end").asText(null), endSec);
            startSec = fallbackStart;
            endSec = fallbackEnd;
        }
        return Math.max(0.0d, endSec - startSec);
    }

    // --- Extracted Methods ---

    /**
     * 尝试执行 VL 分析链路；若失败或触发回退策略则返回 null。
     */
    private ExtractionRequests tryVLAnalysis(String taskId, String videoPath, AnalyzeResult ar, String outputDir, DynamicTimeoutCalculator.TimeoutConfig timeouts) {
        updateProgress(taskId, 0.40, "执行 VL 视觉语言模型分析...");
        int vlTimeoutSec = resolveVLAnalyzeTimeoutSec(taskId, ar, timeouts);
        TaskProgressWatchdogBridge.SignalEmitter analysisSignalEmitter =
            (progress, message) -> updateProgress(taskId, progress, message);
        taskProgressWatchdogBridge.resetTask(taskId);
        TaskProgressWatchdogBridge.MonitorHandle vlMonitor =
            taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "analysis_extraction", analysisSignalEmitter);
        try {
            VLAnalysisResult vlResult = grpcClient.analyzeWithVL(
                taskId,
                videoPath,
                ar,
                outputDir,
                vlTimeoutSec
            );
            if (isInterruptedVLResult(vlResult)) {
                String reason = firstNonBlank(vlResult.errorMsg, "VL analysis interrupted");
                throw new RuntimeException("VL analysis interrupted: " + reason);
            }

            if (vlResult.success && vlResult.vlEnabled && !vlResult.usedFallback) {
                logger.info("[{}] VL Analysis Success! Skipping legacy flow.", taskId);
                List<JavaCVFFmpegService.ScreenshotRequest> screenshots = convertScreenshotRequests(vlResult.screenshotRequests);
                List<JavaCVFFmpegService.ClipRequest> clips = convertClipRequests(vlResult.clipRequests);
                return startExtractionPipeline(taskId, videoPath, outputDir, screenshots, clips, 0);
            }
            logger.warn("[{}] VL Analysis fallback reason: {}", taskId, vlResult.errorMsg);
        } catch (Exception e) {
            if (isInterruptedFailure(e)) {
                throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
            }
            logger.error("[{}] VL Analysis failed with exception (timeout={}s)", taskId, vlTimeoutSec, e);
        } finally {
            taskProgressWatchdogBridge.stopMonitor(taskId, vlMonitor, analysisSignalEmitter);
        }
        return null;
    }
    private ExtractionRequests runLegacyAnalysis(String taskId, String videoPath, AnalyzeResult ar, Stage1Result s1, String outputDir, DynamicTimeoutCalculator.TimeoutConfig timeouts) throws Exception {
        // 加载 Semantic Units，兼容 AnalyzeResponse 的数组结构与对象包装结构。
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
        // 执行核心分析（CV 校验 + 知识分类）。
        AnalysisResults analysisResults = executeHybridAnalysis(taskId, videoPath, unitsList, s1.step2JsonPath, outputDir);

        // 2. Merge & Update
        updateSemanticUnits(unitsList, analysisResults.cvResults, analysisResults.classResults);
        Object updatedRoot = originallyArray ? unitsList : unitsMap;
        updateAnalyzeResultInlinePayload(ar, updatedRoot, unitsList.size());

        // 3. Generate Material Requests
        updateProgress(taskId, 0.70, "正在生成素材提取请求...");
        List<MaterialGenerationInput> matInputs = convertToMatInputs(unitsList, analysisResults.cvResults);
        MaterialGenerationResult matRes = grpcClient.generateMaterialRequests(taskId, matInputs, videoPath, outputDir, 600);
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
     * 执行混合分析流程：并行 CV 校验、知识分类与缓存复用。
     */
    private AnalysisResults executeHybridAnalysis(String taskId, String videoPath, List<Map<String, Object>> unitsList, String step2JsonPath, String outputDir) {
        updateProgress(taskId, 0.45, "正在执行分阶段分析（CV/CF 并行）...");
        
        Map<String, CVValidationUnitResult> cvResults = new ConcurrentHashMap<>();
        List<KnowledgeResultItem> classResults = new ArrayList<>();
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
                });
                if (cvFutures != null) cvFuturesList.addAll(cvFutures);
            }

            if (!cvFuturesList.isEmpty()) {
                CompletableFuture.allOf(cvFuturesList.toArray(new CompletableFuture[0])).join();
            }

            if (!cvResults.isEmpty()) {
                List<ClassificationInput> classInputs = convertToClassInputs(unitsList, cvResults);
                List<KnowledgeResultItem> batchRes = knowledgeOrchestrator.classifyParallel(
                    taskId,
                    classInputs,
                    step2JsonPath,
                    outputDir
                );
                if (batchRes != null && !batchRes.isEmpty()) {
                    classResults.addAll(batchRes);
                }
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
        
        logger.info("Staged analysis done. CV: {}, Class: {}", cvResults.size(), classResults.size());
        return new AnalysisResults(cvResults, classResults);
    }
}

