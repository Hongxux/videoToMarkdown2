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
        public String errorMessage;
        public long processingTimeMs;
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
            long phase2aStart = System.currentTimeMillis();
            taskProgressWatchdogBridge.resetTask(taskId);
            TaskProgressWatchdogBridge.MonitorHandle phase2aMonitor =
                taskProgressWatchdogBridge.startMonitor(taskId, outputDir, "phase2a", taskSignalEmitter);
            AnalyzeResult ar;
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
            persistTaskTocMetadata(outputDir, "video", List.of());
            logger.info("Pipeline Complete: {}", taskId);

            flowFlags.put("downloaded_from_url", downloadedFromUrl);
            if (downloadResult != null) {
                flowFlags.put("download_content_type", firstNonBlank(downloadResult.contentType, "unknown"));
                flowFlags.put("download_source_platform", firstNonBlank(downloadResult.sourcePlatform, "unknown"));
            }
            flowFlags.put("used_vl_flow", usedVLFlow);
            flowFlags.put("used_legacy_flow", usedLegacyFlow);

        } catch (Exception e) {
            String normalizedError = extractThrowableMessage(e);
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
        }
        return result;
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
                    outputDir = resolveOutputDirForLocalVideo(sourcePath);
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
                        outputDir = new File(sourcePath).getParentFile().getAbsolutePath();
                        new File(outputDir).mkdirs();
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

            updateProgress(taskId, 0.20, "Extracting book content...");
            long bookFlowStart = System.currentTimeMillis();
            BookMarkdownService.BookProcessingResult bookResult = bookMarkdownService.processBook(
                    taskId,
                    sourcePath,
                    outputDir,
                    toBookServiceOptions(bookOptions)
            );
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
            if (bookOptions != null && bookOptions.chapterSelector != null && !bookOptions.chapterSelector.isBlank()) {
                flowFlags.put("book_chapter_selector", bookOptions.chapterSelector.trim());
            }
            if (bookOptions != null && bookOptions.sectionSelector != null && !bookOptions.sectionSelector.isBlank()) {
                flowFlags.put("book_section_selector", bookOptions.sectionSelector.trim());
            }
            if (bookOptions != null && bookOptions.pageOffset != null) {
                flowFlags.put("book_page_offset", bookOptions.pageOffset);
            }
        } catch (Exception error) {
            String normalizedError = extractThrowableMessage(error);
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
        if ((options.chapterSelector == null || options.chapterSelector.isBlank())
                && (options.sectionSelector == null || options.sectionSelector.isBlank())
                && options.splitByChapter == null
                && options.splitBySection == null
                && options.pageOffset == null) {
            return null;
        }
        return options;
    }

    private boolean shouldProcessAsBook(String source, BookProcessingOptions options) {
        if (options != null
                && ((options.chapterSelector != null && !options.chapterSelector.isBlank())
                || (options.sectionSelector != null && !options.sectionSelector.isBlank())
                || options.splitByChapter != null
                || options.splitBySection != null
                || options.pageOffset != null)) {
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
            DeepSeekUsage deepSeekUsage = loadDeepSeekUsage(outputDir);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("version", "1.0");
            payload.put("generated_at", Instant.now().toString());
            payload.put("task_id", taskId);
            payload.put("success", result != null && result.success);
            payload.put("error_message", result != null ? (result.errorMessage != null ? result.errorMessage : "") : "");
            payload.put("input_video_url", inputVideoUrl != null ? inputVideoUrl : "");
            payload.put("video_title", videoTitle != null ? videoTitle : "");
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
        MaterialGenerationResult matRes = grpcClient.generateMaterialRequests(taskId, matInputs, videoPath, 600);
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

