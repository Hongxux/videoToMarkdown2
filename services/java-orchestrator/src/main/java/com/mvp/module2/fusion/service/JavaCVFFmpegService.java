package com.mvp.module2.fusion.service;

import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.ffmpeg.global.avutil;
import org.bytedeco.javacv.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.sun.management.OperatingSystemMXBean;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

/**
 * JavaCV FFmpeg 服务 (常驻进程版本)
 * 
 * 🚀 优化原理:
 * - 使用 JavaCV 的 JNI 绑定直接调用 FFmpeg 库
 * - 保持 FFmpegFrameGrabber 上下文开启，避免每次操作都 spawn 新进程
 * - 批量操作时复用同一个 Grabber，减少初始化开销
 * 
 * 对比原版 ProcessBuilder 方案:
 * - 原版: 每次截图/切片都 spawn ffmpeg.exe 进程 (~200ms 启动开销)
 * - 优化: JNI 直接调用，无进程开销 (~10ms)
 * 
 * @author Antigravity AI
 * @version 2.0
 */
@Service
public class JavaCVFFmpegService {
    
    private static final Logger logger = LoggerFactory.getLogger(JavaCVFFmpegService.class);
    private static final String SCREENSHOT_EXT = ".jpg";
    private static final float SCREENSHOT_JPEG_QUALITY = 0.90f;
    private static final long MIN_FAST_COPY_FILE_BYTES = 1_024L;
    static final int MOBILE_FAST_COPY_CODEC_ID = avcodec.AV_CODEC_ID_H264;
    
    // 线程池：处理并发请求
    private ExecutorService executorService;
    
    // Grabber 缓存：每个视频文件一个 Grabber（线程安全）
    private ConcurrentHashMap<String, FFmpegFrameGrabber> grabberCache;
    
    // 统计
    private AtomicInteger totalScreenshots = new AtomicInteger(0);
    private AtomicInteger totalClips = new AtomicInteger(0);

    // 生产者-消费者结束哨兵（clip 队列）
    private static final ClipRequest CLIP_POISON_PILL =
        new ClipRequest("__clip_poison__", 0.0, 0.0, "", "");
    
    @PostConstruct
    public void init() {
        // 设置 FFmpeg 日志级别（减少噪音）
        avutil.av_log_set_level(avutil.AV_LOG_ERROR);
        
        // 创建线程池（CPU核心数）
        int threads = Runtime.getRuntime().availableProcessors();
        this.executorService = Executors.newFixedThreadPool(threads);
        this.grabberCache = new ConcurrentHashMap<>();
        
        logger.info("🚀 JavaCVFFmpegService initialized: threads={}, FFmpeg via JNI (no process spawn)", threads);
    }
    
    @PreDestroy
    public void shutdown() {
        // 关闭所有 Grabber
        grabberCache.forEach((path, grabber) -> {
            try {
                grabber.stop();
                grabber.release();
                logger.debug("Closed grabber for: {}", path);
            } catch (Exception e) {
                logger.warn("Error closing grabber for {}: {}", path, e.getMessage());
            }
        });
        grabberCache.clear();
        
        // 关闭线程池
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
        
        logger.info("JavaCVFFmpegService shutdown complete. Stats: screenshots={}, clips={}", 
            totalScreenshots.get(), totalClips.get());
    }
    
    /**
     * 获取或创建视频的 FFmpegFrameGrabber
     * 
     * 🔑 关键优化：复用 Grabber 避免重复解析视频元数据
     */
    private synchronized FFmpegFrameGrabber getGrabber(String videoPath) throws Exception {
        return grabberCache.computeIfAbsent(videoPath, path -> {
            try {
                FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(path);
                grabber.start();
                logger.info("Created new FFmpegFrameGrabber for: {} (duration={}s)", 
                    path, grabber.getLengthInTime() / 1_000_000.0);
                return grabber;
            } catch (Exception e) {
                logger.error("Failed to create grabber for {}: {}", path, e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }

    public double probeVideoDurationSec(String videoPath) {
        if (videoPath == null || videoPath.isEmpty()) {
            return 0.0;
        }

        try {
            FFmpegFrameGrabber grabber = getGrabber(videoPath);
            double durationSec = grabber.getLengthInTime() / 1_000_000.0;
            if (durationSec <= 0) {
                int frames = grabber.getLengthInFrames();
                double fps = grabber.getFrameRate();
                if (frames > 0 && fps > 0) {
                    durationSec = frames / fps;
                }
            }
            if (durationSec > 0) {
                logger.debug("Probed video duration: {}s ({})", durationSec, videoPath);
                return durationSec;
            }
        } catch (Exception e) {
            logger.warn("Failed to probe video duration for {}: {}", videoPath, e.getMessage());
        }

        return 0.0;
    }
    
    /**
     * 释放指定视频的 Grabber（处理完成后调用）
     */
    public void releaseGrabber(String videoPath) {
        FFmpegFrameGrabber grabber = grabberCache.remove(videoPath);
        if (grabber != null) {
            try {
                grabber.stop();
                grabber.release();
                logger.debug("Released grabber for: {}", videoPath);
            } catch (Exception e) {
                logger.warn("Error releasing grabber: {}", e.getMessage());
            }
        }
    }
    
    // =========================================================================
    // 截图请求/结果类
    // =========================================================================
    
    public static class ScreenshotRequest {
        public String screenshotId;
        public double timestampSec;
        public String label;
        public String semanticUnitId;
        
        public ScreenshotRequest(String screenshotId, double timestampSec, String label, String semanticUnitId) {
            this.screenshotId = screenshotId;
            this.timestampSec = timestampSec;
            this.label = label;
            this.semanticUnitId = semanticUnitId;
        }
    }

    public static class ClipSegment {
        public double startSec;
        public double endSec;

        public ClipSegment(double startSec, double endSec) {
            this.startSec = startSec;
            this.endSec = endSec;
        }
    }
    
    public static class ClipRequest {
        public String clipId;
        public double startSec;
        public double endSec;
        public String knowledgeType;
        public String semanticUnitId;
        public List<ClipSegment> segments;
        
        public ClipRequest(String clipId, double startSec, double endSec, String knowledgeType, String semanticUnitId) {
            this(clipId, startSec, endSec, knowledgeType, semanticUnitId, null);
        }

        public ClipRequest(
                String clipId,
                double startSec,
                double endSec,
                String knowledgeType,
                String semanticUnitId,
                List<ClipSegment> segments
        ) {
            this.clipId = clipId;
            this.startSec = startSec;
            this.endSec = endSec;
            this.knowledgeType = knowledgeType;
            this.semanticUnitId = semanticUnitId;
            this.segments = segments != null ? segments : new ArrayList<>();
        }
    }
    
    public static class ExtractionResult {
        public String screenshotsDir;
        public String clipsDir;
        public int successfulScreenshots;
        public int successfulClips;
        public List<String> errors;
        public long elapsedMs;
        
        public ExtractionResult() {
            this.errors = new ArrayList<>();
        }
    }
    
    // =========================================================================
    // 核心提取方法
    // =========================================================================
    
    /**
     * 🚀 批量提取截图和视频片段（异步）
     * 
     * 使用 JavaCV JNI 绑定，避免 spawn FFmpeg 进程
     */
    public CompletableFuture<ExtractionResult> extractAllAsync(
            String videoPath,
            String outputDir,
            List<ScreenshotRequest> screenshotRequests,
            List<ClipRequest> clipRequests,
            int timeoutSeconds
    ) {
        return CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            ExtractionResult result = new ExtractionResult();
            
            try {
                List<ScreenshotRequest> safeScreenshotRequests =
                    screenshotRequests != null ? new ArrayList<>(screenshotRequests) : new ArrayList<>();
                List<ClipRequest> safeClipRequests =
                    clipRequests != null ? new ArrayList<>(clipRequests) : new ArrayList<>();

                // 创建输出目录
                String assetsDir = Paths.get(outputDir, "assets").toString();
                Files.createDirectories(Paths.get(assetsDir));

                result.screenshotsDir = assetsDir;
                result.clipsDir = assetsDir;
                
                logger.info(
                    "🚀 JavaCV extraction starting: {} screenshots, {} clips, timeout={}s (producer-consumer)",
                    safeScreenshotRequests.size(), safeClipRequests.size(), timeoutSeconds
                );

                // 阶段间采用生产者-消费者：material requests 进入队列后即可被消费提取
                // 截图保持顺序复用同一个 grabber；clip 使用多 worker 并发消费。
                CompletableFuture<Integer> screenshotConsumer = CompletableFuture.supplyAsync(
                    () -> extractScreenshotsBatch(videoPath, assetsDir, safeScreenshotRequests),
                    executorService
                );

                int clipWorkers = resolveClipWorkerCount(safeClipRequests.size());
                BlockingQueue<ClipRequest> clipQueue = new LinkedBlockingQueue<>();

                CompletableFuture<Void> clipProducer = CompletableFuture.runAsync(() -> {
                    try {
                        for (ClipRequest req : safeClipRequests) {
                            if (req != null) {
                                clipQueue.put(req);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Clip producer interrupted", e);
                    } finally {
                        for (int i = 0; i < clipWorkers; i++) {
                            try {
                                clipQueue.put(CLIP_POISON_PILL);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                }, executorService);

                List<CompletableFuture<Integer>> clipConsumers = new ArrayList<>();
                for (int i = 0; i < clipWorkers; i++) {
                    final int workerIndex = i;
                    clipConsumers.add(CompletableFuture.supplyAsync(
                        () -> consumeClipQueue(videoPath, assetsDir, clipQueue, workerIndex),
                        executorService
                    ));
                }

                clipProducer.join();
                int clipSuccess = 0;
                for (CompletableFuture<Integer> consumer : clipConsumers) {
                    clipSuccess += consumer.join();
                }
                int screenshotSuccess = screenshotConsumer.join();

                result.successfulScreenshots = screenshotSuccess;
                result.successfulClips = clipSuccess;
                
                // 释放 Grabber（处理完成）
                releaseGrabber(videoPath);
                
                result.elapsedMs = System.currentTimeMillis() - startTime;
                
                logger.info("✅ JavaCV extraction completed: {}/{} screenshots, {}/{} clips in {}ms",
                    result.successfulScreenshots, screenshotRequests.size(),
                    result.successfulClips, clipRequests.size(),
                    result.elapsedMs);
                
            } catch (Exception e) {
                logger.error("JavaCV extraction failed", e);
                result.errors.add(e.getMessage());
            }
            
            return result;
        }, executorService);
    }
    
    /**
     * 批量提取截图（顺序处理以复用 Grabber seek）
     */
    private int extractScreenshotsBatch(String videoPath, String outputDir, List<ScreenshotRequest> requests) {
        if (requests.isEmpty()) return 0;
        
        int success = 0;
        Java2DFrameConverter converter = new Java2DFrameConverter();
        
        try {
            FFmpegFrameGrabber grabber = getGrabber(videoPath);
            
            // 🔑 按时间戳排序，优化 seek 性能
            requests.sort((a, b) -> Double.compare(a.timestampSec, b.timestampSec));
            
            for (ScreenshotRequest req : requests) {
                try {
                    // Seek 到指定时间戳（微秒）
                    long timestamp = (long) (req.timestampSec * 1_000_000);
                    grabber.setTimestamp(timestamp);
                    
                    // 抓取帧
                    Frame frame = grabber.grabImage();
                    if (frame != null && frame.image != null) {
                        // 转换为 BufferedImage
                        BufferedImage image = converter.convert(frame);
                        
                        // 保存为 JPEG（降低编码与写盘开销）
                        Path outputPath = Paths.get(outputDir, req.screenshotId + SCREENSHOT_EXT);
                        Path parentDir = outputPath.getParent();
                        if (parentDir != null) {
                            Files.createDirectories(parentDir);
                        }
                        if (!writeJpeg(outputPath, image, SCREENSHOT_JPEG_QUALITY)) {
                            throw new RuntimeException("Failed to write JPEG: " + outputPath);
                        }
                        
                        success++;
                        totalScreenshots.incrementAndGet();
                        logger.debug("Screenshot saved: {} @ {:.2f}s", req.screenshotId, req.timestampSec);
                    } else {
                        logger.warn("No frame at timestamp {:.2f}s for {}", req.timestampSec, req.screenshotId);
                    }
                } catch (Exception e) {
                    logger.error("Screenshot error: {} - {}", req.screenshotId, e.getMessage());
                }
            }
            
        } catch (Exception e) {
            logger.error("Batch screenshot extraction failed: {}", e.getMessage());
        }
        
        return success;
    }
    
    /**
     * 批量提取视频片段
     * 
     * 注意：视频切片需要独立的 Grabber 和 Recorder，无法复用
     */
    private int extractClipsBatch(String videoPath, String outputDir, List<ClipRequest> requests) {
        if (requests.isEmpty()) return 0;
        
        int success = 0;
        
        for (ClipRequest req : requests) {
            try {
                Path outputPath = Paths.get(outputDir, req.clipId + ".mp4");
                Path parentDir = outputPath.getParent();
                if (parentDir != null) {
                    Files.createDirectories(parentDir);
                }
                boolean extracted;
                if (req.segments != null && !req.segments.isEmpty()) {
                    extracted = extractConcatClip(videoPath, outputPath.toString(), req.segments);
                    if (!extracted) {
                        extracted = extractSingleClip(videoPath, outputPath.toString(), req.startSec, req.endSec);
                    }
                } else {
                    extracted = extractSingleClip(videoPath, outputPath.toString(), req.startSec, req.endSec);
                }
                if (extracted) {
                    success++;
                    totalClips.incrementAndGet();
                }
            } catch (Exception e) {
                logger.error("Clip error: {} - {}", req.clipId, e.getMessage());
            }
        }
        
        return success;
    }

    /**
     * clip 消费者：每个 worker 复用本地 grabber，避免每段 clip 都 start/stop。
     */
    private int consumeClipQueue(
            String videoPath,
            String outputDir,
            BlockingQueue<ClipRequest> queue,
            int workerIndex
    ) {
        int success = 0;
        FFmpegFrameGrabber workerGrabber = null;
        try {
            workerGrabber = createWorkerGrabber(videoPath);
            while (true) {
                ClipRequest req = queue.take();
                if (req == null) continue;
                if (isPoisonPill(req)) {
                    break;
                }

                try {
                    Path outputPath = Paths.get(outputDir, req.clipId + ".mp4");
                    Path parentDir = outputPath.getParent();
                    if (parentDir != null) {
                        Files.createDirectories(parentDir);
                    }
                    boolean extracted = extractClipWithWorkerGrabber(videoPath, workerGrabber, outputPath.toString(), req);
                    if (extracted) {
                        success++;
                        totalClips.incrementAndGet();
                    }
                } catch (Exception e) {
                    logger.error("Clip worker[{}] error: {} - {}", workerIndex, req.clipId, e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Clip worker[{}] init failed: {}", workerIndex, e.getMessage());
        } finally {
            closeGrabber(workerGrabber);
        }
        return success;
    }

    private int resolveClipWorkerCount(int clipCount) {
        if (clipCount <= 0) return 1;
        int cpu = Runtime.getRuntime().availableProcessors();

        // 基线：编码任务按 CPU 一半分配，避免过度争抢。
        int limitByCpu = Math.max(1, cpu / 2);

        // 内存约束：按空闲物理内存估算 worker 容量（每 worker 预留约 768MB）。
        int limitByMemory = limitByCpu;
        // 负载约束：高负载时主动收敛并发，避免任务雪崩。
        int limitByLoad = limitByCpu;

        try {
            java.lang.management.OperatingSystemMXBean baseBean = ManagementFactory.getOperatingSystemMXBean();
            if (baseBean instanceof OperatingSystemMXBean osBean) {
                long freeMemBytes = osBean.getFreeMemorySize();
                if (freeMemBytes > 0) {
                    long perWorkerBytes = 768L * 1024L * 1024L;
                    limitByMemory = Math.max(1, (int) (freeMemBytes / perWorkerBytes));
                }

                double cpuLoad = osBean.getCpuLoad();
                if (cpuLoad >= 0.90) {
                    limitByLoad = 1;
                } else if (cpuLoad >= 0.75) {
                    limitByLoad = Math.max(1, cpu / 4);
                } else if (cpuLoad >= 0.60) {
                    limitByLoad = Math.max(1, cpu / 3);
                }
            }
        } catch (Exception e) {
            logger.debug("Clip worker auto-tuning fallback to CPU-only: {}", e.getMessage());
        }

        int hardCap = Math.max(2, Math.min(cpu, 8));
        int candidate = Math.max(1, Math.min(limitByCpu, Math.min(limitByMemory, limitByLoad)));
        int finalWorkers = Math.max(1, Math.min(candidate, Math.min(clipCount, hardCap)));

        logger.info(
            "Adaptive clip workers: clips={}, cpu={}, byCpu={}, byMem={}, byLoad={}, final={}",
            clipCount, cpu, limitByCpu, limitByMemory, limitByLoad, finalWorkers
        );
        return finalWorkers;
    }

    private boolean isPoisonPill(ClipRequest request) {
        return request == CLIP_POISON_PILL
            || (request.clipId != null && request.clipId.equals(CLIP_POISON_PILL.clipId));
    }

    private FFmpegFrameGrabber createWorkerGrabber(String videoPath) throws Exception {
        FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(videoPath);
        grabber.start();
        return grabber;
    }

    private void closeGrabber(FFmpegFrameGrabber grabber) {
        if (grabber == null) return;
        try {
            grabber.stop();
            grabber.release();
        } catch (Exception e) {
            logger.warn("Error closing worker grabber: {}", e.getMessage());
        }
    }

    private boolean extractClipWithWorkerGrabber(
            String videoPath,
            FFmpegFrameGrabber workerGrabber,
            String outputPath,
            ClipRequest request
    ) {
        if (request == null) return false;

        // 快速通道：优先走 ffmpeg stream copy，失败再回退到 JavaCV 重编码。
        boolean fastCopied = false;
        if (isFastCopyCodecCompatible(workerGrabber)) {
            try {
                if (request.segments != null && !request.segments.isEmpty()) {
                    fastCopied = extractConcatClipFastCopy(videoPath, outputPath, request.segments);
                } else {
                    fastCopied = extractSingleClipFastCopy(
                        videoPath,
                        outputPath,
                        request.startSec,
                        request.endSec
                    );
                }
                if (fastCopied) {
                    logger.debug("Clip fast-copy path hit: {}", outputPath);
                    return true;
                }
                logger.debug("Clip fast-copy fallback to re-encode: {}", outputPath);
            } catch (Exception e) {
                logger.debug("Clip fast-copy unavailable, fallback re-encode: {}", e.getMessage());
            }
        } else {
            logger.debug(
                "Skip fast-copy due to incompatible codec for mobile playback: video={}, codecId={}",
                videoPath,
                workerGrabber != null ? workerGrabber.getVideoCodec() : -1
            );
        }

        if (request.segments != null && !request.segments.isEmpty()) {
            boolean concatOk = extractConcatClipWithGrabber(workerGrabber, outputPath, request.segments);
            if (concatOk) return true;
        }
        return extractSingleClipWithGrabber(workerGrabber, outputPath, request.startSec, request.endSec);
    }

    static boolean isFastCopyCodecCompatible(int codecId) {
        return codecId == MOBILE_FAST_COPY_CODEC_ID;
    }

    private boolean isFastCopyCodecCompatible(FFmpegFrameGrabber workerGrabber) {
        if (workerGrabber == null) {
            return false;
        }
        return isFastCopyCodecCompatible(workerGrabber.getVideoCodec());
    }

    /**
     * 拼接多段视频片段为单个输出（去除段间空白）
     */
    private boolean extractConcatClip(String videoPath, String outputPath, List<ClipSegment> segments) {
        FFmpegFrameGrabber grabber = null;
        try {
            grabber = createWorkerGrabber(videoPath);
            return extractConcatClipWithGrabber(grabber, outputPath, segments);
        } catch (Exception e) {
            logger.error("Concat clip extraction failed: {} - {}", outputPath, e.getMessage());
            return false;
        } finally {
            closeGrabber(grabber);
        }
    }

    private boolean extractConcatClipWithGrabber(
            FFmpegFrameGrabber grabber,
            String outputPath,
            List<ClipSegment> segments
    ) {
        if (grabber == null || segments == null || segments.isEmpty()) {
            return false;
        }

        List<ClipSegment> validSegments = new ArrayList<>();
        for (ClipSegment seg : segments) {
            if (seg != null && seg.endSec > seg.startSec) {
                validSegments.add(seg);
            }
        }
        if (validSegments.isEmpty()) {
            return false;
        }
        validSegments.sort((a, b) -> Double.compare(a.startSec, b.startSec));

        FFmpegFrameRecorder recorder = null;
        try {
            recorder = new FFmpegFrameRecorder(outputPath, grabber.getImageWidth(), grabber.getImageHeight(), grabber.getAudioChannels());
            recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
            recorder.setAudioCodec(avcodec.AV_CODEC_ID_AAC);
            recorder.setFormat("mp4");
            recorder.setFrameRate(grabber.getFrameRate());
            recorder.setVideoBitrate(grabber.getVideoBitrate() > 0 ? grabber.getVideoBitrate() : 2_000_000);
            recorder.setAudioBitrate(128_000);
            recorder.setSampleRate(grabber.getSampleRate() > 0 ? grabber.getSampleRate() : 44100);
            recorder.start();

            long outputOffsetUs = 0L;
            long lastRecordedTs = -1L;
            for (ClipSegment seg : validSegments) {
                long startTimestamp = (long) (seg.startSec * 1_000_000);
                long endTimestamp = (long) (seg.endSec * 1_000_000);
                if (endTimestamp <= startTimestamp) {
                    continue;
                }

                grabber.setTimestamp(startTimestamp);
                Frame frame;
                while ((frame = grabber.grab()) != null) {
                    long currentTs = grabber.getTimestamp();
                    if (currentTs > endTimestamp) {
                        break;
                    }
                    long relativeTs = Math.max(0L, currentTs - startTimestamp);
                    long outputTs = outputOffsetUs + relativeTs;
                    if (outputTs <= lastRecordedTs) {
                        outputTs = lastRecordedTs + 1;
                    }
                    recorder.setTimestamp(outputTs);
                    recorder.record(frame);
                    lastRecordedTs = outputTs;
                }
                long segmentDurationUs = Math.max(0L, endTimestamp - startTimestamp);
                long expectedOffsetUs = outputOffsetUs + segmentDurationUs;
                long adjustedOffsetUs = lastRecordedTs >= 0 ? lastRecordedTs + 1 : expectedOffsetUs;
                outputOffsetUs = Math.max(expectedOffsetUs, adjustedOffsetUs);
            }

            double durationSec = outputOffsetUs / 1_000_000.0;
            logger.debug("Concat clip saved: {} (segments={}, duration={:.2f}s)",
                new File(outputPath).getName(), validSegments.size(), durationSec);
            return true;
        } catch (Exception e) {
            logger.error("Concat clip extraction failed: {} - {}", outputPath, e.getMessage());
            return false;
        } finally {
            try {
                if (recorder != null) {
                    recorder.stop();
                    recorder.release();
                }
            } catch (Exception e) {
                logger.warn("Error closing recorder: {}", e.getMessage());
            }
        }
    }
    
    /**
     * 提取单个视频片段
     */
    private boolean extractSingleClip(String videoPath, String outputPath, double startSec, double endSec) {
        FFmpegFrameGrabber grabber = null;
        try {
            grabber = createWorkerGrabber(videoPath);
            return extractSingleClipWithGrabber(grabber, outputPath, startSec, endSec);
        } catch (Exception e) {
            logger.error("Clip extraction failed: {} - {}", outputPath, e.getMessage());
            return false;
        } finally {
            closeGrabber(grabber);
        }
    }

    private boolean extractSingleClipWithGrabber(
            FFmpegFrameGrabber grabber,
            String outputPath,
            double startSec,
            double endSec
    ) {
        if (grabber == null) return false;

        FFmpegFrameRecorder recorder = null;
        try {
            long startTimestamp = (long) (startSec * 1_000_000);
            long endTimestamp = (long) (endSec * 1_000_000);
            grabber.setTimestamp(startTimestamp);

            recorder = new FFmpegFrameRecorder(outputPath, grabber.getImageWidth(), grabber.getImageHeight(), grabber.getAudioChannels());
            recorder.setVideoCodec(avcodec.AV_CODEC_ID_H264);
            recorder.setAudioCodec(avcodec.AV_CODEC_ID_AAC);
            recorder.setFormat("mp4");
            recorder.setFrameRate(grabber.getFrameRate());
            recorder.setVideoBitrate(grabber.getVideoBitrate() > 0 ? grabber.getVideoBitrate() : 2_000_000);
            recorder.setAudioBitrate(128_000);
            recorder.setSampleRate(grabber.getSampleRate() > 0 ? grabber.getSampleRate() : 44100);
            recorder.start();

            Frame frame;
            while ((frame = grabber.grab()) != null) {
                if (grabber.getTimestamp() > endTimestamp) {
                    break;
                }
                recorder.record(frame);
            }

            logger.debug("Clip saved: {} ({:.1f}s - {:.1f}s)",
                new File(outputPath).getName(), startSec, endSec);
            return true;
        } catch (Exception e) {
            logger.error("Clip extraction failed: {} - {}", outputPath, e.getMessage());
            return false;
        } finally {
            try {
                if (recorder != null) {
                    recorder.stop();
                    recorder.release();
                }
            } catch (Exception e) {
                logger.warn("Error closing recorder: {}", e.getMessage());
            }
        }
    }

    private boolean writeJpeg(Path outputPath, BufferedImage image, float quality) {
        if (image == null || outputPath == null) {
            return false;
        }

        BufferedImage rgbImage = image;
        if (image.getType() != BufferedImage.TYPE_INT_RGB) {
            rgbImage = new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_INT_RGB);
            Graphics2D g = rgbImage.createGraphics();
            try {
                g.drawImage(image, 0, 0, null);
            } finally {
                g.dispose();
            }
        }

        Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpg");
        if (!writers.hasNext()) {
            return false;
        }

        ImageWriter writer = writers.next();
        try (ImageOutputStream ios = ImageIO.createImageOutputStream(outputPath.toFile())) {
            writer.setOutput(ios);
            ImageWriteParam param = writer.getDefaultWriteParam();
            if (param.canWriteCompressed()) {
                param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
                param.setCompressionQuality(Math.max(0.1f, Math.min(quality, 1.0f)));
            }
            writer.write(null, new IIOImage(rgbImage, null, null), param);
            return Files.exists(outputPath) && Files.size(outputPath) > 0;
        } catch (Exception e) {
            logger.error("JPEG write failed: {}", e.getMessage());
            return false;
        } finally {
            writer.dispose();
        }
    }

    private boolean extractSingleClipFastCopy(String videoPath, String outputPath, double startSec, double endSec) {
        if (videoPath == null || videoPath.isBlank() || outputPath == null || outputPath.isBlank()) {
            return false;
        }

        double duration = endSec - startSec;
        if (duration <= 0) {
            return false;
        }

        ProcessBuilder pb = new ProcessBuilder(
            "ffmpeg",
            "-y",
            "-ss", formatSec(startSec),
            "-i", videoPath,
            "-t", formatSec(duration),
            "-c", "copy",
            "-movflags", "+faststart",
            "-avoid_negative_ts", "make_zero",
            outputPath
        );
        return runFastCopyProcess(pb, outputPath, "single");
    }

    private boolean extractConcatClipFastCopy(String videoPath, String outputPath, List<ClipSegment> segments) {
        if (videoPath == null || videoPath.isBlank() || outputPath == null || outputPath.isBlank() || segments == null || segments.isEmpty()) {
            return false;
        }

        List<ClipSegment> valid = new ArrayList<>();
        for (ClipSegment seg : segments) {
            if (seg != null && seg.endSec > seg.startSec) {
                valid.add(seg);
            }
        }
        if (valid.isEmpty()) {
            return false;
        }
        valid.sort(Comparator.comparingDouble(s -> s.startSec));

        Path concatDir = null;
        Path listFile = null;
        List<Path> parts = new ArrayList<>();
        try {
            concatDir = Files.createTempDirectory("clip-fastcopy-");

            int index = 0;
            for (ClipSegment seg : valid) {
                Path part = concatDir.resolve(String.format(Locale.ROOT, "part_%03d.mp4", index++));
                boolean ok = extractSingleClipFastCopy(videoPath, part.toString(), seg.startSec, seg.endSec);
                if (!ok) {
                    return false;
                }
                parts.add(part);
            }

            listFile = concatDir.resolve("concat-list.txt");
            List<String> lines = new ArrayList<>();
            for (Path part : parts) {
                String normalized = part.toAbsolutePath().toString().replace("\\", "/").replace("'", "''");
                lines.add("file '" + normalized + "'");
            }
            Files.write(listFile, lines, StandardCharsets.UTF_8);

            ProcessBuilder pb = new ProcessBuilder(
                "ffmpeg",
                "-y",
                "-f", "concat",
                "-safe", "0",
                "-i", listFile.toString(),
                "-c", "copy",
                "-movflags", "+faststart",
                outputPath
            );
            return runFastCopyProcess(pb, outputPath, "concat");
        } catch (Exception e) {
            logger.debug("Concat fast-copy failed: {}", e.getMessage());
            return false;
        } finally {
            if (concatDir != null) {
                try {
                    try (var pathStream = Files.walk(concatDir)) {
                        pathStream.sorted(Comparator.reverseOrder()).forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (Exception ignored) {
                            }
                        });
                    }
                } catch (Exception ignored) {
                }
            }
        }
    }

    private boolean runFastCopyProcess(ProcessBuilder pb, String outputPath, String mode) {
        Process process = null;
        try {
            pb.redirectErrorStream(true);
            process = pb.start();

            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (output.length() < 1200) {
                        output.append(line).append('\n');
                    }
                }
            }

            boolean finished = process.waitFor(120, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                return false;
            }

            Path target = Paths.get(outputPath);
            boolean ok = process.exitValue() == 0
                && Files.exists(target)
                && Files.size(target) >= MIN_FAST_COPY_FILE_BYTES;
            if (!ok) {
                logger.debug("Fast-copy {} failed: rc={}, out={}", mode, process.exitValue(), output.toString());
            }
            return ok;
        } catch (Exception e) {
            logger.debug("Fast-copy {} exception: {}", mode, e.getMessage());
            return false;
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
    }

    private String formatSec(double seconds) {
        return String.format(Locale.ROOT, "%.3f", Math.max(0.0, seconds));
    }
    
    /**
     * 同步版本
     */
    public ExtractionResult extractAllSync(
            String videoPath,
            String outputDir,
            List<ScreenshotRequest> screenshotRequests,
            List<ClipRequest> clipRequests,
            int timeoutSeconds
    ) {
        try {
            return extractAllAsync(videoPath, outputDir, screenshotRequests, clipRequests, timeoutSeconds)
                .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                throw new RuntimeException("FFmpeg extraction timeout after " + timeoutSeconds + "s", cause);
            }
            throw e;
        }
    }
    
    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format("JavaCVFFmpegService: screenshots=%d, clips=%d, cachedGrabbers=%d",
            totalScreenshots.get(), totalClips.get(), grabberCache.size());
    }
}
