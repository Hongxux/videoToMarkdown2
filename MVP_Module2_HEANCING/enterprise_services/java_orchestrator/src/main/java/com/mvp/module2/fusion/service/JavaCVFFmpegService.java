package com.mvp.module2.fusion.service;

import org.bytedeco.ffmpeg.global.avcodec;
import org.bytedeco.ffmpeg.global.avutil;
import org.bytedeco.javacv.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.awt.image.BufferedImage;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.imageio.ImageIO;

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
                String screenshotsDir = Paths.get(outputDir, "screenshots").toString();
                String clipsDir = Paths.get(outputDir, "clips").toString();
                Files.createDirectories(Paths.get(screenshotsDir));
                Files.createDirectories(Paths.get(clipsDir));
                
                result.screenshotsDir = screenshotsDir;
                result.clipsDir = clipsDir;
                
                logger.info(
                    "🚀 JavaCV extraction starting: {} screenshots, {} clips, timeout={}s (producer-consumer)",
                    safeScreenshotRequests.size(), safeClipRequests.size(), timeoutSeconds
                );

                // 阶段间采用生产者-消费者：material requests 进入队列后即可被消费提取
                // 截图保持顺序复用同一个 grabber；clip 使用多 worker 并发消费。
                CompletableFuture<Integer> screenshotConsumer = CompletableFuture.supplyAsync(
                    () -> extractScreenshotsBatch(videoPath, screenshotsDir, safeScreenshotRequests),
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
                        () -> consumeClipQueue(videoPath, clipsDir, clipQueue, workerIndex),
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
                        
                        // 保存为 PNG
                        String outputPath = Paths.get(outputDir, req.screenshotId + ".png").toString();
                        ImageIO.write(image, "PNG", new File(outputPath));
                        
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
                String outputPath = Paths.get(outputDir, req.clipId + ".mp4").toString();
                boolean extracted;
                if (req.segments != null && !req.segments.isEmpty()) {
                    extracted = extractConcatClip(videoPath, outputPath, req.segments);
                    if (!extracted) {
                        extracted = extractSingleClip(videoPath, outputPath, req.startSec, req.endSec);
                    }
                } else {
                    extracted = extractSingleClip(videoPath, outputPath, req.startSec, req.endSec);
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
                    String outputPath = Paths.get(outputDir, req.clipId + ".mp4").toString();
                    boolean extracted = extractClipWithWorkerGrabber(workerGrabber, outputPath, req);
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
        // 权衡：clip 编码较重，worker 不宜过多；采用 CPU 一半并上限 6。
        int candidate = Math.max(1, Math.min(cpu / 2, 6));
        return Math.max(1, Math.min(candidate, clipCount));
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
            FFmpegFrameGrabber workerGrabber,
            String outputPath,
            ClipRequest request
    ) {
        if (request == null) return false;
        if (request.segments != null && !request.segments.isEmpty()) {
            boolean concatOk = extractConcatClipWithGrabber(workerGrabber, outputPath, request.segments);
            if (concatOk) return true;
        }
        return extractSingleClipWithGrabber(workerGrabber, outputPath, request.startSec, request.endSec);
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
