package com.mvp.module2.fusion.service;

import org.springframework.stereotype.Component;

/**
 * 动态超时计算器
 * 
 * 根据视频时长动态计算各步骤的超时时间
 */
@Component
public class DynamicTimeoutCalculator {
    
    // 基础超时配置（秒）- 🚀 增加 LLM 相关超时
    private static final int BASE_DOWNLOAD_TIMEOUT = 60;
    private static final int BASE_TRANSCRIBE_TIMEOUT = 120;
    private static final int BASE_STAGE1_TIMEOUT = 300;   // 增加: 300s (LLM批量处理)
    private static final int BASE_PHASE2A_TIMEOUT = 600;  // 🚀 增加: 600s (语义分析+LLM)
    private static final int BASE_FFMPEG_TIMEOUT = 120;   // 增加: 120s (JavaCV批量处理)
    private static final int BASE_PHASE2B_TIMEOUT = 900;  // 🚀 增加: 900s (Vision AI验证)
    
    /**
     * 超时配置结果
     */
    public static class TimeoutConfig {
        private int downloadTimeoutSec;
        private int transcribeTimeoutSec;
        private int stage1TimeoutSec;
        private int phase2aTimeoutSec;
        private int ffmpegTimeoutSec;
        private int phase2bTimeoutSec;
        private int totalTimeoutSec;
        
        // Getters
        public int getDownloadTimeoutSec() { return downloadTimeoutSec; }
        public int getTranscribeTimeoutSec() { return transcribeTimeoutSec; }
        public int getStage1TimeoutSec() { return stage1TimeoutSec; }
        public int getPhase2aTimeoutSec() { return phase2aTimeoutSec; }
        public int getFfmpegTimeoutSec() { return ffmpegTimeoutSec; }
        public int getPhase2bTimeoutSec() { return phase2bTimeoutSec; }
        public int getTotalTimeoutSec() { return totalTimeoutSec; }
        
        // Setters
        public void setDownloadTimeoutSec(int v) { downloadTimeoutSec = v; }
        public void setTranscribeTimeoutSec(int v) { transcribeTimeoutSec = v; }
        public void setStage1TimeoutSec(int v) { stage1TimeoutSec = v; }
        public void setPhase2aTimeoutSec(int v) { phase2aTimeoutSec = v; }
        public void setFfmpegTimeoutSec(int v) { ffmpegTimeoutSec = v; }
        public void setPhase2bTimeoutSec(int v) { phase2bTimeoutSec = v; }
        public void setTotalTimeoutSec(int v) { totalTimeoutSec = v; }
        
        @Override
        public String toString() {
            return String.format(
                "TimeoutConfig{download=%ds, transcribe=%ds, stage1=%ds, phase2a=%ds, ffmpeg=%ds, phase2b=%ds, total=%ds}",
                downloadTimeoutSec, transcribeTimeoutSec, stage1TimeoutSec,
                phase2aTimeoutSec, ffmpegTimeoutSec, phase2bTimeoutSec, totalTimeoutSec
            );
        }
    }
    
    /**
     * 根据视频时长计算超时配置
     * 
     * @param videoDurationSec 视频时长（秒）
     * @return 超时配置
     */
    public TimeoutConfig calculateTimeouts(double videoDurationSec) {
        TimeoutConfig config = new TimeoutConfig();
        
        // 计算倍率（视频时长越长，超时越长）
        double multiplier = Math.max(1.0, videoDurationSec / 300.0); // 5分钟为基准
        multiplier = Math.min(multiplier, 5.0); // 最大5倍
        
        // 下载超时 = 基础 + 视频时长 * 0.5（考虑网络速度）
        config.setDownloadTimeoutSec(
            (int) (BASE_DOWNLOAD_TIMEOUT + videoDurationSec * 0.5)
        );
        
        // 转录超时 = 基础 * 倍率（Whisper处理时间约为视频时长的0.5-2倍）
        config.setTranscribeTimeoutSec(
            (int) (BASE_TRANSCRIBE_TIMEOUT * multiplier + videoDurationSec * 1.5)
        );
        
        // Stage1超时 = 基础 * 倍率
        config.setStage1TimeoutSec(
            (int) (BASE_STAGE1_TIMEOUT * multiplier)
        );
        
        // Phase2A超时 = 基础 * 倍率（语义分析）
        config.setPhase2aTimeoutSec(
            (int) (BASE_PHASE2A_TIMEOUT * multiplier)
        );
        
        // FFmpeg超时 = 基础 + (截图数量 + 视频片段数量预估) * 3秒 + 缓冲
        // 🚀 V2: 视频片段数量通常与截图相近，且每个片段耗时更长
        int estimatedScreenshots = (int) (videoDurationSec / 30); // 约每30秒一张
        int estimatedClips = (int) (videoDurationSec / 20);       // 约每20秒一个片段
        int estimatedTotal = estimatedScreenshots + estimatedClips;
        config.setFfmpegTimeoutSec(
            Math.max(BASE_FFMPEG_TIMEOUT * 2, estimatedTotal * 4 + 60) // 增加 2x 基础 + 4秒/项 + 60s缓冲
        );
        
        // Phase2B超时 = 基础 * 倍率（Vision AI验证 + Markdown增强）
        config.setPhase2bTimeoutSec(
            (int) (BASE_PHASE2B_TIMEOUT * multiplier)
        );
        
        // 总超时 = 所有步骤之和 * 1.2（留余量）
        int sum = config.getDownloadTimeoutSec() + config.getTranscribeTimeoutSec()
            + config.getStage1TimeoutSec() + config.getPhase2aTimeoutSec()
            + config.getFfmpegTimeoutSec() + config.getPhase2bTimeoutSec();
        config.setTotalTimeoutSec((int) (sum * 1.2));
        
        return config;
    }
    
    /**
     * 获取默认超时配置（5分钟视频）
     */
    public TimeoutConfig getDefaultTimeouts() {
        return calculateTimeouts(300);
    }
}
