package com.mvp.module2.fusion.resilience;

import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.service.DynamicTimeoutCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * 弹性 gRPC 客户端
 * 
 * 整合：
 * 1. 熔断器 (Circuit Breaker)
 * 2. 重试策略 (Retry Policy)
 * 3. 动态超时 (Dynamic Timeout)
 */
@Service
public class ResilientGrpcClient {
    
    private static final Logger logger = LoggerFactory.getLogger(ResilientGrpcClient.class);
    
    @Autowired
    private PythonGrpcClient grpcClient;
    
    @Autowired
    private CircuitBreaker circuitBreaker;
    
    @Autowired
    private RetryPolicy retryPolicy;
    
    @Autowired
    private DynamicTimeoutCalculator timeoutCalculator;
    
    /**
     * 弹性执行 Stage1 处理
     */
    public CompletableFuture<PythonGrpcClient.Stage1Result> processStage1Resilient(
            String taskId, String videoPath, String subtitlePath,
            String outputDir, int maxStep, double videoDurationSec) {
        
        // 计算动态超时
        DynamicTimeoutCalculator.TimeoutConfig timeouts = 
            timeoutCalculator.calculateTimeouts(videoDurationSec);
        int timeoutSec = timeouts.getStage1TimeoutSec();
        
        // 配置重试
        RetryPolicy.RetryConfig retryConfig = RetryPolicy.RetryConfig.defaultConfig()
            .withMaxRetries(2)
            .withInitialDelay(Duration.ofSeconds(5));
        
        // 执行（熔断器 + 重试）
        return circuitBreaker.execute(
            () -> retryPolicy.executeWithRetry(
                () -> grpcClient.processStage1Async(taskId, videoPath, subtitlePath, outputDir, maxStep, timeoutSec),
                retryConfig,
                "ProcessStage1"
            ),
            "ProcessStage1"
        );
    }
    
    /**
     * 弹性执行 Phase2A 语义分析
     */
    public CompletableFuture<PythonGrpcClient.AnalyzeResult> analyzeSemanticUnitsResilient(
            String taskId, String videoPath, String step2JsonPath,
            String step6JsonPath, String sentenceTimestampsPath,
            String outputDir, double videoDurationSec) {
        
        DynamicTimeoutCalculator.TimeoutConfig timeouts = 
            timeoutCalculator.calculateTimeouts(videoDurationSec);
        int timeoutSec = timeouts.getPhase2aTimeoutSec();
        
        RetryPolicy.RetryConfig retryConfig = RetryPolicy.RetryConfig.defaultConfig()
            .withMaxRetries(2)
            .withInitialDelay(Duration.ofSeconds(3));
        
        return circuitBreaker.execute(
            () -> retryPolicy.executeWithRetry(
                () -> grpcClient.analyzeSemanticUnitsAsync(
                    taskId, videoPath, step2JsonPath, step6JsonPath,
                    sentenceTimestampsPath, outputDir, timeoutSec
                ),
                retryConfig,
                "AnalyzeSemanticUnits"
            ),
            "AnalyzeSemanticUnits"
        );
    }
    
    /**
     * 弹性执行 Phase2B 富文本组装
     */
    public CompletableFuture<PythonGrpcClient.AssembleResult> assembleRichTextResilient(
            String taskId, String videoPath, String semanticUnitsJsonPath,
            String screenshotsDir, String clipsDir, String outputDir,
            String title, double videoDurationSec) {
        
        DynamicTimeoutCalculator.TimeoutConfig timeouts = 
            timeoutCalculator.calculateTimeouts(videoDurationSec);
        int timeoutSec = timeouts.getPhase2bTimeoutSec();
        
        RetryPolicy.RetryConfig retryConfig = RetryPolicy.RetryConfig.defaultConfig()
            .withMaxRetries(2)
            .withInitialDelay(Duration.ofSeconds(3));
        
        return circuitBreaker.execute(
            () -> retryPolicy.executeWithRetry(
                () -> grpcClient.assembleRichTextAsync(
                    taskId, videoPath, semanticUnitsJsonPath,
                    screenshotsDir, clipsDir, outputDir, title, timeoutSec
                ),
                retryConfig,
                "AssembleRichText"
            ),
            "AssembleRichText"
        );
    }
    
    /**
     * 检查服务健康状态
     */
    public boolean isHealthy() {
        return circuitBreaker.getState() != CircuitBreaker.State.OPEN
            && grpcClient.healthCheck();
    }
    
    /**
     * 获取熔断器状态
     */
    public CircuitBreaker.State getCircuitBreakerState() {
        return circuitBreaker.getState();
    }
    
    /**
     * 手动重置熔断器
     */
    public void resetCircuitBreaker() {
        circuitBreaker.reset();
    }
}
