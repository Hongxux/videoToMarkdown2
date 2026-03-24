package com.mvp.module2.fusion.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 重试策略
 * 
 * 支持：
 * 1. 固定间隔重试
 * 2. 指数退避重试
 * 3. 可配置最大重试次数
 */
@Component
public class RetryPolicy {
    
    private static final Logger logger = LoggerFactory.getLogger(RetryPolicy.class);
    
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(2);
    
    /**
     * 重试配置
     */
    public static class RetryConfig {
        public int maxRetries = 3;
        public Duration initialDelay = Duration.ofSeconds(1);
        public Duration maxDelay = Duration.ofSeconds(30);
        public double backoffMultiplier = 2.0;
        public boolean exponentialBackoff = true;
        
        public static RetryConfig defaultConfig() {
            return new RetryConfig();
        }
        
        public RetryConfig withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }
        
        public RetryConfig withInitialDelay(Duration initialDelay) {
            this.initialDelay = initialDelay;
            return this;
        }
        
        public RetryConfig withExponentialBackoff(boolean enabled) {
            this.exponentialBackoff = enabled;
            return this;
        }
    }
    
    /**
     * 带重试的异步执行
     */
    public <T> CompletableFuture<T> executeWithRetry(
            Supplier<CompletableFuture<T>> supplier,
            RetryConfig config,
            String operationName
    ) {
        return executeWithRetryInternal(supplier, config, operationName, 0);
    }
    
    /**
     * 带重试的异步执行 (使用默认配置)
     */
    public <T> CompletableFuture<T> executeWithRetry(
            Supplier<CompletableFuture<T>> supplier,
            String operationName
    ) {
        return executeWithRetry(supplier, RetryConfig.defaultConfig(), operationName);
    }
    
    private <T> CompletableFuture<T> executeWithRetryInternal(
            Supplier<CompletableFuture<T>> supplier,
            RetryConfig config,
            String operationName,
            int attempt
    ) {
        return supplier.get()
            .exceptionallyCompose(error -> {
                if (attempt >= config.maxRetries) {
                    logger.error("[Retry] {} - Max retries ({}) exceeded, giving up", 
                        operationName, config.maxRetries);
                    return CompletableFuture.failedFuture(error);
                }
                
                // 计算延迟
                long delayMs = calculateDelay(config, attempt);
                
                logger.warn("[Retry] {} - Attempt {} failed, retrying in {}ms: {}", 
                    operationName, attempt + 1, delayMs, error.getMessage());
                
                // 延迟后重试
                CompletableFuture<T> retryFuture = new CompletableFuture<>();
                
                scheduler.schedule(() -> {
                    executeWithRetryInternal(supplier, config, operationName, attempt + 1)
                        .whenComplete((result, retryError) -> {
                            if (retryError != null) {
                                retryFuture.completeExceptionally(retryError);
                            } else {
                                retryFuture.complete(result);
                            }
                        });
                }, delayMs, TimeUnit.MILLISECONDS);
                
                return retryFuture;
            });
    }
    
    /**
     * 计算重试延迟
     */
    private long calculateDelay(RetryConfig config, int attempt) {
        if (!config.exponentialBackoff) {
            return config.initialDelay.toMillis();
        }
        
        // 指数退避
        double delay = config.initialDelay.toMillis() * Math.pow(config.backoffMultiplier, attempt);
        
        // 添加随机抖动 (±10%)
        double jitter = delay * 0.1 * (Math.random() * 2 - 1);
        delay += jitter;
        
        // 限制最大延迟
        return Math.min((long) delay, config.maxDelay.toMillis());
    }
    
    /**
     * 关闭调度器
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
    }
}
