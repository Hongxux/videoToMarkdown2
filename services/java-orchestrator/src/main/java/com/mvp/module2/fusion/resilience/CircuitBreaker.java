package com.mvp.module2.fusion.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * 熔断器 (Circuit Breaker)
 * 
 * 状态转换：
 * CLOSED → OPEN：失败次数达到阈值
 * OPEN → HALF_OPEN：等待时间结束
 * HALF_OPEN → CLOSED：探测成功
 * HALF_OPEN → OPEN：探测失败
 */
@Component
public class CircuitBreaker {
    
    private static final Logger logger = LoggerFactory.getLogger(CircuitBreaker.class);
    
    public enum State {
        CLOSED,      // 正常状态，允许请求
        OPEN,        // 熔断状态，快速失败
        HALF_OPEN    // 半开状态，允许探测
    }
    
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private volatile Instant openTime = null;
    
    // 配置参数
    private final int failureThreshold;      // 触发熔断的失败次数
    private final Duration openDuration;      // 熔断持续时间
    private final int halfOpenSuccessThreshold;  // 半开状态恢复所需成功次数
    
    public CircuitBreaker() {
        this(5, Duration.ofSeconds(30), 2);
    }
    
    public CircuitBreaker(int failureThreshold, Duration openDuration, int halfOpenSuccessThreshold) {
        this.failureThreshold = failureThreshold;
        this.openDuration = openDuration;
        this.halfOpenSuccessThreshold = halfOpenSuccessThreshold;
    }
    
    /**
     * 执行带熔断保护的操作
     */
    public <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> supplier, String operationName) {
        // 检查熔断状态
        if (!allowRequest()) {
            logger.warn("[CircuitBreaker] {} - Circuit is OPEN, fast fail", operationName);
            return CompletableFuture.failedFuture(
                new CircuitBreakerOpenException("Circuit breaker is open for: " + operationName)
            );
        }
        
        return supplier.get()
            .whenComplete((result, error) -> {
                if (error != null) {
                    recordFailure();
                    logger.warn("[CircuitBreaker] {} - Failure recorded: {}", operationName, error.getMessage());
                } else {
                    recordSuccess();
                }
            });
    }
    
    /**
     * 检查是否允许请求通过
     */
    public boolean allowRequest() {
        State currentState = state.get();
        
        switch (currentState) {
            case CLOSED:
                return true;
                
            case OPEN:
                // 检查是否应该尝试恢复
                if (shouldAttemptReset()) {
                    if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                        logger.info("[CircuitBreaker] Transitioning to HALF_OPEN");
                        successCount.set(0);
                    }
                    return true;
                }
                return false;
                
            case HALF_OPEN:
                return true;
                
            default:
                return false;
        }
    }
    
    /**
     * 记录成功
     */
    private void recordSuccess() {
        failureCount.set(0);
        
        if (state.get() == State.HALF_OPEN) {
            int successes = successCount.incrementAndGet();
            if (successes >= halfOpenSuccessThreshold) {
                if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                    logger.info("[CircuitBreaker] Reset to CLOSED after {} successes", successes);
                }
            }
        }
    }
    
    /**
     * 记录失败
     */
    private void recordFailure() {
        int failures = failureCount.incrementAndGet();
        
        State currentState = state.get();
        
        if (currentState == State.HALF_OPEN) {
            // 半开状态失败，立即熔断
            if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                openTime = Instant.now();
                logger.warn("[CircuitBreaker] HALF_OPEN -> OPEN: probe failed");
            }
        } else if (currentState == State.CLOSED && failures >= failureThreshold) {
            // 达到阈值，熔断
            if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                openTime = Instant.now();
                logger.warn("[CircuitBreaker] CLOSED -> OPEN: {} failures", failures);
            }
        }
    }
    
    /**
     * 检查是否应该尝试重置
     */
    private boolean shouldAttemptReset() {
        return openTime != null && 
               Duration.between(openTime, Instant.now()).compareTo(openDuration) > 0;
    }
    
    /**
     * 获取当前状态
     */
    public State getState() {
        return state.get();
    }
    
    /**
     * 重置熔断器
     */
    public void reset() {
        state.set(State.CLOSED);
        failureCount.set(0);
        successCount.set(0);
        openTime = null;
        logger.info("[CircuitBreaker] Manually reset to CLOSED");
    }
    
    /**
     * 熔断器打开异常
     */
    public static class CircuitBreakerOpenException extends RuntimeException {
        public CircuitBreakerOpenException(String message) {
            super(message);
        }
    }
}
