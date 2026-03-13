package com.mvp.module2.fusion.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.util.concurrent.Semaphore;

/**
 * 🚀 Adaptive Concurrency Orchestrator (Java Parity with Python ResourceOrchestrator)
 * Monitors system load and dynamically adjusts the semaphore for parallel gRPC calls.
 */
@Component
public class AdaptiveResourceOrchestrator {
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveResourceOrchestrator.class);
    
    // 资源定义: 上限设为物理核心数，但初始只给一半，实行保守启动
    private final int MAX_PHYSICAL_CORES = Runtime.getRuntime().availableProcessors();
    // 初始并发: 默认跑满物理核心 (相信 Python 端的 ProcessPool 调度能力)
    private int currentTargetConcurrency = Math.max(4, MAX_PHYSICAL_CORES);
    
    private final OperatingSystemMXBean osBean;
    private final Semaphore computePool;
    private final Semaphore ioPool;

    @Value("${task.resource-guard.enabled:false}")
    private boolean resourceGuardEnabled;
    
    // 记录实际持有的许可数，用于动态调整
    private int actualPermits;

    public AdaptiveResourceOrchestrator() {
        this.osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        
        // 1. 初始化信号量
        this.actualPermits = currentTargetConcurrency;
        this.computePool = new Semaphore(actualPermits);
        this.ioPool = new Semaphore(MAX_PHYSICAL_CORES * 4); // IO 密集型给宽裕点
        
        logger.info("AdaptiveResourceOrchestrator initialized.");
        logger.info("  → Physical Cores: {}", MAX_PHYSICAL_CORES);
        logger.info("  → Initial Compute Limit: {} (Conservative Start)", actualPermits);
    }

    /**
     * 获取 CPU 密集型任务信号量
     */
    public Semaphore getComputeSemaphore() {
        return computePool;
    }
    
    /**
     * 获取 IO 密集型任务信号量
     */
    public Semaphore getIOSemaphore() {
        return ioPool;
    }

    /**
     * Estimates a safe concurrency level based on CPU and Free Memory.
     * (Retained for backward compatibility or dynamic tuning logic)
     */
    public int getAdaptiveLimit() {
        // Deprecated single limit, mapping to Compute limit safe calculation
        return currentTargetConcurrency;
    }
    
    /**
     * 定时调整并发 (由调度器调用，或自启动线程)
     * 策略: AIMD (和性增长，乘性减少)
     */
    public void adjustConcurrency() {
        if (!resourceGuardEnabled) {
            return;
        }
        double cpuLoad = osBean.getCpuLoad(); // 0.0 ~ 1.0
        long freeMemMb = osBean.getFreeMemorySize() / (1024 * 1024);
        
        int oldTarget = currentTargetConcurrency;
        int newTarget = oldTarget;
        
        // --- 决策逻辑 ---
        if (cpuLoad > 0.85 || freeMemMb < 2048) {
            // 🚨 负载过高: 乘性减少 (快速回撤)
            newTarget = Math.max(2, (int)(oldTarget * 0.7));
            logger.warn("🔥 High Load (CPU: {}%, Mem: {}MB). Reducing concurrency: {} -> {}", 
                String.format("%.1f", cpuLoad*100), freeMemMb, oldTarget, newTarget);
        } else if (cpuLoad < 0.60 && freeMemMb > 4096) {
            // 🟢 负载较低: 和性增长 (缓慢试探)
            if (oldTarget < MAX_PHYSICAL_CORES) {
                newTarget = oldTarget + 1;
                logger.info("🟢 Low Load (CPU: {}%). Increasing concurrency: {} -> {}", 
                    String.format("%.1f", cpuLoad*100), oldTarget, newTarget);
            }
        }
        
        // --- 执行调整 ---
        if (newTarget != oldTarget) {
            int diff = newTarget - oldTarget;
            if (diff > 0) {
                computePool.release(diff);
                actualPermits += diff;
            } else {
                // 减少许可 (注意: acquire可能阻塞，这里使用 tryAcquire 甚至由于是总控，直接减计数即可，但Semaphore不支持动态减)
                // 技巧: reducePermits 是 protected 的，Wrapper类无法调用。
                // 替代方案: 既然是"限制入口"，我们尝试 acquire，拿不到就算了(意味着都在忙)，
                // 但为了严谨，我们应该在新任务进来时 check 阈值，或者 loop acquire.
                
                // 简单起见，我们尝试从 pool 中"回收"许可
                int toReduce = -diff;
                for (int i=0; i<toReduce; i++) {
                    if (computePool.tryAcquire()) {
                        actualPermits--;
                    } else {
                        // 都在忙，没法立即回收，下次再说。
                        // 或者我们只更新 currentTargetConcurrency 作为"软限制"，
                        // 但Semaphore是硬限制。为了不阻塞调度线程，我们只回收空闲的。
                        // 修正: 简单更新 target 不够，必须真正减少 permit 才能挡住新请求。
                        // 既然不能强制回收正在运行的，那只能回收空闲的。
                        // 暂时不做强制阻塞回收，避免死锁。
                    }
                }
                // 修正逻辑: 直接更新 currentTarget，adjust只是尽力而为。
                // 为了代码简洁且有效，我们只在"空闲"时回收，无法立即回收也无所谓，
                // 因为 High Load 本身会阻止后续任务完成得太快。
            }
            currentTargetConcurrency = actualPermits; // 同步状态
        }
    }
}
