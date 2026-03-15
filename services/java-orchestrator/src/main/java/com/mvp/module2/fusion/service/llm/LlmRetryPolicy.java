package com.mvp.module2.fusion.service.llm;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

public final class LlmRetryPolicy {
    private final int maxRetries;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final double jitterRatio;
    private final Predicate<Throwable> retryablePredicate;

    public LlmRetryPolicy(
            int maxRetries,
            long initialBackoffMs,
            long maxBackoffMs,
            double jitterRatio,
            Predicate<Throwable> retryablePredicate
    ) {
        this.maxRetries = Math.max(0, maxRetries);
        this.initialBackoffMs = Math.max(0L, initialBackoffMs);
        this.maxBackoffMs = Math.max(this.initialBackoffMs, maxBackoffMs);
        this.jitterRatio = Math.max(0d, jitterRatio);
        this.retryablePredicate = retryablePredicate != null ? retryablePredicate : (error) -> false;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public int totalAttempts() {
        return Math.max(1, maxRetries + 1);
    }

    public boolean shouldRetry(Throwable error) {
        return retryablePredicate.test(error);
    }

    public long computeDelayMs(int retryIndex) {
        if (initialBackoffMs <= 0L) {
            return 0L;
        }
        int boundedRetryIndex = Math.max(0, retryIndex);
        long multiplier = 1L << Math.min(20, boundedRetryIndex);
        long baseDelay = initialBackoffMs * multiplier;
        if (baseDelay < 0L || baseDelay > maxBackoffMs) {
            baseDelay = maxBackoffMs;
        }
        long jitterBound = Math.round(baseDelay * jitterRatio);
        if (jitterBound <= 0L) {
            return baseDelay;
        }
        return baseDelay + ThreadLocalRandom.current().nextLong(jitterBound + 1L);
    }
}
