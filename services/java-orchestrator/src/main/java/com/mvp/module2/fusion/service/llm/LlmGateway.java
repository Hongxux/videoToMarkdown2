package com.mvp.module2.fusion.service.llm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public final class LlmGateway {
    private static final Logger logger = LoggerFactory.getLogger(LlmGateway.class);

    private final LlmClient client;

    public LlmGateway(LlmClient client) {
        this.client = client;
    }

    public LlmGatewayResult execute(
            LlmPromptRequest request,
            LlmFallbackStrategy fallbackStrategy,
            LlmRetryPolicy retryPolicy,
            boolean streamRequested,
            Consumer<String> onDelta
    ) throws Exception {
        GatewayAttemptResult primaryResult = executeProvider(
                fallbackStrategy.primaryProvider,
                request,
                retryPolicy,
                streamRequested,
                new DeltaRelay(onDelta)
        );
        if (primaryResult.isSuccess()) {
            return primaryResult.toGatewayResult(false);
        }
        if (!fallbackStrategy.hasFallback()) {
            throw buildGatewayFailure(streamRequested, primaryResult, null);
        }
        boolean fallbackStreamEnabled = streamRequested && !primaryResult.deltaRelay.hasEmittedDelta();
        logger.warn(
                "[llm-degrade] Primary provider failed, switching to fallback: request_mode={} primary_provider={} primary_model={} primary_base_url={} attempts={} fallback_provider={} fallback_model={} fallback_base_url={} fallback_stream={} error={}",
                streamRequested ? "stream" : "sync",
                primaryResult.provider.resolveProviderKey(),
                primaryResult.provider.model,
                primaryResult.provider.baseUrl,
                primaryResult.attemptCount,
                fallbackStrategy.fallbackProvider.resolveProviderKey(),
                fallbackStrategy.fallbackProvider.model,
                fallbackStrategy.fallbackProvider.baseUrl,
                fallbackStreamEnabled,
                describeThrowable(primaryResult.error)
        );
        DeltaRelay fallbackRelay = fallbackStreamEnabled ? primaryResult.deltaRelay : DeltaRelay.disabled();
        GatewayAttemptResult fallbackResult = executeProvider(
                fallbackStrategy.fallbackProvider,
                request,
                retryPolicy,
                fallbackStreamEnabled,
                fallbackRelay
        );
        if (fallbackResult.isSuccess()) {
            logger.warn(
                    "[llm-degrade] Request completed via fallback provider: request_mode={} provider={} model={} base_url={} attempts={}",
                    streamRequested ? "stream" : "sync",
                    fallbackStrategy.fallbackProvider.resolveProviderKey(),
                    fallbackStrategy.fallbackProvider.model,
                    fallbackStrategy.fallbackProvider.baseUrl,
                    fallbackResult.attemptCount
            );
            return fallbackResult.toGatewayResult(true);
        }
        throw buildGatewayFailure(streamRequested, primaryResult, fallbackResult);
    }

    private GatewayAttemptResult executeProvider(
            LlmProviderConfig provider,
            LlmPromptRequest request,
            LlmRetryPolicy retryPolicy,
            boolean streamEnabled,
            DeltaRelay deltaRelay
    ) {
        int totalAttempts = retryPolicy.totalAttempts();
        boolean currentStreamMode = streamEnabled;
        boolean partialStreamObserved = false;
        for (int attemptIndex = 0; attemptIndex < totalAttempts; attemptIndex += 1) {
            try {
                LlmResponse response = currentStreamMode
                        ? client.stream(provider, request, deltaRelay.consumer())
                        : client.complete(provider, request);
                String content = String.valueOf(response.content == null ? "" : response.content).trim();
                if (!StringUtils.hasText(content)) {
                    throw new IllegalStateException(provider.resolveDisplayName() + " returned empty");
                }
                return GatewayAttemptResult.success(provider, response, content, attemptIndex + 1, partialStreamObserved, deltaRelay);
            } catch (Exception ex) {
                boolean retryable = retryPolicy.shouldRetry(ex);
                boolean deltaAlreadyEmitted = currentStreamMode && deltaRelay.hasEmittedDelta();
                partialStreamObserved = partialStreamObserved || deltaAlreadyEmitted;
                if (deltaAlreadyEmitted) {
                    currentStreamMode = false;
                    logger.warn(
                            "[llm-retry] Stream failed after delta emission, switching subsequent retries to buffered mode: provider={} attempt={} error={}",
                            provider.resolveProviderKey(),
                            attemptIndex + 1,
                            describeThrowable(ex)
                    );
                }
                if (!retryable || attemptIndex >= totalAttempts - 1) {
                    return GatewayAttemptResult.failure(
                            provider,
                            ex,
                            attemptIndex + 1,
                            partialStreamObserved,
                            retryable,
                            deltaRelay
                    );
                }
                long delayMs = retryPolicy.computeDelayMs(attemptIndex);
                logger.warn(
                        "[llm-retry] Provider request failed, retry {}/{} scheduled: provider={} model={} base_url={} mode={} delayMs={} error={}",
                        attemptIndex + 1,
                        retryPolicy.maxRetries(),
                        provider.resolveProviderKey(),
                        provider.model,
                        provider.baseUrl,
                        currentStreamMode ? "stream" : "buffered",
                        delayMs,
                        describeThrowable(ex)
                );
                sleepBeforeRetry(delayMs, provider, ex);
            }
        }
        return GatewayAttemptResult.failure(
                provider,
                new IllegalStateException(provider.resolveDisplayName() + " provider exhausted without result"),
                totalAttempts,
                partialStreamObserved,
                false,
                deltaRelay
        );
    }

    private void sleepBeforeRetry(long delayMs, LlmProviderConfig provider, Exception ex) {
        if (delayMs <= 0L) {
            return;
        }
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw buildGatewayFailure(
                    true,
                    GatewayAttemptResult.failure(provider, interrupted, 1, false, false, DeltaRelay.disabled()),
                    null
            );
        }
    }

    private IllegalStateException buildGatewayFailure(
            boolean streamRequested,
            GatewayAttemptResult primaryFailure,
            GatewayAttemptResult fallbackFailure
    ) {
        StringBuilder message = new StringBuilder();
        message.append("LLM ")
                .append(streamRequested ? "stream" : "sync")
                .append(" provider chain failed");
        if (primaryFailure != null) {
            message.append("; primary=").append(describeAttempt(primaryFailure));
        }
        if (fallbackFailure != null) {
            message.append("; fallback=").append(describeAttempt(fallbackFailure));
        }
        Throwable cause = fallbackFailure != null && fallbackFailure.error != null
                ? fallbackFailure.error
                : (primaryFailure != null ? primaryFailure.error : null);
        return new IllegalStateException(message.toString(), cause);
    }

    private String describeAttempt(GatewayAttemptResult attempt) {
        if (attempt == null || attempt.provider == null) {
            return "unknown";
        }
        return attempt.provider.resolveProviderKey()
                + "@attempts=" + attempt.attemptCount
                + "@partial_stream=" + attempt.partialStreamObserved
                + "@retryable=" + attempt.retryableFailure
                + "@model=" + String.valueOf(attempt.provider.model)
                + "@base_url=" + String.valueOf(attempt.provider.baseUrl)
                + "@error=" + describeThrowable(attempt.error);
    }

    private String describeThrowable(Throwable ex) {
        if (ex == null) {
            return "";
        }
        List<String> parts = new ArrayList<>();
        Throwable cursor = ex;
        int depth = 0;
        while (cursor != null && depth < 4) {
            String part = cursor.getClass().getSimpleName();
            String message = String.valueOf(cursor.getMessage() == null ? "" : cursor.getMessage()).trim();
            if (StringUtils.hasText(message)) {
                part += ": " + message;
            }
            parts.add(part);
            cursor = cursor.getCause();
            depth += 1;
        }
        return String.join(" -> ", parts);
    }

    private static final class GatewayAttemptResult {
        private final LlmProviderConfig provider;
        private final LlmResponse response;
        private final String content;
        private final Exception error;
        private final int attemptCount;
        private final boolean partialStreamObserved;
        private final boolean retryableFailure;
        private final DeltaRelay deltaRelay;

        private GatewayAttemptResult(
                LlmProviderConfig provider,
                LlmResponse response,
                String content,
                Exception error,
                int attemptCount,
                boolean partialStreamObserved,
                boolean retryableFailure,
                DeltaRelay deltaRelay
        ) {
            this.provider = provider;
            this.response = response;
            this.content = String.valueOf(content == null ? "" : content).trim();
            this.error = error;
            this.attemptCount = Math.max(0, attemptCount);
            this.partialStreamObserved = partialStreamObserved;
            this.retryableFailure = retryableFailure;
            this.deltaRelay = deltaRelay != null ? deltaRelay : DeltaRelay.disabled();
        }

        private boolean isSuccess() {
            return error == null && StringUtils.hasText(content);
        }

        private LlmGatewayResult toGatewayResult(boolean degraded) {
            return new LlmGatewayResult(content, provider, response, degraded, partialStreamObserved, attemptCount);
        }

        private static GatewayAttemptResult success(
                LlmProviderConfig provider,
                LlmResponse response,
                String content,
                int attemptCount,
                boolean partialStreamObserved,
                DeltaRelay deltaRelay
        ) {
            return new GatewayAttemptResult(provider, response, content, null, attemptCount, partialStreamObserved, false, deltaRelay);
        }

        private static GatewayAttemptResult failure(
                LlmProviderConfig provider,
                Exception error,
                int attemptCount,
                boolean partialStreamObserved,
                boolean retryableFailure,
                DeltaRelay deltaRelay
        ) {
            return new GatewayAttemptResult(provider, null, "", error, attemptCount, partialStreamObserved, retryableFailure, deltaRelay);
        }
    }

    private static final class DeltaRelay {
        private final Consumer<String> delegate;
        private volatile boolean emittedDelta;

        private DeltaRelay(Consumer<String> delegate) {
            this.delegate = delegate;
        }

        private Consumer<String> consumer() {
            if (delegate == null) {
                return null;
            }
            return (delta) -> {
                String safeDelta = String.valueOf(delta == null ? "" : delta);
                if (!safeDelta.isEmpty()) {
                    emittedDelta = true;
                }
                try {
                    delegate.accept(safeDelta);
                } catch (Exception callbackError) {
                    logger.warn("[llm-stream] Ignore delta callback failure: {}", String.valueOf(callbackError.getMessage() == null ? "" : callbackError.getMessage()));
                }
            };
        }

        private boolean hasEmittedDelta() {
            return emittedDelta;
        }

        private static DeltaRelay disabled() {
            return new DeltaRelay(null);
        }
    }
}
