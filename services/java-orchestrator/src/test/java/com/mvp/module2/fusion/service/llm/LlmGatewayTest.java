package com.mvp.module2.fusion.service.llm;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LlmGatewayTest {

    @Test
    void shouldFallbackAfterPrimaryRetriesExhausted() throws Exception {
        AtomicInteger primaryCompleteCalls = new AtomicInteger(0);
        AtomicInteger fallbackCompleteCalls = new AtomicInteger(0);
        LlmClient client = new LlmClient() {
            @Override
            public LlmResponse complete(LlmProviderConfig provider, LlmPromptRequest request) throws Exception {
                if ("deepseek".equals(provider.resolveProviderKey())) {
                    primaryCompleteCalls.incrementAndGet();
                    throw new IOException("closed");
                }
                fallbackCompleteCalls.incrementAndGet();
                return new LlmResponse("fallback-result", "{\"provider\":\"qwen\"}", "{\"ok\":true}", "stop");
            }

            @Override
            public LlmResponse stream(LlmProviderConfig provider, LlmPromptRequest request, java.util.function.Consumer<String> onDelta) {
                throw new UnsupportedOperationException("stream is not used");
            }
        };

        LlmGateway gateway = new LlmGateway(client);
        LlmGatewayResult result = gateway.execute(
                new LlmPromptRequest("system", "user", 0.2, 512, false),
                new LlmFallbackStrategy(provider("deepseek"), provider("qwen")),
                new LlmRetryPolicy(3, 0L, 0L, 0d, (error) -> error instanceof IOException),
                false,
                null
        );

        assertEquals("fallback-result", result.content);
        assertEquals("qwen", result.provider.resolveProviderKey());
        assertTrue(result.degraded);
        assertEquals(4, primaryCompleteCalls.get());
        assertEquals(1, fallbackCompleteCalls.get());
    }

    @Test
    void shouldSwitchToBufferedFallbackAfterPartialStreamFailure() throws Exception {
        AtomicInteger primaryStreamCalls = new AtomicInteger(0);
        AtomicInteger fallbackCompleteCalls = new AtomicInteger(0);
        AtomicInteger fallbackStreamCalls = new AtomicInteger(0);
        List<String> deltas = new ArrayList<>();
        LlmClient client = new LlmClient() {
            @Override
            public LlmResponse complete(LlmProviderConfig provider, LlmPromptRequest request) {
                if ("qwen".equals(provider.resolveProviderKey())) {
                    fallbackCompleteCalls.incrementAndGet();
                    return new LlmResponse("recovered-result", "{\"provider\":\"qwen\"}", "{\"ok\":true}", "stop");
                }
                throw new IllegalStateException("unexpected complete call");
            }

            @Override
            public LlmResponse stream(LlmProviderConfig provider, LlmPromptRequest request, java.util.function.Consumer<String> onDelta) throws Exception {
                if ("deepseek".equals(provider.resolveProviderKey())) {
                    primaryStreamCalls.incrementAndGet();
                    onDelta.accept("partial-delta");
                    throw new IOException("closed");
                }
                fallbackStreamCalls.incrementAndGet();
                throw new IllegalStateException("fallback stream should be disabled after partial delta");
            }
        };

        LlmGateway gateway = new LlmGateway(client);
        LlmGatewayResult result = gateway.execute(
                new LlmPromptRequest("system", "user", 0.2, 512, false),
                new LlmFallbackStrategy(provider("deepseek"), provider("qwen")),
                new LlmRetryPolicy(0, 0L, 0L, 0d, (error) -> error instanceof IOException),
                true,
                deltas::add
        );

        assertEquals("recovered-result", result.content);
        assertEquals("qwen", result.provider.resolveProviderKey());
        assertTrue(result.degraded);
        assertEquals(List.of("partial-delta"), deltas);
        assertEquals(1, primaryStreamCalls.get());
        assertEquals(1, fallbackCompleteCalls.get());
        assertEquals(0, fallbackStreamCalls.get());
    }

    private LlmProviderConfig provider(String providerKey) {
        return new LlmProviderConfig(
                providerKey,
                providerKey,
                "https://example.com/v1",
                providerKey + "-model",
                "test-key",
                UnaryOperator.identity()
        );
    }
}
