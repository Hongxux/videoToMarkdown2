package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Authenticator;
import java.net.ConnectException;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DeepSeekAdvisorServiceRetryTest {

    @Test
    void shouldRetryWhenHttpTimeoutThenSucceedForStructuredAdvice() throws Exception {
        DeepSeekAdvisorService service = createConfiguredService();
        AtomicInteger sendCount = new AtomicInteger(0);
        StubHttpClient client = new StubHttpClient(
                sendCount,
                new HttpTimeoutException("request timed out"),
                new StringResponse(
                        200,
                        "{\"choices\":[{\"message\":{\"content\":\"{\\\"background\\\":[\\\"bg\\\"],\\\"contextual_explanations\\\":[\\\"ctx\\\"],\\\"depth\\\":[\\\"depth\\\"],\\\"breadth\\\":[\\\"breadth\\\"]}\"},\"finish_reason\":\"stop\"}]}"
                )
        );
        setField(service, "httpClient", client);

        DeepSeekAdvisorService.StructuredAdviceResult result =
                service.requestStructuredAdvice("Entropy", "context line", "example line", true);

        assertEquals(2, sendCount.get());
        assertEquals("deepseek", result.source);
        assertEquals(List.of("bg"), result.background);
        assertEquals(List.of("ctx"), result.contextualExplanations);
        assertEquals(List.of("depth"), result.depth);
        assertEquals(List.of("breadth"), result.breadth);
    }

    @Test
    void shouldRetryConnectExceptionAndFailAfterMaxAttempts() throws Exception {
        DeepSeekAdvisorService service = createConfiguredService();
        AtomicInteger sendCount = new AtomicInteger(0);
        StubHttpClient client = new StubHttpClient(
                sendCount,
                new ConnectException("connect timeout simulated"),
                new ConnectException("connect timeout simulated"),
                new ConnectException("connect timeout simulated")
        );
        setField(service, "httpClient", client);

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> service.requestStructuredAdvice("Entropy", "context line", "example line", true)
        );

        assertEquals(3, sendCount.get());
        assertEquals("DeepSeek structured advisor call failed: connect timeout simulated", error.getMessage());
        assertNotNull(error.getCause());
        assertEquals(ConnectException.class, error.getCause().getClass());
    }

    private DeepSeekAdvisorService createConfiguredService() throws Exception {
        DeepSeekAdvisorService service = new DeepSeekAdvisorService();
        setField(service, "advisorEnabled", true);
        setField(service, "advisorBaseUrl", "https://api.deepseek.com/v1");
        setField(service, "advisorModel", "deepseek-chat");
        setField(service, "timeoutSeconds", 240);
        setField(service, "connectTimeoutSeconds", 20);
        setField(service, "structuredMaxTokens", 600);
        setField(service, "apiKey", "test-key");
        return service;
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class StubHttpClient extends HttpClient {
        private final AtomicInteger sendCount;
        private final Deque<Object> scriptedResults = new ArrayDeque<>();

        private StubHttpClient(AtomicInteger sendCount, Object... results) {
            this.sendCount = sendCount;
            if (results != null) {
                for (Object result : results) {
                    scriptedResults.addLast(result);
                }
            }
        }

        @Override
        public Optional<CookieHandler> cookieHandler() {
            return Optional.empty();
        }

        @Override
        public Optional<Duration> connectTimeout() {
            return Optional.of(Duration.ofSeconds(20));
        }

        @Override
        public Redirect followRedirects() {
            return Redirect.NEVER;
        }

        @Override
        public Optional<ProxySelector> proxy() {
            return Optional.empty();
        }

        @Override
        public SSLContext sslContext() {
            return null;
        }

        @Override
        public SSLParameters sslParameters() {
            return new SSLParameters();
        }

        @Override
        public Optional<Authenticator> authenticator() {
            return Optional.empty();
        }

        @Override
        public Version version() {
            return Version.HTTP_1_1;
        }

        @Override
        public Optional<Executor> executor() {
            return Optional.empty();
        }

        @Override
        public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
                throws IOException, InterruptedException {
            sendCount.incrementAndGet();
            Object scripted = scriptedResults.isEmpty()
                    ? new StringResponse(200, "{\"choices\":[]}")
                    : scriptedResults.removeFirst();
            if (scripted instanceof HttpTimeoutException timeout) {
                throw timeout;
            }
            if (scripted instanceof ConnectException connect) {
                throw connect;
            }
            if (scripted instanceof IOException io) {
                throw io;
            }
            if (scripted instanceof InterruptedException interrupted) {
                throw interrupted;
            }
            if (scripted instanceof RuntimeException runtime) {
                throw runtime;
            }
            @SuppressWarnings("unchecked")
            HttpResponse<T> response = (HttpResponse<T>) scripted;
            return response;
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request,
                HttpResponse.BodyHandler<T> responseBodyHandler
        ) {
            throw new UnsupportedOperationException("sendAsync is not used");
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request,
                HttpResponse.BodyHandler<T> responseBodyHandler,
                HttpResponse.PushPromiseHandler<T> pushPromiseHandler
        ) {
            throw new UnsupportedOperationException("sendAsync is not used");
        }
    }

    private static final class StringResponse implements HttpResponse<String> {
        private static final HttpHeaders EMPTY_HEADERS = HttpHeaders.of(Map.of(), (name, value) -> true);
        private final int statusCode;
        private final String body;
        private final HttpRequest request;

        private StringResponse(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = body;
            this.request = HttpRequest.newBuilder(URI.create("https://mock.local/v1/chat/completions")).build();
        }

        @Override
        public int statusCode() {
            return statusCode;
        }

        @Override
        public HttpRequest request() {
            return request;
        }

        @Override
        public Optional<HttpResponse<String>> previousResponse() {
            return Optional.empty();
        }

        @Override
        public HttpHeaders headers() {
            return EMPTY_HEADERS;
        }

        @Override
        public String body() {
            return body;
        }

        @Override
        public Optional<SSLSession> sslSession() {
            return Optional.empty();
        }

        @Override
        public URI uri() {
            return request.uri();
        }

        @Override
        public HttpClient.Version version() {
            return HttpClient.Version.HTTP_1_1;
        }
    }
}
