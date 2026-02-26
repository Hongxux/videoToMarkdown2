package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.service.CardStorageService;
import com.mvp.module2.fusion.service.DeepSeekAdvisorService;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MobileCardControllerSyncGenerationTest {

    @Test
    void getCardShouldGenerateSynchronouslyWhenMissing() throws Exception {
        MobileCardController controller = new MobileCardController();
        InMemoryCardStorageService cardStorage = new InMemoryCardStorageService();
        StubDeepSeekAdvisorService advisor = new StubDeepSeekAdvisorService(
                DeepSeekAdvisorService.StructuredAdviceResult.deepseek(
                        List.of("Context line"),
                        List.of("Why it matters"),
                        List.of("Cross-domain hint"),
                        "{\"ok\":true}"
                )
        );
        injectField(controller, "cardStorageService", cardStorage);
        injectField(controller, "deepSeekAdvisorService", advisor);

        ResponseEntity<?> response = controller.getCardByTitle("entropy");
        assertEquals(200, response.getStatusCode().value());
        assertEquals(1, advisor.structuredCalls.get());

        Object body = response.getBody();
        assertTrue(body instanceof Map);
        Map<?, ?> payload = (Map<?, ?>) body;
        assertEquals("entropy", String.valueOf(payload.get("title")).toLowerCase());
        assertTrue(String.valueOf(payload.get("markdown")).contains("First Principles"));
    }

    @Test
    void getCardShouldReuseExistingWithoutTriggeringGeneration() throws Exception {
        MobileCardController controller = new MobileCardController();
        InMemoryCardStorageService cardStorage = new InMemoryCardStorageService();
        cardStorage.markdowns.put("entropy", "## entropy\n\nexisting");
        StubDeepSeekAdvisorService advisor = new StubDeepSeekAdvisorService(
                DeepSeekAdvisorService.StructuredAdviceResult.deepseek(
                        List.of("unused"),
                        List.of("unused"),
                        List.of("unused"),
                        "{\"ok\":true}"
                )
        );
        injectField(controller, "cardStorageService", cardStorage);
        injectField(controller, "deepSeekAdvisorService", advisor);

        ResponseEntity<?> response = controller.getCardByTitle("entropy");
        assertEquals(200, response.getStatusCode().value());
        assertEquals(0, advisor.structuredCalls.get());
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class StubDeepSeekAdvisorService extends DeepSeekAdvisorService {
        private final StructuredAdviceResult structuredAdviceResult;
        private final AtomicInteger structuredCalls = new AtomicInteger(0);

        private StubDeepSeekAdvisorService(StructuredAdviceResult structuredAdviceResult) {
            this.structuredAdviceResult = structuredAdviceResult;
        }

        @Override
        public StructuredAdviceResult requestStructuredAdvice(
                String term,
                String context,
                String contextExample,
                boolean contextDependent
        ) {
            structuredCalls.incrementAndGet();
            return structuredAdviceResult;
        }
    }

    private static class InMemoryCardStorageService extends CardStorageService {
        private final Map<String, String> markdowns = new ConcurrentHashMap<>();

        @Override
        public CardReadResult readCard(String rawTitle) {
            String key = String.valueOf(rawTitle == null ? "" : rawTitle).trim().toLowerCase();
            String markdown = markdowns.get(key);
            Path path = Paths.get("var/cards/" + key + ".md");
            if (markdown == null) {
                return new CardReadResult(rawTitle, "", path, false, "", "concept", List.of(), List.of());
            }
            return new CardReadResult(rawTitle, markdown, path, true, "", "concept", List.of(), List.of());
        }

        @Override
        public CardSaveResult saveCard(String rawTitle, String markdown, CardWriteOptions options) {
            String key = String.valueOf(rawTitle == null ? "" : rawTitle).trim().toLowerCase();
            String safeMarkdown = String.valueOf(markdown == null ? "" : markdown);
            markdowns.put(key, safeMarkdown);
            Path path = Paths.get("var/cards/" + key + ".md");
            return new CardSaveResult(rawTitle, path, safeMarkdown.length(), "now", "today", "concept", List.of());
        }

        @Override
        public List<String> listTitles() {
            return markdowns.keySet().stream().sorted().toList();
        }

        @Override
        public List<CardBacklinkItem> listBacklinks(String rawTitle) throws IOException {
            return List.of();
        }
    }
}
