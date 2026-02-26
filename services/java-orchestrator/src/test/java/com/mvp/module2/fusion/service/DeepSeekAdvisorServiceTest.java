package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DeepSeekAdvisorServiceTest {

    @Test
    void shouldThrowWhenApiKeyMissing() throws Exception {
        DeepSeekAdvisorService service = new DeepSeekAdvisorService();
        setField(service, "advisorEnabled", true);
        setField(service, "apiKey", "");

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> service.requestAdvice("term", "context", true)
        );

        assertEquals("DEEPSEEK_API_KEY is empty", error.getMessage());
    }

    @Test
    void shouldThrowWhenAdvisorDisabled() throws Exception {
        DeepSeekAdvisorService service = new DeepSeekAdvisorService();
        setField(service, "advisorEnabled", false);
        setField(service, "apiKey", "test-key");

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> service.requestAdvice("term", "context", true)
        );

        assertEquals("deepseek.advisor.enabled=false", error.getMessage());
    }

    @Test
    void shouldRejectBlankTerm() {
        DeepSeekAdvisorService service = new DeepSeekAdvisorService();
        assertThrows(IllegalArgumentException.class, () -> service.requestAdvice("  ", "", false));
    }

    @Test
    void shouldParseStructuredAdviceBackgroundField() throws Exception {
        DeepSeekAdvisorService service = new DeepSeekAdvisorService();
        Method method = DeepSeekAdvisorService.class.getDeclaredMethod("parseStructuredAdvice", String.class);
        method.setAccessible(true);

        String raw = "{"
                + "\"background\":[\"背景信息\"],"
                + "\"contextual_explanations\":[\"语境解释\"],"
                + "\"depth\":[\"深度洞察\"],"
                + "\"breadth\":[\"广度关联\"]"
                + "}";
        DeepSeekAdvisorService.StructuredAdviceResult result =
                (DeepSeekAdvisorService.StructuredAdviceResult) method.invoke(service, raw);

        assertEquals(List.of("背景信息"), result.background);
        assertEquals(List.of("语境解释"), result.contextualExplanations);
        assertEquals(List.of("深度洞察"), result.depth);
        assertEquals(List.of("广度关联"), result.breadth);
    }

    @Test
    void shouldParseStructuredBatchBackgroundField() throws Exception {
        DeepSeekAdvisorService service = new DeepSeekAdvisorService();
        Method method = DeepSeekAdvisorService.class.getDeclaredMethod(
                "parseStructuredAdviceBatch",
                String.class,
                String.class,
                String.class
        );
        method.setAccessible(true);

        String raw = "{"
                + "\"items\":[{"
                + "\"term\":\"Entropy\","
                + "\"background\":[\"背景批量\"],"
                + "\"contextual_explanations\":[\"语境批量\"],"
                + "\"depth\":[\"深度批量\"],"
                + "\"breadth\":[\"广度批量\"]"
                + "}]"
                + "}";

        @SuppressWarnings("unchecked")
        Map<String, DeepSeekAdvisorService.StructuredAdviceResult> result =
                (Map<String, DeepSeekAdvisorService.StructuredAdviceResult>) method.invoke(service, raw, "", "");

        DeepSeekAdvisorService.StructuredAdviceResult entropy = result.get("entropy");
        assertNotNull(entropy);
        assertEquals(List.of("背景批量"), entropy.background);
        assertEquals(List.of("语境批量"), entropy.contextualExplanations);
        assertEquals(List.of("深度批量"), entropy.depth);
        assertEquals(List.of("广度批量"), entropy.breadth);
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
