package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ByteArrayResource;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
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
    void shouldParseStructuredAdviceFromWrappedJsonPayload() throws Exception {
        DeepSeekAdvisorService service = new DeepSeekAdvisorService();
        Method method = DeepSeekAdvisorService.class.getDeclaredMethod("parseStructuredAdvice", String.class);
        method.setAccessible(true);

        String raw = "以下为结构化结果：```json\n"
                + "{"
                + "\"result\":{"
                + "\"background\":[\"包装背景\"],"
                + "\"contextual_explanations\":[\"包装语境\"],"
                + "\"depth\":[\"包装深度\"],"
                + "\"breadth\":[\"包装广度\"]"
                + "}"
                + "}\n"
                + "```";
        DeepSeekAdvisorService.StructuredAdviceResult result =
                (DeepSeekAdvisorService.StructuredAdviceResult) method.invoke(service, raw);

        assertEquals(List.of("包装背景"), result.background);
        assertEquals(List.of("包装语境"), result.contextualExplanations);
        assertEquals(List.of("包装深度"), result.depth);
        assertEquals(List.of("包装广度"), result.breadth);
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

    @Test
    void shouldParseStructuredBatchWhenItemsWrappedByDataNode() throws Exception {
        DeepSeekAdvisorService service = new DeepSeekAdvisorService();
        Method method = DeepSeekAdvisorService.class.getDeclaredMethod(
                "parseStructuredAdviceBatch",
                String.class,
                String.class,
                String.class
        );
        method.setAccessible(true);

        String raw = "{"
                + "\"data\":{"
                + "\"items\":[{"
                + "\"term\":\"Entropy\","
                + "\"background\":[\"嵌套背景\"],"
                + "\"contextual_explanations\":[\"嵌套语境\"],"
                + "\"depth\":[\"嵌套深度\"],"
                + "\"breadth\":[\"嵌套广度\"]"
                + "}]"
                + "}"
                + "}";

        @SuppressWarnings("unchecked")
        Map<String, DeepSeekAdvisorService.StructuredAdviceResult> result =
                (Map<String, DeepSeekAdvisorService.StructuredAdviceResult>) method.invoke(service, raw, "", "");

        DeepSeekAdvisorService.StructuredAdviceResult entropy = result.get("entropy");
        assertNotNull(entropy);
        assertEquals(List.of("嵌套背景"), entropy.background);
        assertEquals(List.of("嵌套语境"), entropy.contextualExplanations);
        assertEquals(List.of("嵌套深度"), entropy.depth);
        assertEquals(List.of("嵌套广度"), entropy.breadth);
    }

    @Test
    void shouldBuildPhase2bStructuredUserPromptFromTemplate() throws Exception {
        DeepSeekAdvisorService service = new DeepSeekAdvisorService();
        setField(service, "phase2bStructuredUserPromptResource", new ByteArrayResource(
                "Body:\n{body_text}\nTail".getBytes(StandardCharsets.UTF_8)
        ));
        Method method = DeepSeekAdvisorService.class.getDeclaredMethod("buildPhase2bStructuredUserPrompt", String.class);
        method.setAccessible(true);

        String prompt = (String) method.invoke(service, "line1\nline2");

        assertEquals("Body:\nline1\nline2\nTail", prompt);
    }

    @Test
    void shouldAppendImageConstraintsToPhase2bStructuredSystemPrompt() throws Exception {
        DeepSeekAdvisorService service = new DeepSeekAdvisorService();
        setField(service, "phase2bStructuredSystemPromptResource", new ByteArrayResource(
                "Base phase2b prompt".getBytes(StandardCharsets.UTF_8)
        ));
        Method method = DeepSeekAdvisorService.class.getDeclaredMethod("buildPhase2bStructuredSystemPrompt");
        method.setAccessible(true);

        String prompt = (String) method.invoke(service);

        assertEquals(true, prompt.contains("Base phase2b prompt"));
        assertEquals(true, prompt.contains("Image Marker Hard Constraints"));
        assertEquals(true, prompt.contains("![alt](url)"));
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
