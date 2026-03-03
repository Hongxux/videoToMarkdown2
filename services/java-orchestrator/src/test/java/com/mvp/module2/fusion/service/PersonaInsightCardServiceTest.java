package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersonaInsightCardServiceTest {

    @Test
    void shouldDeduplicateTagsAndKeepOnlyTagsFromRawMarkdown() throws Exception {
        PersonaInsightCardService service = new PersonaInsightCardService();
        Method collectMethod = PersonaInsightCardService.class.getDeclaredMethod("collectTagContexts", List.class);
        collectMethod.setAccessible(true);

        List<Map<String, Object>> nodes = new ArrayList<>();
        nodes.add(node(
                "n-1",
                "这里讨论 `JNI` 与 AST 抽象语法树 的协作细节。",
                List.of("JNI", "jni", "AST 抽象语法树", "幻词")
        ));
        nodes.add(node(
                "n-2",
                "进一步展开 ONNX 计算流 的执行路径。",
                List.of("ONNX 计算流", "onnx 计算流", "GC")
        ));

        @SuppressWarnings("unchecked")
        LinkedHashMap<String, Object> contexts = (LinkedHashMap<String, Object>) collectMethod.invoke(service, nodes);

        assertEquals(3, contexts.size(), "只应保留去重后的术语，并过滤原文不存在术语");
        assertTrue(contexts.containsKey("jni"));
        assertTrue(contexts.containsKey("ast 抽象语法树"));
        assertTrue(contexts.containsKey("onnx 计算流"));
        assertFalse(contexts.containsKey("幻词"));
        assertFalse(contexts.containsKey("gc"));

        Object jniContext = contexts.get("jni");
        Set<?> nodeIds = readSetField(jniContext, "nodeIds");
        assertTrue(nodeIds.contains("n-1"));
    }

    @Test
    void shouldParseBackgroundContextualDepthBreadthFromStructuredJson() throws Exception {
        PersonaInsightCardService service = new PersonaInsightCardService();
        Method parseMethod = PersonaInsightCardService.class.getDeclaredMethod(
                "parseAdviceSectionsFromJson",
                String.class,
                String.class
        );
        parseMethod.setAccessible(true);

        String raw = "{"
                + "\"background\":[\"背景一\"],"
                + "\"contextual_explanations\":[\"语境一\"],"
                + "\"depth\":[\"深度一\"],"
                + "\"breadth\":[\"广度一\"]"
                + "}";
        Object sections = parseMethod.invoke(service, raw, "fallback");
        assertNotNull(sections);

        assertEquals(List.of("背景一"), readListField(sections, "background"));
        assertEquals(List.of("语境一"), readListField(sections, "contextual"));
        assertEquals(List.of("深度一"), readListField(sections, "depth"));
        assertEquals(List.of("广度一"), readListField(sections, "breadth"));
    }

    @Test
    void shouldRenderMarkdownWithBackgroundSection() throws Exception {
        PersonaInsightCardService service = new PersonaInsightCardService();
        Method parseMethod = PersonaInsightCardService.class.getDeclaredMethod(
                "parseAdviceSectionsFromJson",
                String.class,
                String.class
        );
        parseMethod.setAccessible(true);
        Method renderMethod = PersonaInsightCardService.class.getDeclaredMethod(
                "buildInitialCardBodyFromJson",
                Class.forName("com.mvp.module2.fusion.service.PersonaInsightCardService$StructuredAdviceSections")
        );
        renderMethod.setAccessible(true);

        String raw = "{"
                + "\"background\":[\"背景段\"],"
                + "\"contextual_explanations\":[\"语境段\"],"
                + "\"depth\":[\"深度段\"],"
                + "\"breadth\":[\"广度段\"]"
                + "}";
        Object sections = parseMethod.invoke(service, raw, "fallback");
        String markdown = (String) renderMethod.invoke(service, sections);

        assertTrue(markdown.contains("## 背景知识"));
        assertTrue(markdown.contains("## 语境化解释"));
        assertTrue(markdown.contains("## 深度"));
        assertTrue(markdown.contains("## 广度"));
        assertFalse(markdown.contains("### 语境快照@"));
    }

    @Test
    void shouldPreserveMarkdownLineBreaksFromStructuredAdvice() throws Exception {
        PersonaInsightCardService service = new PersonaInsightCardService();
        Method sectionsMethod = PersonaInsightCardService.class.getDeclaredMethod(
                "sectionsFromAdvice",
                DeepSeekAdvisorService.StructuredAdviceResult.class
        );
        sectionsMethod.setAccessible(true);
        Method renderMethod = PersonaInsightCardService.class.getDeclaredMethod(
                "buildInitialCardBodyFromJson",
                Class.forName("com.mvp.module2.fusion.service.PersonaInsightCardService$StructuredAdviceSections")
        );
        renderMethod.setAccessible(true);

        DeepSeekAdvisorService.StructuredAdviceResult advice = DeepSeekAdvisorService.StructuredAdviceResult.deepseek(
                List.of("背景主句\n被动消费闭环：观看/阅读时产生理解幻觉\n| 维度 | 说明 |\n|---|---|\n| A | B |\n- 要点A\n- 要点B"),
                List.of("语境主句"),
                List.of("深度主句"),
                List.of("广度主句"),
                "{}"
        );
        Object sections = sectionsMethod.invoke(service, advice);
        String markdown = (String) renderMethod.invoke(service, sections);

        assertTrue(markdown.contains("1. 背景主句\n    **被动消费闭环：** 观看/阅读时产生理解幻觉\n\n    | 维度 | 说明 |\n    |---|---|\n    | A | B |\n    - 要点A\n    - 要点B"));
        assertFalse(markdown.contains("背景主句 - 要点A - 要点B"));
    }

    private Map<String, Object> node(String nodeId, String rawMarkdown, List<String> insightsTags) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("node_id", nodeId);
        row.put("raw_markdown", rawMarkdown);
        row.put("insights_tags", insightsTags);
        row.put("reason", "test");
        return row;
    }

    @SuppressWarnings("unchecked")
    private Set<?> readSetField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Set<?>) field.get(target);
    }

    @SuppressWarnings("unchecked")
    private List<String> readListField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (List<String>) field.get(target);
    }
}
