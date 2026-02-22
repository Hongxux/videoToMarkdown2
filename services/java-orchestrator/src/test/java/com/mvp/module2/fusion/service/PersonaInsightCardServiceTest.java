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
}
