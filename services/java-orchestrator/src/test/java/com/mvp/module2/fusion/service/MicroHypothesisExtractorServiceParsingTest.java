package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicroHypothesisExtractorServiceParsingTest {

    @Test
    void parseHypothesesShouldSupportContentFocusAndMarkDeleted() throws Exception {
        MicroHypothesisExtractorService service = new MicroHypothesisExtractorService();
        Method method = MicroHypothesisExtractorService.class.getDeclaredMethod("parseHypotheses", String.class);
        method.setAccessible(true);

        String llmOutput = """
                {
                  "items": [
                    {
                      "action": "MARK_DELETED",
                      "content_focus": "底层 C++ 原理深度解析",
                      "inferred_hypothesis": "用户对纯实现细节耐心偏低",
                      "confidence": 0.83
                    },
                    {
                      "action": "RESONANCE",
                      "content_focus": "技术商业化与历史叙事",
                      "inferred_hypothesis": "用户对宏观价值判断高度敏感",
                      "confidence": 0.91
                    }
                  ]
                }
                """;

        @SuppressWarnings("unchecked")
        List<Object> rows = (List<Object>) method.invoke(service, llmOutput);

        assertEquals(2, rows.size());
        assertEquals("DELETED", readField(rows.get(0), "action"));
        assertEquals("底层 C++ 原理深度解析", readField(rows.get(0), "content_type"));
        assertTrue(readDoubleField(rows.get(0), "confidence") >= 0.8d);
        assertEquals("RESONANCE", readField(rows.get(1), "action"));
        assertEquals("技术商业化与历史叙事", readField(rows.get(1), "content_type"));
    }

    private String readField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return String.valueOf(field.get(target));
    }

    private double readDoubleField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        Object value = field.get(target);
        return value instanceof Number number ? number.doubleValue() : Double.parseDouble(String.valueOf(value));
    }
}
