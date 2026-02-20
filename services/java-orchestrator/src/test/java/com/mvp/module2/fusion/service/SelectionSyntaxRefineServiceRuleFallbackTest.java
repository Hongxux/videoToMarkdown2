package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SelectionSyntaxRefineServiceRuleFallbackTest {

    @Test
    void ruleFallbackShouldExpandModifierNounPhrase() throws Exception {
        SelectionSyntaxRefineService service = new SelectionSyntaxRefineService();
        setField(service, "minScoreGain", 0.015d);

        String source = "我们正在讨论智能体的协作机制与边界。";
        int start = source.indexOf("协作");
        int end = start + "协作".length();
        int cursor = start + 1;

        Method method = SelectionSyntaxRefineService.class.getDeclaredMethod(
                "resolveBestCandidateByRule",
                String.class,
                int.class,
                int.class,
                int.class,
                String.class
        );
        method.setAccessible(true);
        SelectionSyntaxRefineService.SelectionRefineResult result =
                (SelectionSyntaxRefineService.SelectionRefineResult) method.invoke(
                        service,
                        source,
                        cursor,
                        start,
                        end,
                        "协作"
                );

        assertTrue(result.endOffset > end || result.startOffset < start);
        assertTrue(result.improved);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}

