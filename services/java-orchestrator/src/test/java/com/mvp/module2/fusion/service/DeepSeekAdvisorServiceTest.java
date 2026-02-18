package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeepSeekAdvisorServiceTest {

    @Test
    void shouldFallbackWhenApiKeyMissing() throws Exception {
        DeepSeekAdvisorService service = new DeepSeekAdvisorService();
        setField(service, "advisorEnabled", true);
        setField(service, "apiKey", "");

        DeepSeekAdvisorService.AdviceResult result = service.requestAdvice(
                "熵增",
                "房间越乱越难自己恢复。",
                true
        );

        assertEquals("fallback", result.source);
        assertTrue(result.advice.contains("熵增"));
    }

    @Test
    void shouldRejectBlankTerm() {
        DeepSeekAdvisorService service = new DeepSeekAdvisorService();
        assertThrows(IllegalArgumentException.class, () -> service.requestAdvice("  ", "", false));
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}

