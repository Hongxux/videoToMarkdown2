package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
