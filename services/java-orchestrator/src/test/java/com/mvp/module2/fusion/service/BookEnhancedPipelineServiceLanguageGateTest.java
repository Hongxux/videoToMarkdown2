package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

class BookEnhancedPipelineServiceLanguageGateTest {

    @Test
    void pureEnglishShouldTranslateAndMixedZhEnShouldSkip() throws Exception {
        BookEnhancedPipelineService service = new BookEnhancedPipelineService();
        setField(service, "skipMixedChineseEnglish", true);

        Method method = BookEnhancedPipelineService.class.getDeclaredMethod("shouldTranslateText", String.class);
        method.setAccessible(true);

        boolean pureEnglish = (boolean) method.invoke(service, "Flooding uses an overlay graph with expected edges.");
        boolean mixedZhEn = (boolean) method.invoke(service, "该段说明 flooding 算法的复杂度。");
        boolean pureChinese = (boolean) method.invoke(service, "这是一段纯中文文本。");

        Assertions.assertTrue(pureEnglish, "纯英文段落应触发翻译");
        Assertions.assertFalse(mixedZhEn, "中英混排段落应跳过翻译");
        Assertions.assertFalse(pureChinese, "纯中文段落应跳过翻译");
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
