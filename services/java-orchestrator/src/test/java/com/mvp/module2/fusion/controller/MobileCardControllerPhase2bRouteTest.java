package com.mvp.module2.fusion.controller;

import org.junit.jupiter.api.Test;
import org.springframework.web.bind.annotation.PostMapping;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MobileCardControllerPhase2bRouteTest {

    @Test
    void phase2bEndpointShouldExposeCanonicalAndLegacyPaths() throws Exception {
        Method method = MobileCardController.class.getDeclaredMethod(
                "phase2bStructuredMarkdown",
                MobileCardController.Phase2bStructuredMarkdownRequest.class
        );

        PostMapping mapping = method.getAnnotation(PostMapping.class);
        List<String> paths = List.of(mapping.value());

        assertEquals(true, paths.contains("/phase2b"));
        assertEquals(true, paths.contains("/phase2b/structured-markdown"));
    }
}
