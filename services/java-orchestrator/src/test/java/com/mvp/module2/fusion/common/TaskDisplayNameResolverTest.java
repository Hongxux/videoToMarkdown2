package com.mvp.module2.fusion.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TaskDisplayNameResolverTest {

    @Test
    void shouldUseFallbackWhenInputIsBlank() {
        assertEquals(
                "task_001",
                TaskDisplayNameResolver.resolveTaskDisplayTitle("   ", "task_001")
        );
    }

    @Test
    void shouldExtractBvIdFromBilibiliUrl() {
        String input = "https://www.bilibili.com/video/BV1XKIJBSEBJ?p=1";
        assertEquals(
                "BV1XKIJBSEBJ",
                TaskDisplayNameResolver.resolveTaskDisplayTitle(input, "fallback")
        );
    }

    @Test
    void shouldReturnLocalFileNameForWindowsPath() {
        String input = "D:\\videos\\intro.mp4";
        assertEquals(
                "intro.mp4",
                TaskDisplayNameResolver.resolveTaskDisplayTitle(input, "fallback")
        );
    }
}
