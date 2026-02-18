package com.mvp.module2.fusion.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VideoInputNormalizerTest {

    @Test
    void shouldKeepWindowsLocalPathUntouched() {
        String input = "D:\\videos\\sample.mp4";
        assertEquals(input, VideoInputNormalizer.normalizeVideoInput(input));
    }

    @Test
    void shouldExtractAndFixMalformedShortLinkFromMixedText() {
        String input = "【Lenny播客】Edwin Chen 访谈 https:\\b23.tv\\jMUuS63";
        assertEquals("https://b23.tv/jMUuS63", VideoInputNormalizer.normalizeVideoInput(input));
    }

    @Test
    void shouldCanonicalizeBilibiliVideoUrlWithBvId() {
        String input = "https://www.bilibili.com/video/BV1xx411c7mD?p=1";
        assertEquals("https://www.bilibili.com/video/BV1xx411c7mD", VideoInputNormalizer.normalizeVideoInput(input));
    }

    @Test
    void shouldCanonicalizeBareBvIdWithOriginalCase() {
        String input = "bv1xx411c7mD";
        assertEquals("https://www.bilibili.com/video/bv1xx411c7mD", VideoInputNormalizer.normalizeVideoInput(input));
    }

    @Test
    void shouldKeepBvOriginalCase() {
        String input = "Bv1Ab4Y1zZ9k";
        assertEquals("https://www.bilibili.com/video/Bv1Ab4Y1zZ9k", VideoInputNormalizer.normalizeVideoInput(input));
    }
}
