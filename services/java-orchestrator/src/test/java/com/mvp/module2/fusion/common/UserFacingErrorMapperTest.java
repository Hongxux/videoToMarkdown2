package com.mvp.module2.fusion.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserFacingErrorMapperTest {

    @Test
    void shouldMapGeoRestrictionToActionableMessage() {
        String raw = "ERROR: [BiliBili] 1VPMXBJEBS: This video may be deleted or geo-restricted.";
        String message = UserFacingErrorMapper.toUserMessage(raw);
        assertTrue(message.contains("地区限制"));
        assertTrue(message.contains("YTDLP_PROXY"));
    }

    @Test
    void shouldMapCookieChallengeToActionableMessage() {
        String raw = "Use --cookies-from-browser or --cookies for the authentication.";
        String message = UserFacingErrorMapper.toUserMessage(raw);
        assertTrue(message.contains("download_cookies_from_browser"));
    }

    @Test
    void shouldMapProxyFailureToActionableMessage() {
        String raw = "Unable to connect to proxy";
        String message = UserFacingErrorMapper.toUserMessage(raw);
        assertTrue(message.contains("下载代理不可用"));
    }

    @Test
    void shouldMapTooManyPingsToGrpcKeepaliveHint() {
        String raw = "Download failed: Too many pings";
        String message = UserFacingErrorMapper.toUserMessage(raw);
        assertTrue(message.contains("Too many pings"));
        assertTrue(message.contains("keepalive"));
    }

    @Test
    void shouldMapBilibiliBvidExtractorFailureToActionableMessage() {
        String raw = "Download failed: yt-dlp 执行失败: ERROR: 1XKIJBSEBJ: An extractor error has occurred. (caused by KeyError('bvid'))";
        String message = UserFacingErrorMapper.toUserMessage(raw);
        assertTrue(message.contains("未提取到 bvid"));
        assertTrue(message.contains("BV 大小写"));
    }

    @Test
    void shouldFallbackToBusyMessageForUnknownError() {
        String message = UserFacingErrorMapper.toUserMessage("some-random-unclassified-error");
        assertEquals(UserFacingErrorMapper.busyMessage(), message);
    }
}
