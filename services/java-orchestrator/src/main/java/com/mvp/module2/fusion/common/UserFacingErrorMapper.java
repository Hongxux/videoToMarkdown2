package com.mvp.module2.fusion.common;

/**
 * 面向用户的错误信息映射。
 * 目标：
 * 1) 统一可读、可执行的错误提示。
 * 2) 对常见下载故障（代理/Cookie/地域限制/超时）返回明确建议。
 */
public final class UserFacingErrorMapper {

    private static final String BUSY_MESSAGE = "系统繁忙，请稍后重试";
    private static final String DOWNLOAD_TIMEOUT_MESSAGE =
        "下载阶段超时（长时间无进展或总耗时超限）。请检查网络、代理、Cookie 配置后重试。";
    private static final String GEO_OR_DELETED_MESSAGE =
        "视频可能已删除、不可见，或受地区限制。请先确认链接可访问，再检查代理配置（video.download_proxy / YTDLP_PROXY）。";
    private static final String BILIBILI_BVID_EXTRACTOR_MESSAGE =
        "B 站页面解析失败（未提取到 bvid）。请确认使用浏览器地址栏原始链接，不要手动改 BV 大小写，必要时升级 yt-dlp 后重试。";
    private static final String COOKIE_REQUIRED_MESSAGE =
        "目标站点需要登录态 Cookie。请配置 download_cookies_from_browser（例如 edge:Default）或 download_cookies_file 后重试。";
    private static final String PROXY_UNAVAILABLE_MESSAGE =
        "下载代理不可用，请检查代理进程与端口；若暂不使用代理，请清空 video.download_proxy 与 YTDLP_PROXY 后重试。";
    private static final String GRPC_PING_THROTTLED_MESSAGE =
        "与处理引擎连接过于频繁（Too many pings）。请稍后重试，或放宽/关闭 gRPC keepalive。";

    private UserFacingErrorMapper() {
    }

    public static String busyMessage() {
        return BUSY_MESSAGE;
    }

    public static boolean isBusyHttpStatus(int statusCode) {
        return statusCode == 429 || statusCode == 503 || statusCode >= 500;
    }

    public static String toUserMessage(String rawErrorMessage) {
        if (rawErrorMessage == null || rawErrorMessage.isBlank()) {
            return BUSY_MESSAGE;
        }

        String normalized = rawErrorMessage.toLowerCase();
        if (containsAny(
                normalized,
                "download idle timeout exceeded",
                "download hard timeout exceeded",
                "download stage timed out",
                "deadline_exceeded",
                "grpc status=deadline_exceeded")) {
            return DOWNLOAD_TIMEOUT_MESSAGE;
        }

        if (containsAny(
                normalized,
                "unable to connect to proxy",
                "proxyerror",
                "winerror 10061",
                "连接代理失败")) {
            return PROXY_UNAVAILABLE_MESSAGE;
        }

        if (containsAny(
                normalized,
                "too many pings",
                "too_many_pings",
                "enhance your calm")) {
            return GRPC_PING_THROTTLED_MESSAGE;
        }

        if (containsAny(
                normalized,
                "cookies-from-browser",
                "use --cookies",
                "not a bot",
                "sign in to confirm",
                "需要登录",
                "cookie")) {
            return COOKIE_REQUIRED_MESSAGE;
        }

        if (containsAny(
                normalized,
                "keyerror('bvid')",
                "extractor error has occurred",
                "未提取到 bvid")) {
            return BILIBILI_BVID_EXTRACTOR_MESSAGE;
        }

        if (containsAny(
                normalized,
                "geo-restricted",
                "region-restricted",
                "region restricted",
                "地区限制",
                "video may be deleted",
                "has been deleted",
                "this video is unavailable",
                "视频不存在")) {
            return GEO_OR_DELETED_MESSAGE;
        }

        return BUSY_MESSAGE;
    }

    private static boolean containsAny(String raw, String... keywords) {
        for (String keyword : keywords) {
            if (raw.contains(keyword)) {
                return true;
            }
        }
        return false;
    }
}
