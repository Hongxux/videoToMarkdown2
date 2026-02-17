package com.mvp.module2.fusion.common;

/**
 * 面向用户的错误信息与拥塞判定映射。
 *
 * 目标：
 * 1) 统一“系统繁忙”文案，避免在多个模块重复硬编码；
 * 2) 统一拥塞状态码判定规则，避免前后端出现不一致。
 */
public final class UserFacingErrorMapper {

    private static final String BUSY_MESSAGE = "系统繁忙，请稍后重试";

    private UserFacingErrorMapper() {
        // 工具类不允许实例化
    }

    public static String busyMessage() {
        return BUSY_MESSAGE;
    }

    public static boolean isBusyHttpStatus(int statusCode) {
        return statusCode == 429 || statusCode == 503 || statusCode >= 500;
    }
}
