package com.mvp.module2.fusion.service;

import org.bytedeco.ffmpeg.global.avcodec;
import org.junit.jupiter.api.Test;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JavaCVFFmpegServiceTest {

    @Test
    void fastCopyCodecCompatibility_allowsOnlyH264() {
        assertTrue(JavaCVFFmpegService.isFastCopyCodecCompatible(avcodec.AV_CODEC_ID_H264));
        assertFalse(JavaCVFFmpegService.isFastCopyCodecCompatible(avcodec.AV_CODEC_ID_AV1));
        assertFalse(JavaCVFFmpegService.isFastCopyCodecCompatible(avcodec.AV_CODEC_ID_HEVC));
        assertFalse(JavaCVFFmpegService.isFastCopyCodecCompatible(avcodec.AV_CODEC_ID_MPEG4));
    }

    @Test
    void topReasonBannerFontSize_usesHeightDiv40Formula() {
        assertEquals(27, JavaCVFFmpegService.computeTopReasonBannerFontSize(1080));
        assertEquals(36, JavaCVFFmpegService.computeTopReasonBannerFontSize(1440));
        assertEquals(54, JavaCVFFmpegService.computeTopReasonBannerFontSize(2160));
    }

    @Test
    void applyTopReasonBannerInPlace_darkensTopArea() {
        BufferedImage image = new BufferedImage(1920, 1080, BufferedImage.TYPE_INT_RGB);
        Graphics2D graphics = image.createGraphics();
        try {
            graphics.setColor(new Color(235, 235, 235));
            graphics.fillRect(0, 0, image.getWidth(), image.getHeight());
        } finally {
            graphics.dispose();
        }

        JavaCVFFmpegService.applyTopReasonBannerInPlace(
            image,
            "大家请看画面顶部的设置区域，这里展示了需要重点核对的关键状态。"
        );

        Color topSample = new Color(image.getRGB(200, 90));
        assertTrue(topSample.getRed() < 235);
        assertTrue(topSample.getGreen() < 235);
        assertTrue(topSample.getBlue() < 235);
    }
}
