package com.mvp.module2.fusion.service;

import org.bytedeco.ffmpeg.global.avcodec;
import org.junit.jupiter.api.Test;

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
}

