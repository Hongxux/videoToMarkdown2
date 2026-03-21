package com.mvp.module2.fusion.worker.watchdog;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WatchdogSignalCodecTest {

    private final WatchdogSignalCodec codec = new WatchdogSignalCodec();

    @Test
    void sanitizeForUserShouldExplainPhase2aSegmentationWait() {
        TaskWatchdog.Signal signal = new TaskWatchdog.Signal(
                "phase2a",
                "running",
                "phase2a_segmentation_running",
                1,
                2,
                7L,
                "soft"
        );

        String message = codec.sanitizeForUser(
                "WATCHDOG_SIGNAL|{\"stage\":\"phase2a\"}",
                signal
        );

        assertEquals("已开始 Phase2A 语义分割 LLM 调用，正在等待结果...", message);
    }

    @Test
    void sanitizeForUserShouldKeepGenericCheckpointForOtherSignals() {
        TaskWatchdog.Signal signal = new TaskWatchdog.Signal(
                "phase2a",
                "running",
                "phase2a_prepare",
                0,
                3,
                1L,
                "hard"
        );

        String message = codec.sanitizeForUser(
                "WATCHDOG_SIGNAL|{\"stage\":\"phase2a\"}",
                signal
        );

        assertEquals("Phase2A running (phase2a_prepare)", message);
    }
}