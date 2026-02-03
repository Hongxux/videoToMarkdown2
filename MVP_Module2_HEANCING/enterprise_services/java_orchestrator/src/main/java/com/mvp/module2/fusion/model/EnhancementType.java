package com.mvp.module2.fusion.model;

public enum EnhancementType {
    TEXT("TEXT"),
    SCREENSHOT("SCREENSHOT"),
    VIDEO("VIDEO"),
    VIDEO_AND_SCREENSHOT("VIDEO_AND_SCREENSHOT");

    private final String value;

    EnhancementType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
