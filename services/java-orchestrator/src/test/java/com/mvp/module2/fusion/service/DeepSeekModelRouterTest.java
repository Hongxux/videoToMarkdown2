package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeepSeekModelRouterTest {

    @Test
    void shouldRouteV32ReasonerAliasesToReasonerModel() {
        assertEquals("deepseek-reasoner", DeepSeekModelRouter.resolveModel("deepseek-reasoner"));
        assertEquals("deepseek-reasoner", DeepSeekModelRouter.resolveModel("deepseek-r1"));
        assertEquals("deepseek-reasoner", DeepSeekModelRouter.resolveModel("v3 reasoner"));
        assertEquals("deepseek-reasoner", DeepSeekModelRouter.resolveModel("v3.2 reasoner"));
        assertEquals("deepseek-reasoner", DeepSeekModelRouter.resolveModel("deepseek-v3.2-reasoner"));
        assertEquals("deepseek-reasoner", DeepSeekModelRouter.resolveModel("deepseek-resoner"));
    }

    @Test
    void shouldKeepV3AliasForChatModel() {
        assertEquals("deepseek-chat", DeepSeekModelRouter.resolveModel("deepseek-v3"));
        assertEquals("deepseek-chat", DeepSeekModelRouter.resolveModel("v3"));
    }
}
