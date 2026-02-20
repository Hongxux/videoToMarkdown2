package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MacroPersonaForgeServiceNormalizationTest {

    @Test
    void normalizeProfileShouldAcceptDualTierOutputSchema() throws Exception {
        MacroPersonaForgeService service = new MacroPersonaForgeService();
        Method method = MacroPersonaForgeService.class.getDeclaredMethod(
                "normalizeProfile",
                Map.class,
                Map.class
        );
        method.setAccessible(true);

        Map<String, Object> candidate = Map.of(
                "evolution_verdict", "用户从工具链执行转向系统性抽象。",
                "surface_context", Map.of(
                        "profession", List.of("后端架构师"),
                        "skillset", List.of("Java", "Kotlin Compose"),
                        "current_challenges", List.of("移动端遥测链路稳定性")
                ),
                "deep_soul_matrix", Map.of(
                        "tech_depth", Map.of("score", 88, "description", "对底层机制保持高热度"),
                        "commercial_acumen", Map.of("score", 72, "description", "重视技术落地价值"),
                        "first_principle", Map.of("score", 81, "description", "倾向追溯根因"),
                        "information_density", Map.of("score", 77, "description", "能接受高密度信息"),
                        "tolerance_for_ambiguity", Map.of("score", 66, "description", "可容忍不确定性"),
                        "design_aesthetics", Map.of("score", 70, "description", "关注交互细节"),
                        "system_thinking", Map.of("score", 86, "description", "关注全链路结构"),
                        "pragmatism", Map.of("score", 79, "description", "偏向工程可落地"),
                        "emotional_resonance", Map.of("score", 60, "description", "对叙事有一定共鸣"),
                        "execution_bias", Map.of("score", 84, "description", "有较强行动倾向")
                )
        );

        @SuppressWarnings("unchecked")
        Map<String, Object> normalized = (Map<String, Object>) method.invoke(service, candidate, candidate);

        assertEquals("用户从工具链执行转向系统性抽象。", normalized.get("evolution_verdict"));
        assertTrue(normalized.containsKey("dimensions"));
        assertTrue(normalized.containsKey("surface_context"));
        assertTrue(normalized.containsKey("deep_soul_matrix"));

        @SuppressWarnings("unchecked")
        Map<String, Object> deep = (Map<String, Object>) normalized.get("deep_soul_matrix");
        @SuppressWarnings("unchecked")
        Map<String, Object> techDepth = (Map<String, Object>) deep.get("tech_depth");
        assertEquals(88, techDepth.get("score"));

        @SuppressWarnings("unchecked")
        Map<String, Object> surface = (Map<String, Object>) normalized.get("surface_context");
        @SuppressWarnings("unchecked")
        List<String> profession = (List<String>) surface.get("profession");
        assertEquals("后端架构师", profession.get(0));
    }
}
