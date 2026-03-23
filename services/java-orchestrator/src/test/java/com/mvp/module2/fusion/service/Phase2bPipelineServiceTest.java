package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class Phase2bPipelineServiceTest {

    @Test
    void shouldExecutePipelineAndExposeAppliedSkills() throws Exception {
        Phase2bPipelineService service = new Phase2bPipelineService();
        DeepSeekAdvisorService advisorService = mock(DeepSeekAdvisorService.class);
        setField(service, "deepSeekAdvisorService", advisorService);
        setField(service, "pipelineEnabled", true);
        setField(service, "maxParallelism", 2);

        String skeletonOutput = """
                ```json
                {
                  "sections": [
                    {
                      "id": "s1",
                      "logic_tags": ["parallel", "contrast"],
                      "scene_tags": ["technical"],
                      "title": "Redis Stream Layout"
                    },
                    {
                      "id": "s2",
                      "logic_tags": ["causal"],
                      "scene_tags": ["technical"],
                      "title": "Consumer Group Reliability"
                    }
                  ]
                }
                ```
                ---
                ## s1: Redis Stream Layout
                - original layout

                ## s2: Consumer Group Reliability
                - original reliability
                """;

        when(advisorService.requestPhase2bSkeletonMarkdownResult("input-body", false))
                .thenReturn(newMarkdownResult(skeletonOutput, "deepseek.phase2b.pipeline.phase1", "deepseek", false));
        when(advisorService.loadSkillContent(anyString())).thenAnswer(invocation -> {
            String skillId = invocation.getArgument(0, String.class);
            return switch (skillId) {
                case "logic_parallel" -> "parallel-rule";
                case "logic_contrast" -> "contrast-rule";
                case "logic_causal" -> "causal-rule";
                case "scene_technical" -> "technical-rule";
                case "obsidian_enhancements" -> "obsidian-rule";
                default -> "";
            };
        });
        when(advisorService.requestPhase2bRefinedSectionResult(anyString(), anyList())).thenAnswer(invocation -> {
            String sectionMarkdown = invocation.getArgument(0, String.class);
            if (sectionMarkdown.contains("## s1:")) {
                return newMarkdownResult("## s1: Redis Stream Layout\n- refined layout", "deepseek.phase2b.pipeline.phase2", "deepseek", false);
            }
            return newMarkdownResult("- refined reliability", "deepseek.phase2b.pipeline.phase2", "deepseek", false);
        });
        when(advisorService.requestPhase2bFactCheckResult(anyString())).thenAnswer(invocation -> {
            String markdown = invocation.getArgument(0, String.class);
            return newMarkdownResult(
                    "> **Core Thesis**: Redis Stream relies on batched storage and consumer-group state.\n\n" + markdown,
                    "deepseek.phase2b.pipeline.phase3",
                    "deepseek",
                    false
            );
        });

        Phase2bPipelineService.Phase2bPipelineResult result = service.executePipeline("input-body", false, null);

        assertEquals(true, result.pipelineUsed);
        assertEquals(false, result.legacyFallback);
        assertEquals(true, result.markdown.contains("> **Core Thesis**"));
        assertEquals(true, result.sections.get(0).skillIds.contains("logic_parallel"));
        assertEquals(true, result.sections.get(0).skillIds.contains("logic_contrast"));
        assertEquals(true, result.sections.get(0).skillIds.contains("scene_technical"));
        assertEquals(true, result.sections.get(0).skillIds.contains("obsidian_enhancements"));
        assertEquals(true, result.sections.get(1).skillIds.contains("logic_causal"));
        assertEquals(true, result.sections.get(1).skillIds.contains("scene_technical"));
    }

    @Test
    void shouldFallbackToLegacyWhenPhase1OutputCannotBeParsed() throws Exception {
        Phase2bPipelineService service = new Phase2bPipelineService();
        DeepSeekAdvisorService advisorService = mock(DeepSeekAdvisorService.class);
        setField(service, "deepSeekAdvisorService", advisorService);
        setField(service, "pipelineEnabled", true);

        when(advisorService.requestPhase2bSkeletonMarkdownResult("input-body", false))
                .thenReturn(newMarkdownResult("not-a-phase1-payload", "deepseek.phase2b.pipeline.phase1", "deepseek", false));
        when(advisorService.requestPhase2bStructuredMarkdownResult("input-body", "", false))
                .thenReturn(newMarkdownResult("# legacy", "deepseek.phase2b", "deepseek", false));

        Phase2bPipelineService.Phase2bPipelineResult result = service.executePipeline("input-body", false, null);

        assertEquals(false, result.pipelineUsed);
        assertEquals(true, result.legacyFallback);
        assertEquals("phase1-parse-failed", result.fallbackReason);
        assertEquals("# legacy", result.markdown);
    }

    @Test
    void shouldKeepPhase1SectionWhenRefineFails() throws Exception {
        Phase2bPipelineService service = new Phase2bPipelineService();
        DeepSeekAdvisorService advisorService = mock(DeepSeekAdvisorService.class);
        setField(service, "deepSeekAdvisorService", advisorService);
        setField(service, "pipelineEnabled", true);

        String skeletonOutput = """
                ```json
                {
                  "sections": [
                    {
                      "id": "s1",
                      "logic_tags": ["causal"],
                      "scene_tags": ["technical"],
                      "title": "Consumer Group Reliability"
                    }
                  ]
                }
                ```
                ---
                ## s1: Consumer Group Reliability
                - original reliability
                """;

        when(advisorService.requestPhase2bSkeletonMarkdownResult("input-body", false))
                .thenReturn(newMarkdownResult(skeletonOutput, "deepseek.phase2b.pipeline.phase1", "deepseek", false));
        when(advisorService.loadSkillContent(anyString())).thenReturn("rule");
        when(advisorService.requestPhase2bRefinedSectionResult(anyString(), anyList()))
                .thenThrow(new IllegalStateException("llm timeout"));
        when(advisorService.requestPhase2bFactCheckResult(anyString())).thenAnswer(invocation ->
                newMarkdownResult(invocation.getArgument(0, String.class), "deepseek.phase2b.pipeline.phase3", "deepseek", false)
        );

        Phase2bPipelineService.Phase2bPipelineResult result = service.executePipeline("input-body", false, null);

        assertEquals(true, result.markdown.contains("- original reliability"));
        assertEquals(true, result.sections.get(0).fallbackUsed);
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private DeepSeekAdvisorService.Phase2bMarkdownResult newMarkdownResult(
            String markdown,
            String source,
            String provider,
            boolean degraded
    ) throws Exception {
        Constructor<DeepSeekAdvisorService.Phase2bMarkdownResult> constructor =
                DeepSeekAdvisorService.Phase2bMarkdownResult.class.getDeclaredConstructor(
                        String.class,
                        String.class,
                        String.class,
                        boolean.class
                );
        constructor.setAccessible(true);
        return constructor.newInstance(markdown, source, provider, degraded);
    }
}
