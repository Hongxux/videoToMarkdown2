package com.mvp.module2.fusion.tools;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.FusionOrchestratorApplication;
import com.mvp.module2.fusion.service.PersonaInsightCardService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class InsightCardTargetedRegenerateTest {

    @Test
    void regenerateAiAgentCardsFromSemanticNodes() throws Exception {
        String taskId = System.getProperty(
                "targeted.card.taskId",
                "storage:c786a1956e66ba020dfb2ed46a3b0c3c_ab_fix_20260221_cards"
        );
        String userId = System.getProperty(
                "targeted.card.userId",
                "targeted_card_regenerate"
        );
        Path markdownPath = Paths.get(System.getProperty(
                "targeted.card.markdownPath",
                "D:/videoToMarkdownTest2/var/storage/storage/c786a1956e66ba020dfb2ed46a3b0c3c_ab_fix_20260221_cards/enhanced_output.md"
        )).toAbsolutePath().normalize();
        Path personaReadingPath = Paths.get(System.getProperty(
                "targeted.card.personaReadingPath",
                "D:/videoToMarkdownTest2/var/storage/storage/c786a1956e66ba020dfb2ed46a3b0c3c_ab_b13_20260222_020400/.mobile_persona_cache/persona_reading/prod_chain_quality_semantic_20260222_v2_1287493312.json"
        )).toAbsolutePath().normalize();
        int waitSeconds = Integer.parseInt(System.getProperty("targeted.card.waitSeconds", "300"));

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> root = mapper.readValue(
                Files.readString(personaReadingPath, StandardCharsets.UTF_8),
                new TypeReference<Map<String, Object>>() {}
        );
        List<Map<String, Object>> nodes = extractTargetNodes(root.get("nodes"));
        if (nodes.isEmpty()) {
            throw new IllegalStateException("no target nodes found for AI agents / agent force");
        }

        try (ConfigurableApplicationContext ctx = new SpringApplicationBuilder(FusionOrchestratorApplication.class)
                .properties(
                        "spring.main.web-application-type=none",
                        "logging.level.root=WARN",
                        "telemetry.persona-reading.insight-cards.force-regenerate=true",
                        "telemetry.persona-reading.insight-cards.max-tags=16"
                )
                .run()) {
            PersonaInsightCardService service = ctx.getBean(PersonaInsightCardService.class);
            service.generateAsync(taskId, userId, markdownPath, nodes);

            Map<String, Object> index = Map.of();
            for (int i = 0; i < Math.max(1, waitSeconds); i += 1) {
                Thread.sleep(1000L);
                index = service.loadIndexSnapshot(taskId, markdownPath);
                if (!index.isEmpty()) {
                    break;
                }
            }
            if (index.isEmpty()) {
                throw new IllegalStateException("insight cards index not generated in wait window");
            }

            Path taskRoot = markdownPath.getParent();
            Path reportPath = taskRoot.resolve(".mobile_persona_cache")
                    .resolve("insight_cards")
                    .resolve("quality_test")
                    .resolve("targeted_regenerate_report.json")
                    .normalize();
            Files.createDirectories(reportPath.getParent());

            Map<String, Object> report = new LinkedHashMap<>();
            report.put("taskId", taskId);
            report.put("userId", userId);
            report.put("markdownPath", markdownPath.toString());
            report.put("personaReadingPath", personaReadingPath.toString());
            report.put("targetNodeCount", nodes.size());
            report.put("index", index);
            Files.writeString(
                    reportPath,
                    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report),
                    StandardCharsets.UTF_8
            );
            System.out.println("TARGETED_REPORT=" + reportPath);
            if (index.containsKey("indexPath")) {
                System.out.println("INDEX_PATH=" + index.get("indexPath"));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractTargetNodes(Object rawNodes) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (!(rawNodes instanceof List<?> list)) {
            return result;
        }
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> map)) {
                continue;
            }
            List<String> tags = extractTags(map.get("insights_tags"));
            if (tags.stream().anyMatch(this::isTargetTag)) {
                result.add((Map<String, Object>) map);
            }
        }
        return result;
    }

    private List<String> extractTags(Object raw) {
        List<String> tags = new ArrayList<>();
        if (raw instanceof List<?> list) {
            for (Object item : list) {
                if (item == null) {
                    continue;
                }
                String value = String.valueOf(item).trim();
                if (!value.isEmpty()) {
                    tags.add(value);
                }
            }
            return tags;
        }
        if (raw != null) {
            String value = String.valueOf(raw).trim();
            if (!value.isEmpty()) {
                tags.add(value);
            }
        }
        return tags;
    }

    private boolean isTargetTag(String tag) {
        String normalized = String.valueOf(tag == null ? "" : tag).trim().toLowerCase(Locale.ROOT);
        return "ai agents".equals(normalized)
                || "agent force".equals(normalized);
    }
}