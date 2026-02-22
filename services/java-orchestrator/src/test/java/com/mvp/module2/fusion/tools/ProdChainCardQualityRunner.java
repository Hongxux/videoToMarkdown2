package com.mvp.module2.fusion.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.FusionOrchestratorApplication;
import com.mvp.module2.fusion.service.PersonaAwareReadingService;
import com.mvp.module2.fusion.service.PersonaInsightCardService;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 生产链质量验证入口：
 * 1) 走 persona-reading 真实解析
 * 2) 走 PersonaInsightCardService 真实卡片渲染与落盘
 * 3) 导出一份简要报告，便于人工核对“输入/输出/渲染结果”
 */
public class ProdChainCardQualityRunner {
    public static void main(String[] args) throws Exception {
        String taskId = args.length >= 1 ? args[0] : "storage:c786a1956e66ba020dfb2ed46a3b0c3c";
        String markdownPathRaw = args.length >= 2
                ? args[1]
                : "var/storage/storage/c786a1956e66ba020dfb2ed46a3b0c3c/enhanced_output.md";
        String userId = args.length >= 3 ? args[2] : "prod_chain_quality";
        int waitSeconds = Integer.parseInt(System.getProperty("prod.chain.wait-seconds", "420"));

        Path markdownPath = Paths.get(markdownPathRaw).toAbsolutePath().normalize();
        String markdown = Files.readString(markdownPath, StandardCharsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        try (ConfigurableApplicationContext ctx = new SpringApplicationBuilder(FusionOrchestratorApplication.class)
                .properties(
                        "spring.main.web-application-type=none",
                        "logging.level.root=WARN"
                )
                .run()) {
            PersonaAwareReadingService readingService = ctx.getBean(PersonaAwareReadingService.class);
            PersonaInsightCardService insightCardService = ctx.getBean(PersonaInsightCardService.class);

            PersonaAwareReadingService.PersonalizedReadingPayload payload =
                    readingService.loadOrCompute(taskId, userId, markdownPath, markdown);
            insightCardService.generateAsync(taskId, userId, markdownPath, payload.nodes);

            Map<String, Object> index = Map.of();
            for (int i = 0; i < Math.max(1, waitSeconds); i += 1) {
                Thread.sleep(1000L);
                index = insightCardService.loadIndexSnapshot(taskId, markdownPath);
                if (!index.isEmpty()) {
                    break;
                }
            }

            Path taskRoot = markdownPath.getParent();
            Path reportPath = taskRoot.resolve(".mobile_persona_cache")
                    .resolve("insight_cards")
                    .resolve("quality_test")
                    .resolve("prod_chain_quality_report.json")
                    .normalize();
            Files.createDirectories(reportPath.getParent());

            Map<String, Object> report = new LinkedHashMap<>();
            report.put("generatedAt", Instant.now().toString());
            report.put("taskId", taskId);
            report.put("userId", userId);
            report.put("markdownPath", markdownPath.toString());
            report.put("nodeCount", payload.nodes == null ? 0 : payload.nodes.size());
            report.put("personaReadingSource", payload.source);
            report.put("personaReadingCachePath", payload.cachePath);
            report.put("personaReadingCacheScope", payload.cacheScope);
            report.put("personaReadingChunkStrategy", payload.chunkStrategy);
            report.put("indexSnapshot", index);
            report.put("indexPresent", !index.isEmpty());

            Files.writeString(
                    reportPath,
                    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report),
                    StandardCharsets.UTF_8
            );
            System.out.println("PROD_CHAIN_REPORT=" + reportPath);
            System.out.println("INDEX_PRESENT=" + (!index.isEmpty()));
            if (index.containsKey("indexPath")) {
                System.out.println("INDEX_PATH=" + index.get("indexPath"));
            }
        }
    }
}
