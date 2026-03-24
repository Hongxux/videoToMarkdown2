package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

@Service
public class TaskCostSummaryService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public Optional<TaskCostSummary> readSummary(Path taskDir) {
        if (taskDir == null) {
            return Optional.empty();
        }
        Path metricsPath = taskDir.resolve("intermediates").resolve("task_metrics_latest.json");
        if (!Files.isRegularFile(metricsPath)) {
            return Optional.empty();
        }
        try {
            JsonNode root = objectMapper.readTree(metricsPath.toFile());
            JsonNode llmCost = root.path("llm_cost");
            if (!llmCost.isObject()) {
                return Optional.empty();
            }
            ResolvedCost resolved = resolveTotalCost(llmCost);
            if (resolved.totalCost == null) {
                return Optional.empty();
            }
            String updatedAtText = trimToEmpty(root.path("generated_at").asText(""));
            Instant updatedAt = parseInstant(updatedAtText);
            if (updatedAt == null) {
                updatedAt = Files.getLastModifiedTime(metricsPath).toInstant();
                updatedAtText = updatedAt.toString();
            }
            return Optional.of(new TaskCostSummary(
                    resolved.currency,
                    resolved.totalCost,
                    formatDisplayText(resolved.currency, resolved.totalCost),
                    updatedAtText,
                    metricsPath.toAbsolutePath().toString()
            ));
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    private ResolvedCost resolveTotalCost(JsonNode llmCost) {
        String currency = trimToEmpty(llmCost.path("currency").asText(""));
        Double totalCost = firstDouble(llmCost, "total_cost");
        if (totalCost != null) {
            return new ResolvedCost(currency, totalCost);
        }

        JsonNode vlNode = llmCost.path("vl");
        JsonNode deepNode = llmCost.path("deepseek_chat");
        String vlCurrency = trimToEmpty(vlNode.path("currency").asText(currency));
        String deepCurrency = trimToEmpty(deepNode.path("currency").asText(currency));
        Double vlTotal = firstDouble(vlNode, "total_cost");
        Double deepTotal = firstDouble(deepNode, "total_cost");

        if (vlTotal != null && deepTotal != null && !vlCurrency.isBlank() && vlCurrency.equalsIgnoreCase(deepCurrency)) {
            return new ResolvedCost(vlCurrency, vlTotal + deepTotal);
        }
        if (vlTotal != null) {
            return new ResolvedCost(!vlCurrency.isBlank() ? vlCurrency : currency, vlTotal);
        }
        if (deepTotal != null) {
            return new ResolvedCost(!deepCurrency.isBlank() ? deepCurrency : currency, deepTotal);
        }
        return new ResolvedCost(currency, null);
    }

    private Double firstDouble(JsonNode node, String... fieldNames) {
        if (node == null || fieldNames == null) {
            return null;
        }
        for (String fieldName : fieldNames) {
            if (fieldName == null || fieldName.isBlank()) {
                continue;
            }
            JsonNode valueNode = node.get(fieldName);
            if (valueNode == null || valueNode.isMissingNode() || valueNode.isNull()) {
                continue;
            }
            if (valueNode.isIntegralNumber() || valueNode.isFloatingPointNumber()) {
                return Math.max(0d, valueNode.asDouble(0d));
            }
            String text = trimToEmpty(valueNode.asText(""));
            if (text.isEmpty()) {
                continue;
            }
            try {
                return Math.max(0d, Double.parseDouble(text));
            } catch (Exception ignored) {
                // Ignore parse errors and continue trying fallback fields.
            }
        }
        return null;
    }

    private Instant parseInstant(String rawValue) {
        try {
            return rawValue == null || rawValue.isBlank() ? null : Instant.parse(rawValue);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
    }

    private String formatDisplayText(String currency, double totalCost) {
        String normalizedCurrency = trimToEmpty(currency).toUpperCase(Locale.ROOT);
        double sanitizedCost = Math.max(0d, totalCost);
        String amount = sanitizedCost < 0.01d
                ? String.format(Locale.ROOT, "%.4f", sanitizedCost)
                : String.format(Locale.ROOT, "%.2f", sanitizedCost);
        if ("CNY".equals(normalizedCurrency)) {
            return "¥" + amount;
        }
        if ("USD".equals(normalizedCurrency)) {
            return "$" + amount;
        }
        if (normalizedCurrency.isBlank()) {
            return amount;
        }
        return normalizedCurrency + " " + amount;
    }

    private record ResolvedCost(String currency, Double totalCost) {
    }

    public record TaskCostSummary(
            String currency,
            double totalCost,
            String displayText,
            String updatedAt,
            String sourcePath
    ) {
        public Map<String, Object> toPayload() {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("currency", currency != null ? currency : "");
            payload.put("totalCost", totalCost);
            payload.put("displayText", displayText != null ? displayText : "");
            payload.put("updatedAt", updatedAt != null ? updatedAt : "");
            payload.put("sourcePath", sourcePath != null ? sourcePath : "");
            return payload;
        }

        public Instant updatedAtInstant() {
            try {
                return updatedAt == null || updatedAt.isBlank() ? null : Instant.parse(updatedAt);
            } catch (Exception ignored) {
                return null;
            }
        }
    }
}
