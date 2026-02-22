package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

@Service
public class MacroPersonaForgeService {
    private static final Logger logger = LoggerFactory.getLogger(MacroPersonaForgeService.class);
    private static final Pattern UNSAFE_PATH_SEGMENT = Pattern.compile("[^A-Za-z0-9._-]");
    private static final int DEFAULT_SCORE = 50;
    private static final List<String> DEEP_MATRIX_KEYS = List.of(
            "tech_depth",
            "commercial_acumen",
            "first_principle",
            "information_density",
            "tolerance_for_ambiguity",
            "design_aesthetics",
            "system_thinking",
            "pragmatism",
            "emotional_resonance",
            "execution_bias"
    );

    private static final List<Map<String, String>> DIMENSION_DEFS = List.of(
            Map.of("id", "technical_depth", "name", "技术深潜偏好"),
            Map.of("id", "product_philosophy", "name", "产品哲学关注"),
            Map.of("id", "execution_pragmatism", "name", "执行务实倾向"),
            Map.of("id", "abstraction_preference", "name", "抽象建模倾向"),
            Map.of("id", "detail_patience", "name", "细节耐心强度"),
            Map.of("id", "business_sensitivity", "name", "商业价值敏感度"),
            Map.of("id", "risk_tolerance", "name", "风险容忍区间"),
            Map.of("id", "learning_curiosity", "name", "学习探索驱动力"),
            Map.of("id", "expression_structure", "name", "表达结构化程度"),
            Map.of("id", "systems_thinking", "name", "系统性思维强度")
    );

    private static final String DEFAULT_FORGE_SYSTEM_PROMPT = String.join("\n",
            "你是人格审判庭，只输出 JSON 对象。",
            "输出必须包含 dimensions(10项) 与 evolution_verdict。",
            "禁止输出解释过程和 markdown。"
    );

    private static final String DEFAULT_FORGE_USER_PROMPT = String.join("\n",
            "当前十维画像：",
            "{current_profile}",
            "",
            "近期微观假说切片：",
            "{hypothesis_slices}",
            "",
            "任务：交叉比对漂移点，更新十维 score(0~100)+description，并给出进化论断。"
    );

    @Value("${telemetry.macro-forge.enabled:true}")
    private boolean enabled;
    @Value("${telemetry.macro-forge.base-url:https://api.deepseek.com/v1}")
    private String baseUrl;
    @Value("${telemetry.macro-forge.model:deepseek-v3}")
    private String model;
    @Value("${telemetry.macro-forge.api-key:${DEEPSEEK_API_KEY:}}")
    private String apiKey;
    @Value("${telemetry.macro-forge.timeout-seconds:90}")
    private int timeoutSeconds;
    @Value("${telemetry.macro-forge.hypothesis-threshold:30}")
    private int hypothesisThreshold;
    @Value("${telemetry.macro-forge.interval-days:3}")
    private int intervalDays;
    @Value("${telemetry.macro-forge.core-articles-threshold:5}")
    private int coreArticlesThreshold;
    @Value("${telemetry.macro-forge.max-slices-per-forge:180}")
    private int maxSlicesPerForge;
    @Value("${telemetry.micro-hypothesis.cache-root:var/telemetry/cognitive-cache}")
    private String cacheRoot;
    @Value("${telemetry.macro-forge.prompt.system-resource:classpath:prompts/telemetry/macro-forge/system-zh.txt}")
    private Resource systemPromptResource;
    @Value("${telemetry.macro-forge.prompt.user-resource:classpath:prompts/telemetry/macro-forge/user-zh.txt}")
    private Resource userPromptResource;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(8)).build();
    private final ConcurrentHashMap<String, Object> userLocks = new ConcurrentHashMap<>();
    private final Map<String, String> promptTemplateCache = new ConcurrentHashMap<>();

    @Autowired(required = false)
    private TelemetryLlmInteractionLogService telemetryLlmInteractionLogService;

    public void maybeForge(String rawUserKey) {
        if (!enabled) return;
        String userKey = normalizeUserKey(rawUserKey);
        Object lock = userLocks.computeIfAbsent(userKey, ignored -> new Object());
        synchronized (lock) {
            try {
                doMaybeForge(userKey);
            } catch (Exception ex) {
                logger.warn("macro forge failed: userKey={} err={}", userKey, ex.getMessage());
            }
        }
    }

    private void doMaybeForge(String userKey) throws Exception {
        Path userDir = resolveUserDir(userKey);
        Path cognitivePath = userDir.resolve("cognitive_cache.ndjson");
        if (!Files.isRegularFile(cognitivePath)) return;

        ForgeState state = loadState(userDir.resolve("macro_forge_state.json"));
        List<CognitiveEntry> pendingEntries = loadPendingEntries(cognitivePath, state.lastConsumedAt);
        if (pendingEntries.isEmpty()) return;

        List<Map<String, String>> pendingSlices = collectPendingSlices(pendingEntries, maxSlicesPerForge);
        int pendingSliceCount = pendingSlices.size();
        int pendingCoreArticles = countPendingCoreArticles(pendingEntries);
        long daysSinceLastForge = state.lastForgeAt == null ? Long.MAX_VALUE
                : ChronoUnit.DAYS.between(state.lastForgeAt, Instant.now());

        boolean shouldForge = pendingSliceCount >= Math.max(1, hypothesisThreshold)
                || pendingCoreArticles >= Math.max(1, coreArticlesThreshold)
                || (daysSinceLastForge >= Math.max(1, intervalDays) && pendingSliceCount > 0);
        if (!shouldForge) return;

        Map<String, Object> currentProfile = loadCurrentProfile(userDir.resolve("persona_10d.json"), userKey);
        Map<String, Object> forged = forgeByLlm(userKey, currentProfile, pendingSlices);
        String source = "llm";
        if (forged == null) {
            forged = forgeByHeuristic(currentProfile, pendingSlices);
            source = "heuristic";
        }
        if (forged == null) return;

        forged.put("userKey", userKey);
        forged.put("updatedAt", Instant.now().toString());
        Files.writeString(
                userDir.resolve("persona_10d.json"),
                objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(forged),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING
        );

        Map<String, Object> history = new LinkedHashMap<>();
        history.put("userKey", userKey);
        history.put("generatedAt", Instant.now().toString());
        history.put("source", source);
        history.put("sliceCount", pendingSliceCount);
        history.put("coreArticleCount", pendingCoreArticles);
        history.put("evolutionVerdict", String.valueOf(forged.getOrDefault("evolution_verdict", "")));
        history.put("dimensions", forged.get("dimensions"));
        Files.writeString(
                userDir.resolve("macro_forge_history.ndjson"),
                objectMapper.writeValueAsString(history) + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND
        );

        state.lastForgeAt = Instant.now();
        state.lastConsumedAt = pendingEntries.get(pendingEntries.size() - 1).generatedAt;
        state.totalForgeCount += 1;
        state.lastSource = source;
        saveState(userDir.resolve("macro_forge_state.json"), state);
    }

    private Map<String, Object> forgeByLlm(
            String userKey,
            Map<String, Object> currentProfile,
            List<Map<String, String>> pendingSlices
    ) {
        String endpoint = normalizeEndpoint(baseUrl);
        String resolvedModel = DeepSeekModelRouter.resolveModel(model);
        Map<String, Object> interaction = new LinkedHashMap<>();
        Instant startedAt = Instant.now();
        interaction.put("status", "INIT");
        interaction.put("model", valueToString(model).trim());
        interaction.put("resolvedModel", resolvedModel);
        interaction.put("endpoint", endpoint);
        interaction.put("sliceCount", pendingSlices == null ? 0 : pendingSlices.size());
        if (!StringUtils.hasText(endpoint) || !StringUtils.hasText(resolvedModel)) {
            interaction.put("status", "SKIPPED_CONFIG");
            interaction.put("error", "missing endpoint or model");
            persistLlmInteractionAsync(userKey, interaction, startedAt);
            return null;
        }
        try {
            String systemPrompt = buildForgeSystemPrompt();
            String userPrompt = buildForgeUserPrompt(currentProfile, pendingSlices);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("model", resolvedModel);
            payload.put("temperature", 0.35);
            payload.put("max_tokens", 1500);
            payload.put("stream", false);
            payload.put("messages", List.of(
                    Map.of("role", "system", "content", systemPrompt),
                    Map.of("role", "user", "content", userPrompt)
            ));
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(URI.create(endpoint + "/chat/completions"))
                    .timeout(Duration.ofSeconds(Math.max(12, timeoutSeconds)))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)));
            if (StringUtils.hasText(apiKey)) {
                requestBuilder.header("Authorization", "Bearer " + apiKey.trim());
            }
            interaction.put("requestBody", payload);
            HttpResponse<String> response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
            interaction.put("httpStatus", response.statusCode());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                interaction.put("status", "HTTP_ERROR");
                interaction.put("responseBodyPreview", summarizeBody(response.body()));
                return null;
            }
            JsonNode root = objectMapper.readTree(response.body());
            String content = root.path("choices").path(0).path("message").path("content").asText("");
            interaction.put("responseBodyPreview", summarizeBody(content));
            String objectText = extractJsonObject(content);
            if (objectText == null) {
                interaction.put("status", "PARSE_EMPTY");
                return null;
            }
            Map<String, Object> parsed = objectMapper.readValue(objectText, new TypeReference<Map<String, Object>>() {});
            Map<String, Object> normalized = normalizeProfile(parsed, currentProfile);
            interaction.put("status", "OK");
            interaction.put("parsedProfile", normalized);
            return normalized;
        } catch (Exception ex) {
            logger.warn("macro forge llm exception: {}", ex.getMessage());
            interaction.put("status", "EXCEPTION");
            interaction.put("error", ex.getMessage());
            return null;
        } finally {
            persistLlmInteractionAsync(userKey, interaction, startedAt);
        }
    }

    private String buildForgeSystemPrompt() {
        return loadPromptTemplate("macro_system", systemPromptResource, DEFAULT_FORGE_SYSTEM_PROMPT);
    }

    private String buildForgeUserPrompt(Map<String, Object> currentProfile, List<Map<String, String>> pendingSlices)
            throws Exception {
        String profileJson = objectMapper.writeValueAsString(currentProfile);
        String slicesJson = objectMapper.writeValueAsString(pendingSlices);
        return loadPromptTemplate("macro_user", userPromptResource, DEFAULT_FORGE_USER_PROMPT)
                .replace("{current_profile}", profileJson)
                .replace("{hypothesis_slices}", slicesJson);
    }

    private String loadPromptTemplate(String cacheKey, Resource resource, String fallback) {
        return promptTemplateCache.computeIfAbsent(cacheKey, key -> readPromptTemplate(resource, fallback, cacheKey));
    }

    private String readPromptTemplate(Resource resource, String fallback, String templateName) {
        if (resource == null || !resource.exists()) {
            logger.warn("macro forge prompt missing ({}), fallback to default", templateName);
            return fallback;
        }
        try (InputStream input = resource.getInputStream()) {
            String template = StreamUtils.copyToString(input, StandardCharsets.UTF_8).trim();
            if (StringUtils.hasText(template)) {
                return template;
            }
            logger.warn("macro forge prompt empty ({}), fallback to default", templateName);
        } catch (IOException ex) {
            logger.warn("macro forge prompt load failed ({}): {}", templateName, ex.getMessage());
        }
        return fallback;
    }

    private Map<String, Object> forgeByHeuristic(Map<String, Object> currentProfile, List<Map<String, String>> pendingSlices) {
        Map<String, Object> profile = normalizeProfile(currentProfile, currentProfile);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> dims = (List<Map<String, Object>>) profile.get("dimensions");
        int deleted = 0, resonance = 0, notes = 0, lexical = 0;
        for (Map<String, String> row : pendingSlices) {
            String action = row.getOrDefault("action", "");
            if ("DELETED".equals(action)) deleted++;
            else if ("RESONANCE".equals(action)) resonance++;
            else if ("NOTE_SAVED".equals(action)) notes++;
            else if ("LEXICAL_CARD_OPENED".equals(action)) lexical++;
        }
        adjust(dims, "detail_patience", resonance + notes + lexical - deleted);
        adjust(dims, "technical_depth", notes + lexical - deleted / 2);
        adjust(dims, "product_philosophy", resonance + notes / 2);
        adjust(dims, "business_sensitivity", resonance);
        adjust(dims, "learning_curiosity", lexical + resonance);
        profile.put("evolution_verdict", (resonance + notes + lexical > deleted + 2)
                ? "近期从被动阅读转向主动建模，核心关注正在收敛。"
                : "近期画像出现结构性漂移，已按行为证据完成重塑。");
        profile.put("deep_soul_matrix", buildDeepSoulMatrixFromDimensions(dims));
        return profile;
    }

    private void adjust(List<Map<String, Object>> dims, String id, int deltaUnit) {
        if (deltaUnit == 0) return;
        for (Map<String, Object> dim : dims) {
            if (!id.equals(String.valueOf(dim.getOrDefault("id", "")))) continue;
            int score = readInt(dim.get("score"), DEFAULT_SCORE);
            score = Math.max(0, Math.min(100, score + deltaUnit * 2));
            dim.put("score", score);
            dim.put("description", "该维度当前评估为 " + score + " 分，已依据近期微观反馈更新。");
            return;
        }
    }

    private Map<String, Object> loadCurrentProfile(Path profilePath, String userKey) {
        try {
            if (Files.isRegularFile(profilePath)) {
                Map<String, Object> parsed = objectMapper.readValue(
                        Files.readString(profilePath, StandardCharsets.UTF_8),
                        new TypeReference<Map<String, Object>>() {}
                );
                return normalizeProfile(parsed, parsed);
            }
        } catch (Exception ex) {
            logger.warn("load profile failed: {}", ex.getMessage());
        }
        Map<String, Object> base = new LinkedHashMap<>();
        base.put("userKey", userKey);
        List<Map<String, Object>> dims = new ArrayList<>();
        for (Map<String, String> def : DIMENSION_DEFS) {
            dims.add(new LinkedHashMap<>(Map.of(
                    "id", def.get("id"),
                    "name", def.get("name"),
                    "score", DEFAULT_SCORE,
                    "description", "初始化中性状态，等待更多行为信号。"
            )));
        }
        base.put("dimensions", dims);
        base.put("evolution_verdict", "初始画像已建立。");
        base.put("surface_context", defaultSurfaceContext());
        base.put("deep_soul_matrix", buildDeepSoulMatrixFromDimensions(dims));
        return base;
    }

    private Map<String, Object> normalizeProfile(Map<String, Object> candidate, Map<String, Object> fallback) {
        Map<String, Object> source = candidate != null ? candidate : (fallback != null ? fallback : Map.of());
        Map<String, Object> profile = new LinkedHashMap<>();
        List<Map<String, Object>> sourceDims = extractSourceDimensions(source);
        if (sourceDims.isEmpty() && fallback != null && fallback != source) {
            sourceDims = extractSourceDimensions(fallback);
        }
        List<Map<String, Object>> dims = new ArrayList<>();
        for (Map<String, String> def : DIMENSION_DEFS) {
            Map<String, Object> dim = sourceDims.stream()
                    .filter(it -> def.get("id").equals(String.valueOf(it.getOrDefault("id", ""))))
                    .findFirst()
                    .orElseGet(() -> new LinkedHashMap<>(Map.of(
                            "id", def.get("id"), "name", def.get("name"),
                            "score", DEFAULT_SCORE, "description", "该维度保持中性评估。"
                    )));
            dim.put("id", def.get("id"));
            dim.put("name", String.valueOf(dim.getOrDefault("name", def.get("name"))));
            dim.put("score", Math.max(0, Math.min(100, readInt(dim.get("score"), DEFAULT_SCORE))));
            String desc = String.valueOf(dim.getOrDefault("description", "该维度保持中性评估。")).trim();
            dim.put("description", desc.isBlank() ? "该维度保持中性评估。" : desc);
            dims.add(dim);
        }
        profile.put("dimensions", dims);
        profile.put("evolution_verdict", String.valueOf(source.getOrDefault("evolution_verdict", "画像已更新。")));
        profile.put("surface_context", extractSurfaceContext(source, fallback));
        profile.put("deep_soul_matrix", extractDeepSoulMatrix(source, fallback, dims));
        return profile;
    }

    private List<Map<String, Object>> extractSourceDimensions(Map<String, Object> source) {
        List<Map<String, Object>> sourceDims = parseDimensionsList(source.get("dimensions"));
        if (!sourceDims.isEmpty()) {
            return sourceDims;
        }
        Object deepRaw = source.get("deep_soul_matrix");
        if (!(deepRaw instanceof Map<?, ?> deepMatrix)) {
            return List.of();
        }
        List<Map<String, Object>> output = new ArrayList<>();
        for (Map<String, String> def : DIMENSION_DEFS) {
            Map<String, Object> extracted = extractDimFromDeepMatrix(def, deepMatrix);
            if (extracted != null) {
                output.add(extracted);
            }
        }
        return output;
    }

    private List<Map<String, Object>> parseDimensionsList(Object raw) {
        List<Map<String, Object>> sourceDims = new ArrayList<>();
        if (!(raw instanceof List<?> list)) {
            return sourceDims;
        }
        for (Object row : list) {
            if (row instanceof Map<?, ?> map) {
                Map<String, Object> dim = new LinkedHashMap<>();
                dim.put("id", valueToString(map.get("id")));
                dim.put("name", valueToString(map.get("name")));
                dim.put("score", readInt(map.get("score"), DEFAULT_SCORE));
                dim.put("description", valueToString(map.get("description")));
                sourceDims.add(dim);
            }
        }
        return sourceDims;
    }

    private Map<String, Object> extractDimFromDeepMatrix(Map<String, String> def, Map<?, ?> deepMatrix) {
        String id = def.get("id");
        for (String alias : dimensionSourceAliases(id)) {
            Object raw = readByNormalizedKey(deepMatrix, alias);
            if (raw == null) {
                continue;
            }
            Map<String, Object> dim = new LinkedHashMap<>();
            dim.put("id", id);
            dim.put("name", def.get("name"));
            if (raw instanceof Map<?, ?> map) {
                dim.put("score", readInt(map.get("score"), DEFAULT_SCORE));
                dim.put("description", valueToString(map.get("description")));
            } else {
                dim.put("score", readInt(raw, DEFAULT_SCORE));
                dim.put("description", "该维度来自 deep_soul_matrix 映射。");
            }
            return dim;
        }
        return null;
    }

    private List<String> dimensionSourceAliases(String id) {
        return switch (id) {
            case "technical_depth" -> List.of("technical_depth", "tech_depth");
            case "product_philosophy" -> List.of("product_philosophy", "commercial_acumen", "emotional_resonance");
            case "execution_pragmatism" -> List.of("execution_pragmatism", "pragmatism", "execution_bias");
            case "abstraction_preference" -> List.of("abstraction_preference", "first_principle", "system_thinking");
            case "detail_patience" -> List.of("detail_patience", "information_density");
            case "business_sensitivity" -> List.of("business_sensitivity", "commercial_acumen");
            case "risk_tolerance" -> List.of("risk_tolerance", "tolerance_for_ambiguity");
            case "learning_curiosity" -> List.of("learning_curiosity", "first_principle", "execution_bias");
            case "expression_structure" -> List.of("expression_structure", "design_aesthetics");
            case "systems_thinking" -> List.of("systems_thinking", "system_thinking");
            default -> List.of(id);
        };
    }

    private Map<String, Object> extractSurfaceContext(Map<String, Object> source, Map<String, Object> fallback) {
        Map<String, Object> surface = parseSurfaceContext(source.get("surface_context"));
        if (!surface.isEmpty()) {
            return surface;
        }
        if (fallback != null && fallback != source) {
            surface = parseSurfaceContext(fallback.get("surface_context"));
            if (!surface.isEmpty()) {
                return surface;
            }
        }
        return defaultSurfaceContext();
    }

    private Map<String, Object> parseSurfaceContext(Object raw) {
        if (!(raw instanceof Map<?, ?> map)) {
            return Map.of();
        }
        Map<String, Object> surface = new LinkedHashMap<>();
        surface.put("profession", normalizeStringList(readByNormalizedKey(map, "profession")));
        surface.put("skillset", normalizeStringList(readByNormalizedKey(map, "skillset")));
        surface.put("current_challenges", normalizeStringList(
                firstNonNull(readByNormalizedKey(map, "current_challenges"), readByNormalizedKey(map, "challenges"))
        ));
        return surface;
    }

    private Map<String, Object> defaultSurfaceContext() {
        Map<String, Object> surface = new LinkedHashMap<>();
        surface.put("profession", List.of());
        surface.put("skillset", List.of());
        surface.put("current_challenges", List.of());
        return surface;
    }

    private Map<String, Object> extractDeepSoulMatrix(
            Map<String, Object> source,
            Map<String, Object> fallback,
            List<Map<String, Object>> dims
    ) {
        Map<String, Object> deep = parseDeepSoulMatrix(source.get("deep_soul_matrix"));
        if (!deep.isEmpty()) {
            return deep;
        }
        if (fallback != null && fallback != source) {
            deep = parseDeepSoulMatrix(fallback.get("deep_soul_matrix"));
            if (!deep.isEmpty()) {
                return deep;
            }
        }
        return buildDeepSoulMatrixFromDimensions(dims);
    }

    private Map<String, Object> parseDeepSoulMatrix(Object raw) {
        if (!(raw instanceof Map<?, ?> map)) {
            return Map.of();
        }
        Map<String, Object> deep = new LinkedHashMap<>();
        for (String key : DEEP_MATRIX_KEYS) {
            Object entry = readByNormalizedKey(map, key);
            if (entry instanceof Map<?, ?> metric) {
                Map<String, Object> normalized = new LinkedHashMap<>();
                normalized.put("score", Math.max(0, Math.min(100, readInt(metric.get("score"), DEFAULT_SCORE))));
                String desc = valueToString(metric.get("description")).trim();
                normalized.put("description", desc.isBlank() ? "该维度保持中性评估。" : desc);
                deep.put(key, normalized);
            }
        }
        return deep;
    }

    private Map<String, Object> buildDeepSoulMatrixFromDimensions(List<Map<String, Object>> dims) {
        Map<String, Object> byId = new LinkedHashMap<>();
        for (Map<String, Object> dim : dims) {
            byId.put(String.valueOf(dim.getOrDefault("id", "")), dim);
        }
        Map<String, Object> deep = new LinkedHashMap<>();
        for (String key : DEEP_MATRIX_KEYS) {
            Map<String, Object> source = resolveDeepMatrixSourceFromDims(byId, key);
            Map<String, Object> metric = new LinkedHashMap<>();
            metric.put("score", Math.max(0, Math.min(100, readInt(source.get("score"), DEFAULT_SCORE))));
            String desc = valueToString(source.get("description")).trim();
            metric.put("description", desc.isBlank() ? "该维度保持中性评估。" : desc);
            deep.put(key, metric);
        }
        return deep;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> resolveDeepMatrixSourceFromDims(Map<String, Object> byId, String deepKey) {
        String dimId = switch (deepKey) {
            case "tech_depth" -> "technical_depth";
            case "commercial_acumen" -> "business_sensitivity";
            case "first_principle" -> "abstraction_preference";
            case "information_density" -> "detail_patience";
            case "tolerance_for_ambiguity" -> "risk_tolerance";
            case "design_aesthetics" -> "expression_structure";
            case "system_thinking" -> "systems_thinking";
            case "pragmatism" -> "execution_pragmatism";
            case "emotional_resonance" -> "product_philosophy";
            case "execution_bias" -> "learning_curiosity";
            default -> "";
        };
        Object raw = byId.get(dimId);
        if (raw instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return Map.of("score", DEFAULT_SCORE, "description", "该维度保持中性评估。");
    }

    private ForgeState loadState(Path statePath) {
        ForgeState state = new ForgeState();
        if (!Files.isRegularFile(statePath)) return state;
        try {
            Map<String, Object> root = objectMapper.readValue(
                    Files.readString(statePath, StandardCharsets.UTF_8),
                    new TypeReference<Map<String, Object>>() {}
            );
            state.lastForgeAt = parseInstant(String.valueOf(root.getOrDefault("lastForgeAt", "")));
            state.lastConsumedAt = parseInstant(String.valueOf(root.getOrDefault("lastConsumedAt", "")));
            state.totalForgeCount = readInt(root.get("totalForgeCount"), 0);
            state.lastSource = String.valueOf(root.getOrDefault("lastSource", ""));
        } catch (Exception ex) {
            logger.warn("load forge state failed: {}", ex.getMessage());
        }
        return state;
    }

    private void saveState(Path statePath, ForgeState state) {
        try {
            Map<String, Object> root = new LinkedHashMap<>();
            root.put("lastForgeAt", state.lastForgeAt != null ? state.lastForgeAt.toString() : "");
            root.put("lastConsumedAt", state.lastConsumedAt != null ? state.lastConsumedAt.toString() : "");
            root.put("totalForgeCount", state.totalForgeCount);
            root.put("lastSource", state.lastSource);
            Files.writeString(
                    statePath,
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING
            );
        } catch (Exception ex) {
            logger.warn("save forge state failed: {}", ex.getMessage());
        }
    }

    private List<CognitiveEntry> loadPendingEntries(Path cognitivePath, Instant consumedAt) {
        List<CognitiveEntry> rows = new ArrayList<>();
        try {
            for (String line : Files.readAllLines(cognitivePath, StandardCharsets.UTF_8)) {
                String trimmed = line == null ? "" : line.trim();
                if (trimmed.isEmpty()) continue;
                Map<String, Object> root = objectMapper.readValue(trimmed, new TypeReference<Map<String, Object>>() {});
                Instant generatedAt = parseInstant(String.valueOf(root.getOrDefault("generatedAt", "")));
                if (generatedAt == null) continue;
                if (consumedAt != null && !generatedAt.isAfter(consumedAt)) continue;
                List<Map<String, String>> items = parseItems(root.get("items"));
                if (items.isEmpty()) continue;
                CognitiveEntry entry = new CognitiveEntry();
                entry.generatedAt = generatedAt;
                entry.taskId = String.valueOf(root.getOrDefault("taskId", ""));
                entry.items = items;
                rows.add(entry);
            }
        } catch (Exception ex) {
            logger.warn("load pending entries failed: {}", ex.getMessage());
        }
        rows.sort(Comparator.comparing(it -> it.generatedAt));
        return rows;
    }

    private List<Map<String, String>> parseItems(Object rawItems) {
        List<Map<String, String>> items = new ArrayList<>();
        if (!(rawItems instanceof List<?> list)) return items;
        for (Object row : list) {
            if (!(row instanceof Map<?, ?> map)) continue;
            String action = normalizeAction(valueToString(map.get("action")));
            String type = valueToString(map.get("content_type")).trim();
            String hypothesis = valueToString(map.get("inferred_hypothesis")).trim();
            if (action.isEmpty() || type.isEmpty() || hypothesis.isEmpty()) continue;
            items.add(new LinkedHashMap<>(Map.of(
                    "action", action,
                    "content_type", type,
                    "inferred_hypothesis", hypothesis
            )));
        }
        return items;
    }

    private List<Map<String, String>> collectPendingSlices(List<CognitiveEntry> entries, int limit) {
        List<Map<String, String>> items = new ArrayList<>();
        int cap = Math.max(20, limit);
        for (CognitiveEntry entry : entries) {
            for (Map<String, String> row : entry.items) {
                items.add(row);
                if (items.size() >= cap) return items;
            }
        }
        return items;
    }

    private int countPendingCoreArticles(List<CognitiveEntry> entries) {
        Set<String> keys = new LinkedHashSet<>();
        for (CognitiveEntry entry : entries) {
            boolean core = entry.items.stream().anyMatch(it ->
                    "RESONANCE".equals(it.get("action"))
                            || "NOTE_SAVED".equals(it.get("action"))
                            || "LEXICAL_CARD_OPENED".equals(it.get("action")));
            if (!core) continue;
            String key = entry.taskId == null || entry.taskId.isBlank() ? entry.generatedAt.toString() : entry.taskId;
            keys.add(key);
        }
        return keys.size();
    }

    private String normalizeAction(String action) {
        String normalized = action == null ? "" : action.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "DELETED", "RESONANCE", "NOTE_SAVED", "LEXICAL_CARD_OPENED", "COLD_SIGNAL" -> normalized;
            default -> "";
        };
    }

    private Path resolveUserDir(String userKey) throws Exception {
        Path root = Paths.get(cacheRoot).toAbsolutePath().normalize();
        Path userDir = root.resolve(userKey).normalize();
        if (!userDir.startsWith(root)) throw new IllegalStateException("invalid user path");
        Files.createDirectories(userDir);
        return userDir;
    }

    private String normalizeUserKey(String raw) {
        String userKey = (raw == null ? "" : raw.trim());
        if (userKey.isBlank()) userKey = "anonymous";
        userKey = UNSAFE_PATH_SEGMENT.matcher(userKey).replaceAll("_").replaceAll("_+", "_");
        return userKey.isBlank() ? "anonymous" : userKey;
    }

    private String normalizeEndpoint(String raw) {
        String endpoint = raw == null ? "" : raw.trim();
        if (endpoint.endsWith("/")) endpoint = endpoint.substring(0, endpoint.length() - 1);
        if (!endpoint.isEmpty() && !endpoint.matches("(?i).*/v\\d+$")) endpoint += "/v1";
        return endpoint;
    }

    private String extractJsonObject(String text) {
        if (text == null) return null;
        int start = text.indexOf('{');
        if (start < 0) return null;
        int depth = 0;
        for (int i = start; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '{') depth++;
            else if (c == '}') {
                depth--;
                if (depth == 0) return text.substring(start, i + 1);
            }
        }
        return null;
    }

    private Instant parseInstant(String text) {
        try {
            String v = text == null ? "" : text.trim();
            return v.isEmpty() ? null : Instant.parse(v);
        } catch (Exception ex) {
            return null;
        }
    }

    private int readInt(Object raw, int fallback) {
        try {
            return Integer.parseInt(String.valueOf(raw));
        } catch (Exception ex) {
            return fallback;
        }
    }

    private List<String> normalizeStringList(Object raw) {
        List<String> output = new ArrayList<>();
        if (raw instanceof List<?> list) {
            for (Object item : list) {
                String value = valueToString(item).trim();
                if (!value.isBlank()) {
                    output.add(value);
                }
            }
            return output;
        }
        String text = valueToString(raw).trim();
        if (!text.isBlank()) {
            output.add(text);
        }
        return output;
    }

    private Object firstNonNull(Object first, Object second) {
        return first != null ? first : second;
    }

    private Object readByNormalizedKey(Map<?, ?> source, String targetKey) {
        if (source == null || source.isEmpty()) {
            return null;
        }
        if (source.containsKey(targetKey)) {
            return source.get(targetKey);
        }
        String normalizedTarget = normalizeKey(targetKey);
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            if (normalizedTarget.equals(normalizeKey(String.valueOf(entry.getKey())))) {
                return entry.getValue();
            }
        }
        return null;
    }

    private String normalizeKey(String key) {
        if (key == null) {
            return "";
        }
        return key.replaceAll("[\\s_\\-]", "").toLowerCase(Locale.ROOT);
    }

    private String valueToString(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private String summarizeBody(String body) {
        String text = body == null ? "" : body.replace('\n', ' ').trim();
        return text.length() <= 220 ? text : text.substring(0, 220) + "...";
    }

    private void persistLlmInteractionAsync(String userKey, Map<String, Object> interaction, Instant startedAt) {
        if (telemetryLlmInteractionLogService == null || interaction == null) {
            return;
        }
        interaction.put("durationMs", Duration.between(startedAt, Instant.now()).toMillis());
        telemetryLlmInteractionLogService.appendAsync(
                "macro_forge",
                normalizeUserKey(userKey),
                "",
                interaction
        );
    }

    private static class ForgeState {
        private Instant lastForgeAt;
        private Instant lastConsumedAt;
        private int totalForgeCount;
        private String lastSource = "";
    }

    private static class CognitiveEntry {
        private Instant generatedAt;
        private String taskId;
        private List<Map<String, String>> items = new ArrayList<>();
    }
}
