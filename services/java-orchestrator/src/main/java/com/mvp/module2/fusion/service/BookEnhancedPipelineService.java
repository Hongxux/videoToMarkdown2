package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.service.llm.LlmErrorDescriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 书籍增强链路：
 * 1) 结构保护（图片/代码/表格/公式占位）
 * 2) 英文段落翻译（复用 step3.5 语义）
 * 3) Phase2A 语义分割 + Phase2B 结构化组装
 * 4) 占位符回填，保证图文相对位置不被破坏
 */
@Service
public class BookEnhancedPipelineService {

    private static final Logger logger = LoggerFactory.getLogger(BookEnhancedPipelineService.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Pattern TOKEN_PATTERN = Pattern.compile("\\[\\[SYS_(?:MEDIA|INLINE)_[A-Z0-9]+\\]\\]");
    private static final Pattern INLINE_IMAGE_PATTERN = Pattern.compile("!\\[[^\\]]*\\]\\([^\\)]+\\)|!\\[\\[[^\\]]+\\]\\]");
    private static final Pattern INLINE_CODE_PATTERN = Pattern.compile("`[^`\\n]+`");
    private static final Pattern INLINE_MATH_PATTERN = Pattern.compile("\\$[^$\\n]+\\$");

    private static final Pattern LETTER_PATTERN = Pattern.compile("[A-Za-z]");
    private static final Pattern CJK_PATTERN = Pattern.compile("[\\u4E00-\\u9FFF]");

    private static final String TRANSLATION_SYSTEM_PROMPT =
            "你是中英字幕翻译与母语化改写助手。"
                    + "在忠实原意的前提下，输出自然流畅、口语化的中文表达。"
                    + "对特定名词保留英文原词，优先使用“中文译名（英文原词）”格式，例如“深度求索（deepseek）”。"
                    + "输出必须是可解析 JSON。"
                    + "任何形如 [[SYS_MEDIA_...]] 或 [[SYS_INLINE_...]] 的占位符必须逐字保留，不得改写、丢失、重排。";

    private static final String TRANSLATION_PROMPT_TEMPLATE = """
            请将以下英文句子翻译成中文，并进行中文母语化重写。

            核心要求：请将这段译文按中文母语者的表达习惯进行重写，使其流畅、自然，同时保留原意和专有术语。
            专有名词规则：
            1) 对特定名词必须保留英文原词，建议统一使用“中文译名（英文原词）”格式。
            2) 示例：deepseek 应翻译为“深度求索（deepseek）”。
            3) 若无稳定中文译名，至少保留英文原词，不得丢失。

            占位符规则：
            1) 任何形如 [[SYS_MEDIA_...]] 或 [[SYS_INLINE_...]] 的占位符必须逐字保留。
            2) 不得删除、改写、拆分、合并、移动占位符。

            【句子列表】
            %s

            【字段约束】
            - 只返回句子标识与译文，不要返回其他字段

            【输出格式】只输出 JSON，不要输出其他解释文字：
            {
              "t": [
                {
                  "sid": "S001",
                  "tt": "翻译并重写后的中文句子"
                }
              ]
            }
            """;

    @Autowired
    private PythonGrpcClient grpcClient;

    @Value("${book.enhanced-pipeline.enabled:true}")
    private boolean enhancedEnabled;

    @Value("${book.enhanced-pipeline.translation.enabled:true}")
    private boolean translationEnabled;

    @Value("${book.enhanced-pipeline.translation.base-url:https://api.deepseek.com/v1}")
    private String translationBaseUrl;

    @Value("${book.enhanced-pipeline.translation.model:deepseek-chat}")
    private String translationModel;

    @Value("${book.enhanced-pipeline.translation.timeout-seconds:45}")
    private int translationTimeoutSec;

    @Value("${book.enhanced-pipeline.translation.max-tokens:2048}")
    private int translationMaxTokens;

    @Value("${book.enhanced-pipeline.translation.temperature:0.2}")
    private double translationTemperature;

    @Value("${book.enhanced-pipeline.translation.window-size:30}")
    private int translationWindowSize;

    @Value("${book.enhanced-pipeline.translation.skip-mixed-zh-en:true}")
    private boolean skipMixedChineseEnglish;

    @Value("${book.enhanced-pipeline.phase2a-timeout-seconds:1200}")
    private int phase2aTimeoutSec;

    @Value("${book.enhanced-pipeline.phase2b-timeout-seconds:1800}")
    private int phase2bTimeoutSec;

    @Value("${DEEPSEEK_API_KEY:}")
    private String apiKey;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(8))
            .build();

    public static class EnhancedResult {
        public boolean success;
        public boolean enhancementApplied;
        public boolean translationAttempted;
        public boolean translationApplied;
        public String markdownPath;
        public String jsonPath;
        public String errorMessage;
        public int protectedBlockCount;
        public int translatedBlockCount;
        public int phase2SemanticUnitCount;
        public Map<String, Long> stageTimingsMs = new LinkedHashMap<>();
    }

    private static final class InlineProtectionResult {
        private String protectedText;
        private final LinkedHashMap<String, String> tokenToOriginal = new LinkedHashMap<>();
    }

    private static final class BlockUnit {
        private final int index;
        private String text;
        private final boolean translatable;

        private BlockUnit(int index, String text, boolean translatable) {
            this.index = index;
            this.text = text;
            this.translatable = translatable;
        }
    }

    private static final class TranslateCandidate {
        private final String sid;
        private final int blockIndex;
        private final String originalText;

        private TranslateCandidate(String sid, int blockIndex, String originalText) {
            this.sid = sid;
            this.blockIndex = blockIndex;
            this.originalText = originalText;
        }
    }

    private static final class TranslationOutcome {
        private final List<String> orderedBlocks;
        private final LinkedHashMap<String, String> inlineTokenMap;
        private final int translatedCount;
        private final boolean translationAttempted;
        private final boolean translationApplied;

        private TranslationOutcome(
                List<String> orderedBlocks,
                LinkedHashMap<String, String> inlineTokenMap,
                int translatedCount,
                boolean translationAttempted,
                boolean translationApplied
        ) {
            this.orderedBlocks = orderedBlocks;
            this.inlineTokenMap = inlineTokenMap;
            this.translatedCount = translatedCount;
            this.translationAttempted = translationAttempted;
            this.translationApplied = translationApplied;
        }
    }

    private static final class SyntheticInputPaths {
        private final Path step2Path;
        private final Path step6Path;
        private final Path sentenceTsPath;

        private SyntheticInputPaths(Path step2Path, Path step6Path, Path sentenceTsPath) {
            this.step2Path = step2Path;
            this.step6Path = step6Path;
            this.sentenceTsPath = sentenceTsPath;
        }
    }

    public boolean isEnabled() {
        return enhancedEnabled;
    }

    public boolean isTranslationEnabled() {
        return enhancedEnabled && translationEnabled;
    }

    public EnhancedResult enhanceBook(
            String taskId,
            String sourcePath,
            String outputDir,
            BookMarkdownService.BookProcessingResult baseResult
    ) {
        EnhancedResult result = new EnhancedResult();
        long totalStart = System.currentTimeMillis();

        try {
            if (!enhancedEnabled) {
                result.success = false;
                result.errorMessage = "book.enhanced-pipeline.enabled=false";
                return result;
            }
            if (baseResult == null || !baseResult.success || !StringUtils.hasText(baseResult.markdownPath)) {
                result.success = false;
                result.errorMessage = "base markdown result unavailable";
                return result;
            }

            Path outputRoot = Paths.get(outputDir).toAbsolutePath().normalize();
            Files.createDirectories(outputRoot);
            Path enhancedIntermediateRoot = outputRoot.resolve("intermediates").resolve("book_enhanced");
            Files.createDirectories(enhancedIntermediateRoot);

            long loadStart = System.currentTimeMillis();
            String sourceMarkdown = loadSelectedMarkdown(outputRoot, baseResult.markdownPath);
            result.stageTimingsMs.put("load_markdown", System.currentTimeMillis() - loadStart);
            if (!StringUtils.hasText(sourceMarkdown)) {
                throw new IllegalStateException("empty markdown source for enhancement");
            }

            long protectStart = System.currentTimeMillis();
            BookMarkdownProtectionUtils.ProtectionResult protectionResult =
                    BookMarkdownProtectionUtils.protectMarkdown(sourceMarkdown);
            result.protectedBlockCount = protectionResult.getProtectedCount();
            result.stageTimingsMs.put("protect_blocks", System.currentTimeMillis() - protectStart);

            long translationStart = System.currentTimeMillis();
            TranslationOutcome translationOutcome = translateBlocksIfNeeded(
                    protectionResult,
                    sourceMarkdown,
                    taskId
            );
            result.translatedBlockCount = translationOutcome.translatedCount;
            result.translationAttempted = translationOutcome.translationAttempted;
            result.translationApplied = translationOutcome.translationApplied;
            result.stageTimingsMs.put("translate_blocks", System.currentTimeMillis() - translationStart);

            long synthStart = System.currentTimeMillis();
            SyntheticInputPaths syntheticPaths = writeSyntheticPhase2Inputs(
                    translationOutcome.orderedBlocks,
                    enhancedIntermediateRoot
            );
            result.stageTimingsMs.put("build_phase2_inputs", System.currentTimeMillis() - synthStart);

            long phase2aStart = System.currentTimeMillis();
            PythonGrpcClient.AnalyzeResult analyzeResult = grpcClient.analyzeSemanticUnits(
                    taskId,
                    sourcePath,
                    syntheticPaths.step2Path.toString(),
                    syntheticPaths.step6Path.toString(),
                    syntheticPaths.sentenceTsPath.toString(),
                    outputRoot.toString(),
                    normalizePositive(phase2aTimeoutSec, 1200)
            );
            result.stageTimingsMs.put("phase2a_segmentation", System.currentTimeMillis() - phase2aStart);
            if (analyzeResult == null || !analyzeResult.success) {
                throw new IllegalStateException("phase2a failed: "
                        + firstNonBlank(analyzeResult != null ? analyzeResult.errorMsg : null, "unknown"));
            }
            result.phase2SemanticUnitCount = resolveSemanticUnitCount(analyzeResult);

            Path screenshotsDir = outputRoot.resolve("screenshots");
            Path clipsDir = outputRoot.resolve("video_clips");
            Files.createDirectories(screenshotsDir);
            Files.createDirectories(clipsDir);

            long phase2bStart = System.currentTimeMillis();
            PythonGrpcClient.AssembleResult assembleResult = grpcClient.assembleRichText(
                    taskId,
                    sourcePath,
                    analyzeResult,
                    screenshotsDir.toString(),
                    clipsDir.toString(),
                    outputRoot.toString(),
                    resolveTitle(sourceMarkdown, sourcePath),
                    normalizePositive(phase2bTimeoutSec, 1800)
            );
            result.stageTimingsMs.put("phase2b_assemble", System.currentTimeMillis() - phase2bStart);
            if (assembleResult == null || !assembleResult.success) {
                throw new IllegalStateException("phase2b failed: "
                        + firstNonBlank(assembleResult != null ? assembleResult.errorMsg : null, "unknown"));
            }
            if (!StringUtils.hasText(assembleResult.markdownPath)) {
                throw new IllegalStateException("phase2b markdownPath is empty");
            }

            Path phase2bMarkdownPath = Paths.get(assembleResult.markdownPath).toAbsolutePath().normalize();
            if (!Files.isRegularFile(phase2bMarkdownPath)) {
                throw new IllegalStateException("phase2b markdown file missing: " + phase2bMarkdownPath);
            }

            long restoreStart = System.currentTimeMillis();
            String phase2bMarkdown = Files.readString(phase2bMarkdownPath, StandardCharsets.UTF_8);
            LinkedHashMap<String, String> allTokenMap = new LinkedHashMap<>();
            allTokenMap.putAll(protectionResult.getTokenToOriginalBlock());
            allTokenMap.putAll(translationOutcome.inlineTokenMap);
            String restoredMarkdown = BookMarkdownProtectionUtils.restoreProtectedBlocks(phase2bMarkdown, allTokenMap);
            String preferredMarkdownFileName = StringUtils.hasText(baseResult.preferredMarkdownFileName)
                    ? baseResult.preferredMarkdownFileName.trim()
                    : "book_enhanced.md";
            Path enhancedMarkdownPath = outputRoot.resolve(preferredMarkdownFileName);
            Files.writeString(enhancedMarkdownPath, restoredMarkdown, StandardCharsets.UTF_8);
            result.stageTimingsMs.put("restore_protected_blocks", System.currentTimeMillis() - restoreStart);

            result.success = true;
            result.enhancementApplied = true;
            result.markdownPath = enhancedMarkdownPath.toString();
            result.jsonPath = resolveSemanticJsonPath(outputRoot, assembleResult, baseResult);
            return result;
        } catch (Exception error) {
            logger.warn("[{}] Book enhanced pipeline failed, fallback to base result: {}", taskId, LlmErrorDescriber.describe(error), error);
            result.success = false;
            result.errorMessage = LlmErrorDescriber.describe(error);
            return result;
        } finally {
            result.stageTimingsMs.put("total", System.currentTimeMillis() - totalStart);
        }
    }

    private String loadSelectedMarkdown(Path outputRoot, String primaryMarkdownPath) throws Exception {
        LinkedHashSet<Path> selected = new LinkedHashSet<>();

        Path sectionsRoot = outputRoot.resolve("sections");
        selected.addAll(listMarkdownFiles(sectionsRoot, "section-"));

        Path chaptersRoot = outputRoot.resolve("chapters");
        List<Path> nestedSections = listMarkdownFiles(chaptersRoot, "section-");
        selected.addAll(nestedSections);

        if (selected.isEmpty()) {
            selected.addAll(listTopLevelMarkdown(chaptersRoot, "chapter-"));
        }

        if (selected.isEmpty() && StringUtils.hasText(primaryMarkdownPath)) {
            Path primary = Paths.get(primaryMarkdownPath).toAbsolutePath().normalize();
            if (Files.isRegularFile(primary)) {
                selected.add(primary);
            }
        }

        if (selected.isEmpty()) {
            return "";
        }

        StringBuilder merged = new StringBuilder();
        for (Path path : selected) {
            String text = Files.readString(path, StandardCharsets.UTF_8).trim();
            if (!StringUtils.hasText(text)) {
                continue;
            }
            if (merged.length() > 0) {
                merged.append("\n\n");
            }
            merged.append(text);
        }
        return merged.toString();
    }

    private List<Path> listMarkdownFiles(Path root, String prefix) throws Exception {
        if (root == null || !Files.isDirectory(root)) {
            return Collections.emptyList();
        }
        try (Stream<Path> stream = Files.walk(root)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(path -> {
                        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
                        return name.endsWith(".md") && name.startsWith(prefix.toLowerCase(Locale.ROOT));
                    })
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    private List<Path> listTopLevelMarkdown(Path root, String prefix) throws Exception {
        if (root == null || !Files.isDirectory(root)) {
            return Collections.emptyList();
        }
        try (Stream<Path> stream = Files.list(root)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(path -> {
                        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
                        return name.endsWith(".md") && name.startsWith(prefix.toLowerCase(Locale.ROOT));
                    })
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    private TranslationOutcome translateBlocksIfNeeded(
            BookMarkdownProtectionUtils.ProtectionResult protectionResult,
            String rawMarkdown,
            String taskId
    ) {
        List<BookMarkdownProtectionUtils.ProtectedBlock> protectedBlocks = protectionResult.getBlocks();
        if (protectedBlocks == null || protectedBlocks.isEmpty()) {
            return new TranslationOutcome(List.of(), new LinkedHashMap<>(), 0, false, false);
        }

        LinkedHashSet<String> usedTokens = new LinkedHashSet<>(protectionResult.getTokenToOriginalBlock().keySet());
        LinkedHashMap<String, String> inlineTokenMap = new LinkedHashMap<>();
        List<BlockUnit> units = new ArrayList<>();
        List<TranslateCandidate> candidates = new ArrayList<>();

        int textCounter = 0;
        for (int i = 0; i < protectedBlocks.size(); i++) {
            BookMarkdownProtectionUtils.ProtectedBlock block = protectedBlocks.get(i);
            if (block == null) {
                continue;
            }
            if (block.getType() == BookMarkdownProtectionUtils.BlockType.PROTECTED) {
                units.add(new BlockUnit(i, firstNonBlank(block.getToken(), block.getWorkingText()), false));
                continue;
            }

            InlineProtectionResult inline = protectInlineStructures(
                    firstNonBlank(block.getWorkingText(), ""),
                    rawMarkdown,
                    usedTokens
            );
            if (!inline.tokenToOriginal.isEmpty()) {
                inlineTokenMap.putAll(inline.tokenToOriginal);
            }
            String text = firstNonBlank(inline.protectedText, "").trim();
            boolean shouldTranslate = shouldTranslateText(text);
            BlockUnit unit = new BlockUnit(i, text, shouldTranslate);
            units.add(unit);
            if (shouldTranslate) {
                textCounter += 1;
                candidates.add(new TranslateCandidate(
                        String.format(Locale.ROOT, "S%04d", textCounter),
                        i,
                        text
                ));
            }
        }

        boolean canTranslate = translationEnabled && StringUtils.hasText(apiKey);
        boolean translationAttempted = false;
        int translatedCount = 0;
        if (!candidates.isEmpty() && canTranslate) {
            translationAttempted = true;
            Map<Integer, String> translatedByBlock = translateCandidates(candidates, taskId);
            for (BlockUnit unit : units) {
                if (!unit.translatable) {
                    continue;
                }
                String translated = translatedByBlock.get(unit.index);
                if (!StringUtils.hasText(translated)) {
                    continue;
                }
                if (!hasSameTokenSequence(unit.text, translated)) {
                    logger.warn("[{}] translation token mismatch, fallback original block={}", taskId, unit.index);
                    continue;
                }
                unit.text = translated.trim();
                translatedCount += 1;
            }
        }

        List<String> ordered = units.stream()
                .map(unit -> firstNonBlank(unit.text, ""))
                .filter(StringUtils::hasText)
                .collect(Collectors.toList());
        return new TranslationOutcome(
                ordered,
                inlineTokenMap,
                translatedCount,
                translationAttempted,
                translatedCount > 0
        );
    }

    private InlineProtectionResult protectInlineStructures(
            String input,
            String collisionRefText,
            Set<String> usedTokens
    ) {
        InlineProtectionResult result = new InlineProtectionResult();
        String working = firstNonBlank(input, "");
        working = replaceInlinePattern(working, collisionRefText, INLINE_IMAGE_PATTERN, usedTokens, result.tokenToOriginal);
        working = replaceInlinePattern(working, collisionRefText, INLINE_CODE_PATTERN, usedTokens, result.tokenToOriginal);
        working = replaceInlinePattern(working, collisionRefText, INLINE_MATH_PATTERN, usedTokens, result.tokenToOriginal);
        result.protectedText = working;
        return result;
    }

    private String replaceInlinePattern(
            String input,
            String collisionRefText,
            Pattern pattern,
            Set<String> usedTokens,
            LinkedHashMap<String, String> tokenMap
    ) {
        if (!StringUtils.hasText(input) || pattern == null) {
            return firstNonBlank(input, "");
        }
        Matcher matcher = pattern.matcher(input);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String matched = firstNonBlank(matcher.group(), "");
            if (!StringUtils.hasText(matched)) {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(matched));
                continue;
            }
            String token = buildUniqueInlineToken(collisionRefText, usedTokens);
            if (usedTokens != null) {
                usedTokens.add(token);
            }
            tokenMap.put(token, matched);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(token));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private String buildUniqueInlineToken(String source, Set<String> usedTokens) {
        String safeSource = firstNonBlank(source, "");
        for (int i = 0; i < 16; i++) {
            String token = "[[SYS_INLINE_" + UUID.randomUUID().toString().replace("-", "")
                    .substring(0, 20).toUpperCase(Locale.ROOT) + "]]";
            if (safeSource.contains(token)) {
                continue;
            }
            if (usedTokens != null && usedTokens.contains(token)) {
                continue;
            }
            return token;
        }
        return "[[SYS_INLINE_" + UUID.randomUUID().toString().replace("-", "").toUpperCase(Locale.ROOT) + "]]";
    }

    private boolean shouldTranslateText(String text) {
        String input = firstNonBlank(text, "");
        if (!StringUtils.hasText(input)) {
            return false;
        }
        int letterCount = countMatches(LETTER_PATTERN, input);
        if (letterCount <= 0) {
            return false;
        }
        int cjkCount = countMatches(CJK_PATTERN, input);
        // 仅在纯英文段落触发翻译；中英混排段落默认保持原文，避免术语上下文被误改写。
        if (skipMixedChineseEnglish && cjkCount > 0) {
            return false;
        }
        return true;
    }

    private int countMatches(Pattern pattern, String text) {
        Matcher matcher = pattern.matcher(firstNonBlank(text, ""));
        int count = 0;
        while (matcher.find()) {
            count += 1;
        }
        return count;
    }

    private Map<Integer, String> translateCandidates(List<TranslateCandidate> candidates, String taskId) {
        LinkedHashMap<Integer, String> translatedByBlock = new LinkedHashMap<>();
        if (candidates == null || candidates.isEmpty()) {
            return translatedByBlock;
        }
        int window = Math.max(1, translationWindowSize);
        for (int start = 0; start < candidates.size(); start += window) {
            int end = Math.min(candidates.size(), start + window);
            List<TranslateCandidate> batch = candidates.subList(start, end);
            try {
                Map<String, String> translatedBySid = requestTranslationBatch(batch);
                for (TranslateCandidate candidate : batch) {
                    String translated = firstNonBlank(translatedBySid.get(candidate.sid), "").trim();
                    if (!StringUtils.hasText(translated)) {
                        continue;
                    }
                    translatedByBlock.put(candidate.blockIndex, translated);
                }
            } catch (Exception error) {
                logger.warn("[{}] translation batch failed, start={}, end={}, err={}",
                        taskId, start, end, error.getMessage());
            }
        }
        return translatedByBlock;
    }

    private Map<String, String> requestTranslationBatch(List<TranslateCandidate> batch) throws Exception {
        if (batch == null || batch.isEmpty()) {
            return Map.of();
        }
        List<Map<String, String>> sentenceArray = new ArrayList<>();
        for (TranslateCandidate candidate : batch) {
            Map<String, String> item = new LinkedHashMap<>();
            item.put("sid", candidate.sid);
            item.put("text", firstNonBlank(candidate.originalText, ""));
            sentenceArray.add(item);
        }
        String sentenceJson = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(sentenceArray);
        String userPrompt = String.format(Locale.ROOT, TRANSLATION_PROMPT_TEMPLATE, sentenceJson);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("model", resolveModel());
        payload.put("temperature", translationTemperature);
        payload.put("max_tokens", Math.max(512, translationMaxTokens));
        payload.put("stream", false);
        payload.put("response_format", Map.of("type", "json_object"));
        payload.put("messages", List.of(
                Map.of("role", "system", "content", TRANSLATION_SYSTEM_PROMPT),
                Map.of("role", "user", "content", userPrompt)
        ));
        String payloadJson = OBJECT_MAPPER.writeValueAsString(payload);

        HttpRequest request = HttpRequest.newBuilder(URI.create(normalizeBaseUrl(translationBaseUrl) + "/chat/completions"))
                .timeout(Duration.ofSeconds(Math.max(20, translationTimeoutSec)))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", "Bearer " + apiKey.trim())
                .POST(HttpRequest.BodyPublishers.ofString(payloadJson))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        String responseBody = firstNonBlank(response.body(), "");
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("translation HTTP " + response.statusCode() + ": " + summarize(responseBody));
        }

        String content = extractCompletionContent(responseBody);
        if (!StringUtils.hasText(content)) {
            return Map.of();
        }
        JsonNode root = parseJsonText(content);
        JsonNode translated = root.path("t");
        if (!translated.isArray()) {
            return Map.of();
        }
        LinkedHashMap<String, String> translatedBySid = new LinkedHashMap<>();
        for (JsonNode item : translated) {
            String sid = firstNonBlank(item.path("sid").asText(""), "").trim();
            String tt = firstNonBlank(item.path("tt").asText(""), "").trim();
            if (!StringUtils.hasText(sid) || !StringUtils.hasText(tt)) {
                continue;
            }
            translatedBySid.put(sid, tt);
        }
        return translatedBySid;
    }

    private String extractCompletionContent(String responseBody) throws Exception {
        JsonNode root = OBJECT_MAPPER.readTree(firstNonBlank(responseBody, ""));
        JsonNode choices = root.path("choices");
        if (!choices.isArray() || choices.isEmpty()) {
            return "";
        }
        return firstNonBlank(choices.get(0).path("message").path("content").asText(""), "").trim();
    }

    private JsonNode parseJsonText(String content) throws Exception {
        String trimmed = firstNonBlank(content, "").trim();
        if (trimmed.startsWith("```")) {
            trimmed = trimmed.replaceAll("^```(?:json)?\\s*", "");
            trimmed = trimmed.replaceAll("\\s*```$", "");
        }
        try {
            return OBJECT_MAPPER.readTree(trimmed);
        } catch (Exception ignored) {
            Matcher matcher = Pattern.compile("\\{[\\s\\S]*\\}").matcher(trimmed);
            if (matcher.find()) {
                return OBJECT_MAPPER.readTree(matcher.group());
            }
            throw ignored;
        }
    }

    private boolean hasSameTokenSequence(String original, String translated) {
        List<String> originalTokens = extractTokens(original);
        List<String> translatedTokens = extractTokens(translated);
        return originalTokens.equals(translatedTokens);
    }

    private List<String> extractTokens(String text) {
        List<String> tokens = new ArrayList<>();
        Matcher matcher = TOKEN_PATTERN.matcher(firstNonBlank(text, ""));
        while (matcher.find()) {
            tokens.add(matcher.group());
        }
        return tokens;
    }

    private SyntheticInputPaths writeSyntheticPhase2Inputs(List<String> blocks, Path enhancedIntermediateRoot) throws Exception {
        Files.createDirectories(enhancedIntermediateRoot);
        List<Map<String, Object>> correctedSubtitles = new ArrayList<>();
        List<Map<String, Object>> pureTextScript = new ArrayList<>();
        LinkedHashMap<String, Map<String, Double>> sentenceTimestamps = new LinkedHashMap<>();

        int index = 0;
        for (String block : blocks) {
            String text = firstNonBlank(block, "").trim();
            if (!StringUtils.hasText(text)) {
                continue;
            }
            index += 1;
            String sid = String.format(Locale.ROOT, "S%05d", index);
            String pid = String.format(Locale.ROOT, "P%05d", index);
            double start = (index - 1) * 1.0d;
            double end = index * 1.0d;

            Map<String, Object> subtitle = new LinkedHashMap<>();
            subtitle.put("subtitle_id", sid);
            subtitle.put("corrected_text", text);
            subtitle.put("start_sec", start);
            subtitle.put("end_sec", end);
            correctedSubtitles.add(subtitle);

            Map<String, Object> paragraph = new LinkedHashMap<>();
            paragraph.put("paragraph_id", pid);
            paragraph.put("text", text);
            paragraph.put("source_sentence_ids", List.of(sid));
            pureTextScript.add(paragraph);

            Map<String, Double> ts = new LinkedHashMap<>();
            ts.put("start_sec", start);
            ts.put("end_sec", end);
            sentenceTimestamps.put(sid, ts);
        }

        if (correctedSubtitles.isEmpty()) {
            correctedSubtitles.add(Map.of(
                    "subtitle_id", "S00001",
                    "corrected_text", "EMPTY_CONTENT",
                    "start_sec", 0.0d,
                    "end_sec", 1.0d
            ));
            pureTextScript.add(Map.of(
                    "paragraph_id", "P00001",
                    "text", "EMPTY_CONTENT",
                    "source_sentence_ids", List.of("S00001")
            ));
            sentenceTimestamps.put("S00001", Map.of("start_sec", 0.0d, "end_sec", 1.0d));
        }

        Path step2Path = enhancedIntermediateRoot.resolve("step2_correction_output.json");
        Path step6Path = enhancedIntermediateRoot.resolve("step6_merge_cross_output.json");
        Path sentenceTsPath = enhancedIntermediateRoot.resolve("sentence_timestamps.json");

        OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
                .writeValue(step2Path.toFile(), Map.of("corrected_subtitles", correctedSubtitles));
        OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
                .writeValue(step6Path.toFile(), Map.of("pure_text_script", pureTextScript));
        OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
                .writeValue(sentenceTsPath.toFile(), sentenceTimestamps);

        return new SyntheticInputPaths(step2Path, step6Path, sentenceTsPath);
    }

    private int resolveSemanticUnitCount(PythonGrpcClient.AnalyzeResult analyzeResult) {
        if (analyzeResult == null) {
            return 0;
        }
        if (analyzeResult.semanticUnitsInline != null) {
            return analyzeResult.semanticUnitsInline.unitCount;
        }
        if (analyzeResult.semanticUnitsRef != null) {
            return analyzeResult.semanticUnitsRef.unitCount;
        }
        return 0;
    }

    private String resolveSemanticJsonPath(
            Path outputRoot,
            PythonGrpcClient.AssembleResult assembleResult,
            BookMarkdownService.BookProcessingResult baseResult
    ) {
        try {
            if (assembleResult != null && StringUtils.hasText(assembleResult.jsonPath)) {
                Path phase2JsonPath = Paths.get(assembleResult.jsonPath).toAbsolutePath().normalize();
                if (Files.isRegularFile(phase2JsonPath)) {
                    Path enhancedJsonPath = outputRoot.resolve("book_enhanced_semantic_units.json");
                    Files.copy(phase2JsonPath, enhancedJsonPath, StandardCopyOption.REPLACE_EXISTING);
                    return enhancedJsonPath.toString();
                }
            }
        } catch (Exception error) {
            logger.warn("copy enhanced semantic json failed: {}", error.getMessage());
        }
        return baseResult != null ? firstNonBlank(baseResult.metadataPath, "") : "";
    }

    private String resolveTitle(String markdown, String sourcePath) {
        String text = firstNonBlank(markdown, "");
        for (String line : text.split("\\R")) {
            String trimmed = firstNonBlank(line, "").trim();
            if (!trimmed.startsWith("# ")) {
                continue;
            }
            String title = trimmed.substring(2).trim();
            if (StringUtils.hasText(title)) {
                return title;
            }
        }
        if (!StringUtils.hasText(sourcePath)) {
            return "Book";
        }
        try {
            String name = Paths.get(sourcePath).getFileName().toString();
            int dot = name.lastIndexOf('.');
            return dot > 0 ? name.substring(0, dot) : name;
        } catch (Exception ignored) {
            return "Book";
        }
    }

    private String resolveModel() {
        String resolved = DeepSeekModelRouter.resolveModel(translationModel);
        if (StringUtils.hasText(resolved)) {
            return resolved;
        }
        return firstNonBlank(translationModel, "deepseek-chat");
    }

    private String normalizeBaseUrl(String baseUrl) {
        String endpoint = firstNonBlank(baseUrl, "").trim();
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        if (!StringUtils.hasText(endpoint)) {
            throw new IllegalStateException("book.enhanced-pipeline.translation.base-url is empty");
        }
        if (!endpoint.matches("(?i).*/v\\d+$")) {
            endpoint = endpoint + "/v1";
        }
        return endpoint;
    }

    private int normalizePositive(int value, int fallback) {
        return value > 0 ? value : fallback;
    }

    private String summarize(String text) {
        String normalized = firstNonBlank(text, "").replace('\n', ' ').trim();
        if (normalized.length() <= 280) {
            return normalized;
        }
        return normalized.substring(0, 280) + "...";
    }

    private String firstNonBlank(String first, String fallback) {
        if (StringUtils.hasText(first)) {
            return first;
        }
        return fallback == null ? "" : fallback;
    }
}
