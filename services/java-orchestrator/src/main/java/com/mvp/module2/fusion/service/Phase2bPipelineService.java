package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class Phase2bPipelineService {

    private static final Logger logger = LoggerFactory.getLogger(Phase2bPipelineService.class);
    private static final Pattern JSON_CODE_BLOCK_PATTERN = Pattern.compile("(?s)```json\\s*(\\{.*?})\\s*```");
    private static final Pattern SECTION_HEADER_PATTERN = Pattern.compile("(?m)^##\\s+(s\\d+)\\s*:\\s*(.+?)\\s*$");
    private static final AtomicInteger EXECUTOR_THREAD_COUNTER = new AtomicInteger(0);

    @Autowired
    private DeepSeekAdvisorService deepSeekAdvisorService;

    @Value("${phase2b.pipeline.enabled:true}")
    private boolean pipelineEnabled = true;

    @Value("${phase2b.pipeline.max-parallelism:4}")
    private int maxParallelism = 4;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Object executorLock = new Object();
    private volatile ExecutorService refineExecutor;

    public boolean isPipelineEnabled() {
        return pipelineEnabled;
    }

    public Phase2bPipelineResult executePipeline(
            String bodyText,
            boolean blendMode,
            Consumer<String> onDelta
    ) {
        String safeBody = normalizeText(bodyText);
        if (!StringUtils.hasText(safeBody)) {
            throw new IllegalArgumentException("bodyText cannot be empty");
        }
        if (!pipelineEnabled) {
            return executeLegacyFallback(safeBody, blendMode, "phase2b.pipeline.enabled=false");
        }

        emitDelta(onDelta, "Phase 1/3：骨架提取中...");
        DeepSeekAdvisorService.Phase2bMarkdownResult phase1Result =
                deepSeekAdvisorService.requestPhase2bSkeletonMarkdownResult(safeBody, blendMode);

        SkeletonParseResult skeleton;
        try {
            skeleton = parsePhase1Output(phase1Result.markdown);
        } catch (Exception ex) {
            logger.warn("phase2b pipeline phase1 parse failed: err={}", ex.getMessage());
            emitDelta(onDelta, "Phase 1 解析失败，已回退到单次 Phase2B。");
            return executeLegacyFallback(safeBody, blendMode, "phase1-parse-failed");
        }
        if (skeleton.sections.isEmpty()) {
            emitDelta(onDelta, "Phase 1 未得到有效 section，已回退到单次 Phase2B。");
            return executeLegacyFallback(safeBody, blendMode, "phase1-empty-sections");
        }

        emitDelta(onDelta, "Phase 2/3：按 section 并发精修中...");
        List<SectionRunResult> sectionResults = refineSectionsInParallel(skeleton.sections, onDelta);
        String assembledMarkdown = assembleMarkdown(sectionResults);
        if (!StringUtils.hasText(assembledMarkdown)) {
            emitDelta(onDelta, "Phase 2 拼接为空，已回退到单次 Phase2B。");
            return executeLegacyFallback(safeBody, blendMode, "phase2-assembled-empty");
        }

        emitDelta(onDelta, "Phase 3/3：全局纠错中...");
        String finalMarkdown = assembledMarkdown;
        String pipelineFailureReason = "";
        LinkedHashSet<String> providers = new LinkedHashSet<>();
        collectProvider(providers, phase1Result.provider);
        boolean degraded = phase1Result.degraded;
        for (SectionRunResult sectionResult : sectionResults) {
            collectProvider(providers, sectionResult.provider);
            degraded = degraded || sectionResult.degraded || sectionResult.fallbackUsed;
        }
        try {
            DeepSeekAdvisorService.Phase2bMarkdownResult phase3Result =
                    deepSeekAdvisorService.requestPhase2bFactCheckResult(assembledMarkdown);
            if (StringUtils.hasText(phase3Result.markdown)) {
                finalMarkdown = phase3Result.markdown;
            }
            collectProvider(providers, phase3Result.provider);
            degraded = degraded || phase3Result.degraded;
        } catch (Exception ex) {
            pipelineFailureReason = "phase3-failed";
            degraded = true;
            logger.warn("phase2b pipeline phase3 failed: err={}", ex.getMessage());
            emitDelta(onDelta, "Phase 3 纠错失败，已回退到 Phase 2 拼接结果。");
        }
        emitDelta(onDelta, "三阶段管道完成。");
        return new Phase2bPipelineResult(
                finalMarkdown,
                buildPipelineSource(joinProviders(providers), blendMode),
                joinProviders(providers),
                degraded,
                true,
                false,
                pipelineFailureReason,
                buildSectionDebugViews(sectionResults)
        );
    }

    @PreDestroy
    public void shutdown() {
        ExecutorService executor = refineExecutor;
        if (executor == null) {
            return;
        }
        executor.shutdown();
    }

    private Phase2bPipelineResult executeLegacyFallback(String bodyText, boolean blendMode, String fallbackReason) {
        DeepSeekAdvisorService.Phase2bMarkdownResult legacyResult =
                deepSeekAdvisorService.requestPhase2bStructuredMarkdownResult(bodyText, "", blendMode);
        return new Phase2bPipelineResult(
                legacyResult.markdown,
                legacyResult.source,
                legacyResult.provider,
                legacyResult.degraded || StringUtils.hasText(fallbackReason),
                false,
                true,
                String.valueOf(fallbackReason == null ? "" : fallbackReason).trim(),
                List.of()
        );
    }

    private List<SectionRunResult> refineSectionsInParallel(List<Phase2bSection> sections, Consumer<String> onDelta) {
        if (sections == null || sections.isEmpty()) {
            return List.of();
        }
        ExecutorService executor = resolveRefineExecutor();
        List<CompletableFuture<SectionRunResult>> futures = new ArrayList<>();
        for (Phase2bSection section : sections) {
            futures.add(CompletableFuture.supplyAsync(() -> refineSingleSection(section, onDelta), executor));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        List<SectionRunResult> results = new ArrayList<>();
        for (CompletableFuture<SectionRunResult> future : futures) {
            results.add(future.join());
        }
        return results;
    }

    private SectionRunResult refineSingleSection(Phase2bSection section, Consumer<String> onDelta) {
        emitDelta(onDelta, "精修 " + section.id + "：" + section.title);
        List<String> skillIds = resolveAppliedSkillIds(section);
        try {
            DeepSeekAdvisorService.Phase2bMarkdownResult refineResult =
                    deepSeekAdvisorService.requestPhase2bRefinedSectionResult(section.fullMarkdown, skillIds);
            String refinedBody = stripSectionHeader(refineResult.markdown, section.id);
            if (!StringUtils.hasText(refinedBody)) {
                throw new IllegalStateException("empty refined section");
            }
            return SectionRunResult.refined(
                    section,
                    skillIds,
                    refinedBody,
                    refineResult.provider,
                    refineResult.degraded
            );
        } catch (Exception ex) {
            logger.warn("phase2b pipeline refine failed: section={} err={}", section.id, ex.getMessage());
            return SectionRunResult.fallback(section, skillIds, section.bodyMarkdown, ex.getMessage());
        }
    }

    private List<String> resolveAppliedSkillIds(Phase2bSection section) {
        LinkedHashSet<String> skillIds = new LinkedHashSet<>();
        for (String logicTag : section.logicTags) {
            String normalizedTag = normalizeTag(logicTag);
            if (!StringUtils.hasText(normalizedTag)) {
                continue;
            }
            skillIds.add("logic_" + normalizedTag);
        }
        for (String sceneTag : section.sceneTags) {
            String normalizedTag = normalizeTag(sceneTag);
            if (!StringUtils.hasText(normalizedTag)) {
                continue;
            }
            skillIds.add("scene_" + normalizedTag);
        }
        skillIds.add("obsidian_enhancements");

        List<String> resolved = new ArrayList<>();
        for (String skillId : skillIds) {
            if (StringUtils.hasText(deepSeekAdvisorService.loadSkillContent(skillId))) {
                resolved.add(skillId);
            }
        }
        return resolved;
    }

    private SkeletonParseResult parsePhase1Output(String rawOutput) throws Exception {
        String normalized = normalizeText(rawOutput);
        String metadataJson = extractMetadataJson(normalized);
        if (!StringUtils.hasText(metadataJson)) {
            throw new IllegalStateException("phase1 metadata json missing");
        }
        JsonNode root = objectMapper.readTree(metadataJson);
        JsonNode sectionsNode = root.path("sections");
        if (!sectionsNode.isArray() || sectionsNode.isEmpty()) {
            throw new IllegalStateException("phase1 sections metadata missing");
        }

        Map<String, SectionBlock> blocksById = extractSectionBlocks(normalized);
        List<Phase2bSection> sections = new ArrayList<>();
        int order = 0;
        for (JsonNode sectionNode : sectionsNode) {
            String id = readText(sectionNode, "id");
            String title = readText(sectionNode, "title");
            if (!StringUtils.hasText(id)) {
                throw new IllegalStateException("phase1 section id missing");
            }
            SectionBlock block = blocksById.get(id.toLowerCase(Locale.ROOT));
            if (block == null) {
                throw new IllegalStateException("phase1 section block missing: " + id);
            }
            sections.add(new Phase2bSection(
                    order,
                    id.trim(),
                    StringUtils.hasText(title) ? title.trim() : block.title,
                    readTags(sectionNode, "logic_tags"),
                    readTags(sectionNode, "scene_tags"),
                    block.header,
                    block.bodyMarkdown,
                    block.fullMarkdown
            ));
            order += 1;
        }
        return new SkeletonParseResult(metadataJson, sections);
    }

    private Map<String, SectionBlock> extractSectionBlocks(String rawOutput) {
        String markdownPart = extractMarkdownPart(rawOutput);
        Matcher matcher = SECTION_HEADER_PATTERN.matcher(markdownPart);
        List<HeaderMatch> headerMatches = new ArrayList<>();
        while (matcher.find()) {
            headerMatches.add(new HeaderMatch(matcher.start(), matcher.end(), matcher.group(1), matcher.group(2)));
        }
        Map<String, SectionBlock> blocks = new LinkedHashMap<>();
        for (int index = 0; index < headerMatches.size(); index += 1) {
            HeaderMatch headerMatch = headerMatches.get(index);
            int end = index + 1 < headerMatches.size() ? headerMatches.get(index + 1).start : markdownPart.length();
            String fullSection = trimBlankLines(markdownPart.substring(headerMatch.start, end));
            String bodyMarkdown = fullSection;
            int newline = fullSection.indexOf('\n');
            if (newline >= 0) {
                bodyMarkdown = trimBlankLines(fullSection.substring(newline + 1));
            } else if (fullSection.startsWith("## ")) {
                bodyMarkdown = "";
            }
            blocks.put(
                    headerMatch.id.toLowerCase(Locale.ROOT),
                    new SectionBlock(
                            fullSection.substring(0, Math.max(0, newline >= 0 ? newline : fullSection.length())).trim(),
                            String.valueOf(headerMatch.title == null ? "" : headerMatch.title).trim(),
                            bodyMarkdown,
                            fullSection
                    )
            );
        }
        return blocks;
    }

    private String extractMetadataJson(String rawOutput) {
        Matcher matcher = JSON_CODE_BLOCK_PATTERN.matcher(rawOutput);
        if (matcher.find()) {
            return String.valueOf(matcher.group(1) == null ? "" : matcher.group(1)).trim();
        }
        String[] parts = rawOutput.split("(?m)^---\\s*$", 2);
        if (parts.length > 0) {
            String candidate = String.valueOf(parts[0] == null ? "" : parts[0]).trim();
            if (candidate.startsWith("{") && candidate.endsWith("}")) {
                return candidate;
            }
        }
        return "";
    }

    private String extractMarkdownPart(String rawOutput) {
        Matcher matcher = JSON_CODE_BLOCK_PATTERN.matcher(rawOutput);
        if (matcher.find()) {
            String remainder = rawOutput.substring(matcher.end()).trim();
            if (remainder.startsWith("---")) {
                remainder = remainder.substring(3).trim();
            }
            return remainder;
        }
        String[] parts = rawOutput.split("(?m)^---\\s*$", 2);
        if (parts.length == 2) {
            return String.valueOf(parts[1] == null ? "" : parts[1]).trim();
        }
        return rawOutput;
    }

    private List<String> readTags(JsonNode node, String fieldName) {
        List<String> tags = new ArrayList<>();
        if (node == null || !StringUtils.hasText(fieldName)) {
            return tags;
        }
        JsonNode tagNode = node.path(fieldName);
        if (!tagNode.isArray()) {
            return tags;
        }
        LinkedHashSet<String> deduped = new LinkedHashSet<>();
        for (JsonNode item : tagNode) {
            String normalized = normalizeTag(item == null ? "" : item.asText(""));
            if (!StringUtils.hasText(normalized)) {
                continue;
            }
            deduped.add(normalized);
        }
        tags.addAll(deduped);
        return tags;
    }

    private String readText(JsonNode node, String fieldName) {
        if (node == null || !StringUtils.hasText(fieldName)) {
            return "";
        }
        JsonNode child = node.get(fieldName);
        if (child == null || child.isNull()) {
            return "";
        }
        return String.valueOf(child.asText("")).trim();
    }

    private String assembleMarkdown(List<SectionRunResult> sectionResults) {
        if (sectionResults == null || sectionResults.isEmpty()) {
            return "";
        }
        List<String> sections = new ArrayList<>();
        for (SectionRunResult sectionResult : sectionResults) {
            String body = trimBlankLines(sectionResult.markdownBody);
            if (!StringUtils.hasText(body)) {
                sections.add("- **" + sectionResult.section.title + "**");
                continue;
            }
            sections.add("- **" + sectionResult.section.title + "**\n" + indentBlock(body, "    "));
        }
        return String.join("\n\n", sections).trim();
    }

    private List<SectionDebugView> buildSectionDebugViews(List<SectionRunResult> sectionResults) {
        if (sectionResults == null || sectionResults.isEmpty()) {
            return List.of();
        }
        List<SectionDebugView> views = new ArrayList<>();
        for (SectionRunResult sectionResult : sectionResults) {
            views.add(new SectionDebugView(
                    sectionResult.section.id,
                    sectionResult.section.title,
                    sectionResult.section.logicTags,
                    sectionResult.section.sceneTags,
                    sectionResult.skillIds,
                    sectionResult.fallbackUsed,
                    sectionResult.failureReason
            ));
        }
        return views;
    }

    private ExecutorService resolveRefineExecutor() {
        ExecutorService executor = refineExecutor;
        if (executor != null) {
            return executor;
        }
        synchronized (executorLock) {
            if (refineExecutor == null) {
                int parallelism = Math.max(1, Math.min(maxParallelism, 8));
                // 单独线程池避免并发 section 精修与公共线程池互相争抢。
                refineExecutor = Executors.newFixedThreadPool(parallelism, runnable -> {
                    Thread thread = new Thread(runnable, "phase2b-pipeline-" + EXECUTOR_THREAD_COUNTER.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
            }
            return refineExecutor;
        }
    }

    private void collectProvider(LinkedHashSet<String> providers, String provider) {
        String safeProvider = String.valueOf(provider == null ? "" : provider).trim();
        if (!StringUtils.hasText(safeProvider)) {
            return;
        }
        providers.add(safeProvider);
    }

    private String joinProviders(LinkedHashSet<String> providers) {
        if (providers == null || providers.isEmpty()) {
            return "deepseek";
        }
        return String.join(",", providers);
    }

    private String buildPipelineSource(String provider, boolean blendMode) {
        String suffix = blendMode ? "phase2b.pipeline.blend" : "phase2b.pipeline";
        String safeProvider = String.valueOf(provider == null ? "" : provider).trim();
        if (!StringUtils.hasText(safeProvider) || safeProvider.contains(",")) {
            return suffix;
        }
        return safeProvider + "." + suffix;
    }

    private String stripSectionHeader(String markdown, String sectionId) {
        String normalized = normalizeText(markdown);
        if (!StringUtils.hasText(normalized)) {
            return "";
        }
        Matcher matcher = SECTION_HEADER_PATTERN.matcher(normalized);
        if (matcher.find()) {
            String matchedId = String.valueOf(matcher.group(1) == null ? "" : matcher.group(1)).trim();
            int bodyStart = matcher.end();
            if (sectionId.equalsIgnoreCase(matchedId) || normalized.startsWith("## ")) {
                return trimBlankLines(normalized.substring(bodyStart));
            }
        }
        if (normalized.startsWith("## ")) {
            int newline = normalized.indexOf('\n');
            return newline >= 0 ? trimBlankLines(normalized.substring(newline + 1)) : "";
        }
        return trimBlankLines(normalized);
    }

    private void emitDelta(Consumer<String> onDelta, String message) {
        if (onDelta == null || !StringUtils.hasText(message)) {
            return;
        }
        onDelta.accept(message.trim());
    }

    private String normalizeText(String text) {
        return String.valueOf(text == null ? "" : text)
                .replace("\r\n", "\n")
                .replace('\r', '\n')
                .trim();
    }

    private String normalizeTag(String tag) {
        String normalized = String.valueOf(tag == null ? "" : tag).trim().toLowerCase(Locale.ROOT);
        if (!StringUtils.hasText(normalized)) {
            return "";
        }
        return normalized.replace('-', '_').replace(' ', '_');
    }

    private String trimBlankLines(String text) {
        String normalized = String.valueOf(text == null ? "" : text)
                .replace("\r\n", "\n")
                .replace('\r', '\n');
        int start = 0;
        int end = normalized.length();
        while (start < end && normalized.charAt(start) == '\n') {
            start += 1;
        }
        while (end > start && normalized.charAt(end - 1) == '\n') {
            end -= 1;
        }
        return normalized.substring(start, end);
    }

    private String indentBlock(String text, String indent) {
        String safeIndent = String.valueOf(indent == null ? "" : indent);
        String normalized = String.valueOf(text == null ? "" : text)
                .replace("\r\n", "\n")
                .replace('\r', '\n');
        String[] lines = normalized.split("\n", -1);
        for (int index = 0; index < lines.length; index += 1) {
            if (lines[index].isEmpty()) {
                continue;
            }
            lines[index] = safeIndent + lines[index];
        }
        return String.join("\n", lines);
    }

    private record HeaderMatch(int start, int end, String id, String title) {
    }

    private record SectionBlock(
            String header,
            String title,
            String bodyMarkdown,
            String fullMarkdown
    ) {
    }

    private record SkeletonParseResult(
            String metadataJson,
            List<Phase2bSection> sections
    ) {
    }

    private record Phase2bSection(
            int order,
            String id,
            String title,
            List<String> logicTags,
            List<String> sceneTags,
            String header,
            String bodyMarkdown,
            String fullMarkdown
    ) {
    }

    private static final class SectionRunResult {
        private final Phase2bSection section;
        private final List<String> skillIds;
        private final String markdownBody;
        private final String provider;
        private final boolean degraded;
        private final boolean fallbackUsed;
        private final String failureReason;

        private SectionRunResult(
                Phase2bSection section,
                List<String> skillIds,
                String markdownBody,
                String provider,
                boolean degraded,
                boolean fallbackUsed,
                String failureReason
        ) {
            this.section = section;
            this.skillIds = skillIds == null ? List.of() : List.copyOf(skillIds);
            this.markdownBody = String.valueOf(markdownBody == null ? "" : markdownBody).trim();
            this.provider = String.valueOf(provider == null ? "" : provider).trim();
            this.degraded = degraded;
            this.fallbackUsed = fallbackUsed;
            this.failureReason = String.valueOf(failureReason == null ? "" : failureReason).trim();
        }

        private static SectionRunResult refined(
                Phase2bSection section,
                List<String> skillIds,
                String markdownBody,
                String provider,
                boolean degraded
        ) {
            return new SectionRunResult(section, skillIds, markdownBody, provider, degraded, false, "");
        }

        private static SectionRunResult fallback(
                Phase2bSection section,
                List<String> skillIds,
                String markdownBody,
                String failureReason
        ) {
            return new SectionRunResult(section, skillIds, markdownBody, "", true, true, failureReason);
        }
    }

    public static final class Phase2bPipelineResult {
        public final String markdown;
        public final String source;
        public final String provider;
        public final boolean degraded;
        public final boolean pipelineUsed;
        public final boolean legacyFallback;
        public final String fallbackReason;
        public final List<SectionDebugView> sections;

        private Phase2bPipelineResult(
                String markdown,
                String source,
                String provider,
                boolean degraded,
                boolean pipelineUsed,
                boolean legacyFallback,
                String fallbackReason,
                List<SectionDebugView> sections
        ) {
            this.markdown = String.valueOf(markdown == null ? "" : markdown).trim();
            this.source = String.valueOf(source == null ? "" : source).trim();
            this.provider = String.valueOf(provider == null ? "" : provider).trim();
            this.degraded = degraded;
            this.pipelineUsed = pipelineUsed;
            this.legacyFallback = legacyFallback;
            this.fallbackReason = String.valueOf(fallbackReason == null ? "" : fallbackReason).trim();
            this.sections = sections == null ? List.of() : List.copyOf(sections);
        }

        public Map<String, Object> toPayload() {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("pipelineUsed", pipelineUsed);
            payload.put("legacyFallback", legacyFallback);
            payload.put("fallbackReason", fallbackReason);
            List<Map<String, Object>> sectionPayload = new ArrayList<>();
            for (SectionDebugView section : sections) {
                sectionPayload.add(section.toPayload());
            }
            payload.put("sections", sectionPayload);
            payload.put("sectionCount", sections.size());
            return payload;
        }
    }

    public static final class SectionDebugView {
        public final String id;
        public final String title;
        public final List<String> logicTags;
        public final List<String> sceneTags;
        public final List<String> skillIds;
        public final boolean fallbackUsed;
        public final String failureReason;

        private SectionDebugView(
                String id,
                String title,
                List<String> logicTags,
                List<String> sceneTags,
                List<String> skillIds,
                boolean fallbackUsed,
                String failureReason
        ) {
            this.id = String.valueOf(id == null ? "" : id).trim();
            this.title = String.valueOf(title == null ? "" : title).trim();
            this.logicTags = logicTags == null ? List.of() : List.copyOf(logicTags);
            this.sceneTags = sceneTags == null ? List.of() : List.copyOf(sceneTags);
            this.skillIds = skillIds == null ? List.of() : List.copyOf(skillIds);
            this.fallbackUsed = fallbackUsed;
            this.failureReason = String.valueOf(failureReason == null ? "" : failureReason).trim();
        }

        private Map<String, Object> toPayload() {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("id", id);
            payload.put("title", title);
            payload.put("logicTags", logicTags);
            payload.put("sceneTags", sceneTags);
            payload.put("skillIds", skillIds);
            payload.put("fallbackUsed", fallbackUsed);
            payload.put("failureReason", failureReason);
            return payload;
        }
    }
}
