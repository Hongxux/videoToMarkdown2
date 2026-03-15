package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.service.CardStorageService;
import com.mvp.module2.fusion.service.DeepSeekAdvisorService;
import com.mvp.module2.fusion.service.Phase2bArticleLinkService;
import com.mvp.module2.fusion.service.SelectionSyntaxRefineService;
import com.mvp.module2.fusion.websocket.TaskWebSocketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/api/mobile/cards")
public class MobileCardController {

    private static final Logger logger = LoggerFactory.getLogger(MobileCardController.class);
    private static final int CARD_CANDIDATES_TOPK_DEFAULT = 1200;
    private static final int CARD_CANDIDATES_TOPK_MIN = 20;
    private static final int CARD_CANDIDATES_TOPK_MAX = 1500;
    private static final int CARD_CANDIDATES_CONTEXT_MAX_CHARS = 20000;
    private static final long CARD_GENERATION_WAIT_MS = 1_800L;
    private static final long CARD_GENERATION_POLL_MS = 120L;
    private static final Pattern THOUGHT_SIGNAL_PATTERN = Pattern.compile(
            "(?i)(因为|因此|所以|意味着|导致|说明|主张|判断|边界|反例|条件|取舍|however|therefore|because|implies|suggests|under|unless|if\\b)"
    );
    private static final Pattern CONTEXT_EXAMPLE_PATTERN = Pattern.compile(
            "(?i)(例如|比如|例子|语境|原文|上下文|quote|context|example|case)"
    );
    private static final Pattern DICTIONARY_LEAD_PATTERN = Pattern.compile(
            "(?i)^(?:#{1,6}\\s*)?.{0,96}(?:是(?:一种|指|指的是)?|通常指|可定义为|又称|缩写|means\\b|refers to\\b|is\\s+(?:a|an|the)\\b)"
    );
    private static final Pattern PHASE2B_PROGRESS_CHANNEL_PATTERN = Pattern.compile("^[A-Za-z0-9:_\\-.]{6,96}$");

    @Autowired
    private CardStorageService cardStorageService;

    @Autowired
    private DeepSeekAdvisorService deepSeekAdvisorService;

    @Autowired
    private SelectionSyntaxRefineService selectionSyntaxRefineService;

    @Autowired(required = false)
    private Phase2bArticleLinkService phase2bArticleLinkService;

    @Autowired(required = false)
    private TaskWebSocketHandler taskWebSocketHandler;

    private final Map<String, String> inFlightCardGeneration = new ConcurrentHashMap<>();

    @GetMapping("/titles")
    public ResponseEntity<Map<String, Object>> listCardTitles() {
        List<String> titles = cardStorageService.listTitles();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("titles", titles);
        payload.put("count", titles.size());
        return ResponseEntity.ok(payload);
    }

    @GetMapping
    public ResponseEntity<?> getCardByTerm(
            @RequestParam(value = "term", required = false) String term
    ) {
        if (!StringUtils.hasText(term)) {
            return ResponseEntity.badRequest().body(Map.of("message", "term is required"));
        }
        return getCardByTitle(term);
    }

    @PostMapping("/titles/candidates")
    public ResponseEntity<Map<String, Object>> listCardTitleCandidates(
            @RequestBody(required = false) CardTitleCandidatesRequest request
    ) {
        List<String> allTitles = cardStorageService.listTitles();
        int topK = resolveCardCandidatesTopK(request != null ? request.topK : null);
        String context = normalizeCandidateContext(request != null ? request.context : null);
        List<String> candidates = selectCardTitleCandidates(allTitles, context, topK);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("titles", candidates);
        payload.put("count", candidates.size());
        payload.put("totalTitles", allTitles.size());
        payload.put("topK", topK);
        payload.put("contextLength", context.length());
        return ResponseEntity.ok(payload);
    }

    @GetMapping({"/{title}", "/concept/{title}"})
    public ResponseEntity<?> getCardByTitle(@PathVariable String title) {
        try {
            CardStorageService.CardReadResult result = cardStorageService.readCard(title);
            if (!result.exists || !StringUtils.hasText(result.markdown)) {
                result = ensureCardReadySynchronously(title, result);
            }
            if (!result.exists || !StringUtils.hasText(result.markdown)) {
                return ResponseEntity.status(404).body(Map.of("message", "card not found or not generated"));
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("title", result.title);
            payload.put("markdown", result.markdown);
            payload.put("path", result.path != null ? result.path.toString() : "");
            payload.put("created", result.created);
            payload.put("type", result.type);
            payload.put("tags", result.tags);
            payload.put("aliases", result.aliases);
            payload.put("exists", true);
            return ResponseEntity.ok(payload);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("read card failed: title={} err={}", title, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "read card failed"));
        }
    }

    private CardStorageService.CardReadResult ensureCardReadySynchronously(
            String title,
            CardStorageService.CardReadResult current
    ) throws IOException {
        CardStorageService.CardReadResult latest = current;
        if (latest != null && latest.exists && StringUtils.hasText(latest.markdown)) {
            return latest;
        }
        if (deepSeekAdvisorService == null || cardStorageService == null) {
            return latest;
        }
        String normalizedTitle = String.valueOf(title == null ? "" : title).trim();
        if (!StringUtils.hasText(normalizedTitle)) {
            return latest;
        }

        String generationKey = normalizedTitle.toLowerCase(Locale.ROOT);
        String generationToken = Integer.toHexString(generationKey.hashCode());
        String existingToken = inFlightCardGeneration.putIfAbsent(generationKey, generationToken);
        if (existingToken != null && existingToken.equals(generationToken)) {
            return waitForCardReady(normalizedTitle, CARD_GENERATION_WAIT_MS);
        }

        try {
            CardStorageService.CardReadResult doubleChecked = cardStorageService.readCard(normalizedTitle);
            if (doubleChecked.exists && StringUtils.hasText(doubleChecked.markdown)) {
                return doubleChecked;
            }
            DeepSeekAdvisorService.StructuredAdviceResult advice =
                    deepSeekAdvisorService.requestStructuredAdvice(normalizedTitle, "", "", false);
            String generatedMarkdown = renderAutoGeneratedCardMarkdown(normalizedTitle, advice);
            if (!StringUtils.hasText(generatedMarkdown)) {
                return waitForCardReady(normalizedTitle, CARD_GENERATION_WAIT_MS);
            }
            CardStorageService.CardWriteOptions options = new CardStorageService.CardWriteOptions();
            options.contextDependent = Boolean.FALSE;
            options.type = "concept";
            options.tags = List.of("auto-generated", "mobile-sync");
            cardStorageService.saveCard(normalizedTitle, generatedMarkdown, options);
            return cardStorageService.readCard(normalizedTitle);
        } catch (Exception ex) {
            logger.warn("sync generate card failed: title={} err={}", normalizedTitle, ex.getMessage());
            return waitForCardReady(normalizedTitle, CARD_GENERATION_WAIT_MS);
        } finally {
            if (existingToken == null) {
                inFlightCardGeneration.remove(generationKey, generationToken);
            }
        }
    }

    private CardStorageService.CardReadResult waitForCardReady(String title, long timeoutMs) throws IOException {
        long waitUntil = System.currentTimeMillis() + Math.max(0L, timeoutMs);
        CardStorageService.CardReadResult latest = cardStorageService.readCard(title);
        while (System.currentTimeMillis() < waitUntil) {
            if (latest.exists && StringUtils.hasText(latest.markdown)) {
                return latest;
            }
            try {
                Thread.sleep(CARD_GENERATION_POLL_MS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return latest;
            }
            latest = cardStorageService.readCard(title);
        }
        return latest;
    }

    private String renderAutoGeneratedCardMarkdown(
            String title,
            DeepSeekAdvisorService.StructuredAdviceResult advice
    ) {
        if (advice == null) {
            return "";
        }
        List<String> contextual = normalizeAdviceItems(advice.contextualExplanations);
        List<String> depth = normalizeAdviceItems(advice.depth);
        List<String> breadth = normalizeAdviceItems(advice.breadth);
        if (contextual.isEmpty() && depth.isEmpty() && breadth.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("## ").append(title).append('\n').append('\n');
        appendAdviceSection(builder, "## Context", contextual);
        appendAdviceSection(builder, "## First Principles", depth);
        appendAdviceSection(builder, "## Broader Links", breadth);
        return builder.toString().trim();
    }

    private void appendAdviceSection(StringBuilder builder, String heading, List<String> lines) {
        if (builder == null || lines == null || lines.isEmpty()) {
            return;
        }
        builder.append(heading).append('\n');
        int index = 1;
        for (String line : lines) {
            String text = String.valueOf(line == null ? "" : line).trim();
            if (!StringUtils.hasText(text)) {
                continue;
            }
            builder.append(index).append(". ").append(text).append('\n');
            index += 1;
            if (index > 6) {
                break;
            }
        }
        builder.append('\n');
    }

    private List<String> normalizeAdviceItems(List<String> items) {
        if (items == null || items.isEmpty()) {
            return List.of();
        }
        List<String> normalized = new ArrayList<>();
        for (String item : items) {
            String text = String.valueOf(item == null ? "" : item).trim();
            if (!StringUtils.hasText(text)) {
                continue;
            }
            normalized.add(text);
            if (normalized.size() >= 6) {
                break;
            }
        }
        return normalized;
    }

    @PostMapping(value = {"/{title}", "/concept/{title}"}, consumes = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<?> saveCardByTitle(
            @PathVariable String title,
            @RequestBody(required = false) String markdown,
            @RequestParam(value = "sourceTaskId", required = false) String sourceTaskId,
            @RequestParam(value = "sourcePath", required = false) String sourcePath,
            @RequestParam(value = "isContextDependent", defaultValue = "false") boolean isContextDependent,
            @RequestParam(value = "type", required = false) String type,
            @RequestParam(value = "created", required = false) String created,
            @RequestParam(value = "tags", required = false) String tags
    ) {
        if (markdown == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "missing markdown content"));
        }
        ThoughtQualityCheckResult quality = validateThoughtCard(markdown);
        if (!quality.accepted) {
            return ResponseEntity.unprocessableEntity().body(Map.of(
                    "message", quality.message,
                    "code", "pseudo_atomicity_detected"
            ));
        }
        try {
            CardStorageService.CardWriteOptions options = new CardStorageService.CardWriteOptions();
            options.contextDependent = isContextDependent;
            options.type = type;
            options.created = created;
            options.tags = parseCardTags(tags);
            options.sourceTaskId = sourceTaskId;
            options.sourcePath = sourcePath;
            CardStorageService.CardSaveResult result = cardStorageService.saveCard(title, markdown, options);
            return ResponseEntity.ok(toCardSavePayload(result));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("save concept card failed: title={} err={}", title, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "save concept card failed"));
        }
    }

    @GetMapping({"/{title}/backlinks", "/concept/{title}/backlinks"})
    public ResponseEntity<?> getCardBacklinks(@PathVariable String title) {
        try {
            List<CardStorageService.CardBacklinkItem> items = cardStorageService.listBacklinks(title);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("title", title);
            payload.put("items", items);
            payload.put("count", items.size());
            return ResponseEntity.ok(payload);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("list backlinks failed: title={} err={}", title, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "list backlinks failed"));
        }
    }

    @PostMapping(value = "/thought", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> saveThought(@RequestBody CardThoughtSaveRequest request) {
        if (request == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "request body is required"));
        }
        try {
            CardStorageService.CardSaveResult result = cardStorageService.saveThought(
                    request.source,
                    request.anchor,
                    request.content
            );
            return ResponseEntity.ok(toCardSavePayload(result));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("save local thought failed: source={} err={}", request.source, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "save local thought failed"));
        }
    }

    @PostMapping("/ai-advice")
    public ResponseEntity<?> getCardAiAdvice(@RequestBody CardAdviceRequest request) {
        if (request == null || request.term == null || request.term.trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "term is required"));
        }
        try {
            DeepSeekAdvisorService.AdviceResult advice = deepSeekAdvisorService.requestAdvice(
                    request.term,
                    request.context,
                    request.contextExample,
                    Boolean.TRUE.equals(request.isContextDependent)
            );
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("term", request.term.trim());
            payload.put("advice", advice.advice);
            payload.put("source", advice.source);
            payload.put("isContextDependent", Boolean.TRUE.equals(request.isContextDependent));
            return ResponseEntity.ok(payload);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("message", ex.getMessage()));
        } catch (Exception ex) {
            logger.warn("get ai advice failed: term={} err={}", request.term, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "get ai advice failed"));
        }
    }

    @PostMapping("/selection-refine")
    public ResponseEntity<?> refineSelection(@RequestBody CardSelectionRefineRequest request) {
        if (request == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "request body is required"));
        }
        // 注意：这里不能 trim。前端传入的 offset 是基于原始窗口字符串，trim 会导致偏移错位。
        String sourceText = String.valueOf(request.sourceText == null ? "" : request.sourceText);
        if (sourceText.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("message", "sourceText is required"));
        }
        if (sourceText.length() > 600) {
            return ResponseEntity.badRequest().body(Map.of("message", "sourceText too long"));
        }
        int cursorOffset = Math.max(0, Math.min(sourceText.length(), request.cursorOffset));
        int currentStart = Math.max(0, Math.min(sourceText.length(), request.currentStartOffset));
        int currentEnd = Math.max(currentStart, Math.min(sourceText.length(), request.currentEndOffset));
        try {
            SelectionSyntaxRefineService.SelectionRefineResult result = selectionSyntaxRefineService.refineSelection(
                    sourceText,
                    cursorOffset,
                    String.valueOf(request.currentTerm == null ? "" : request.currentTerm).trim(),
                    currentStart,
                    currentEnd
            );
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("improved", result.improved);
            payload.put("term", result.term);
            payload.put("startOffset", result.startOffset);
            payload.put("endOffset", result.endOffset);
            payload.put("confidence", result.confidence);
            payload.put("source", result.source);
            return ResponseEntity.ok(payload);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("message", ex.getMessage()));
        } catch (Exception ex) {
            logger.warn("selection refine failed: err={}", ex.getMessage());
            // 前端属于静默增强链路：失败时返回 improved=false，避免影响即时选词体验。
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("improved", false);
            payload.put("term", "");
            payload.put("startOffset", currentStart);
            payload.put("endOffset", currentEnd);
            payload.put("confidence", 0.0);
            payload.put("source", "error");
            return ResponseEntity.ok(payload);
        }
    }

    @PostMapping("/phase2b/link-metadata")
    public ResponseEntity<?> phase2bLinkMetadata(@RequestBody(required = false) Phase2bLinkMetadataRequest request) {
        List<String> normalizedLinkUrls = normalizePhase2bLinkUrls(request != null ? request.linkUrls : List.of());
        if (normalizedLinkUrls.isEmpty()) {
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "links", List.of(),
                    "count", 0
            ));
        }
        List<Map<String, Object>> linksPayload = new ArrayList<>();
        if (phase2bArticleLinkService != null) {
            try {
                List<Phase2bArticleLinkService.LinkMetadata> metadataList =
                        phase2bArticleLinkService.prefetchLinkMetadata(normalizedLinkUrls);
                for (Phase2bArticleLinkService.LinkMetadata metadata : metadataList) {
                    if (metadata == null) {
                        continue;
                    }
                    linksPayload.add(metadata.toPayload());
                }
            } catch (Exception error) {
                logger.warn("phase2b link metadata prefetch failed: err={}", error.getMessage());
            }
        }
        if (linksPayload.isEmpty()) {
            for (String url : normalizedLinkUrls) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("url", url);
                item.put("siteType", inferPhase2bSiteType(url));
                item.put("title", "");
                item.put("status", "failed");
                linksPayload.add(item);
            }
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("links", linksPayload);
        payload.put("count", linksPayload.size());
        return ResponseEntity.ok(payload);
    }

    @PostMapping("/phase2b/structured-markdown")
    public ResponseEntity<?> phase2bStructuredMarkdown(@RequestBody(required = false) Phase2bStructuredMarkdownRequest request) {
        if (request == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "request body is required"));
        }
        String progressChannel = normalizePhase2bProgressChannel(request.progressChannel);
        String progressRequestId = normalizePhase2bProgressRequestId(request.requestId);
        String bodyText = StringUtils.hasText(request.bodyText)
                ? request.bodyText
                : String.valueOf(request.sourceText == null ? "" : request.sourceText);
        String safeBodyText = String.valueOf(bodyText == null ? "" : bodyText).trim();
        List<String> normalizedLinkUrls = normalizePhase2bLinkUrls(request.linkUrls);
        if (!StringUtils.hasText(safeBodyText) && normalizedLinkUrls.isEmpty()) {
            emitPhase2bProgress(progressChannel, progressRequestId, "failed", "请求内容为空，无法处理", true, false);
            return ResponseEntity.badRequest().body(Map.of("message", "bodyText or linkUrls is required"));
        }

        try {
            emitPhase2bProgress(progressChannel, progressRequestId, "accepted", "请求已接收，开始准备多源重构任务", false, false);

            List<Phase2bArticleLinkService.ExtractedLinkArticle> extractedArticles = List.of();
            List<String> linkWarnings = new ArrayList<>();
            if (!normalizedLinkUrls.isEmpty()) {
                emitPhase2bProgress(progressChannel, progressRequestId, "fetching_reference", "正在抓取知乎/掘金文章文本...", false, false);
                Phase2bArticleLinkService.LinkBatchExtractionResult extraction = extractPhase2bArticles(normalizedLinkUrls);
                extractedArticles = extraction.articles;
                if (extraction.failures != null && !extraction.failures.isEmpty()) {
                    linkWarnings.addAll(extraction.failures);
                }
                if (extraction.ignoredLinks != null && !extraction.ignoredLinks.isEmpty()) {
                    linkWarnings.add("未抓取成功: " + String.join(", ", extraction.ignoredLinks));
                }
                emitPhase2bProgress(progressChannel, progressRequestId, "cleaning_data", "正在剔除噪音与清洗数据...", false, false);
            }

            String userInstruction = StringUtils.hasText(safeBodyText)
                    ? safeBodyText
                    : "请基于参考信源提炼结构化 Markdown，保留关键论据、结论和执行建议。";
            boolean blendMode = extractedArticles != null && !extractedArticles.isEmpty();
            if (!blendMode && !normalizedLinkUrls.isEmpty() && !StringUtils.hasText(safeBodyText)) {
                throw new IllegalStateException("链接抓取失败，且没有可用文本输入");
            }
            String llmSourceText = blendMode
                    ? buildPhase2bBlendSourceText(extractedArticles, userInstruction)
                    : userInstruction;
            if (blendMode) {
                emitPhase2bProgress(progressChannel, progressRequestId, "semantic_fusion", "语义网络构建中，准备融合您的额外观点...", false, false);
            }

            AtomicInteger chunkIndex = new AtomicInteger(0);
            emitPhase2bProgress(progressChannel, progressRequestId, "phase2b_deep", "Phase2B 深度重构中...", false, false);
            DeepSeekAdvisorService.Phase2bMarkdownResult phase2bResult =
                    deepSeekAdvisorService.requestPhase2bStructuredMarkdownStreamedResult(
                    llmSourceText,
                    "",
                    blendMode,
                    (delta) -> emitPhase2bMarkdownDelta(
                            progressChannel,
                            progressRequestId,
                            String.valueOf(delta == null ? "" : delta),
                            chunkIndex.getAndIncrement()
                    )
            );
            emitPhase2bProgress(progressChannel, progressRequestId, "post_processing", "正在整理最终 Markdown...", false, false);
            String finalMarkdown = String.valueOf(phase2bResult == null ? "" : phase2bResult.markdown).trim();
            emitPhase2bMarkdownFinal(progressChannel, progressRequestId, finalMarkdown, chunkIndex.get());

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("markdown", finalMarkdown);
            payload.put("source", phase2bResult == null ? "" : phase2bResult.source);
            payload.put("provider", phase2bResult == null ? "" : phase2bResult.provider);
            payload.put("degraded", phase2bResult != null && phase2bResult.degraded);
            payload.put("links", buildPhase2bLinkPayload(extractedArticles));
            if (!linkWarnings.isEmpty()) {
                payload.put("linkWarnings", linkWarnings);
            }
            emitPhase2bProgress(
                    progressChannel,
                    progressRequestId,
                    "completed",
                    "结构化完成",
                    true,
                    true
            );
            return ResponseEntity.ok(payload);
        } catch (IllegalArgumentException ex) {
            emitPhase2bProgress(progressChannel, progressRequestId, "failed", safePhase2bProgressMessage(ex.getMessage()), true, false);
            return ResponseEntity.badRequest().body(Map.of("message", ex.getMessage()));
        } catch (Exception ex) {
            logger.warn("phase2b structured markdown failed: err={}", ex.getMessage());
            emitPhase2bProgress(progressChannel, progressRequestId, "failed", "结构化失败，请稍后重试", true, false);
            return ResponseEntity.status(500).body(Map.of("message", "phase2b structured markdown failed"));
        }
    }

    private List<String> normalizePhase2bLinkUrls(List<String> rawLinkUrls) {
        if (phase2bArticleLinkService == null) {
            return List.of();
        }
        return phase2bArticleLinkService.normalizeSupportedLinks(rawLinkUrls);
    }

    private Phase2bArticleLinkService.LinkBatchExtractionResult extractPhase2bArticles(List<String> normalizedLinkUrls) {
        if (phase2bArticleLinkService == null) {
            throw new IllegalStateException("phase2b link service unavailable");
        }
        return phase2bArticleLinkService.extractArticles(normalizedLinkUrls);
    }

    private String buildPhase2bBlendSourceText(
            List<Phase2bArticleLinkService.ExtractedLinkArticle> articles,
            String userInstruction
    ) {
        String safeInstruction = String.valueOf(userInstruction == null ? "" : userInstruction).trim();
        StringBuilder builder = new StringBuilder();
        builder.append("## 输入流A：参考信源（文章正文）").append('\n');
        int index = 1;
        for (Phase2bArticleLinkService.ExtractedLinkArticle article : articles) {
            if (article == null || !StringUtils.hasText(article.markdown)) {
                continue;
            }
            builder.append('\n');
            builder.append("### 参考文章 ").append(index).append('\n');
            builder.append("- 标题：").append(StringUtils.hasText(article.title) ? article.title : "未命名文章").append('\n');
            builder.append("- 平台：").append(StringUtils.hasText(article.siteType) ? article.siteType : "generic").append('\n');
            builder.append("- 链接：").append(StringUtils.hasText(article.finalUrl) ? article.finalUrl : article.requestedUrl).append('\n');
            builder.append('\n');
            builder.append(article.markdown.trim()).append('\n');
            index += 1;
        }
        builder.append('\n');
        builder.append("## 输入流B：用户指令（必须严格执行）").append('\n');
        if (StringUtils.hasText(safeInstruction)) {
            builder.append(safeInstruction).append('\n');
        } else {
            builder.append("请围绕参考文章进行结构化重构，优先输出可执行的学习笔记。").append('\n');
        }
        builder.append('\n');
        builder.append("## 约束").append('\n');
        builder.append("- 输出保持 Markdown").append('\n');
        builder.append("- 若出现图片 Markdown 标记，必须保持相对顺序和相对位置，不得改写路径").append('\n');
        return builder.toString().trim();
    }

    private List<Map<String, Object>> buildPhase2bLinkPayload(List<Phase2bArticleLinkService.ExtractedLinkArticle> articles) {
        if (articles == null || articles.isEmpty()) {
            return List.of();
        }
        List<Map<String, Object>> payload = new ArrayList<>();
        for (Phase2bArticleLinkService.ExtractedLinkArticle article : articles) {
            if (article == null) {
                continue;
            }
            payload.add(article.toPayload());
        }
        return payload;
    }

    private void emitPhase2bMarkdownDelta(String channel, String requestId, String delta, int chunkIndex) {
        if (taskWebSocketHandler == null) {
            return;
        }
        String normalizedChannel = normalizePhase2bProgressChannel(channel);
        if (!StringUtils.hasText(normalizedChannel)) {
            return;
        }
        taskWebSocketHandler.broadcastPhase2bMarkdownChunk(
                normalizedChannel,
                normalizePhase2bProgressRequestId(requestId),
                String.valueOf(delta == null ? "" : delta),
                Math.max(0, chunkIndex),
                false
        );
    }

    private void emitPhase2bMarkdownFinal(String channel, String requestId, String finalMarkdown, int finalChunkIndex) {
        if (taskWebSocketHandler == null) {
            return;
        }
        String normalizedChannel = normalizePhase2bProgressChannel(channel);
        if (!StringUtils.hasText(normalizedChannel)) {
            return;
        }
        taskWebSocketHandler.broadcastPhase2bMarkdownFinal(
                normalizedChannel,
                normalizePhase2bProgressRequestId(requestId),
                String.valueOf(finalMarkdown == null ? "" : finalMarkdown),
                Math.max(0, finalChunkIndex)
        );
    }

    private String inferPhase2bSiteType(String urlLike) {
        String url = String.valueOf(urlLike == null ? "" : urlLike).trim().toLowerCase(Locale.ROOT);
        if (url.contains("zhuanlan.zhihu.com") || url.contains("zhihu.com/question/")) {
            return "zhihu";
        }
        if (url.contains("juejin.cn")) {
            return "juejin";
        }
        return "generic";
    }

    private void emitPhase2bProgress(
            String channel,
            String requestId,
            String status,
            String message,
            boolean done,
            boolean success
    ) {
        if (taskWebSocketHandler == null) {
            return;
        }
        String normalizedChannel = normalizePhase2bProgressChannel(channel);
        if (!StringUtils.hasText(normalizedChannel)) {
            return;
        }
        taskWebSocketHandler.broadcastPhase2bProgress(
                normalizedChannel,
                normalizePhase2bProgressRequestId(requestId),
                String.valueOf(status == null ? "" : status),
                String.valueOf(message == null ? "" : message),
                done,
                success
        );
    }

    private String normalizePhase2bProgressChannel(String raw) {
        String value = String.valueOf(raw == null ? "" : raw).trim();
        if (!PHASE2B_PROGRESS_CHANNEL_PATTERN.matcher(value).matches()) {
            return "";
        }
        return value;
    }

    private String normalizePhase2bProgressRequestId(String raw) {
        String value = String.valueOf(raw == null ? "" : raw).trim();
        if (value.isEmpty()) {
            return "";
        }
        if (value.length() > 120) {
            return value.substring(0, 120);
        }
        return value;
    }

    private String safePhase2bProgressMessage(String raw) {
        String value = String.valueOf(raw == null ? "" : raw).trim();
        if (value.isEmpty()) {
            return "请求参数不合法";
        }
        if (value.length() > 180) {
            return value.substring(0, 180);
        }
        return value;
    }

    private Map<String, Object> toCardSavePayload(CardStorageService.CardSaveResult result) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("title", result.title);
        payload.put("path", result.path != null ? result.path.toString() : "");
        payload.put("size", result.size);
        payload.put("updatedAt", result.updatedAt);
        payload.put("created", result.created);
        payload.put("type", result.type);
        payload.put("tags", result.tags);
        payload.put("aliases", result.aliases);
        payload.put("targetType", result.targetType);
        payload.put("targetPath", result.targetPath);
        payload.put("locator", result.locator != null ? result.locator : Map.of());
        payload.put("revision", result.revision);
        return payload;
    }

    private List<String> parseCardTags(String rawTags) {
        if (rawTags == null || rawTags.isBlank()) {
            return null;
        }
        String normalized = rawTags.trim();
        if (normalized.startsWith("[") && normalized.endsWith("]") && normalized.length() >= 2) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        if (normalized.isBlank()) {
            return List.of();
        }
        String[] parts = normalized.split(",");
        List<String> tags = new ArrayList<>();
        for (String part : parts) {
            String tag = part == null ? "" : part.trim();
            if (!tag.isEmpty()) {
                tags.add(tag);
            }
        }
        return tags;
    }

    private ThoughtQualityCheckResult validateThoughtCard(String markdown) {
        String normalized = String.valueOf(markdown == null ? "" : markdown).replace("\r\n", "\n").trim();
        if (normalized.isEmpty()) {
            return ThoughtQualityCheckResult.rejected("card content cannot be empty");
        }
        if (normalized.length() < 24) {
            return ThoughtQualityCheckResult.rejected("card is too short; please provide a standalone thought");
        }

        String firstLine = "";
        for (String line : normalized.split("\n")) {
            String trimmed = String.valueOf(line == null ? "" : line).trim();
            if (!trimmed.isEmpty()) {
                firstLine = trimmed;
                break;
            }
        }

        boolean hasThoughtSignal = THOUGHT_SIGNAL_PATTERN.matcher(normalized).find();
        boolean hasContextExample = CONTEXT_EXAMPLE_PATTERN.matcher(normalized).find();
        boolean dictionaryLead = !firstLine.isEmpty() && DICTIONARY_LEAD_PATTERN.matcher(firstLine).find();

        if (dictionaryLead && (!hasThoughtSignal || !hasContextExample)) {
            return ThoughtQualityCheckResult.rejected(
                    "card looks like a term definition. Please write a claim + mechanism + context example + boundary."
            );
        }
        if (!hasThoughtSignal) {
            return ThoughtQualityCheckResult.rejected(
                    "card must contain reasoning signals (for example: 因为/因此/意味着 or because/therefore)."
            );
        }
        if (!hasContextExample) {
            return ThoughtQualityCheckResult.rejected(
                    "card must cite current context as an example (for example: 例如/语境/上下文 or context/example)."
            );
        }
        return ThoughtQualityCheckResult.accepted();
    }

    private int resolveCardCandidatesTopK(Integer requestedTopK) {
        if (requestedTopK == null) {
            return CARD_CANDIDATES_TOPK_DEFAULT;
        }
        return Math.max(CARD_CANDIDATES_TOPK_MIN, Math.min(CARD_CANDIDATES_TOPK_MAX, requestedTopK));
    }

    private String normalizeCandidateContext(String rawContext) {
        String safe = String.valueOf(rawContext == null ? "" : rawContext).trim();
        if (safe.length() <= CARD_CANDIDATES_CONTEXT_MAX_CHARS) {
            return safe;
        }
        return safe.substring(0, CARD_CANDIDATES_CONTEXT_MAX_CHARS);
    }

    private List<String> selectCardTitleCandidates(List<String> allTitles, String context, int topK) {
        if (allTitles == null || allTitles.isEmpty() || topK <= 0) {
            return List.of();
        }
        List<String> normalized = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String rawTitle : allTitles) {
            String title = String.valueOf(rawTitle == null ? "" : rawTitle).trim();
            if (title.isEmpty()) {
                continue;
            }
            String key = title.toLowerCase(Locale.ROOT);
            if (!seen.add(key)) {
                continue;
            }
            normalized.add(title);
        }
        if (normalized.isEmpty()) {
            return List.of();
        }

        String lowerContext = String.valueOf(context == null ? "" : context).toLowerCase(Locale.ROOT);
        if (lowerContext.isBlank()) {
            normalized.sort(Comparator.comparing(String::toLowerCase, String.CASE_INSENSITIVE_ORDER));
            if (normalized.size() <= topK) {
                return normalized;
            }
            return normalized.subList(0, topK);
        }

        Map<String, Integer> scores = new LinkedHashMap<>();
        for (String title : normalized) {
            String lowerTitle = title.toLowerCase(Locale.ROOT);
            int firstIndex = lowerContext.indexOf(lowerTitle);
            if (firstIndex < 0) {
                continue;
            }
            int count = 0;
            int from = 0;
            while (from >= 0 && from < lowerContext.length()) {
                int found = lowerContext.indexOf(lowerTitle, from);
                if (found < 0) {
                    break;
                }
                count += 1;
                if (count >= 8) {
                    break;
                }
                from = found + lowerTitle.length();
            }
            int score = 80;
            score += Math.max(0, 30 - Math.min(30, firstIndex / 120));
            score += Math.min(40, count * 8);
            score += Math.min(18, title.length() / 2);
            scores.put(title, score);
        }

        List<String> ranked = new ArrayList<>(normalized);
        ranked.sort((a, b) -> {
            int scoreA = scores.getOrDefault(a, 0);
            int scoreB = scores.getOrDefault(b, 0);
            if (scoreA != scoreB) {
                return Integer.compare(scoreB, scoreA);
            }
            if (a.length() != b.length()) {
                return Integer.compare(b.length(), a.length());
            }
            return String.CASE_INSENSITIVE_ORDER.compare(a, b);
        });
        if (ranked.size() <= topK) {
            return ranked;
        }
        return ranked.subList(0, topK);
    }

    public static class CardTitleCandidatesRequest {
        public String context;
        public Integer topK;
    }

    public static class CardThoughtSaveRequest {
        public String source;
        public String anchor;
        public String content;
    }

    public static class CardAdviceRequest {
        public String term;
        public String context;
        public String contextExample;
        public Boolean isContextDependent;
    }

    public static class CardSelectionRefineRequest {
        public String sourceText;
        public int cursorOffset;
        public String currentTerm;
        public int currentStartOffset;
        public int currentEndOffset;
    }

    public static class Phase2bLinkMetadataRequest {
        public List<String> linkUrls;
    }

    public static class Phase2bStructuredMarkdownRequest {
        public String bodyText;
        public String sourceText;
        public String filterRequirement;
        public String progressChannel;
        public String requestId;
        public List<String> linkUrls;
    }

    private static class ThoughtQualityCheckResult {
        private final boolean accepted;
        private final String message;

        private ThoughtQualityCheckResult(boolean accepted, String message) {
            this.accepted = accepted;
            this.message = message;
        }

        private static ThoughtQualityCheckResult accepted() {
            return new ThoughtQualityCheckResult(true, "");
        }

        private static ThoughtQualityCheckResult rejected(String message) {
            return new ThoughtQualityCheckResult(false, message);
        }
    }
}
