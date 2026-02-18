package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.service.CardStorageService;
import com.mvp.module2.fusion.service.DeepSeekAdvisorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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

@RestController
@RequestMapping("/api/mobile/cards")
public class MobileCardController {

    private static final Logger logger = LoggerFactory.getLogger(MobileCardController.class);
    private static final int CARD_CANDIDATES_TOPK_DEFAULT = 1200;
    private static final int CARD_CANDIDATES_TOPK_MIN = 20;
    private static final int CARD_CANDIDATES_TOPK_MAX = 1500;
    private static final int CARD_CANDIDATES_CONTEXT_MAX_CHARS = 20000;

    @Autowired
    private CardStorageService cardStorageService;

    @Autowired
    private DeepSeekAdvisorService deepSeekAdvisorService;

    @GetMapping("/titles")
    public ResponseEntity<Map<String, Object>> listCardTitles() {
        List<String> titles = cardStorageService.listTitles();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("titles", titles);
        payload.put("count", titles.size());
        return ResponseEntity.ok(payload);
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
            if (!result.exists) {
                return ResponseEntity.status(404).body(Map.of("message", "card not found"));
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("title", result.title);
            payload.put("markdown", result.markdown);
            payload.put("path", result.path.toString());
            payload.put("created", result.created);
            payload.put("type", result.type);
            payload.put("tags", result.tags);
            payload.put("exists", true);
            return ResponseEntity.ok(payload);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("read card failed: title={} err={}", title, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "read card failed"));
        }
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
        public Boolean isContextDependent;
    }
}
