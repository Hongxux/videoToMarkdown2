package com.mvp.module2.fusion.service;

import ai.djl.MalformedModelException;
import ai.djl.ModelException;
import ai.djl.huggingface.translator.TextEmbeddingTranslatorFactory;
import ai.djl.inference.Predictor;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.TranslateException;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

@Service
public class SelectionSyntaxRefineService {

    private static final Logger logger = LoggerFactory.getLogger(SelectionSyntaxRefineService.class);

    private static final Pattern MODIFIER_NOUN_PATTERN = Pattern.compile(
            "[\\u4e00-\\u9fffA-Za-z0-9]{1,8}(?:的|地|得|之)[\\u4e00-\\u9fffA-Za-z0-9]{1,8}"
    );
    private static final Set<Character> HARD_BOUNDARY_CHARS = Set.of(
            ',', '.', ';', ':', '!', '?', '，', '。', '；', '：', '！', '？', '\n', '\r'
    );
    private static final Set<Character> EDGE_TRIM_CHARS = Set.of(
            ' ', '\t', '\n', '\r',
            ',', '.', ';', ':', '!', '?', '\'', '"',
            '，', '。', '；', '：', '！', '？', '、',
            '（', '）', '(', ')', '【', '】', '[', ']',
            '《', '》', '<', '>', '“', '”', '‘', '’'
    );
    private static final Set<String> STOPWORD_SET = Set.of(
            "这个", "那个", "这些", "那些", "我们", "你们", "他们", "她们", "它们", "一个", "一种"
    );

    @Value("${mobile.selection-refine.enabled:true}")
    private boolean enabled;

    @Value("${mobile.selection-refine.model:bert-tiny-chinese}")
    private String model;

    @Value("${mobile.selection-refine.max-source-chars:560}")
    private int maxSourceChars;

    @Value("${mobile.selection-refine.expand-chars:10}")
    private int expandChars;

    @Value("${mobile.selection-refine.context-window-chars:28}")
    private int contextWindowChars;

    @Value("${mobile.selection-refine.min-score-gain:0.015}")
    private double minScoreGain;

    private final AtomicReference<ZooModel<String, float[]>> embeddingModelRef = new AtomicReference<>();
    private final Object modelLoadLock = new Object();

    public SelectionRefineResult refineSelection(
            String sourceText,
            int cursorOffset,
            String currentTerm,
            int currentStartOffset,
            int currentEndOffset
    ) {
        String safeSource = String.valueOf(sourceText == null ? "" : sourceText);
        if (safeSource.isBlank()) {
            throw new IllegalArgumentException("sourceText cannot be empty");
        }
        String boundedSource = boundSourceText(safeSource);
        int sourceLen = boundedSource.length();
        int safeCursor = clamp(cursorOffset, 0, sourceLen);
        int safeCurrentStart = clamp(currentStartOffset, 0, sourceLen);
        int safeCurrentEnd = clamp(currentEndOffset, safeCurrentStart, sourceLen);
        String currentSpan = boundedSource.substring(safeCurrentStart, safeCurrentEnd);
        String safeCurrentTerm = String.valueOf(currentTerm == null ? "" : currentTerm).trim();
        if (safeCurrentTerm.isEmpty()) {
            safeCurrentTerm = currentSpan;
        }

        SelectionRefineResult fallback = SelectionRefineResult.notImproved(
                safeCurrentTerm,
                safeCurrentStart,
                safeCurrentEnd,
                0.0,
                "fallback"
        );
        SelectionRefineResult ruleBasedResult = resolveBestCandidateByRule(
                boundedSource,
                safeCursor,
                safeCurrentStart,
                safeCurrentEnd,
                safeCurrentTerm
        );
        if (!enabled) {
            return SelectionRefineResult.notImproved(
                    safeCurrentTerm,
                    safeCurrentStart,
                    safeCurrentEnd,
                    0.0,
                    "disabled"
            );
        }

        try {
            DjlScore score = resolveBestCandidateByDjl(
                    boundedSource,
                    safeCursor,
                    safeCurrentStart,
                    safeCurrentEnd
            );
            if (score == null || score.candidate == null) {
                return ruleBasedResult;
            }
            CandidateSpan candidate = score.candidate;
            if (!score.improved) {
                if (ruleBasedResult.improved) {
                    return ruleBasedResult;
                }
                return SelectionRefineResult.notImproved(
                        candidate.text,
                        candidate.startOffset,
                        candidate.endOffset,
                        score.confidence,
                        "djl-bert-tiny"
                );
            }
            return SelectionRefineResult.improved(
                    candidate.text,
                    candidate.startOffset,
                    candidate.endOffset,
                    score.confidence,
                    "djl-bert-tiny"
            );
        } catch (Exception ex) {
            logger.debug("selection syntax refine degraded to fallback: {}", ex.getMessage());
            if (ruleBasedResult.improved) {
                return ruleBasedResult;
            }
            return fallback;
        }
    }

    private SelectionRefineResult resolveBestCandidateByRule(
            String sourceText,
            int cursorOffset,
            int currentStart,
            int currentEnd,
            String safeCurrentTerm
    ) {
        List<CandidateSpan> candidates = buildCandidateSpans(sourceText, cursorOffset, currentStart, currentEnd);
        if (candidates.isEmpty()) {
            return SelectionRefineResult.notImproved(
                    safeCurrentTerm,
                    currentStart,
                    currentEnd,
                    0.0,
                    "rule-empty"
            );
        }
        String context = buildContextSnippet(sourceText, cursorOffset);
        CandidateSpan best = null;
        double bestScore = Double.NEGATIVE_INFINITY;
        double currentScore = Double.NEGATIVE_INFINITY;

        for (CandidateSpan candidate : candidates) {
            double score = scoreCandidateByRule(
                    sourceText,
                    candidate,
                    cursorOffset,
                    currentStart,
                    currentEnd,
                    context
            );
            if (candidate.startOffset == currentStart && candidate.endOffset == currentEnd) {
                currentScore = score;
            }
            if (best == null || score > bestScore) {
                best = candidate;
                bestScore = score;
            }
        }
        if (best == null) {
            return SelectionRefineResult.notImproved(
                    safeCurrentTerm,
                    currentStart,
                    currentEnd,
                    0.0,
                    "rule-empty"
            );
        }
        if (currentScore == Double.NEGATIVE_INFINITY) {
            CandidateSpan currentSpan = normalizeCandidate(sourceText, currentStart, currentEnd);
            if (currentSpan != null) {
                currentScore = scoreCandidateByRule(
                        sourceText,
                        currentSpan,
                        cursorOffset,
                        currentStart,
                        currentEnd,
                        context
                );
            } else {
                currentScore = bestScore;
            }
        }

        boolean boundaryExpanded = isBoundaryExpanded(best, currentStart, currentEnd);
        double requiredGain = Math.max(0.0, minScoreGain * 0.6);
        boolean improved = boundaryExpanded && bestScore >= (currentScore + requiredGain);
        if (!improved && boundaryExpanded) {
            int boundaryGain = Math.max(0, currentStart - best.startOffset)
                    + Math.max(0, best.endOffset - currentEnd);
            boolean candidateHasModifierNoun = MODIFIER_NOUN_PATTERN.matcher(best.text).find();
            boolean currentHasModifierNoun = MODIFIER_NOUN_PATTERN.matcher(String.valueOf(safeCurrentTerm)).find();
            improved = boundaryGain >= 2 || (candidateHasModifierNoun && !currentHasModifierNoun);
        }

        double confidence = clampDouble(sigmoid(bestScore), 0.0, 1.0);
        if (improved) {
            return SelectionRefineResult.improved(
                    best.text,
                    best.startOffset,
                    best.endOffset,
                    confidence,
                    "rule-fallback"
            );
        }
        return SelectionRefineResult.notImproved(
                best.text,
                best.startOffset,
                best.endOffset,
                confidence,
                "rule-fallback"
        );
    }

    private DjlScore resolveBestCandidateByDjl(
            String sourceText,
            int cursorOffset,
            int currentStart,
            int currentEnd
    ) throws ModelException, IOException, TranslateException {
        String context = buildContextSnippet(sourceText, cursorOffset);
        if (!StringUtils.hasText(context)) {
            return null;
        }
        List<CandidateSpan> candidates = buildCandidateSpans(sourceText, cursorOffset, currentStart, currentEnd);
        if (candidates.isEmpty()) {
            return null;
        }
        ZooModel<String, float[]> modelInstance = getOrLoadEmbeddingModel();
        if (modelInstance == null) {
            return null;
        }

        try (Predictor<String, float[]> predictor = modelInstance.newPredictor()) {
            float[] contextVector = predictor.predict(context);
            if (!isValidVector(contextVector)) {
                return null;
            }

            CandidateSpan best = null;
            double bestScore = Double.NEGATIVE_INFINITY;
            double currentScore = Double.NEGATIVE_INFINITY;

            for (CandidateSpan candidate : candidates) {
                float[] candidateVector;
                try {
                    candidateVector = predictor.predict(candidate.text);
                } catch (TranslateException ex) {
                    continue;
                }
                if (!isValidVector(candidateVector)) {
                    continue;
                }
                double score = scoreCandidate(
                        sourceText,
                        candidate,
                        contextVector,
                        candidateVector,
                        cursorOffset,
                        currentStart,
                        currentEnd
                );
                if (candidate.startOffset == currentStart && candidate.endOffset == currentEnd) {
                    currentScore = score;
                }
                if (best == null || score > bestScore) {
                    best = candidate;
                    bestScore = score;
                }
            }
            if (best == null) {
                return null;
            }
            if (currentScore == Double.NEGATIVE_INFINITY) {
                currentScore = bestScore - Math.max(0.0, minScoreGain);
            }
            boolean improved = isBoundaryExpanded(best, currentStart, currentEnd)
                    && bestScore >= (currentScore + Math.max(0.0, minScoreGain));
            double confidence = clampDouble(sigmoid(bestScore), 0.0, 1.0);
            return new DjlScore(best, confidence, improved);
        }
    }

    private List<CandidateSpan> buildCandidateSpans(
            String sourceText,
            int cursorOffset,
            int currentStart,
            int currentEnd
    ) {
        String source = String.valueOf(sourceText == null ? "" : sourceText);
        int sourceLen = source.length();
        int safeStart = clamp(currentStart, 0, sourceLen);
        int safeEnd = clamp(currentEnd, safeStart, sourceLen);
        int safeCursor = clamp(cursorOffset, 0, sourceLen);
        int maxExpand = Math.max(2, expandChars);

        List<CandidateSpan> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (int start = safeStart; start >= Math.max(0, safeStart - maxExpand); start -= 1) {
            if (start < safeStart && isHardBoundary(source.charAt(start))) {
                break;
            }
            for (int end = safeEnd; end <= Math.min(sourceLen, safeEnd + maxExpand); end += 1) {
                if (end > safeEnd && isHardBoundary(source.charAt(end - 1))) {
                    break;
                }
                if (safeCursor < start || safeCursor > end) {
                    continue;
                }
                CandidateSpan candidate = normalizeCandidate(source, start, end);
                if (candidate == null) continue;
                if (!seen.add(candidate.startOffset + ":" + candidate.endOffset)) continue;
                result.add(candidate);
            }
        }

        CandidateSpan current = normalizeCandidate(source, safeStart, safeEnd);
        if (current != null && seen.add(current.startOffset + ":" + current.endOffset)) {
            result.add(current);
        }

        return result;
    }

    private CandidateSpan normalizeCandidate(String source, int start, int end) {
        int sourceLen = source.length();
        int safeStart = clamp(start, 0, sourceLen);
        int safeEnd = clamp(end, safeStart, sourceLen);
        while (safeStart < safeEnd && isEdgeTrim(source.charAt(safeStart))) {
            safeStart += 1;
        }
        while (safeEnd > safeStart && isEdgeTrim(source.charAt(safeEnd - 1))) {
            safeEnd -= 1;
        }
        if (safeEnd <= safeStart) {
            return null;
        }
        String text = source.substring(safeStart, safeEnd).trim();
        if (!StringUtils.hasText(text)) {
            return null;
        }
        if (text.length() > 24) {
            return null;
        }
        return new CandidateSpan(text, safeStart, safeEnd);
    }

    private String buildContextSnippet(String sourceText, int cursorOffset) {
        String source = String.valueOf(sourceText == null ? "" : sourceText);
        if (!StringUtils.hasText(source)) return "";
        int sourceLen = source.length();
        int safeCursor = clamp(cursorOffset, 0, sourceLen);
        int radius = Math.max(8, contextWindowChars);
        int start = Math.max(0, safeCursor - radius);
        int end = Math.min(sourceLen, safeCursor + radius);
        String snippet = source.substring(start, end).trim();
        if (snippet.length() >= 4) {
            return snippet;
        }
        return source.trim();
    }

    private double scoreCandidate(
            String sourceText,
            CandidateSpan candidate,
            float[] contextVector,
            float[] candidateVector,
            int cursorOffset,
            int currentStart,
            int currentEnd
    ) {
        double cosine = cosineSimilarity(contextVector, candidateVector);
        int len = Math.max(0, candidate.endOffset - candidate.startOffset);
        double lengthBonus;
        if (len >= 2 && len <= 12) {
            lengthBonus = 0.08;
        } else if (len <= 18) {
            lengthBonus = 0.03;
        } else {
            lengthBonus = -0.05;
        }

        double expandBonus = 0.0;
        if (candidate.startOffset < currentStart) {
            expandBonus += 0.05;
        }
        if (candidate.endOffset > currentEnd) {
            expandBonus += 0.05;
        }

        double connectorBonus = MODIFIER_NOUN_PATTERN.matcher(candidate.text).find() ? 0.04 : 0.0;
        double stopwordPenalty = STOPWORD_SET.contains(candidate.text) ? 0.20 : 0.0;
        double center = candidate.startOffset + ((candidate.endOffset - candidate.startOffset) / 2.0);
        double centerPenalty = Math.abs(cursorOffset - center) / Math.max(1.0, len) * 0.04;
        String exact = sourceText.substring(candidate.startOffset, candidate.endOffset);
        double punctuationPenalty = exact.chars().anyMatch(ch -> HARD_BOUNDARY_CHARS.contains((char) ch)) ? 0.08 : 0.0;

        return cosine + lengthBonus + expandBonus + connectorBonus - stopwordPenalty - centerPenalty - punctuationPenalty;
    }

    private double scoreCandidateByRule(
            String sourceText,
            CandidateSpan candidate,
            int cursorOffset,
            int currentStart,
            int currentEnd,
            String contextSnippet
    ) {
        int len = Math.max(0, candidate.endOffset - candidate.startOffset);
        double lengthBonus;
        if (len >= 2 && len <= 12) {
            lengthBonus = 0.10;
        } else if (len <= 18) {
            lengthBonus = 0.04;
        } else {
            lengthBonus = -0.06;
        }

        double expandBonus = 0.0;
        if (candidate.startOffset < currentStart) {
            expandBonus += 0.06;
        }
        if (candidate.endOffset > currentEnd) {
            expandBonus += 0.06;
        }

        double connectorBonus = MODIFIER_NOUN_PATTERN.matcher(candidate.text).find() ? 0.05 : 0.0;
        double stopwordPenalty = STOPWORD_SET.contains(candidate.text) ? 0.24 : 0.0;
        double center = candidate.startOffset + ((candidate.endOffset - candidate.startOffset) / 2.0);
        double centerPenalty = Math.abs(cursorOffset - center) / Math.max(1.0, len) * 0.04;
        String exact = sourceText.substring(candidate.startOffset, candidate.endOffset);
        double punctuationPenalty = exact.chars().anyMatch(ch -> HARD_BOUNDARY_CHARS.contains((char) ch)) ? 0.10 : 0.0;
        double contextOverlapBonus = calcCharOverlapRatio(candidate.text, contextSnippet) * 0.08;

        return lengthBonus + expandBonus + connectorBonus + contextOverlapBonus
                - stopwordPenalty - centerPenalty - punctuationPenalty;
    }

    private double calcCharOverlapRatio(String leftText, String rightText) {
        String left = String.valueOf(leftText == null ? "" : leftText);
        String right = String.valueOf(rightText == null ? "" : rightText);
        if (!StringUtils.hasText(left) || !StringUtils.hasText(right)) {
            return 0.0;
        }
        Set<Character> leftChars = new HashSet<>();
        for (int i = 0; i < left.length(); i += 1) {
            char ch = left.charAt(i);
            if (Character.isWhitespace(ch) || isEdgeTrim(ch)) continue;
            leftChars.add(ch);
        }
        if (leftChars.isEmpty()) {
            return 0.0;
        }
        Set<Character> rightChars = new HashSet<>();
        for (int i = 0; i < right.length(); i += 1) {
            char ch = right.charAt(i);
            if (Character.isWhitespace(ch) || isEdgeTrim(ch)) continue;
            rightChars.add(ch);
        }
        if (rightChars.isEmpty()) {
            return 0.0;
        }
        int hit = 0;
        for (Character ch : leftChars) {
            if (rightChars.contains(ch)) {
                hit += 1;
            }
        }
        return (double) hit / (double) Math.max(1, leftChars.size());
    }

    private boolean isBoundaryExpanded(CandidateSpan candidate, int currentStart, int currentEnd) {
        if (candidate == null) return false;
        return candidate.startOffset < currentStart || candidate.endOffset > currentEnd;
    }

    private ZooModel<String, float[]> getOrLoadEmbeddingModel() throws ModelException, IOException {
        ZooModel<String, float[]> existing = embeddingModelRef.get();
        if (existing != null) {
            return existing;
        }
        synchronized (modelLoadLock) {
            existing = embeddingModelRef.get();
            if (existing != null) {
                return existing;
            }
            ZooModel<String, float[]> loaded = loadEmbeddingModel();
            embeddingModelRef.set(loaded);
            return loaded;
        }
    }

    private ZooModel<String, float[]> loadEmbeddingModel() throws IOException, ModelException, MalformedModelException {
        String modelUrl = resolveModelUrl(model);
        Criteria<String, float[]> criteria = Criteria.builder()
                .setTypes(String.class, float[].class)
                .optModelUrls(modelUrl)
                .optEngine("PyTorch")
                .optTranslatorFactory(new TextEmbeddingTranslatorFactory())
                .build();
        return criteria.loadModel();
    }

    private String resolveModelUrl(String rawModel) {
        String normalized = String.valueOf(rawModel == null ? "" : rawModel).trim();
        if (normalized.isEmpty()) {
            normalized = "bert-tiny-chinese";
        }
        if (normalized.equalsIgnoreCase("bert-tiny-chinese")) {
            // 默认映射到可直接通过 DJL HuggingFace Hub 拉取的中文 tiny 模型。
            normalized = "uer/roberta-tiny-word-chinese-cluecorpussmall";
        }
        if (normalized.startsWith("djl://")
                || normalized.startsWith("http://")
                || normalized.startsWith("https://")) {
            return normalized;
        }
        return "djl://ai.djl.huggingface.pytorch/" + normalized;
    }

    @PreDestroy
    public void closeModel() {
        ZooModel<String, float[]> modelInstance = embeddingModelRef.getAndSet(null);
        if (modelInstance != null) {
            modelInstance.close();
        }
    }

    private String boundSourceText(String sourceText) {
        String safe = String.valueOf(sourceText == null ? "" : sourceText);
        if (safe.length() <= Math.max(40, maxSourceChars)) {
            return safe;
        }
        return safe.substring(0, Math.max(40, maxSourceChars));
    }

    private boolean isHardBoundary(char ch) {
        return HARD_BOUNDARY_CHARS.contains(ch);
    }

    private boolean isEdgeTrim(char ch) {
        return EDGE_TRIM_CHARS.contains(ch);
    }

    private boolean isValidVector(float[] vector) {
        if (vector == null || vector.length == 0) return false;
        for (float v : vector) {
            if (Float.isNaN(v) || Float.isInfinite(v)) return false;
        }
        return true;
    }

    private double cosineSimilarity(float[] left, float[] right) {
        if (left == null || right == null || left.length == 0 || right.length == 0) {
            return -1.0;
        }
        int size = Math.min(left.length, right.length);
        double dot = 0.0;
        double normLeft = 0.0;
        double normRight = 0.0;
        for (int i = 0; i < size; i += 1) {
            double l = left[i];
            double r = right[i];
            dot += l * r;
            normLeft += l * l;
            normRight += r * r;
        }
        if (normLeft <= 0.0 || normRight <= 0.0) {
            return -1.0;
        }
        return dot / (Math.sqrt(normLeft) * Math.sqrt(normRight));
    }

    private double sigmoid(double value) {
        if (value > 20) return 1.0;
        if (value < -20) return 0.0;
        return 1.0 / (1.0 + Math.exp(-value));
    }

    private int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    private double clampDouble(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    private static class CandidateSpan {
        private final String text;
        private final int startOffset;
        private final int endOffset;

        private CandidateSpan(String text, int startOffset, int endOffset) {
            this.text = String.valueOf(text == null ? "" : text).trim();
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }
    }

    private static class DjlScore {
        private final CandidateSpan candidate;
        private final double confidence;
        private final boolean improved;

        private DjlScore(CandidateSpan candidate, double confidence, boolean improved) {
            this.candidate = candidate;
            this.confidence = confidence;
            this.improved = improved;
        }
    }

    public static class SelectionRefineResult {
        public final boolean improved;
        public final String term;
        public final int startOffset;
        public final int endOffset;
        public final double confidence;
        public final String source;

        private SelectionRefineResult(boolean improved, String term, int startOffset, int endOffset, double confidence, String source) {
            this.improved = improved;
            this.term = String.valueOf(term == null ? "" : term).trim();
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.confidence = confidence;
            this.source = String.valueOf(source == null ? "" : source);
        }

        public static SelectionRefineResult improved(String term, int startOffset, int endOffset, double confidence, String source) {
            return new SelectionRefineResult(true, term, startOffset, endOffset, confidence, source);
        }

        public static SelectionRefineResult notImproved(String term, int startOffset, int endOffset, double confidence, String source) {
            return new SelectionRefineResult(false, term, startOffset, endOffset, confidence, source);
        }
    }
}
