package com.mvp.module2.fusion.service;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
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
import java.util.stream.Stream;

@Service
public class CardStorageService {

    private static final Logger logger = LoggerFactory.getLogger(CardStorageService.class);
    private static final String CARD_EXTENSION = ".md";
    private static final String FRONTMATTER_BOUNDARY = "---";
    private static final String FRONTMATTER_TITLE_KEY = "title";
    private static final String FRONTMATTER_CREATED_KEY = "created";
    private static final String FRONTMATTER_TAGS_KEY = "tags";
    private static final String FRONTMATTER_TYPE_KEY = "type";
    private static final String FRONTMATTER_ALIASES_KEY = "aliases";
    private static final String BACKLINK_HEADER = "## Backlinks";
    private static final String MERGE_DRAFT_HEADER = "## Draft To Merge";
    private static final String TEAR_CALLOUT_HEADER = "> [!TEAR]";
    private static final int MAX_TITLE_LENGTH = 120;
    private static final Pattern ILLEGAL_TITLE_CHARS = Pattern.compile("[\\\\/:*?\"<>|\\p{Cntrl}]");
    private static final Pattern SIMPLE_DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private static final Pattern TRAILING_DOT_OR_SPACE = Pattern.compile("[\\.\\s]+$");
    private static final Pattern WIKILINK_PATTERN = Pattern.compile("\\[\\[([^\\]\\n|]+)(?:\\|[^\\]\\n]*)?\\]\\]");
    private static final Set<String> WINDOWS_RESERVED_BASENAMES = Set.of(
            "CON", "PRN", "AUX", "NUL",
            "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
    );

    @Value("${task.cards.root:var/storage/cards}")
    private String configuredCardsRoot;

    private Path cardsRoot;
    private final Map<String, String> titleIndex = new ConcurrentHashMap<>();
    private final Map<String, String> titleDisplayIndex = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        this.cardsRoot = resolveCardsRoot();
        try {
            Files.createDirectories(this.cardsRoot);
        } catch (IOException ex) {
            logger.warn("create cards directory failed: {} err={}", this.cardsRoot, ex.getMessage());
        }
        refreshTitleIndex();
    }

    public List<String> listTitles() {
        if (this.cardsRoot == null) {
            init();
        }
        List<String> titles = new ArrayList<>(titleDisplayIndex.values());
        titles.sort(String.CASE_INSENSITIVE_ORDER);
        return titles;
    }

    public List<CardBacklinkItem> listBacklinks(String rawTitle) throws IOException {
        if (this.cardsRoot == null) {
            init();
        }
        String targetStorageTitle = resolveStorageTitle(rawTitle);
        String targetStorageKey = normalizeLookupKey(targetStorageTitle);
        Path targetPath = resolveCardPath(targetStorageTitle);
        Set<String> targetKeys = new LinkedHashSet<>();
        targetKeys.add(targetStorageKey);
        targetKeys.add(normalizeLookupKey(rawTitle));
        if (Files.exists(targetPath) && Files.isRegularFile(targetPath)) {
            try {
                CardDocument targetDoc = parseDocument(Files.readString(targetPath, StandardCharsets.UTF_8));
                targetKeys.addAll(buildTermKeysForCard(targetStorageTitle, targetDoc.frontmatter));
            } catch (Exception ex) {
                logger.warn("scan target card aliases failed: title={} err={}", targetStorageTitle, ex.getMessage());
            }
        }
        targetKeys.removeIf(key -> !StringUtils.hasText(key));
        if (!Files.isDirectory(cardsRoot)) {
            return List.of();
        }
        List<CardBacklinkItem> backlinks = new ArrayList<>();
        try (Stream<Path> stream = Files.list(cardsRoot)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(CARD_EXTENSION))
                    .forEach(path -> {
                        String filename = path.getFileName().toString();
                        String sourceStorageTitle = filename.substring(0, filename.length() - CARD_EXTENSION.length());
                        if (!StringUtils.hasText(sourceStorageTitle)) {
                            return;
                        }
                        if (normalizeLookupKey(sourceStorageTitle).equals(targetStorageKey)) {
                            return;
                        }
                        try {
                            String raw = Files.readString(path, StandardCharsets.UTF_8);
                            CardDocument doc = parseDocument(raw);
                            int count = countMatchedWikilinks(doc.body, targetKeys);
                            if (count > 0) {
                                String sourceTitle = resolveDisplayTitle(sourceStorageTitle, doc.frontmatter);
                                backlinks.add(new CardBacklinkItem(sourceTitle, count));
                            }
                        } catch (Exception ex) {
                            logger.warn("scan backlinks failed: source={} err={}", sourceStorageTitle, ex.getMessage());
                        }
                    });
        }
        backlinks.sort(Comparator.comparing(item -> item.sourceTitle, String.CASE_INSENSITIVE_ORDER));
        return backlinks;
    }

    public CardReadResult readCard(String rawTitle) throws IOException {
        String storageTitle = resolveStorageTitle(rawTitle);
        Path cardPath = resolveCardPath(storageTitle);
        if (!Files.exists(cardPath) || !Files.isRegularFile(cardPath)) {
            CardMetadata meta = buildMetadata(storageTitle, normalizeDisplayTerm(rawTitle), Map.of(), null);
            return new CardReadResult(meta.title, "", cardPath, false, meta.created, meta.type, meta.tags, meta.aliases);
        }
        String raw = Files.readString(cardPath, StandardCharsets.UTF_8);
        CardDocument doc = parseDocument(raw);
        CardMetadata meta = buildMetadata(storageTitle, normalizeDisplayTerm(rawTitle), doc.frontmatter, null);
        return new CardReadResult(meta.title, doc.body, cardPath, true, meta.created, meta.type, meta.tags, meta.aliases);
    }

    public CardSaveResult saveCard(String rawTitle, String markdown, CardWriteOptions options) throws IOException {
        String requestedTitle = normalizeDisplayTerm(rawTitle);
        String storageTitle = resolveStorageTitle(rawTitle);
        Path cardPath = resolveCardPath(storageTitle);
        if (cardPath.getParent() != null) {
            Files.createDirectories(cardPath.getParent());
        }

        CardDocument existingDoc = Files.exists(cardPath) && Files.isRegularFile(cardPath)
                ? parseDocument(Files.readString(cardPath, StandardCharsets.UTF_8))
                : CardDocument.empty();
        CardDocument incomingDoc = parseDocument(normalizeMarkdown(markdown));

        String existingBody = normalizeMarkdown(existingDoc.body).trim();
        String incomingBody = normalizeMarkdown(incomingDoc.body).trim();
        CardMetadata metadata = buildMetadata(storageTitle, requestedTitle, existingDoc.frontmatter, options);
        String mergedBody = mergeBodiesPreservingManualEdits(existingBody, incomingBody);

        String persisted = renderDocument(metadata, mergedBody);
        writeStringAtomically(cardPath, persisted);
        Instant updatedAt = Instant.now();

        refreshTitleIndex();
        String locatorTitle = StringUtils.hasText(requestedTitle) ? requestedTitle : metadata.title;
        return new CardSaveResult(
                metadata.title,
                cardPath,
                persisted.length(),
                updatedAt.toString(),
                metadata.created,
                metadata.type,
                metadata.tags,
                metadata.aliases,
                "global",
                cardPath,
                Map.of("kind", "title", "value", locatorTitle),
                buildRevision(updatedAt, persisted.length())
        );
    }

    public CardSaveResult saveThought(String rawSourcePath, String rawAnchorText, String thoughtContent) throws IOException {
        String sourcePathText = String.valueOf(rawSourcePath == null ? "" : rawSourcePath).trim();
        String anchorText = normalizeMarkdown(rawAnchorText).trim();
        String normalizedThought = normalizeMarkdown(thoughtContent).trim();
        if (sourcePathText.isEmpty()) {
            throw new IllegalArgumentException("source must be markdown file");
        }
        if (anchorText.isEmpty()) {
            throw new IllegalArgumentException("anchor is required");
        }
        if (normalizedThought.isEmpty()) {
            throw new IllegalArgumentException("content is required");
        }

        Path sourcePath = resolveSourceMarkdownPath(sourcePathText);
        String existing = normalizeMarkdown(Files.readString(sourcePath, StandardCharsets.UTF_8));
        String callout = renderThoughtCallout(normalizedThought);
        String updated = insertThoughtCalloutAfterAnchor(existing, anchorText, callout);
        if (!updated.equals(existing)) {
            writeStringAtomically(sourcePath, updated);
        }

        Instant updatedAt = Instant.now();
        Map<String, Object> locator = new LinkedHashMap<>();
        locator.put("kind", "anchor");
        locator.put("anchor", anchorText);
        locator.put("callout", "TEAR");
        return new CardSaveResult(
                "",
                sourcePath,
                updated.length(),
                updatedAt.toString(),
                "",
                "local",
                List.of(),
                List.of(),
                "local",
                sourcePath,
                locator,
                buildRevision(updatedAt, updated.length())
        );
    }

    private CardMetadata buildMetadata(
            String storageTitle,
            String requestedTitle,
            Map<String, String> existing,
            CardWriteOptions options
    ) {
        String optionCreated = options != null ? normalizeDate(options.created) : "";
        String existingCreated = normalizeDate(existing.get(FRONTMATTER_CREATED_KEY));
        String created = firstNonBlank(optionCreated, existingCreated, LocalDate.now().toString());

        String optionType = options != null ? normalizeType(options.type) : "";
        String existingType = normalizeType(existing.get(FRONTMATTER_TYPE_KEY));
        String fallbackType = (options != null && Boolean.TRUE.equals(options.contextDependent)) ? "context" : "concept";
        String type = firstNonBlank(optionType, existingType, fallbackType);

        List<String> tags = options != null && options.tags != null
                ? sanitizeTags(options.tags)
                : parseTags(existing.get(FRONTMATTER_TAGS_KEY));

        String existingTitle = normalizeDisplayTerm(existing.get(FRONTMATTER_TITLE_KEY));
        String title = firstNonBlank(existingTitle, normalizeDisplayTerm(requestedTitle), storageTitle);

        List<String> aliases = new ArrayList<>(parseAliases(existing.get(FRONTMATTER_ALIASES_KEY)));
        if (options != null && options.aliases != null && !options.aliases.isEmpty()) {
            aliases.addAll(options.aliases);
        }
        String normalizedRequested = normalizeDisplayTerm(requestedTitle);
        if (StringUtils.hasText(normalizedRequested)
                && !normalizedRequested.equalsIgnoreCase(title)
                && !normalizedRequested.equalsIgnoreCase(storageTitle)) {
            aliases.add(normalizedRequested);
        }
        aliases = sanitizeAliases(aliases, title, storageTitle);
        return new CardMetadata(title, created, type, tags, aliases);
    }

    private String renderDocument(CardMetadata metadata, String body) {
        StringBuilder out = new StringBuilder();
        out.append(FRONTMATTER_BOUNDARY).append('\n');
        out.append(FRONTMATTER_TITLE_KEY).append(": ").append(yamlQuote(metadata.title)).append('\n');
        out.append(FRONTMATTER_CREATED_KEY).append(": ").append(yamlQuote(metadata.created)).append('\n');
        out.append(FRONTMATTER_TAGS_KEY).append(": ").append(renderTagsInline(metadata.tags)).append('\n');
        out.append(FRONTMATTER_TYPE_KEY).append(": ").append(yamlQuote(metadata.type)).append('\n');
        out.append(FRONTMATTER_ALIASES_KEY).append(": ").append(renderAliasesInline(metadata.aliases)).append('\n');
        out.append(FRONTMATTER_BOUNDARY).append('\n');
        out.append('\n');

        String normalizedBody = normalizeMarkdown(body).trim();
        if (!normalizedBody.isEmpty()) {
            out.append(normalizedBody).append('\n');
        }
        return out.toString();
    }

    private CardDocument parseDocument(String markdown) {
        String text = normalizeMarkdown(markdown);
        if (!text.startsWith(FRONTMATTER_BOUNDARY + "\n")) {
            return new CardDocument(Map.of(), text);
        }
        String[] lines = text.split("\n", -1);
        if (lines.length < 3 || !FRONTMATTER_BOUNDARY.equals(lines[0].trim())) {
            return new CardDocument(Map.of(), text);
        }

        int closeIndex = -1;
        for (int i = 1; i < lines.length; i += 1) {
            if (FRONTMATTER_BOUNDARY.equals(lines[i].trim())) {
                closeIndex = i;
                break;
            }
        }
        if (closeIndex < 0) {
            return new CardDocument(Map.of(), text);
        }

        Map<String, String> frontmatter = new LinkedHashMap<>();
        for (int i = 1; i < closeIndex; i += 1) {
            String line = lines[i];
            int delimiter = line.indexOf(':');
            if (delimiter <= 0) {
                continue;
            }
            String key = line.substring(0, delimiter).trim().toLowerCase(Locale.ROOT);
            String value = line.substring(delimiter + 1).trim();
            if (value.isEmpty()) {
                ParsedFrontmatterList parsedList = parseIndentedList(lines, i + 1, closeIndex);
                if (!parsedList.items.isEmpty()) {
                    frontmatter.put(key, renderYamlInlineList(parsedList.items));
                    i = parsedList.lastConsumedIndex;
                    continue;
                }
            }
            if (!key.isEmpty()) {
                frontmatter.put(key, stripYamlQuotes(value));
            }
        }

        StringBuilder body = new StringBuilder();
        for (int i = closeIndex + 1; i < lines.length; i += 1) {
            body.append(lines[i]);
            if (i < lines.length - 1) {
                body.append('\n');
            }
        }
        return new CardDocument(frontmatter, body.toString());
    }

    private ParsedFrontmatterList parseIndentedList(String[] lines, int startIndex, int closeIndex) {
        List<String> items = new ArrayList<>();
        int lastConsumedIndex = startIndex - 1;
        for (int i = startIndex; i < closeIndex; i += 1) {
            String line = lines[i];
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                lastConsumedIndex = i;
                continue;
            }
            if (!Character.isWhitespace(line.charAt(0))) {
                break;
            }
            if (trimmed.startsWith("- ")) {
                String item = stripYamlQuotes(trimmed.substring(2).trim());
                if (!item.isEmpty()) {
                    items.add(item);
                }
                lastConsumedIndex = i;
                continue;
            }
            break;
        }
        return new ParsedFrontmatterList(items, lastConsumedIndex);
    }

    private String mergeBodiesPreservingManualEdits(String existingBodyRaw, String incomingBodyRaw) {
        String existing = normalizeMarkdown(existingBodyRaw).trim();
        String incoming = normalizeMarkdown(incomingBodyRaw).trim();
        if (incoming.isEmpty()) {
            return existing;
        }
        if (existing.isEmpty()) {
            return incoming;
        }
        if (existing.equals(incoming)) {
            return existing;
        }
        if (incoming.contains(existing)) {
            return incoming;
        }
        if (existing.contains(incoming)) {
            return existing;
        }
        if (existing.contains(MERGE_DRAFT_HEADER) && existing.contains(incoming)) {
            return existing;
        }
        return existing
                + "\n\n"
                + MERGE_DRAFT_HEADER
                + "\n\n"
                + "```markdown\n"
                + incoming
                + "\n```";
    }

    private int countMatchedWikilinks(String markdownBody, Set<String> targetKeys) {
        if (!StringUtils.hasText(markdownBody) || targetKeys == null || targetKeys.isEmpty()) {
            return 0;
        }
        int count = 0;
        var matcher = WIKILINK_PATTERN.matcher(markdownBody);
        while (matcher.find()) {
            String rawTarget = String.valueOf(matcher.group(1)).trim();
            if (!StringUtils.hasText(rawTarget)) {
                continue;
            }
            String linkTargetKey = normalizeLinkTargetKey(rawTarget);
            if (targetKeys.contains(linkTargetKey)) {
                count += 1;
            }
        }
        return count;
    }

    private String normalizeLinkTargetKey(String rawTarget) {
        String safe = String.valueOf(rawTarget == null ? "" : rawTarget).trim();
        if (!StringUtils.hasText(safe)) {
            return "";
        }
        return normalizeLookupKey(safe);
    }

    private void writeStringAtomically(Path target, String content) throws IOException {
        Path parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path baseDir = parent != null ? parent : Paths.get(".");
        Path tmp = Files.createTempFile(baseDir, target.getFileName().toString() + ".", ".tmp");
        try {
            Files.writeString(tmp, content, StandardCharsets.UTF_8);
            try {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (Exception ex) {
            try {
                Files.deleteIfExists(tmp);
            } catch (Exception ignored) {
                // noop
            }
            throw ex;
        }
    }

    private String buildRevision(Instant updatedAt, int size) {
        return updatedAt.toEpochMilli() + ":" + size;
    }

    private Path resolveSourceMarkdownPath(String rawSourcePath) {
        Path workspaceRoot = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        Path candidate = Paths.get(rawSourcePath);
        if (!candidate.isAbsolute()) {
            candidate = workspaceRoot.resolve(candidate);
        }
        Path normalized = candidate.toAbsolutePath().normalize();
        if (!Files.exists(normalized) || !Files.isRegularFile(normalized)) {
            throw new IllegalArgumentException("source markdown file does not exist");
        }
        String name = String.valueOf(normalized.getFileName() == null ? "" : normalized.getFileName().toString()).toLowerCase(Locale.ROOT);
        if (!(name.endsWith(".md") || name.endsWith(".markdown"))) {
            throw new IllegalArgumentException("source must be markdown file");
        }
        return normalized;
    }

    private String renderThoughtCallout(String thoughtContent) {
        String normalized = normalizeMarkdown(thoughtContent).trim();
        StringBuilder out = new StringBuilder();
        out.append(TEAR_CALLOUT_HEADER).append('\n');
        String[] lines = normalized.split("\n", -1);
        for (String line : lines) {
            out.append("> ").append(line).append('\n');
        }
        return out.toString().trim();
    }

    private String insertThoughtCalloutAfterAnchor(String markdown, String anchorText, String callout) {
        String text = normalizeMarkdown(markdown);
        int anchorIndex = text.indexOf(anchorText);
        if (anchorIndex < 0) {
            throw new IllegalArgumentException("anchor not found in source markdown");
        }
        int paragraphEnd = findParagraphBoundary(text, anchorIndex + anchorText.length());
        String leadingTail = text.substring(paragraphEnd).stripLeading();
        String normalizedCallout = normalizeMarkdown(callout).trim();
        if (leadingTail.startsWith(normalizedCallout)) {
            return text;
        }

        String before = text.substring(0, paragraphEnd);
        String after = text.substring(paragraphEnd);
        StringBuilder out = new StringBuilder();
        out.append(before);
        if (!before.endsWith("\n\n")) {
            if (!before.endsWith("\n")) {
                out.append('\n');
            }
            out.append('\n');
        }
        out.append(normalizedCallout).append('\n');
        if (!after.isEmpty() && !after.startsWith("\n")) {
            out.append('\n');
        }
        out.append(after);
        return normalizeMarkdown(out.toString());
    }

    private int findParagraphBoundary(String markdown, int fromIndex) {
        String text = normalizeMarkdown(markdown);
        int index = Math.max(0, Math.min(fromIndex, text.length()));
        while (index < text.length()) {
            if (text.charAt(index) == '\n') {
                int cursor = index;
                while (cursor < text.length() && text.charAt(cursor) == '\n') {
                    cursor += 1;
                }
                if (cursor - index >= 2) {
                    return index;
                }
            }
            index += 1;
        }
        return text.length();
    }

    private String stripYamlQuotes(String value) {
        String safe = String.valueOf(value == null ? "" : value).trim();
        if (safe.length() >= 2) {
            if ((safe.startsWith("\"") && safe.endsWith("\"")) || (safe.startsWith("'") && safe.endsWith("'"))) {
                safe = safe.substring(1, safe.length() - 1);
            }
        }
        return safe.replace("\\\"", "\"");
    }

    private List<String> parseTags(String rawTags) {
        return sanitizeTags(parseYamlInlineList(rawTags));
    }

    private List<String> parseAliases(String rawAliases) {
        return sanitizeAliases(parseYamlInlineList(rawAliases), "", "");
    }

    private List<String> parseYamlInlineList(String rawList) {
        String safe = String.valueOf(rawList == null ? "" : rawList).trim();
        if (safe.isEmpty()) {
            return List.of();
        }
        if (safe.startsWith("[") && safe.endsWith("]")) {
            safe = safe.substring(1, safe.length() - 1);
        }
        if (safe.isBlank()) {
            return List.of();
        }
        String[] pieces = safe.split(",");
        List<String> values = new ArrayList<>();
        for (String piece : pieces) {
            String value = stripYamlQuotes(piece).trim();
            if (!value.isEmpty()) {
                values.add(value);
            }
        }
        return values;
    }

    private List<String> sanitizeTags(List<String> tags) {
        if (tags == null || tags.isEmpty()) {
            return List.of();
        }
        LinkedHashMap<String, String> unique = new LinkedHashMap<>();
        for (String raw : tags) {
            String tag = String.valueOf(raw == null ? "" : raw).trim();
            if (tag.isEmpty()) {
                continue;
            }
            if (tag.length() > 36) {
                tag = tag.substring(0, 36).trim();
            }
            if (tag.isEmpty()) {
                continue;
            }
            unique.putIfAbsent(tag.toLowerCase(Locale.ROOT), tag);
        }
        return new ArrayList<>(unique.values());
    }

    private List<String> sanitizeAliases(List<String> aliases, String title, String storageTitle) {
        if (aliases == null || aliases.isEmpty()) {
            return List.of();
        }
        String normalizedTitle = normalizeLookupKey(title);
        String normalizedStorageTitle = normalizeLookupKey(storageTitle);
        LinkedHashMap<String, String> unique = new LinkedHashMap<>();
        for (String raw : aliases) {
            String alias = normalizeDisplayTerm(raw);
            if (!StringUtils.hasText(alias)) {
                continue;
            }
            if (alias.length() > 120) {
                alias = alias.substring(0, 120).trim();
            }
            if (!StringUtils.hasText(alias)) {
                continue;
            }
            String key = normalizeLookupKey(alias);
            if (key.equals(normalizedTitle) || key.equals(normalizedStorageTitle)) {
                continue;
            }
            unique.putIfAbsent(key, alias);
        }
        return new ArrayList<>(unique.values());
    }

    private String renderTagsInline(List<String> tags) {
        List<String> safeTags = sanitizeTags(tags);
        if (safeTags.isEmpty()) {
            return "[]";
        }
        List<String> parts = new ArrayList<>(safeTags.size());
        for (String tag : safeTags) {
            parts.add(yamlQuote(tag));
        }
        return "[" + String.join(", ", parts) + "]";
    }

    private String renderAliasesInline(List<String> aliases) {
        List<String> safeAliases = sanitizeAliases(aliases, "", "");
        if (safeAliases.isEmpty()) {
            return "[]";
        }
        List<String> parts = new ArrayList<>(safeAliases.size());
        for (String alias : safeAliases) {
            parts.add(yamlQuote(alias));
        }
        return "[" + String.join(", ", parts) + "]";
    }

    private String renderYamlInlineList(List<String> values) {
        if (values == null || values.isEmpty()) {
            return "[]";
        }
        List<String> safeValues = new ArrayList<>();
        for (String value : values) {
            String safe = stripYamlQuotes(value).trim();
            if (!safe.isEmpty()) {
                safeValues.add(safe);
            }
        }
        if (safeValues.isEmpty()) {
            return "[]";
        }
        List<String> parts = new ArrayList<>(safeValues.size());
        for (String safeValue : safeValues) {
            parts.add(yamlQuote(safeValue));
        }
        return "[" + String.join(", ", parts) + "]";
    }

    private String yamlQuote(String value) {
        String safe = String.valueOf(value == null ? "" : value).replace("\"", "\\\"");
        return "\"" + safe + "\"";
    }

    private String normalizeDate(String value) {
        String safe = String.valueOf(value == null ? "" : value).trim();
        if (safe.isEmpty()) {
            return "";
        }
        return SIMPLE_DATE_PATTERN.matcher(safe).matches() ? safe : "";
    }

    private String normalizeType(String value) {
        String safe = String.valueOf(value == null ? "" : value).trim().toLowerCase(Locale.ROOT);
        if (safe.isEmpty()) {
            return "";
        }
        if ("context".equals(safe) || "context-dependent".equals(safe)) {
            return "context";
        }
        if ("concept".equals(safe)) {
            return "concept";
        }
        if ("thought".equals(safe)) {
            return "thought";
        }
        return "";
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (StringUtils.hasText(value)) {
                return value.trim();
            }
        }
        return "";
    }

    private String normalizeMarkdown(String markdown) {
        return String.valueOf(markdown == null ? "" : markdown).replace("\r\n", "\n");
    }

    private void refreshTitleIndex() {
        titleIndex.clear();
        titleDisplayIndex.clear();
        if (cardsRoot == null || !Files.isDirectory(cardsRoot)) {
            return;
        }
        try (Stream<Path> stream = Files.list(cardsRoot)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(CARD_EXTENSION))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString().toLowerCase(Locale.ROOT)))
                    .forEach(path -> {
                        String filename = path.getFileName().toString();
                        String storageTitle = filename.substring(0, filename.length() - CARD_EXTENSION.length());
                        if (!StringUtils.hasText(storageTitle)) {
                            return;
                        }
                        Map<String, String> frontmatter = Map.of();
                        try {
                            String rawCard = Files.readString(path, StandardCharsets.UTF_8);
                            CardDocument doc = parseDocument(rawCard);
                            frontmatter = doc.frontmatter;
                        } catch (Exception ex) {
                            logger.warn("read card index metadata failed: file={} err={}", filename, ex.getMessage());
                        }
                        List<String> terms = collectTermsForCard(storageTitle, frontmatter);
                        for (String term : terms) {
                            registerIndexTerm(term, storageTitle);
                        }
                    });
        } catch (IOException ex) {
            logger.warn("refresh card title index failed: {} err={}", cardsRoot, ex.getMessage());
        }
    }

    private void registerIndexTerm(String rawTerm, String storageTitle) {
        String term = normalizeDisplayTerm(rawTerm);
        if (!StringUtils.hasText(term) || !StringUtils.hasText(storageTitle)) {
            return;
        }
        String key = normalizeLookupKey(term);
        titleIndex.putIfAbsent(key, storageTitle);
        titleDisplayIndex.putIfAbsent(key, term);
    }

    private List<String> collectTermsForCard(String storageTitle, Map<String, String> frontmatter) {
        LinkedHashSet<String> terms = new LinkedHashSet<>();
        String displayTitle = resolveDisplayTitle(storageTitle, frontmatter);
        if (StringUtils.hasText(displayTitle)) {
            terms.add(displayTitle);
        }
        if (StringUtils.hasText(storageTitle)) {
            terms.add(storageTitle);
        }
        for (String alias : parseAliases(frontmatter.get(FRONTMATTER_ALIASES_KEY))) {
            if (StringUtils.hasText(alias)) {
                terms.add(alias);
            }
        }
        return new ArrayList<>(terms);
    }

    private Set<String> buildTermKeysForCard(String storageTitle, Map<String, String> frontmatter) {
        Set<String> keys = new LinkedHashSet<>();
        for (String term : collectTermsForCard(storageTitle, frontmatter)) {
            String key = normalizeLookupKey(term);
            if (StringUtils.hasText(key)) {
                keys.add(key);
            }
        }
        return keys;
    }

    private String resolveDisplayTitle(String storageTitle, Map<String, String> frontmatter) {
        String frontmatterTitle = normalizeDisplayTerm(frontmatter.get(FRONTMATTER_TITLE_KEY));
        return StringUtils.hasText(frontmatterTitle) ? frontmatterTitle : storageTitle;
    }

    private String resolveStorageTitle(String rawTitle) {
        String lookupKey = normalizeLookupKey(rawTitle);
        if (StringUtils.hasText(lookupKey)) {
            String indexed = titleIndex.get(lookupKey);
            if (StringUtils.hasText(indexed)) {
                return indexed;
            }
        }
        return normalizeTitle(rawTitle);
    }

    private String normalizeTitle(String rawTitle) {
        String title = String.valueOf(rawTitle == null ? "" : rawTitle).trim();
        if (title.isEmpty()) {
            throw new IllegalArgumentException("card title cannot be empty");
        }
        title = ILLEGAL_TITLE_CHARS.matcher(title).replaceAll("_");
        title = title.replaceAll("\\s+", " ").trim();
        title = ensureFileSystemSafeTitle(title);
        if (title.equals(".") || title.equals("..")) {
            throw new IllegalArgumentException("invalid card title");
        }
        if (title.length() > MAX_TITLE_LENGTH) {
            title = title.substring(0, MAX_TITLE_LENGTH).trim();
            title = ensureFileSystemSafeTitle(title);
        }
        if (title.isEmpty()) {
            throw new IllegalArgumentException("card title cannot be empty");
        }
        return title;
    }

    private String ensureFileSystemSafeTitle(String title) {
        String safe = TRAILING_DOT_OR_SPACE.matcher(String.valueOf(title == null ? "" : title)).replaceAll("");
        if (isWindowsReservedBasename(safe)) {
            safe = "_" + safe;
        }
        return safe;
    }

    private boolean isWindowsReservedBasename(String title) {
        String safe = String.valueOf(title == null ? "" : title).trim();
        if (safe.isEmpty()) {
            return false;
        }
        int dotIndex = safe.indexOf('.');
        String basename = dotIndex >= 0 ? safe.substring(0, dotIndex) : safe;
        if (basename.isBlank()) {
            return false;
        }
        return WINDOWS_RESERVED_BASENAMES.contains(basename.toUpperCase(Locale.ROOT));
    }

    private String normalizeDisplayTerm(String rawTerm) {
        String term = String.valueOf(rawTerm == null ? "" : rawTerm).trim();
        if (!StringUtils.hasText(term)) {
            return "";
        }
        return term.replaceAll("\\s+", " ").trim();
    }

    private String normalizeLookupKey(String term) {
        return normalizeDisplayTerm(term).toLowerCase(Locale.ROOT);
    }

    private Path resolveCardPath(String safeTitle) {
        if (cardsRoot == null) {
            init();
        }
        Path candidate = cardsRoot.resolve(safeTitle + CARD_EXTENSION).toAbsolutePath().normalize();
        Path normalizedRoot = cardsRoot.toAbsolutePath().normalize();
        if (!candidate.startsWith(normalizedRoot)) {
            throw new IllegalArgumentException("invalid card path");
        }
        return candidate;
    }

    private Path resolveCardsRoot() {
        if (StringUtils.hasText(configuredCardsRoot)) {
            return Paths.get(configuredCardsRoot.trim()).toAbsolutePath().normalize();
        }
        Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        for (int i = 0; i < 8; i += 1) {
            Path candidate = current.resolve("var").resolve("storage").resolve("cards");
            if (Files.isDirectory(candidate)) {
                return candidate.toAbsolutePath().normalize();
            }
            Path parent = current.getParent();
            if (parent == null) {
                break;
            }
            current = parent;
        }
        return Paths.get("var", "storage", "cards").toAbsolutePath().normalize();
    }

    private static class ParsedFrontmatterList {
        private final List<String> items;
        private final int lastConsumedIndex;

        private ParsedFrontmatterList(List<String> items, int lastConsumedIndex) {
            this.items = items == null ? List.of() : List.copyOf(items);
            this.lastConsumedIndex = lastConsumedIndex;
        }
    }

    private static class CardDocument {
        private final Map<String, String> frontmatter;
        private final String body;

        private CardDocument(Map<String, String> frontmatter, String body) {
            this.frontmatter = frontmatter == null ? Map.of() : frontmatter;
            this.body = body == null ? "" : body;
        }

        private static CardDocument empty() {
            return new CardDocument(Map.of(), "");
        }
    }

    private static class CardMetadata {
        private final String title;
        private final String created;
        private final String type;
        private final List<String> tags;
        private final List<String> aliases;

        private CardMetadata(String title, String created, String type, List<String> tags, List<String> aliases) {
            this.title = title;
            this.created = created;
            this.type = type;
            this.tags = tags == null ? List.of() : List.copyOf(tags);
            this.aliases = aliases == null ? List.of() : List.copyOf(aliases);
        }
    }

    public static class CardWriteOptions {
        public Boolean contextDependent;
        public String type;
        public String created;
        public List<String> tags;
        public List<String> aliases;
        public String sourceTaskId;
        public String sourcePath;
    }

    public static class CardBacklinkItem {
        public final String sourceTitle;
        public final int count;

        public CardBacklinkItem(String sourceTitle, int count) {
            this.sourceTitle = String.valueOf(sourceTitle == null ? "" : sourceTitle).trim();
            this.count = Math.max(0, count);
        }
    }

    public static class CardReadResult {
        public final String title;
        public final String markdown;
        public final Path path;
        public final boolean exists;
        public final String created;
        public final String type;
        public final List<String> tags;
        public final List<String> aliases;

        public CardReadResult(
                String title,
                String markdown,
                Path path,
                boolean exists,
                String created,
                String type,
                List<String> tags,
                List<String> aliases
        ) {
            this.title = title;
            this.markdown = markdown == null ? "" : markdown;
            this.path = path;
            this.exists = exists;
            this.created = created == null ? "" : created;
            this.type = type == null ? "concept" : type;
            this.tags = tags == null ? List.of() : List.copyOf(tags);
            this.aliases = aliases == null ? List.of() : List.copyOf(aliases);
        }
    }

    public static class CardSaveResult {
        public final String title;
        public final Path path;
        public final int size;
        public final String updatedAt;
        public final String created;
        public final String type;
        public final List<String> tags;
        public final List<String> aliases;
        public final String targetType;
        public final String targetPath;
        public final Map<String, Object> locator;
        public final String revision;

        public CardSaveResult(
                String title,
                Path path,
                int size,
                String updatedAt,
                String created,
                String type,
                List<String> tags
        ) {
            this(
                    title,
                    path,
                    size,
                    updatedAt,
                    created,
                    type,
                    tags,
                    List.of(),
                    "global",
                    path,
                    Map.of("kind", "title", "value", title),
                    updatedAt
            );
        }

        public CardSaveResult(
                String title,
                Path path,
                int size,
                String updatedAt,
                String created,
                String type,
                List<String> tags,
                List<String> aliases,
                String targetType,
                Path targetPath,
                Map<String, Object> locator,
                String revision
        ) {
            this.title = title;
            this.path = path;
            this.size = size;
            this.updatedAt = updatedAt;
            this.created = created == null ? "" : created;
            this.type = type == null ? "concept" : type;
            this.tags = tags == null ? List.of() : List.copyOf(tags);
            this.aliases = aliases == null ? List.of() : List.copyOf(aliases);
            this.targetType = StringUtils.hasText(targetType) ? targetType.trim() : "global";
            this.targetPath = targetPath == null ? "" : targetPath.toString();
            this.locator = locator == null ? Map.of() : Map.copyOf(locator);
            this.revision = StringUtils.hasText(revision) ? revision.trim() : this.updatedAt;
        }
    }
}
