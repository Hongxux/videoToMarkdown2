package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
public class TaskBundleExportService {
    private static final Logger logger = LoggerFactory.getLogger(TaskBundleExportService.class);
    private static final String META_FILE_NAME = "mobile_task_meta.json";
    private static final String MANIFEST_FILE_NAME = "export_manifest.json";
    private static final String OBSIDIAN_NOTE_PREFIX = "obsidian-note:";
    private static final int COPY_BUFFER_BYTES = 64 * 1024;
    private static final Pattern MARKDOWN_LINK_PATTERN = Pattern.compile("(!?\\[[^\\]]*])\\((<[^>]+>|[^)\\s]+)([^)]*)\\)");
    private static final Pattern HTML_ASSET_ATTR_PATTERN = Pattern.compile("(?i)(<(?:img|video|source|audio|a)\\b[^>]*?\\b(?:src|poster|href)\\s*=\\s*)([\"'])(.*?)(\\2)");
    private static final Pattern OBSIDIAN_EMBED_PATTERN = Pattern.compile("!\\[\\[([^\\]]+)\\]\\]");
    private static final Pattern OBSIDIAN_WIKILINK_PATTERN = Pattern.compile("(^|[^!])\\[\\[([^\\]]+)\\]\\]", Pattern.MULTILINE);
    private static final Pattern API_TASK_ASSET_PATH_PATTERN = Pattern.compile("^/api/mobile/tasks/[^/]+/asset$", Pattern.CASE_INSENSITIVE);
    private static final Set<String> MARKDOWN_EXTENSIONS = Set.of(".md", ".markdown");
    private static final Set<String> IMAGE_EXTENSIONS = Set.of(".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg");
    private static final Set<String> VIDEO_EXTENSIONS = Set.of(".mp4", ".webm", ".mov", ".m4v");
    private static final Set<String> EXTERNAL_URL_PREFIXES = Set.of("http://", "https://", "data:", "blob:", "mailto:", "tel:", "javascript:");
    private final ObjectMapper objectMapper = new ObjectMapper();

    public FlatTaskExportPlan planFlatExport(String taskId, Path taskRoot, Path mainMarkdownPath) throws IOException {
        Path normalizedRoot = requireDirectory(taskRoot);
        Path normalizedMainMarkdown = requireFileWithinRoot(normalizedRoot, mainMarkdownPath);
        LinkedHashMap<Path, CollectedFile> collectedByPath = new LinkedHashMap<>();
        Deque<CollectedFile> markdownQueue = new ArrayDeque<>();
        collectMarkdown(collectedByPath, markdownQueue, normalizedRoot, normalizedMainMarkdown, ExportRole.MAIN_MARKDOWN, null, null, null);
        for (AnchorMountedEntry anchorEntry : collectMountedAnchorEntries(normalizedRoot)) {
            if (anchorEntry.notePath() != null) {
                collectMarkdown(collectedByPath, markdownQueue, normalizedRoot, anchorEntry.notePath(), ExportRole.ANCHOR_NOTE, anchorEntry.anchorId(), anchorEntry.noteKey(), null);
            }
            for (Path attachmentPath : anchorEntry.attachmentPaths()) {
                collectBinary(collectedByPath, normalizedRoot, attachmentPath, ExportRole.ANCHOR_ATTACHMENT, anchorEntry.anchorId(), anchorEntry.noteKey(), anchorEntry.notePath() != null ? toRelativePath(normalizedRoot, anchorEntry.notePath()) : null);
            }
        }
        while (!markdownQueue.isEmpty()) {
            CollectedFile current = markdownQueue.removeFirst();
            for (String rawUrl : extractLocalReferenceUrls(current.markdownContent())) {
                ResolvedReference ref = resolveLocalReference(normalizedRoot, current.path().getParent(), rawUrl);
                if (ref == null) {
                    continue;
                }
                if (isMarkdownFile(ref.path().getFileName() != null ? ref.path().getFileName().toString() : "")) {
                    collectMarkdown(collectedByPath, markdownQueue, normalizedRoot, ref.path(), ExportRole.LINKED_MARKDOWN, current.anchorId(), current.noteKey(), current.relativePath());
                } else {
                    collectBinary(collectedByPath, normalizedRoot, ref.path(), ExportRole.REFERENCED_ASSET, current.anchorId(), current.noteKey(), current.relativePath());
                }
            }
        }
        List<CollectedFile> orderedFiles = new ArrayList<>(collectedByPath.values());
        orderedFiles.sort(Comparator.comparingInt((CollectedFile item) -> item.role().priority()).thenComparingInt(item -> item.role().ordinal()).thenComparing(CollectedFile::relativePath));
        FlatNameAllocator allocator = new FlatNameAllocator();
        List<ExportedFile> exportedFiles = new ArrayList<>();
        Map<Path, String> entryNameByPath = new HashMap<>();
        for (CollectedFile file : orderedFiles) {
            String entryName = allocator.allocate(suggestFlatEntryName(taskId, file));
            exportedFiles.add(new ExportedFile(file, entryName));
            entryNameByPath.put(file.path(), entryName);
        }
        List<PreparedZipEntry> entries = new ArrayList<>();
        String mainMarkdownEntryName = "";
        int markdownCount = 0;
        int binaryCount = 0;
        for (ExportedFile exported : exportedFiles) {
            CollectedFile file = exported.file();
            if (file.path().equals(normalizedMainMarkdown)) {
                mainMarkdownEntryName = exported.entryName();
            }
            if (file.kind() == CollectedKind.MARKDOWN) {
                markdownCount += 1;
                String rewritten = rewriteMarkdownLinks(file.markdownContent(), normalizedRoot, file.path().getParent(), entryNameByPath);
                entries.add(new PreparedZipEntry(exported.entryName(), rewritten.getBytes(StandardCharsets.UTF_8), null, file.relativePath(), file.kind().apiValue(), file.role().apiValue(), file.anchorId()));
            } else {
                binaryCount += 1;
                entries.add(new PreparedZipEntry(exported.entryName(), null, file.path(), file.relativePath(), file.kind().apiValue(), file.role().apiValue(), file.anchorId()));
            }
        }
        entries.add(new PreparedZipEntry(MANIFEST_FILE_NAME, buildManifestBytes(taskId, normalizedRoot, mainMarkdownEntryName, exportedFiles), null, MANIFEST_FILE_NAME, "generated", "manifest", null));
        return new FlatTaskExportPlan(taskId, normalizedRoot, mainMarkdownEntryName, List.copyOf(entries), exportedFiles.size(), markdownCount, binaryCount);
    }

    public ExportZipResult writeFlatZipStreaming(FlatTaskExportPlan plan, ZipOutputStream zos) throws IOException {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(zos, "zos");
        int exportedCount = 0;
        long exportedBytes = 0L;
        byte[] buffer = new byte[COPY_BUFFER_BYTES];
        for (PreparedZipEntry entry : plan.entries()) {
            zos.putNextEntry(new ZipEntry(entry.entryName()));
            if (entry.inlineBytes() != null) {
                zos.write(entry.inlineBytes());
                exportedBytes += entry.inlineBytes().length;
            } else if (entry.sourcePath() != null) {
                try (InputStream inputStream = Files.newInputStream(entry.sourcePath())) {
                    exportedBytes += copyInChunks(inputStream, zos, buffer);
                }
            }
            zos.closeEntry();
            exportedCount += 1;
        }
        return new ExportZipResult(exportedCount, exportedBytes);
    }

    private void collectMarkdown(Map<Path, CollectedFile> collectedByPath, Deque<CollectedFile> markdownQueue, Path taskRoot, Path candidate, ExportRole role, String anchorId, String noteKey, String sourceMarkdown) throws IOException {
        Path normalized = requireFileWithinRoot(taskRoot, candidate);
        if (collectedByPath.containsKey(normalized)) {
            return;
        }
        CollectedFile created = new CollectedFile(normalized, toRelativePath(taskRoot, normalized), CollectedKind.MARKDOWN, role, anchorId, noteKey, sourceMarkdown, Files.readString(normalized, StandardCharsets.UTF_8));
        collectedByPath.put(normalized, created);
        markdownQueue.addLast(created);
    }

    private void collectBinary(Map<Path, CollectedFile> collectedByPath, Path taskRoot, Path candidate, ExportRole role, String anchorId, String noteKey, String sourceMarkdown) throws IOException {
        Path normalized = requireFileWithinRoot(taskRoot, candidate);
        if (collectedByPath.containsKey(normalized)) {
            return;
        }
        collectedByPath.put(normalized, new CollectedFile(normalized, toRelativePath(taskRoot, normalized), CollectedKind.BINARY, role, anchorId, noteKey, sourceMarkdown, null));
    }

    private List<AnchorMountedEntry> collectMountedAnchorEntries(Path taskRoot) {
        Path metaPath = taskRoot.resolve(META_FILE_NAME).normalize();
        if (!metaPath.startsWith(taskRoot) || !Files.isRegularFile(metaPath)) {
            return List.of();
        }
        try {
            JsonNode root = objectMapper.readTree(metaPath.toFile());
            JsonNode notesByMarkdown = root.path("notesByMarkdown");
            if (!notesByMarkdown.isObject()) {
                return List.of();
            }
            List<AnchorMountedEntry> output = new ArrayList<>();
            Iterator<Map.Entry<String, JsonNode>> noteIterator = notesByMarkdown.fields();
            while (noteIterator.hasNext()) {
                Map.Entry<String, JsonNode> noteEntry = noteIterator.next();
                JsonNode anchors = noteEntry.getValue().path("anchors");
                if (!anchors.isObject()) {
                    continue;
                }
                Iterator<Map.Entry<String, JsonNode>> anchorIterator = anchors.fields();
                while (anchorIterator.hasNext()) {
                    Map.Entry<String, JsonNode> anchorEntry = anchorIterator.next();
                    JsonNode latestRevision = lastRevisionNode(anchorEntry.getValue().path("revisions"));
                    String notePathText = latestRevision != null ? trimToNull(latestRevision.path("notePath").asText(null)) : null;
                    if (notePathText == null) {
                        notePathText = trimToNull(anchorEntry.getValue().path("mountedPath").asText(null));
                    }
                    Path notePath = resolvePathWithinTask(taskRoot, notePathText);
                    LinkedHashSet<Path> attachments = new LinkedHashSet<>();
                    if (latestRevision != null && latestRevision.path("files").isArray()) {
                        for (JsonNode fileNode : latestRevision.path("files")) {
                            Path one = resolvePathWithinTask(taskRoot, trimToNull(fileNode.path("path").asText(null)));
                            if (one != null && Files.isRegularFile(one)) {
                                attachments.add(one);
                            }
                        }
                    }
                    if (notePath != null || !attachments.isEmpty()) {
                        output.add(new AnchorMountedEntry(trimToNull(anchorEntry.getKey()), trimToNull(noteEntry.getKey()), notePath, List.copyOf(attachments)));
                    }
                }
            }
            return output;
        } catch (Exception ex) {
            logger.warn("read export anchor metadata failed: metaPath={} err={}", metaPath, ex.getMessage());
            return List.of();
        }
    }

    private JsonNode lastRevisionNode(JsonNode revisionsNode) {
        if (revisionsNode == null || !revisionsNode.isArray() || revisionsNode.isEmpty()) {
            return null;
        }
        for (int index = revisionsNode.size() - 1; index >= 0; index -= 1) {
            JsonNode candidate = revisionsNode.get(index);
            if (candidate != null && candidate.isObject()) {
                return candidate;
            }
        }
        return null;
    }

    private List<String> extractLocalReferenceUrls(String markdown) {
        if (markdown == null || markdown.isBlank()) {
            return List.of();
        }
        LinkedHashSet<String> urls = new LinkedHashSet<>();
        Matcher markdownMatcher = MARKDOWN_LINK_PATTERN.matcher(markdown);
        while (markdownMatcher.find()) {
            String rawUrl = stripAngleBrackets(markdownMatcher.group(2));
            if (trimToNull(rawUrl) != null) {
                urls.add(rawUrl);
            }
        }
        Matcher htmlMatcher = HTML_ASSET_ATTR_PATTERN.matcher(markdown);
        while (htmlMatcher.find()) {
            String rawUrl = trimToNull(htmlMatcher.group(3));
            if (rawUrl != null) {
                urls.add(rawUrl);
            }
        }
        Matcher obsidianEmbedMatcher = OBSIDIAN_EMBED_PATTERN.matcher(markdown);
        while (obsidianEmbedMatcher.find()) {
            ObsidianLinkParts link = parseObsidianLinkParts(obsidianEmbedMatcher.group(1));
            String notePath = link != null ? trimToNull(link.notePath()) : null;
            if (notePath != null) {
                urls.add(notePath);
            }
        }
        Matcher obsidianWikilinkMatcher = OBSIDIAN_WIKILINK_PATTERN.matcher(markdown);
        while (obsidianWikilinkMatcher.find()) {
            ObsidianLinkParts link = parseObsidianLinkParts(obsidianWikilinkMatcher.group(2));
            String notePath = link != null ? trimToNull(link.notePath()) : null;
            if (notePath != null) {
                urls.add(notePath);
            }
        }
        return List.copyOf(urls);
    }

    private String rewriteMarkdownLinks(String markdown, Path taskRoot, Path baseDir, Map<Path, String> entryNameByPath) {
        if (markdown == null || markdown.isBlank()) {
            return markdown != null ? markdown : "";
        }
        String afterMarkdown = rewriteMarkdownStyleLinks(markdown, taskRoot, baseDir, entryNameByPath);
        String afterHtml = rewriteHtmlAssetLinks(afterMarkdown, taskRoot, baseDir, entryNameByPath);
        String afterObsidianEmbed = rewriteObsidianEmbedLinks(afterHtml, taskRoot, baseDir, entryNameByPath);
        return rewriteObsidianWikilinks(afterObsidianEmbed, taskRoot, baseDir, entryNameByPath);
    }

    private String rewriteMarkdownStyleLinks(String markdown, Path taskRoot, Path baseDir, Map<Path, String> entryNameByPath) {
        Matcher matcher = MARKDOWN_LINK_PATTERN.matcher(markdown);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String replacementUrl = resolveRewrittenEntryName(taskRoot, baseDir, stripAngleBrackets(matcher.group(2)), entryNameByPath);
            if (replacementUrl == null) {
                matcher.appendReplacement(buffer, Matcher.quoteReplacement(matcher.group(0)));
            } else {
                matcher.appendReplacement(buffer, Matcher.quoteReplacement(matcher.group(1) + "(" + replacementUrl + matcher.group(3) + ")"));
            }
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private String rewriteHtmlAssetLinks(String markdown, Path taskRoot, Path baseDir, Map<Path, String> entryNameByPath) {
        Matcher matcher = HTML_ASSET_ATTR_PATTERN.matcher(markdown);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String replacementUrl = resolveRewrittenEntryName(taskRoot, baseDir, matcher.group(3), entryNameByPath);
            if (replacementUrl == null) {
                matcher.appendReplacement(buffer, Matcher.quoteReplacement(matcher.group(0)));
            } else {
                matcher.appendReplacement(buffer, Matcher.quoteReplacement(matcher.group(1) + matcher.group(2) + replacementUrl + matcher.group(4)));
            }
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private String rewriteObsidianEmbedLinks(String markdown, Path taskRoot, Path baseDir, Map<Path, String> entryNameByPath) {
        Matcher matcher = OBSIDIAN_EMBED_PATTERN.matcher(markdown);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String rewrittenBody = rewriteObsidianLinkBody(taskRoot, baseDir, matcher.group(1), entryNameByPath);
            if (rewrittenBody == null) {
                matcher.appendReplacement(buffer, Matcher.quoteReplacement(matcher.group(0)));
            } else {
                matcher.appendReplacement(buffer, Matcher.quoteReplacement("![[" + rewrittenBody + "]]"));
            }
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private String rewriteObsidianWikilinks(String markdown, Path taskRoot, Path baseDir, Map<Path, String> entryNameByPath) {
        Matcher matcher = OBSIDIAN_WIKILINK_PATTERN.matcher(markdown);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String rewrittenBody = rewriteObsidianLinkBody(taskRoot, baseDir, matcher.group(2), entryNameByPath);
            if (rewrittenBody == null) {
                matcher.appendReplacement(buffer, Matcher.quoteReplacement(matcher.group(0)));
            } else {
                matcher.appendReplacement(buffer, Matcher.quoteReplacement(matcher.group(1) + "[[" + rewrittenBody + "]]"));
            }
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private String rewriteObsidianLinkBody(Path taskRoot, Path baseDir, String rawBody, Map<Path, String> entryNameByPath) {
        ObsidianLinkParts link = parseObsidianLinkParts(rawBody);
        String notePath = link != null ? trimToNull(link.notePath()) : null;
        if (notePath == null) {
            return null;
        }
        ResolvedReference ref = resolveLocalReference(taskRoot, baseDir, notePath);
        if (ref == null) {
            return null;
        }
        String entryName = entryNameByPath.get(ref.path());
        if (entryName == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder(entryName);
        String noteAnchor = trimToNull(link.noteAnchor());
        if (noteAnchor != null) {
            builder.append('#').append(noteAnchor);
        }
        String alias = trimToNull(link.alias());
        if (alias != null) {
            builder.append('|').append(alias);
        }
        return builder.toString();
    }

    private String resolveRewrittenEntryName(Path taskRoot, Path baseDir, String rawUrl, Map<Path, String> entryNameByPath) {
        ResolvedReference ref = resolveLocalReference(taskRoot, baseDir, rawUrl);
        if (ref == null) {
            return null;
        }
        String entryName = entryNameByPath.get(ref.path());
        return entryName == null ? null : entryName + ref.suffix();
    }

    private ResolvedReference resolveLocalReference(Path taskRoot, Path baseDir, String rawUrl) {
        String url = trimToNull(rawUrl);
        if (url == null) {
            return null;
        }
        String lower = url.toLowerCase(Locale.ROOT);
        if (lower.startsWith("#")) {
            return null;
        }
        if (lower.startsWith(OBSIDIAN_NOTE_PREFIX)) {
            return resolveObsidianNoteReference(taskRoot, baseDir, url);
        }
        if (lower.startsWith("/api/mobile/tasks/")) {
            return resolveTaskApiAssetReference(taskRoot, url);
        }
        for (String prefix : EXTERNAL_URL_PREFIXES) {
            if (lower.startsWith(prefix)) {
                return null;
            }
        }
        UrlParts urlParts = splitUrlParts(url);
        String decodedPath = decodeUrlComponent(stripAngleBrackets(urlParts.pathPart()));
        if (trimToNull(decodedPath) == null) {
            return null;
        }
        try {
            Path candidate = Paths.get(decodedPath);
            Path normalized = candidate.isAbsolute() ? candidate.toAbsolutePath().normalize() : baseDir.resolve(candidate).normalize();
            if (!normalized.startsWith(taskRoot) || !Files.isRegularFile(normalized)) {
                return null;
            }
            return new ResolvedReference(normalized, urlParts.suffix());
        } catch (Exception ex) {
            return null;
        }
    }

    private ResolvedReference resolveObsidianNoteReference(Path taskRoot, Path baseDir, String rawUrl) {
        String payload = trimToNull(rawUrl);
        if (payload == null || payload.length() <= OBSIDIAN_NOTE_PREFIX.length()) {
            return null;
        }
        UrlParts urlParts = splitUrlParts(payload.substring(OBSIDIAN_NOTE_PREFIX.length()));
        String decodedPath = decodeUrlComponent(stripAngleBrackets(urlParts.pathPart()));
        if (trimToNull(decodedPath) == null) {
            return null;
        }
        try {
            Path candidate = Paths.get(decodedPath);
            Path normalized = candidate.isAbsolute() ? candidate.toAbsolutePath().normalize() : baseDir.resolve(candidate).normalize();
            if (!normalized.startsWith(taskRoot) || !Files.isRegularFile(normalized)) {
                return null;
            }
            return new ResolvedReference(normalized, urlParts.suffix());
        } catch (Exception ex) {
            return null;
        }
    }

    private ResolvedReference resolveTaskApiAssetReference(Path taskRoot, String rawUrl) {
        String url = trimToNull(rawUrl);
        if (url == null) {
            return null;
        }
        int hashIndex = url.indexOf('#');
        String suffix = hashIndex >= 0 ? url.substring(hashIndex) : "";
        String withoutHash = hashIndex >= 0 ? url.substring(0, hashIndex) : url;
        int queryIndex = withoutHash.indexOf('?');
        if (queryIndex <= 0) {
            return null;
        }
        String route = withoutHash.substring(0, queryIndex);
        if (!API_TASK_ASSET_PATH_PATTERN.matcher(route).matches()) {
            return null;
        }
        String encodedPath = findQueryParamValue(withoutHash.substring(queryIndex + 1), "path");
        Path resolved = resolvePathWithinTask(taskRoot, encodedPath);
        return resolved != null ? new ResolvedReference(resolved, suffix) : null;
    }

    private String findQueryParamValue(String query, String name) {
        String normalizedName = trimToNull(name);
        String normalizedQuery = trimToNull(query);
        if (normalizedName == null || normalizedQuery == null) {
            return null;
        }
        for (String part : normalizedQuery.split("&")) {
            if (part == null || part.isBlank()) {
                continue;
            }
            int equalsIndex = part.indexOf('=');
            String key = equalsIndex >= 0 ? part.substring(0, equalsIndex) : part;
            if (!normalizedName.equals(decodeUrlComponent(key))) {
                continue;
            }
            String value = equalsIndex >= 0 ? part.substring(equalsIndex + 1) : "";
            return decodeUrlComponent(value);
        }
        return null;
    }

    private Path requireDirectory(Path taskRoot) throws IOException {
        if (taskRoot == null) {
            throw new IOException("task root is null");
        }
        Path normalized = taskRoot.toAbsolutePath().normalize();
        if (!Files.isDirectory(normalized)) {
            throw new IOException("task root does not exist: " + normalized);
        }
        return normalized;
    }

    private Path requireFileWithinRoot(Path taskRoot, Path file) throws IOException {
        if (file == null) {
            throw new IOException("file is null");
        }
        Path normalized = file.toAbsolutePath().normalize();
        if (!normalized.startsWith(taskRoot) || !Files.isRegularFile(normalized)) {
            throw new IOException("file is outside task root or missing: " + normalized);
        }
        return normalized;
    }

    private Path resolvePathWithinTask(Path taskRoot, String rawPath) {
        String normalizedText = trimToNull(rawPath);
        if (normalizedText == null) {
            return null;
        }
        String decoded = decodeUrlComponent(normalizedText).replace('\\', '/');
        try {
            Path candidate = Paths.get(decoded);
            Path normalized = candidate.isAbsolute() ? candidate.toAbsolutePath().normalize() : taskRoot.resolve(candidate).normalize();
            if (!normalized.startsWith(taskRoot) || !Files.isRegularFile(normalized)) {
                return null;
            }
            return normalized;
        } catch (Exception ex) {
            return null;
        }
    }

    private UrlParts splitUrlParts(String url) {
        int queryIndex = url.indexOf('?');
        int hashIndex = url.indexOf('#');
        int cutIndex = queryIndex >= 0 && hashIndex >= 0 ? Math.min(queryIndex, hashIndex) : Math.max(queryIndex, hashIndex);
        return cutIndex < 0 ? new UrlParts(url, "") : new UrlParts(url.substring(0, cutIndex), url.substring(cutIndex));
    }

    private String suggestFlatEntryName(String taskId, CollectedFile file) {
        String originalName = file.path().getFileName() != null ? file.path().getFileName().toString() : "file";
        String safe = sanitizeFlatFilename(originalName);
        if (shouldPrefixTaskIdForBinaryAsset(file)) {
            String taskIdPrefix = sanitizeTaskIdPrefix(taskId);
            if (taskIdPrefix != null) {
                safe = sanitizeFlatFilename(taskIdPrefix + "_" + safe);
            }
        }
        if ((file.role() == ExportRole.ANCHOR_NOTE || file.role() == ExportRole.ANCHOR_ATTACHMENT) && file.anchorId() != null) {
            if (shouldPrefixTaskIdForBinaryAsset(file)) {
                return safe;
            }
            return sanitizeFlatFilename("anchor_" + file.anchorId() + "_" + safe);
        }
        return safe;
    }

    private boolean shouldPrefixTaskIdForBinaryAsset(CollectedFile file) {
        if (file == null || file.kind() != CollectedKind.BINARY) {
            return false;
        }
        String filename = file.path().getFileName() != null ? file.path().getFileName().toString() : "";
        return hasExtension(filename, IMAGE_EXTENSIONS) || hasExtension(filename, VIDEO_EXTENSIONS);
    }

    private String sanitizeTaskIdPrefix(String taskId) {
        String normalized = trimToNull(taskId);
        if (normalized == null) {
            return null;
        }
        String safe = sanitizeFlatFilename(normalized);
        return safe.isBlank() ? null : safe;
    }

    private String sanitizeFlatFilename(String raw) {
        String candidate = raw == null ? "file" : raw.trim();
        if (candidate.isEmpty()) {
            candidate = "file";
        }
        candidate = candidate.replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_').replace('?', '_').replace('"', '_').replace('<', '_').replace('>', '_').replace('|', '_');
        return candidate.isBlank() ? "file" : candidate;
    }

    private boolean isMarkdownFile(String filename) {
        String normalized = trimToNull(filename);
        if (normalized == null) {
            return false;
        }
        String lower = normalized.toLowerCase(Locale.ROOT);
        for (String extension : MARKDOWN_EXTENSIONS) {
            if (lower.endsWith(extension)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasExtension(String filename, Set<String> extensions) {
        String normalized = trimToNull(filename);
        if (normalized == null) {
            return false;
        }
        String lower = normalized.toLowerCase(Locale.ROOT);
        for (String extension : extensions) {
            if (lower.endsWith(extension)) {
                return true;
            }
        }
        return false;
    }

    private ObsidianLinkParts parseObsidianLinkParts(String rawBody) {
        String body = trimToNull(rawBody);
        if (body == null) {
            return null;
        }
        int splitIndex = body.indexOf('|');
        String targetPart = splitIndex >= 0 ? body.substring(0, splitIndex) : body;
        String aliasPart = splitIndex >= 0 ? body.substring(splitIndex + 1) : "";
        String rawTarget = trimToNull(targetPart);
        if (rawTarget == null) {
            return null;
        }
        String notePathPart = rawTarget;
        String noteAnchor = "";
        int hashIndex = rawTarget.indexOf('#');
        if (hashIndex >= 0) {
            notePathPart = rawTarget.substring(0, hashIndex).trim();
            noteAnchor = rawTarget.substring(hashIndex + 1).trim();
        }
        return new ObsidianLinkParts(body, rawTarget, trimToNull(aliasPart), normalizeObsidianNotePath(notePathPart), noteAnchor);
    }

    private String normalizeObsidianNotePath(String rawPath) {
        String normalized = trimToNull(rawPath);
        if (normalized == null) {
            return "";
        }
        String decoded = decodeUrlComponent(normalized).replace('\\', '/');
        while (decoded.startsWith("./")) {
            decoded = decoded.substring(2);
        }
        decoded = decoded.replaceFirst("^/+", "");
        if (decoded.isBlank()) {
            return "";
        }
        int lastSlash = decoded.lastIndexOf('/');
        int lastDot = decoded.lastIndexOf('.');
        if (lastDot > lastSlash) {
            return decoded;
        }
        return decoded + ".md";
    }

    private String toRelativePath(Path taskRoot, Path file) {
        return taskRoot.relativize(file).toString().replace('\\', '/');
    }

    private String stripAngleBrackets(String rawUrl) {
        String value = trimToNull(rawUrl);
        if (value == null) {
            return null;
        }
        return value.startsWith("<") && value.endsWith(">") && value.length() >= 2 ? value.substring(1, value.length() - 1).trim() : value;
    }

    private String decodeUrlComponent(String value) {
        if (value == null) {
            return null;
        }
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (Exception ex) {
            return value;
        }
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private byte[] buildManifestBytes(String taskId, Path taskRoot, String mainMarkdownEntryName, List<ExportedFile> exportedFiles) throws IOException {
        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("schema", "mobile_flat_export.v1");
        manifest.put("generatedAt", Instant.now().toString());
        manifest.put("taskId", taskId != null ? taskId : "");
        manifest.put("layout", "flat");
        manifest.put("taskRoot", taskRoot.toString());
        manifest.put("mainMarkdown", mainMarkdownEntryName);
        List<Map<String, Object>> items = new ArrayList<>();
        for (ExportedFile exported : exportedFiles) {
            CollectedFile file = exported.file();
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("entryName", exported.entryName());
            item.put("originalPath", file.relativePath());
            item.put("kind", file.kind().apiValue());
            item.put("role", file.role().apiValue());
            if (file.anchorId() != null) {
                item.put("anchorId", file.anchorId());
            }
            if (file.noteKey() != null) {
                item.put("noteKey", file.noteKey());
            }
            if (file.sourceMarkdown() != null) {
                item.put("sourceMarkdown", file.sourceMarkdown());
            }
            items.add(item);
        }
        manifest.put("items", items);
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(manifest);
    }

    private long copyInChunks(InputStream inputStream, ZipOutputStream zos, byte[] buffer) throws IOException {
        long total = 0L;
        long sinceFlush = 0L;
        int read;
        while ((read = inputStream.read(buffer)) >= 0) {
            if (read == 0) {
                continue;
            }
            zos.write(buffer, 0, read);
            total += read;
            sinceFlush += read;
            if (sinceFlush >= 256L * 1024L) {
                zos.flush();
                sinceFlush = 0L;
            }
        }
        if (sinceFlush > 0L) {
            zos.flush();
        }
        return total;
    }

    public record FlatTaskExportPlan(String taskId, Path taskRoot, String mainMarkdownEntryName, List<PreparedZipEntry> entries, int collectedFileCount, int markdownCount, int binaryCount) {}
    public record PreparedZipEntry(String entryName, byte[] inlineBytes, Path sourcePath, String originalPath, String kind, String role, String anchorId) {}
    public record ExportZipResult(int exportedCount, long exportedBytes) {}
    private record CollectedFile(Path path, String relativePath, CollectedKind kind, ExportRole role, String anchorId, String noteKey, String sourceMarkdown, String markdownContent) {}
    private record ExportedFile(CollectedFile file, String entryName) {}
    private record AnchorMountedEntry(String anchorId, String noteKey, Path notePath, List<Path> attachmentPaths) {}
    private record ResolvedReference(Path path, String suffix) {}
    private record UrlParts(String pathPart, String suffix) {}
    private record ObsidianLinkParts(String raw, String rawTarget, String alias, String notePath, String noteAnchor) {}

    private enum CollectedKind {
        MARKDOWN("markdown"), BINARY("binary");
        private final String apiValue;
        CollectedKind(String apiValue) { this.apiValue = apiValue; }
        public String apiValue() { return apiValue; }
    }

    private enum ExportRole {
        MAIN_MARKDOWN("main_markdown", 0), ANCHOR_NOTE("anchor_note", 1), LINKED_MARKDOWN("linked_markdown", 2), REFERENCED_ASSET("referenced_asset", 3), ANCHOR_ATTACHMENT("anchor_attachment", 4);
        private final String apiValue;
        private final int priority;
        ExportRole(String apiValue, int priority) { this.apiValue = apiValue; this.priority = priority; }
        public String apiValue() { return apiValue; }
        public int priority() { return priority; }
    }

    private static final class FlatNameAllocator {
        private final Set<String> usedNames = new LinkedHashSet<>();
        private String allocate(String requestedName) {
            String safe = requestedName == null || requestedName.isBlank() ? "file" : requestedName;
            if (usedNames.add(safe)) {
                return safe;
            }
            String base = safe;
            String extension = "";
            int lastDot = safe.lastIndexOf('.');
            if (lastDot > 0 && lastDot < safe.length() - 1) {
                base = safe.substring(0, lastDot);
                extension = safe.substring(lastDot);
            }
            for (int index = 2; ; index += 1) {
                String candidate = base + "__" + index + extension;
                if (usedNames.add(candidate)) {
                    return candidate;
                }
            }
        }
    }
}