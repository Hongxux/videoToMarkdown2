package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class Phase2bArticleLinkService {

    private static final Logger logger = LoggerFactory.getLogger(Phase2bArticleLinkService.class);
    private static final Pattern ZHIHU_LINK_PATTERN = Pattern.compile("^/p/(\\d+)$");
    private static final Pattern ZHIHU_QUESTION_ANSWER_LINK_PATTERN = Pattern.compile("^/question/(\\d+)/answer/(\\d+)$");
    private static final Pattern JUEJIN_LINK_PATTERN = Pattern.compile("^/post/(\\d+)$");
    private static final Pattern SUMMARY_JSON_PATTERN = Pattern.compile("(?s)(\\{\\s*\"output_root\".*\\})\\s*$");
    private static final Pattern HTML_TITLE_PATTERN = Pattern.compile("(?is)<title[^>]*>(.*?)</title>");
    private static final Pattern META_TAG_PATTERN = Pattern.compile("(?is)<meta\\s+[^>]*>");
    private static final Pattern META_ATTR_PATTERN = Pattern.compile("(?is)([a-zA-Z_:][-a-zA-Z0-9_:.]*)\\s*=\\s*(['\"])(.*?)\\2");
    private static final Pattern LD_JSON_SCRIPT_PATTERN = Pattern.compile(
            "(?is)<script[^>]*type\\s*=\\s*['\"]application/ld\\+json['\"][^>]*>(.*?)</script>"
    );
    private static final int PROCESS_OUTPUT_TAIL_MAX_CHARS = 2200;

    @Value("${phase2b.link-extractor.python-command:D:/New_ANACONDA/envs/whisper_env/python.exe}")
    private String pythonCommand;

    @Value("${phase2b.link-extractor.script-path:D:/videoToMarkdownTest2/BettaFish/MindSpider/DeepSentimentCrawling/main.py}")
    private String scriptPathRaw;

    @Value("${phase2b.link-extractor.output-dir:D:/videoToMarkdownTest2/var/playwright_md}")
    private String outputDirRaw;

    @Value("${phase2b.link-extractor.timeout-seconds:420}")
    private int timeoutSeconds;

    @Value("${phase2b.link-extractor.page-timeout-ms:80000}")
    private int pageTimeoutMs;

    @Value("${phase2b.link-extractor.max-images-per-page:128}")
    private int maxImagesPerPage;

    @Value("${phase2b.link-extractor.max-links-per-request:6}")
    private int maxLinksPerRequest;

    @Value("${phase2b.link-extractor.headless:false}")
    private boolean headless;

    @Value("${phase2b.link-extractor.auto-login-fallback:true}")
    private boolean autoLoginFallback;

    @Value("${phase2b.link-extractor.zhihu-cdp-mode:true}")
    private boolean zhihuCdpMode;

    @Value("${phase2b.link-extractor.disable-zhihu-persistent-context:false}")
    private boolean disableZhihuPersistentContext;

    @Value("${phase2b.link-extractor.storage-state-path:D:/videoToMarkdownTest2/var/zhihu_storage_state.json}")
    private String storageStatePathRaw;

    @Value("${phase2b.link-extractor.zhihu-request-interval-sec:2.0}")
    private double zhihuRequestIntervalSec;

    @Value("${phase2b.link-extractor.allow-manual-intervention-on-auth-block:true}")
    private boolean allowManualInterventionOnAuthBlock;

    @Value("${phase2b.link-extractor.manual-login-wait-seconds:600}")
    private int manualLoginWaitSeconds;

    @Value("${phase2b.link-extractor.prefetch-timeout-ms:3800}")
    private int prefetchTimeoutMs;

    @Value("${phase2b.link-extractor.prefetch-user-agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36}")
    private String prefetchUserAgent;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Object titleHttpClientLock = new Object();
    private volatile HttpClient titleHttpClient;

    private static final class ExtractorRunResult {
        final int exitCode;
        final String output;

        ExtractorRunResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = String.valueOf(output == null ? "" : output);
        }
    }

    public List<String> normalizeSupportedLinks(List<String> rawLinks) {
        if (rawLinks == null || rawLinks.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> dedup = new LinkedHashSet<>();
        for (String raw : rawLinks) {
            String normalized = normalizeSupportedLink(raw);
            if (!StringUtils.hasText(normalized)) {
                continue;
            }
            dedup.add(normalized);
            if (dedup.size() >= Math.max(1, maxLinksPerRequest)) {
                break;
            }
        }
        return new ArrayList<>(dedup);
    }

    public List<LinkMetadata> prefetchLinkMetadata(List<String> rawLinks) {
        List<String> normalizedLinks = normalizeSupportedLinks(rawLinks);
        if (normalizedLinks.isEmpty()) {
            return List.of();
        }
        List<LinkMetadata> output = new ArrayList<>();
        for (String url : normalizedLinks) {
            String siteType = normalizeSiteType("", url);
            String title = "";
            String status = "resolved";
            try {
                title = fetchArticleTitle(url);
            } catch (Exception error) {
                logger.debug("prefetch title failed: url={} err={}", url, error.getMessage());
                status = "failed";
            }
            if (!StringUtils.hasText(title)) {
                if ("resolved".equals(status)) {
                    status = "failed";
                }
            }
            output.add(new LinkMetadata(url, siteType, title, status));
        }
        return output;
    }

    public LinkBatchExtractionResult extractArticles(List<String> rawLinks) {
        return extractArticles(rawLinks, false, false);
    }

    public LinkBatchExtractionResult extractArticlesForBook(List<String> rawLinks) {
        return extractArticles(rawLinks, true, true);
    }

    private LinkBatchExtractionResult extractArticles(
            List<String> rawLinks,
            boolean persistOutputToDisk,
            boolean saveImagesToDisk
    ) {
        List<String> normalizedLinks = normalizeSupportedLinks(rawLinks);
        if (normalizedLinks.isEmpty()) {
            return LinkBatchExtractionResult.empty();
        }

        Path scriptPath = resolveScriptPath();
        if (!Files.isRegularFile(scriptPath)) {
            throw new IllegalStateException("link extractor script not found: " + scriptPath);
        }
        Path outputDir = resolveOutputDir();
        if (persistOutputToDisk) {
            try {
                Files.createDirectories(outputDir);
            } catch (Exception error) {
                throw new IllegalStateException("create output directory failed: " + outputDir, error);
            }
        }

        Path workingDirectory = resolveWorkingDirectory(scriptPath);
        boolean headlessMode = headless;
        Path storageStatePath = resolveOptionalPath(storageStatePathRaw, workingDirectory);
        List<String> command = buildExtractorCommand(
                scriptPath,
                outputDir,
                normalizedLinks,
                storageStatePath,
                headlessMode,
                autoLoginFallback,
                zhihuCdpMode,
                disableZhihuPersistentContext,
                persistOutputToDisk,
                saveImagesToDisk,
                Math.max(30, manualLoginWaitSeconds)
        );
        ExtractorRunResult runResult = runExtractorCommand(command, workingDirectory);
        JsonNode summaryNode = parseSummaryJson(runResult.output);

        if (shouldTriggerManualInterventionRetry(
                normalizedLinks,
                summaryNode,
                runResult.output,
                headlessMode,
                autoLoginFallback
        )) {
            int waitSeconds = Math.max(30, manualLoginWaitSeconds);
            logger.warn(
                    "auth block detected, retry with manual intervention: links={}, waitSeconds={}",
                    normalizedLinks.size(),
                    waitSeconds
            );
            List<String> manualCommand = buildExtractorCommand(
                    scriptPath,
                    outputDir,
                    normalizedLinks,
                    storageStatePath,
                    false,
                    true,
                    false,
                    false,
                    persistOutputToDisk,
                    saveImagesToDisk,
                    waitSeconds
            );
            runResult = runExtractorCommand(manualCommand, workingDirectory);
            summaryNode = parseSummaryJson(runResult.output);
        }

        return ensureSuccessfulExtraction(summaryNode, normalizedLinks, runResult.exitCode, runResult.output);
    }

    private List<String> buildExtractorCommand(
            Path scriptPath,
            Path outputDir,
            List<String> normalizedLinks,
            Path storageStatePath,
            boolean headlessMode,
            boolean enableAutoLoginFallback,
            boolean enableZhihuCdpMode,
            boolean disablePersistentContext,
            boolean persistOutputToDisk,
            boolean saveImagesToDisk,
            int manualLoginWaitSecondsValue
    ) {
        List<String> command = new ArrayList<>();
        boolean useAutoLoginFallback = enableAutoLoginFallback && !headlessMode;
        command.add(resolvePythonCommand());
        command.add("-X");
        command.add("utf8");
        command.add(scriptPath.toString());
        command.add("--out-dir");
        command.add(outputDir.toString());
        command.add("--site");
        command.add("auto");
        command.add("--output-name");
        command.add("article.md");
        command.add(headlessMode ? "--headless" : "--headed");
        if (useAutoLoginFallback) {
            command.add("--auto-login-fallback");
            command.add("--manual-login-wait-seconds");
            command.add(String.valueOf(Math.max(30, manualLoginWaitSecondsValue)));
        } else {
            command.add("--no-auto-login-fallback");
        }
        command.add(enableZhihuCdpMode ? "--zhihu-cdp-mode" : "--disable-zhihu-cdp-mode");
        if (disablePersistentContext) {
            command.add("--disable-zhihu-persistent-context");
        }
        if (storageStatePath != null) {
            command.add("--storage-state");
            command.add(storageStatePath.toString());
        }
        command.add("--zhihu-request-interval-sec");
        command.add(String.valueOf(Math.max(0d, zhihuRequestIntervalSec)));
        command.add(saveImagesToDisk ? "--save-images" : "--no-save-images");
        if (!persistOutputToDisk) {
            command.add("--in-memory-output");
        }
        command.add("--max-images");
        command.add(String.valueOf(Math.max(0, maxImagesPerPage)));
        command.add("--timeout-ms");
        command.add(String.valueOf(Math.max(15000, pageTimeoutMs)));
        command.addAll(normalizedLinks);
        return command;
    }

    private ExtractorRunResult runExtractorCommand(List<String> command, Path workingDirectory) {
        Path logPath = null;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.redirectErrorStream(true);
            processBuilder.directory(workingDirectory.toFile());
            logPath = Files.createTempFile("phase2b-link-extractor-", ".log");
            processBuilder.redirectOutput(logPath.toFile());
            Process process = processBuilder.start();
            boolean completed = process.waitFor(Math.max(30, timeoutSeconds), TimeUnit.SECONDS);
            if (!completed) {
                process.destroyForcibly();
                throw new IllegalStateException("link extractor timeout: " + Duration.ofSeconds(Math.max(30, timeoutSeconds)));
            }
            int exitCode = process.exitValue();
            String output = Files.readString(logPath, StandardCharsets.UTF_8);
            return new ExtractorRunResult(exitCode, output);
        } catch (Exception error) {
            throw new IllegalStateException("run link extractor failed: " + error.getMessage(), error);
        } finally {
            if (logPath != null) {
                try {
                    Files.deleteIfExists(logPath);
                } catch (Exception ignored) {
                }
            }
        }
    }

    private LinkBatchExtractionResult ensureSuccessfulExtraction(
            JsonNode summaryNode,
            List<String> normalizedLinks,
            int exitCode,
            String processOutput
    ) {
        if (summaryNode == null) {
            throw new IllegalStateException("extract summary parse failed, output tail: " + tail(processOutput, PROCESS_OUTPUT_TAIL_MAX_CHARS));
        }
        if (exitCode != 0) {
            throw new IllegalStateException("extract process failed, exitCode=" + exitCode
                    + ", output tail: " + tail(processOutput, PROCESS_OUTPUT_TAIL_MAX_CHARS));
        }
        LinkBatchExtractionResult result = readSummary(summaryNode, normalizedLinks);
        if (result.articles.isEmpty()) {
            String failureMessage = result.failures.isEmpty()
                    ? "no article extracted from supported links"
                    : String.join(" | ", result.failures);
            throw new IllegalStateException("extract article failed: " + failureMessage);
        }
        return result;
    }

    private boolean shouldTriggerManualInterventionRetry(
            List<String> normalizedLinks,
            JsonNode summaryNode,
            String processOutput,
            boolean headlessMode,
            boolean enableAutoLoginFallback
    ) {
        if (!allowManualInterventionOnAuthBlock) {
            return false;
        }
        if (!containsZhihuLink(normalizedLinks)) {
            return false;
        }
        if (!headlessMode && enableAutoLoginFallback) {
            return false;
        }
        if (containsAuthBlockSignal(processOutput)) {
            return true;
        }
        if (summaryNode != null) {
            JsonNode failuresNode = summaryNode.path("failures");
            if (failuresNode.isArray()) {
                for (JsonNode failureNode : failuresNode) {
                    String error = text(failureNode, "error");
                    if (containsAuthBlockSignal(error)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean containsZhihuLink(List<String> links) {
        if (links == null || links.isEmpty()) {
            return false;
        }
        for (String link : links) {
            String value = String.valueOf(link == null ? "" : link).toLowerCase(Locale.ROOT);
            if (value.contains("zhihu.com")) {
                return true;
            }
        }
        return false;
    }

    private boolean containsAuthBlockSignal(String textLike) {
        String text = String.valueOf(textLike == null ? "" : textLike).toLowerCase(Locale.ROOT);
        if (!StringUtils.hasText(text)) {
            return false;
        }
        return text.contains("status=403")
                || text.contains("status=401")
                || text.contains("/account/unhuman")
                || text.contains("anti-bot")
                || text.contains("cookie d_c0 is missing")
                || text.contains("cookies are empty")
                || text.contains("/signin");
    }

    private LinkBatchExtractionResult readSummary(JsonNode summaryNode, List<String> requestedLinks) {
        String outputRootRaw = text(summaryNode, "output_root");
        Path workingDir = resolveWorkingDirectory(resolveScriptPath());
        Path outputRoot = StringUtils.hasText(outputRootRaw)
                ? resolveSummaryPath(outputRootRaw, resolveOutputDir(), workingDir)
                : resolveOutputDir();

        Map<String, ExtractedLinkArticle> byRequestedUrl = new LinkedHashMap<>();
        JsonNode pagesNode = summaryNode.path("pages");
        if (pagesNode.isArray()) {
            for (JsonNode pageNode : pagesNode) {
                String requestedUrl = normalizeSupportedLink(text(pageNode, "url"));
                if (!StringUtils.hasText(requestedUrl)) {
                    continue;
                }
                String outputDirRaw = text(pageNode, "output_dir");
                String markdownPathRaw = text(pageNode, "markdown_path");
                String markdown = text(pageNode, "markdown_content");
                Path pageDir = StringUtils.hasText(outputDirRaw)
                        ? resolveSummaryPath(outputDirRaw, outputRoot, workingDir)
                        : null;
                if (!StringUtils.hasText(markdown)) {
                    Path markdownPath = null;
                    if (StringUtils.hasText(markdownPathRaw)) {
                        markdownPath = resolveSummaryPath(
                                markdownPathRaw,
                                pageDir != null ? pageDir : outputRoot,
                                workingDir
                        );
                    } else if (pageDir != null) {
                        markdownPath = pageDir.resolve("article.md").normalize();
                    }
                    if (markdownPath != null && Files.isRegularFile(markdownPath)) {
                        markdown = readUtf8(markdownPath);
                    }
                }
                if (!StringUtils.hasText(markdown)) {
                    continue;
                }
                JsonNode metadata = objectMapper.createObjectNode();
                if (pageDir != null) {
                    Path resultJsonPath = pageDir.resolve("result.json").normalize();
                    metadata = readJson(resultJsonPath);
                }
                String finalUrl = normalizeSupportedLink(
                        firstNonBlank(text(pageNode, "final_url"), text(metadata, "final_url"), requestedUrl)
                );
                String siteType = normalizeSiteType(firstNonBlank(text(pageNode, "site_type"), text(metadata, "site_type")), finalUrl);
                String title = firstNonBlank(
                        text(pageNode, "title"),
                        text(metadata, "title"),
                        inferTitleFromMarkdown(markdown, finalUrl)
                );
                List<String> imageRelativePaths = readRelativeImagePaths(pageNode);
                if (imageRelativePaths.isEmpty()) {
                    imageRelativePaths = readRelativeImagePaths(metadata);
                }
                byRequestedUrl.put(requestedUrl, new ExtractedLinkArticle(
                        requestedUrl,
                        finalUrl,
                        siteType,
                        title,
                        pageDir != null ? pageDir.toString() : "",
                        markdown,
                        imageRelativePaths
                ));
            }
        }

        List<String> failures = new ArrayList<>();
        JsonNode failuresNode = summaryNode.path("failures");
        if (failuresNode.isArray()) {
            for (JsonNode failureNode : failuresNode) {
                String failedUrl = normalizeSupportedLink(text(failureNode, "url"));
                String error = firstNonBlank(text(failureNode, "error"), "unknown");
                if (StringUtils.hasText(failedUrl)) {
                    failures.add(failedUrl + ": " + error);
                } else {
                    failures.add(error);
                }
            }
        }

        List<ExtractedLinkArticle> articles = new ArrayList<>();
        List<String> ignored = new ArrayList<>();
        for (String url : requestedLinks) {
            ExtractedLinkArticle article = byRequestedUrl.get(url);
            if (article == null) {
                ignored.add(url);
                continue;
            }
            articles.add(article);
        }
        return new LinkBatchExtractionResult(articles, failures, ignored);
    }

    private List<String> readRelativeImagePaths(JsonNode metadata) {
        if (metadata == null || metadata.isMissingNode()) {
            return Collections.emptyList();
        }
        JsonNode imagesNode = metadata.path("downloaded_images");
        if (!imagesNode.isArray()) {
            return Collections.emptyList();
        }
        Set<String> dedup = new LinkedHashSet<>();
        for (JsonNode imageNode : imagesNode) {
            String rel = normalizeRelativeAssetPath(text(imageNode, "relative_path"));
            if (!StringUtils.hasText(rel)) {
                continue;
            }
            dedup.add(rel);
        }
        return new ArrayList<>(dedup);
    }

    private JsonNode parseSummaryJson(String processOutput) {
        String raw = String.valueOf(processOutput == null ? "" : processOutput).trim();
        if (!StringUtils.hasText(raw)) {
            return null;
        }
        JsonNode direct = parseJson(raw);
        if (direct != null && direct.has("output_root")) {
            return direct;
        }
        Matcher matcher = SUMMARY_JSON_PATTERN.matcher(raw);
        if (matcher.find()) {
            JsonNode node = parseJson(matcher.group(1));
            if (node != null && node.has("output_root")) {
                return node;
            }
        }
        int markerIndex = raw.lastIndexOf("\"output_root\"");
        if (markerIndex < 0) {
            return null;
        }
        int start = raw.lastIndexOf('{', markerIndex);
        int end = raw.lastIndexOf('}');
        if (start < 0 || end < start) {
            return null;
        }
        String candidate = raw.substring(start, end + 1);
        JsonNode node = parseJson(candidate);
        if (node != null && node.has("output_root")) {
            return node;
        }
        return null;
    }

    private JsonNode parseJson(String jsonLike) {
        String json = String.valueOf(jsonLike == null ? "" : jsonLike).trim();
        if (!StringUtils.hasText(json)) {
            return null;
        }
        try {
            return objectMapper.readTree(json);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String readUtf8(Path path) {
        if (path == null || !Files.isRegularFile(path)) {
            return "";
        }
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (Exception error) {
            logger.warn("read file failed: {} err={}", path, error.getMessage());
            return "";
        }
    }

    private JsonNode readJson(Path path) {
        String raw = readUtf8(path);
        JsonNode node = parseJson(raw);
        return node == null ? objectMapper.createObjectNode() : node;
    }

    private String fetchArticleTitle(String url) throws Exception {
        if (!StringUtils.hasText(url)) {
            return "";
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .GET()
                .timeout(Duration.ofMillis(Math.max(1200, prefetchTimeoutMs)))
                .header("User-Agent", String.valueOf(prefetchUserAgent == null ? "" : prefetchUserAgent).trim())
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
                .build();
        HttpResponse<String> response = resolveTitleHttpClient().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            return "";
        }
        String html = String.valueOf(response.body() == null ? "" : response.body());
        if (!StringUtils.hasText(html)) {
            return "";
        }
        String title = extractHtmlTitle(html);
        if (!StringUtils.hasText(title)) {
            return "";
        }
        return normalizeHtmlTitle(title);
    }

    private HttpClient resolveTitleHttpClient() {
        HttpClient client = titleHttpClient;
        if (client != null) {
            return client;
        }
        synchronized (titleHttpClientLock) {
            if (titleHttpClient == null) {
                titleHttpClient = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofMillis(Math.max(1200, prefetchTimeoutMs)))
                        .followRedirects(HttpClient.Redirect.NORMAL)
                        .build();
            }
            return titleHttpClient;
        }
    }

    private String extractHtmlTitle(String html) {
        String safeHtml = String.valueOf(html == null ? "" : html);
        String metaTitle = extractMetaContentByKeys(safeHtml, List.of("og:title", "twitter:title", "title"));
        if (StringUtils.hasText(metaTitle)) {
            return metaTitle;
        }
        String ldJsonTitle = extractLdJsonTitle(safeHtml);
        if (StringUtils.hasText(ldJsonTitle)) {
            return ldJsonTitle;
        }
        Matcher titleMatcher = HTML_TITLE_PATTERN.matcher(safeHtml);
        if (titleMatcher.find()) {
            return String.valueOf(titleMatcher.group(1) == null ? "" : titleMatcher.group(1)).trim();
        }
        return "";
    }

    private String extractMetaContentByKeys(String html, List<String> keys) {
        if (!StringUtils.hasText(html) || keys == null || keys.isEmpty()) {
            return "";
        }
        Set<String> allowed = new LinkedHashSet<>();
        for (String key : keys) {
            String normalized = String.valueOf(key == null ? "" : key).trim().toLowerCase(Locale.ROOT);
            if (StringUtils.hasText(normalized)) {
                allowed.add(normalized);
            }
        }
        if (allowed.isEmpty()) {
            return "";
        }
        Matcher metaMatcher = META_TAG_PATTERN.matcher(html);
        while (metaMatcher.find()) {
            String tag = String.valueOf(metaMatcher.group() == null ? "" : metaMatcher.group());
            Map<String, String> attributes = parseMetaAttributes(tag);
            String key = firstNonBlank(attributes.get("property"), attributes.get("name")).toLowerCase(Locale.ROOT);
            if (!allowed.contains(key)) {
                continue;
            }
            String content = firstNonBlank(attributes.get("content"), attributes.get("value"), attributes.get("title"));
            if (StringUtils.hasText(content)) {
                return content.trim();
            }
        }
        return "";
    }

    private Map<String, String> parseMetaAttributes(String tag) {
        Map<String, String> attributes = new LinkedHashMap<>();
        Matcher attrMatcher = META_ATTR_PATTERN.matcher(String.valueOf(tag == null ? "" : tag));
        while (attrMatcher.find()) {
            String name = String.valueOf(attrMatcher.group(1) == null ? "" : attrMatcher.group(1))
                    .trim()
                    .toLowerCase(Locale.ROOT);
            String value = String.valueOf(attrMatcher.group(3) == null ? "" : attrMatcher.group(3)).trim();
            if (!StringUtils.hasText(name) || !StringUtils.hasText(value)) {
                continue;
            }
            attributes.put(name, value);
        }
        return attributes;
    }

    private String extractLdJsonTitle(String html) {
        Matcher scriptMatcher = LD_JSON_SCRIPT_PATTERN.matcher(String.valueOf(html == null ? "" : html));
        while (scriptMatcher.find()) {
            String rawJson = String.valueOf(scriptMatcher.group(1) == null ? "" : scriptMatcher.group(1)).trim();
            if (!StringUtils.hasText(rawJson)) {
                continue;
            }
            JsonNode jsonNode = parseJson(rawJson);
            if (jsonNode == null) {
                continue;
            }
            String title = findTitleInLdJson(jsonNode, 0);
            if (StringUtils.hasText(title)) {
                return title.trim();
            }
        }
        return "";
    }

    private String findTitleInLdJson(JsonNode node, int depth) {
        if (node == null || node.isMissingNode() || depth > 8) {
            return "";
        }
        if (node.isObject()) {
            String headline = String.valueOf(node.path("headline").asText("")).trim();
            if (StringUtils.hasText(headline)) {
                return headline;
            }
            String name = String.valueOf(node.path("name").asText("")).trim();
            if (StringUtils.hasText(name)) {
                return name;
            }
            String title = String.valueOf(node.path("title").asText("")).trim();
            if (StringUtils.hasText(title)) {
                return title;
            }
            var fieldNames = node.fieldNames();
            while (fieldNames.hasNext()) {
                String field = fieldNames.next();
                String nested = findTitleInLdJson(node.path(field), depth + 1);
                if (StringUtils.hasText(nested)) {
                    return nested;
                }
            }
            return "";
        }
        if (node.isArray()) {
            for (JsonNode item : node) {
                String nested = findTitleInLdJson(item, depth + 1);
                if (StringUtils.hasText(nested)) {
                    return nested;
                }
            }
        }
        return "";
    }

    private String normalizeHtmlTitle(String titleLike) {
        String title = String.valueOf(titleLike == null ? "" : titleLike)
                .replaceAll("(?is)<[^>]+>", " ")
                .replace("&nbsp;", " ")
                .replace("&amp;", "&")
                .replace("&quot;", "\"")
                .replace("&#39;", "'")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replaceAll("\\s+", " ")
                .trim();
        title = title.replaceAll("(?i)\\s*[-|]+\\s*zhihu.*$", "").trim();
        title = title.replaceAll("(?i)\\s*[-|]+\\s*juejin.*$", "").trim();
        if (title.length() > 120) {
            return title.substring(0, 120);
        }
        return title;
    }

    private Path resolveScriptPath() {
        Path configured = Paths.get(String.valueOf(scriptPathRaw == null ? "" : scriptPathRaw).trim());
        if (configured.isAbsolute()) {
            return configured.normalize();
        }
        return resolveWorkingDirectory(null).resolve(configured).normalize();
    }

    private Path resolveOutputDir() {
        Path configured = Paths.get(String.valueOf(outputDirRaw == null ? "" : outputDirRaw).trim());
        if (configured.isAbsolute()) {
            return configured.normalize();
        }
        return resolveWorkingDirectory(null).resolve(configured).normalize();
    }

    private Path resolveWorkingDirectory(Path scriptPath) {
        try {
            Path root = Paths.get("").toAbsolutePath().normalize();
            if (scriptPath != null && scriptPath.isAbsolute()) {
                Path candidate = scriptPath.getParent();
                if (candidate != null) {
                    Path repoCandidate = candidate.getParent();
                    if (repoCandidate != null && Files.isDirectory(repoCandidate)) {
                        return repoCandidate.normalize();
                    }
                }
            }
            return root;
        } catch (Exception ignored) {
            return Paths.get(".").toAbsolutePath().normalize();
        }
    }

    private String resolvePythonCommand() {
        String configured = String.valueOf(pythonCommand == null ? "" : pythonCommand).trim();
        return configured.isEmpty() ? "python" : configured;
    }

    private Path resolvePath(String rawPath, Path fallbackBase) {
        if (!StringUtils.hasText(rawPath)) {
            return fallbackBase;
        }
        try {
            Path path = Paths.get(rawPath.trim());
            if (path.isAbsolute()) {
                return path.normalize();
            }
            return fallbackBase.resolve(path).normalize();
        } catch (Exception error) {
            return fallbackBase;
        }
    }

    private Path resolveSummaryPath(String rawPath, Path preferredBase, Path workingDir) {
        if (!StringUtils.hasText(rawPath)) {
            return preferredBase;
        }
        try {
            Path candidate = Paths.get(rawPath.trim());
            if (candidate.isAbsolute()) {
                return candidate.normalize();
            }
            Path fromWorkingDir = workingDir.resolve(candidate).normalize();
            if (Files.exists(fromWorkingDir)) {
                return fromWorkingDir;
            }
            Path fromPreferredBase = preferredBase.resolve(candidate).normalize();
            if (Files.exists(fromPreferredBase)) {
                return fromPreferredBase;
            }
            return fromPreferredBase;
        } catch (Exception error) {
            logger.warn("resolve summary path failed: path={} err={}", rawPath, error.getMessage());
            return preferredBase;
        }
    }

    private Path resolveOptionalPath(String rawPath, Path fallbackBase) {
        if (!StringUtils.hasText(rawPath)) {
            return null;
        }
        try {
            Path configured = Paths.get(rawPath.trim());
            if (configured.isAbsolute()) {
                return configured.normalize();
            }
            return fallbackBase.resolve(configured).normalize();
        } catch (Exception error) {
            logger.warn("resolve optional path failed: path={} err={}", rawPath, error.getMessage());
            return null;
        }
    }

    private String normalizeSupportedLink(String rawUrl) {
        String raw = String.valueOf(rawUrl == null ? "" : rawUrl).trim();
        if (!StringUtils.hasText(raw)) {
            return "";
        }
        String candidate = raw.replaceAll("[)\\],.;!?]+$", "");
        if (!candidate.matches("(?i)^https?://.*$")) {
            candidate = "https://" + candidate.replaceAll("^/+", "");
        }
        try {
            URI uri = URI.create(candidate);
            String host = String.valueOf(uri.getHost() == null ? "" : uri.getHost()).toLowerCase(Locale.ROOT);
            String path = String.valueOf(uri.getPath() == null ? "" : uri.getPath()).replaceAll("/+$", "");
            Matcher zhihuMatcher = ZHIHU_LINK_PATTERN.matcher(path);
            if ("zhuanlan.zhihu.com".equals(host) && zhihuMatcher.matches()) {
                return "https://zhuanlan.zhihu.com/p/" + zhihuMatcher.group(1);
            }
            Matcher zhihuAnswerMatcher = ZHIHU_QUESTION_ANSWER_LINK_PATTERN.matcher(path);
            if (("www.zhihu.com".equals(host) || "zhihu.com".equals(host)) && zhihuAnswerMatcher.matches()) {
                return "https://www.zhihu.com/question/" + zhihuAnswerMatcher.group(1)
                        + "/answer/" + zhihuAnswerMatcher.group(2);
            }
            Matcher juejinMatcher = JUEJIN_LINK_PATTERN.matcher(path);
            if (("juejin.cn".equals(host) || "www.juejin.cn".equals(host)) && juejinMatcher.matches()) {
                return "https://juejin.cn/post/" + juejinMatcher.group(1);
            }
            return "";
        } catch (Exception ignored) {
            return "";
        }
    }

    private String normalizeRelativeAssetPath(String pathLike) {
        String value = String.valueOf(pathLike == null ? "" : pathLike).trim().replace('\\', '/');
        if (!StringUtils.hasText(value)) {
            return "";
        }
        while (value.startsWith("./")) {
            value = value.substring(2);
        }
        value = value.replaceAll("/+", "/");
        if (value.startsWith("/")) {
            value = value.substring(1);
        }
        return value;
    }

    private String normalizeSiteType(String siteTypeRaw, String url) {
        String siteType = String.valueOf(siteTypeRaw == null ? "" : siteTypeRaw).trim().toLowerCase(Locale.ROOT);
        if ("zhihu".equals(siteType) || "juejin".equals(siteType)) {
            return siteType;
        }
        if (url.contains("zhuanlan.zhihu.com")) {
            return "zhihu";
        }
        if (url.contains("zhihu.com/question/")) {
            return "zhihu";
        }
        if (url.contains("juejin.cn")) {
            return "juejin";
        }
        return "generic";
    }

    private String inferTitleFromMarkdown(String markdown, String url) {
        String text = String.valueOf(markdown == null ? "" : markdown).replace("\r\n", "\n");
        String[] lines = text.split("\n");
        for (String line : lines) {
            String trimmed = String.valueOf(line == null ? "" : line).trim();
            if (!StringUtils.hasText(trimmed)) {
                continue;
            }
            if (trimmed.startsWith("#")) {
                String heading = trimmed.replaceFirst("^#+\\s*", "").trim();
                if (StringUtils.hasText(heading)) {
                    return heading;
                }
            }
        }
        return StringUtils.hasText(url) ? url : "未命名文章";
    }

    private String tail(String text, int maxChars) {
        String safe = String.valueOf(text == null ? "" : text);
        if (safe.length() <= Math.max(64, maxChars)) {
            return safe;
        }
        return safe.substring(safe.length() - Math.max(64, maxChars));
    }

    private String text(JsonNode node, String key) {
        if (node == null || key == null || key.isBlank()) {
            return "";
        }
        return String.valueOf(node.path(key).asText("")).trim();
    }

    private String firstNonBlank(String... values) {
        if (values == null || values.length == 0) {
            return "";
        }
        for (String value : values) {
            if (StringUtils.hasText(value)) {
                return value.trim();
            }
        }
        return "";
    }

    public static final class ExtractedLinkArticle {
        public final String requestedUrl;
        public final String finalUrl;
        public final String siteType;
        public final String title;
        public final String pageOutputDir;
        public final String markdown;
        public final List<String> imageRelativePaths;

        public ExtractedLinkArticle(
                String requestedUrl,
                String finalUrl,
                String siteType,
                String title,
                String pageOutputDir,
                String markdown,
                List<String> imageRelativePaths
        ) {
            this.requestedUrl = String.valueOf(requestedUrl == null ? "" : requestedUrl);
            this.finalUrl = String.valueOf(finalUrl == null ? "" : finalUrl);
            this.siteType = String.valueOf(siteType == null ? "" : siteType);
            this.title = String.valueOf(title == null ? "" : title);
            this.pageOutputDir = String.valueOf(pageOutputDir == null ? "" : pageOutputDir);
            this.markdown = String.valueOf(markdown == null ? "" : markdown);
            this.imageRelativePaths = imageRelativePaths != null
                    ? List.copyOf(imageRelativePaths)
                    : Collections.emptyList();
        }

        public Map<String, Object> toPayload() {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("url", requestedUrl);
            payload.put("finalUrl", finalUrl);
            payload.put("siteType", siteType);
            payload.put("title", title);
            payload.put("imagePaths", imageRelativePaths);
            return payload;
        }
    }

    public static final class LinkMetadata {
        public final String url;
        public final String siteType;
        public final String title;
        public final String status;

        public LinkMetadata(String url, String siteType, String title, String status) {
            this.url = String.valueOf(url == null ? "" : url);
            this.siteType = String.valueOf(siteType == null ? "" : siteType);
            this.title = String.valueOf(title == null ? "" : title);
            this.status = String.valueOf(status == null ? "" : status);
        }

        public Map<String, Object> toPayload() {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("url", url);
            payload.put("siteType", siteType);
            payload.put("title", title);
            payload.put("status", status);
            return payload;
        }
    }

    public static final class LinkBatchExtractionResult {
        public final List<ExtractedLinkArticle> articles;
        public final List<String> failures;
        public final List<String> ignoredLinks;

        public LinkBatchExtractionResult(
                List<ExtractedLinkArticle> articles,
                List<String> failures,
                List<String> ignoredLinks
        ) {
            this.articles = articles != null ? List.copyOf(articles) : Collections.emptyList();
            this.failures = failures != null ? List.copyOf(failures) : Collections.emptyList();
            this.ignoredLinks = ignoredLinks != null ? List.copyOf(ignoredLinks) : Collections.emptyList();
        }

        public static LinkBatchExtractionResult empty() {
            return new LinkBatchExtractionResult(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        }
    }
}
