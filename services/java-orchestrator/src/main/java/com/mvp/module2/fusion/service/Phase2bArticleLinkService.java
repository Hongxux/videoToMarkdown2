package com.mvp.module2.fusion.service;

import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class Phase2bArticleLinkService {

    public List<String> normalizeSupportedLinks(List<String> rawLinks) {
        return List.of();
    }

    public List<LinkMetadata> prefetchLinkMetadata(List<String> rawLinks) {
        return List.of();
    }

    public LinkBatchExtractionResult extractArticles(List<String> rawLinks) {
        return LinkBatchExtractionResult.empty();
    }

    public LinkBatchExtractionResult extractArticlesForBook(List<String> rawLinks) {
        return LinkBatchExtractionResult.empty();
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
