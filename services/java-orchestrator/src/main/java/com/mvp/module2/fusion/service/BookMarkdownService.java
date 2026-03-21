package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.service.llm.LlmErrorDescriber;
import org.apache.pdfbox.contentstream.PDFStreamEngine;
import org.apache.pdfbox.contentstream.operator.Operator;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.graphics.PDXObject;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDDocumentOutline;
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDOutlineItem;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.util.Matrix;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.imageio.ImageIO;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.Normalizer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Service
public class BookMarkdownService {

    private static final Logger logger = LoggerFactory.getLogger(BookMarkdownService.class);
    private static final Pattern CHAPTER_PATTERN = Pattern.compile(
            "^(?:\\u7b2c\\s*[0-9\\u4e00-\\u9fa5]+\\s*[\\u7ae0\\u8282\\u5377\\u56de\\u7bc7\\u90e8].*|(?i)(chapter|part)\\s+\\d+.*)$"
    );
    private static final Pattern SECTION_PATTERN = Pattern.compile(
            "^(?:\\u7b2c\\s*[0-9\\u4e00-\\u9fa5]+\\s*\\u8282.*|\\d+(?:\\.\\d+){1,6}\\s+.+|[\\u4e00-\\u9fa5]+\\u3001.+)$"
    );
    private static final Pattern RANGE_PATTERN = Pattern.compile("^(\\d+)\\s*-\\s*(\\d+)$");
    private static final Pattern CHAPTER_SECTION_KEY_PATTERN = Pattern.compile("(?i)^c(\\d+)s(\\d+)$");
    private static final Pattern CHAPTER_SECTION_DOT_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)$");
    private static final Pattern CHAPTER_SECTION_LEAF_KEY_PATTERN = Pattern.compile("(?i)^c(\\d+)s(\\d+)t(\\d+)$");
    private static final Pattern CHAPTER_SECTION_LEAF_DOT_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)$");
    private static final Pattern CHAPTER_SECTION_LEAF_KEY_RANGE_PATTERN = Pattern.compile("(?i)^c(\\d+)s(\\d+)t(\\d+)\\s*-\\s*c(\\d+)s(\\d+)t(\\d+)$");
    private static final Pattern CHAPTER_SECTION_LEAF_DOT_RANGE_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)\\s*-\\s*(\\d+)\\.(\\d+)\\.(\\d+)$");
    private static final Pattern TOC_TRAILING_PAGE_PATTERN = Pattern.compile("^(.+?)\\s*(?:\\.{2,}|\\s{2,}|\\s)(\\d{1,4})$");
    private static final Pattern TOC_NUMERIC_TITLE_PATTERN = Pattern.compile("^(\\d+(?:\\.\\d+){0,6})\\.?\\s+(.+)$");
    private static final Pattern PDF_CHAPTER_TITLE_PATTERN = Pattern.compile("^(?i)(chapter|part)\\s+\\d+\\b.*|^\\d+\\s+.+");
    private static final Pattern PDF_SECTION_TITLE_PATTERN = Pattern.compile("^(?i)(\\d+\\.){1,6}\\s*.+|^\\d+\\.\\d+(?:\\.\\d+){0,6}\\s+.+");
    private static final Pattern MARKDOWN_IMAGE_PATTERN = Pattern.compile("!\\[[^\\]]*]\\(([^)]+)\\)");
    private static final String SELECTOR_TOKEN_SPLIT_REGEX = "[,;\\s\\uFF0C\\uFF1B]+";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Charset GB18030 = Charset.forName("GB18030");
    private static final int PDF_TOC_MAX_SCAN_PAGES = 80;
    private static final int PDF_TOC_PRIORITY_SCAN_PAGES = 32;
    private static final int PDF_TOC_MIN_LINES_PER_PAGE = 3;

    @Autowired(required = false)
    private PythonGrpcClient grpcClient;

    @Value("${book.pdf.extractor.strategy:auto}")
    private String pdfExtractorStrategy = "auto";

    @Value("${book.pdf.extractor.prefer-mineru:true}")
    private boolean preferMineruExtractor = true;

    @Value("${book.pdf.extractor.grpc-timeout-seconds:300}")
    private int bookPdfExtractorTimeoutSec = 300;

    @Value("${book.pdf.extractor.grpc-timeout-per-page-seconds:25}")
    private int bookPdfExtractorTimeoutPerPageSec = 25;

    public static class BookProcessingOptions {
        public String chapterSelector;
        public String sectionSelector;
        public Boolean splitByChapter;
        public Boolean splitBySection;
        public Integer pageOffset;
        public String bookTitle;
        public String leafTitle;
        public String leafOutlineIndex;
        public String storageKey;
    }

    public static class BookProbeResult {
        public boolean success;
        public String sourcePath;
        public String bookTitle;
        public String format;
        public int totalPages;
        public int chapterCount;
        public int sectionCount;
        public Integer appliedPageOffset;
        public Integer detectedPageOffset;
        public String pageMapStrategy;
        public List<Map<String, Object>> chapters = new ArrayList<>();
        public List<Map<String, Object>> sections = new ArrayList<>();
        public List<Map<String, Object>> leafSections = new ArrayList<>();
        public String errorMessage;
    }

    public static class BookProcessingResult {
        public boolean success;
        public String markdownPath;
        public String metadataPath;
        public String contentType;
        public String preferredMarkdownFileName;
        public String bookTitle;
        public String leafTitle;
        public String leafOutlineIndex;
        public String leafSelector;
        public String storageKey;
        public List<Map<String, Object>> bookSectionTree = new ArrayList<>();
        public int chapterCount;
        public int sectionCount;
        public int unitCount;
        public String errorMessage;
    }

    public static class BookCategoryEvidence {
        public String sourcePath;
        public String contentType;
        public String bookTitle;
        public String frontMatterText;
        public String prefaceText;
        public List<String> tocTitles = new ArrayList<>();
    }

    private static class Section {
        String title;
        String selector;
        int startPage = -1;
        int endPage = -1;
        String markdownBody;
        String trimStartAnchorTitle;
        String trimEndBeforeTitle;
        List<String> paragraphs = new ArrayList<>();
        List<String> images = new ArrayList<>();
        List<List<List<String>>> tables = new ArrayList<>();
        List<SectionBlock> blocks = new ArrayList<>();

        Section(String title) {
            this.title = title;
        }
    }

    private static class Chapter {
        String title;
        String selector;
        int startPage = -1;
        int endPage = -1;
        List<Section> sections = new ArrayList<>();

        Chapter(String title) {
            this.title = title;
        }
    }

    private static class BookData {
        String title;
        Integer appliedPageOffset;
        Integer detectedPageOffset;
        String pageMapStrategy;
        List<Chapter> chapters = new ArrayList<>();
        List<Map<String, Object>> leafSections = new ArrayList<>();
    }

    private static class SectionBlock {
        enum BlockType {
            PARAGRAPH,
            IMAGE,
            TABLE
        }

        BlockType type;
        String text;
        String imagePath;
        List<List<String>> table;
        int pageNo;
    }

    private static class PdfImagePlacement {
        PDImageXObject image;
        float topRatio;

        PdfImagePlacement(PDImageXObject image, float topRatio) {
            this.image = image;
            this.topRatio = topRatio;
        }
    }

    private static class ExtractedPdfImage {
        String relativePath;
        float topRatio;

        ExtractedPdfImage(String relativePath, float topRatio) {
            this.relativePath = relativePath;
            this.topRatio = topRatio;
        }
    }

    private static class PdfOutlineNode {
        String title;
        int pageNo;
        List<PdfOutlineNode> children = new ArrayList<>();
    }

    private static class PdfTocEntry {
        String title;
        String displayTitle;
        int level;
        int pageNo;
        int sourcePageNo;
        String outlineIndex;
        Integer chapterNo;
        Integer sectionNo;
        Integer leafNo;
    }

    private static class SectionSelectorRef {
        Chapter chapter;
        Section section;
        int chapterIndex;
        int sectionIndex;
        int globalIndex;
        String key;
    }

    private static class OrderedLeafRef {
        String selector;
        String title;
        String outlineIndex;
        int startPage = -1;
        int endPage = -1;
    }

    private static class ContinuousLeafSelection {
        String startSelector;
        String endSelector;
        String startTitle;
        String endTitle;
        String startOutlineIndex;
        String endOutlineIndex;
        String trimStartAnchorTitle;
        String trimEndBeforeTitle;
        int startPage = -1;
        int endPage = -1;
        int leafCount = 0;
    }

    private static class LeafTaskDescriptor {
        String bookTitle;
        String leafTitle;
        String outlineIndex;
        String sectionSelector;
        String storageKey;
        String markdownFileName;
    }

    public BookProcessingResult processBook(
            String taskId,
            String sourcePath,
            String outputDir,
            BookProcessingOptions options
    ) {
        BookProcessingResult result = new BookProcessingResult();
        try {
            Path source = resolveBookSourcePath(sourcePath);
            if (!Files.isRegularFile(source)) {
                throw new IllegalArgumentException("Book file not found: " + source);
            }

            Path outputRoot = Paths.get(outputDir).toAbsolutePath().normalize();
            Files.createDirectories(outputRoot);
            Path imageAssetsRoot = outputRoot.resolve("assets").resolve("book_images");
            Files.createDirectories(imageAssetsRoot);

            String ext = lowerExt(source.getFileName().toString());
            BookData data = extractBook(taskId, source, outputRoot, imageAssetsRoot, options);
            if (!".pdf".equals(ext)) {
                data = applyBookSelectors(data, options);
            }
            if (data.chapters.isEmpty()) {
                throw new IllegalArgumentException("No chapter/section matched selector");
            }

            boolean splitByChapter = options == null || options.splitByChapter == null || options.splitByChapter;
            boolean splitBySection = options != null && Boolean.TRUE.equals(options.splitBySection);
            LeafTaskDescriptor leafTask = resolveLeafTaskDescriptor(data, source, options);
            Path markdownPath = writeMarkdownOutputs(data, outputRoot, splitByChapter, splitBySection, leafTask);
            Path selectedPdfPath = writeSelectedPdfOutput(source, outputRoot, data, markdownPath);
            Path metadataPath = writeAbstractMetadata(taskId, source, outputRoot, data, leafTask);
            cleanupOriginalTaskPdf(source, outputRoot, selectedPdfPath);

            int sectionCount = 0;
            for (Chapter chapter : data.chapters) {
                sectionCount += chapter.sections.size();
            }

            result.success = true;
            result.markdownPath = markdownPath.toString();
            result.metadataPath = metadataPath.toString();
            result.contentType = "book";
            result.preferredMarkdownFileName = markdownPath.getFileName() != null
                    ? markdownPath.getFileName().toString()
                    : null;
            result.bookTitle = firstNonBlank(
                    data.title,
                    firstNonBlank(leafTask != null ? leafTask.bookTitle : null, stripExt(source.getFileName().toString()))
            );
            result.leafTitle = leafTask != null ? leafTask.leafTitle : null;
            result.leafOutlineIndex = leafTask != null ? leafTask.outlineIndex : null;
            result.leafSelector = leafTask != null ? leafTask.sectionSelector : null;
            result.storageKey = leafTask != null ? leafTask.storageKey : null;
            result.bookSectionTree = buildBookSectionTreePayload(data);
            result.chapterCount = data.chapters.size();
            result.sectionCount = sectionCount;
            result.unitCount = sectionCount;
            return result;
        } catch (Exception error) {
            logger.error("Book markdown processing failed, source={}, output={}", sourcePath, outputDir, error);
            result.success = false;
            result.errorMessage = LlmErrorDescriber.describe(error);
            return result;
        }
    }

    public BookProbeResult probeBook(String sourcePath) {
        return probeBook(sourcePath, null);
    }

    public BookProbeResult probeBook(String sourcePath, Integer pageOffset) {
        BookProbeResult result = new BookProbeResult();
        try {
            Path source = resolveBookSourcePath(sourcePath);
            if (!Files.isRegularFile(source)) {
                throw new IllegalArgumentException("Book file not found: " + source);
            }
            String ext = lowerExt(source.getFileName().toString());
            BookData data;
            int totalPages = 0;
            if (".pdf".equals(ext)) {
                try (PDDocument pdf = PDDocument.load(source.toFile())) {
                    totalPages = pdf.getNumberOfPages();
                    data = buildPdfBookStructure(pdf, source, pageOffset);
                }
            } else if (".txt".equals(ext) || ".md".equals(ext)) {
                data = parsePlainText(source);
            } else if (".epub".equals(ext)) {
                data = parseEpubStructure(source);
            } else {
                throw new IllegalArgumentException("Unsupported book format: " + ext);
            }
            annotateSelectors(data);
            fillProbeResult(result, source, ext, data, totalPages);
            result.success = true;
            return result;
        } catch (Exception error) {
            logger.warn("Probe book failed, sourcePath={}, err={}", sourcePath, LlmErrorDescriber.describe(error));
            result.success = false;
            result.errorMessage = LlmErrorDescriber.describe(error);
            return result;
        }
    }

    public BookCategoryEvidence buildCategoryEvidence(
            String sourcePath,
            String fallbackTitle,
            List<Map<String, Object>> bookSectionTree
    ) {
        BookCategoryEvidence evidence = new BookCategoryEvidence();
        evidence.sourcePath = firstNonBlank(sourcePath, "");
        evidence.bookTitle = firstNonBlank(fallbackTitle, "");
        evidence.frontMatterText = "";
        evidence.prefaceText = "";
        evidence.contentType = "book";
        List<String> fallbackTocTitles = collectTocTitlesFromTree(bookSectionTree);
        evidence.tocTitles = new ArrayList<>(fallbackTocTitles);

        try {
            Path source = resolveBookSourcePath(sourcePath);
            if (!Files.isRegularFile(source)) {
                evidence.bookTitle = firstNonBlank(evidence.bookTitle, stripExt(Path.of(sourcePath).getFileName().toString()));
                return evidence;
            }
            String ext = lowerExt(source.getFileName().toString());
            evidence.contentType = switch (ext) {
                case ".pdf" -> "book_pdf";
                case ".epub" -> "book_epub";
                case ".md" -> "book_markdown";
                case ".txt" -> "book_text";
                default -> "book";
            };
            evidence.bookTitle = firstNonBlank(evidence.bookTitle, stripExt(source.getFileName().toString()));
            if (".pdf".equals(ext)) {
                fillPdfCategoryEvidence(source, evidence, fallbackTocTitles);
                return evidence;
            }
            fillTextLikeCategoryEvidence(source, evidence, fallbackTocTitles);
            return evidence;
        } catch (Exception error) {
            logger.warn("Build book category evidence failed, sourcePath={}, err={}", sourcePath, error.getMessage());
            return evidence;
        }
    }

    private BookData extractBook(
            String taskId,
            Path source,
            Path outputRoot,
            Path imageAssetsRoot,
            BookProcessingOptions options
    ) throws Exception {
        String ext = lowerExt(source.getFileName().toString());
        if (".txt".equals(ext) || ".md".equals(ext)) {
            BookData data = parsePlainText(source);
            annotateSelectors(data);
            return data;
        }
        if (".pdf".equals(ext)) {
            return parsePdf(taskId, source, outputRoot, imageAssetsRoot, options);
        }
        if (".epub".equals(ext)) {
            BookData data = parseEpub(source, outputRoot, imageAssetsRoot);
            annotateSelectors(data);
            return data;
        }
        throw new IllegalArgumentException("Unsupported book format: " + ext);
    }

    private void fillPdfCategoryEvidence(
            Path source,
            BookCategoryEvidence evidence,
            List<String> fallbackTocTitles
    ) throws Exception {
        try (PDDocument pdf = PDDocument.load(source.toFile())) {
            if (pdf.getDocumentInformation() != null) {
                evidence.bookTitle = firstNonBlank(
                        normalize(pdf.getDocumentInformation().getTitle()),
                        evidence.bookTitle
                );
            }
            int totalPages = pdf.getNumberOfPages();
            PDFTextStripper pageStripper = new PDFTextStripper();
            pageStripper.setSortByPosition(true);
            evidence.frontMatterText = trimEvidenceText(
                    extractPdfPageRangeText(pdf, pageStripper, 1, Math.min(totalPages, 6)),
                    5000
            );
            evidence.prefaceText = trimEvidenceText(
                    extractPdfPrefaceText(pdf, pageStripper, totalPages),
                    3000
            );
            List<PdfTocEntry> tocEntries = parsePdfTocEntries(pdf, totalPages);
            List<String> tocTitles = new ArrayList<>();
            for (PdfTocEntry tocEntry : tocEntries) {
                if (tocEntry == null) {
                    continue;
                }
                String title = firstNonBlank(tocEntry.displayTitle, tocEntry.title);
                if (title.isBlank()) {
                    continue;
                }
                tocTitles.add(title);
                if (tocTitles.size() >= 40) {
                    break;
                }
            }
            evidence.tocTitles = !tocTitles.isEmpty() ? normalizeDistinctTitles(tocTitles) : new ArrayList<>(fallbackTocTitles);
        }
    }

    private void fillTextLikeCategoryEvidence(
            Path source,
            BookCategoryEvidence evidence,
            List<String> fallbackTocTitles
    ) throws Exception {
        String ext = lowerExt(source.getFileName().toString());
        String content = readText(source);
        evidence.frontMatterText = trimEvidenceText(content, 5000);
        evidence.prefaceText = trimEvidenceText(extractPrefaceFromText(content), 3000);
        if (".epub".equals(ext)) {
            BookData data = parseEpubStructure(source);
            evidence.bookTitle = firstNonBlank(data.title, evidence.bookTitle);
            evidence.tocTitles = !fallbackTocTitles.isEmpty()
                    ? new ArrayList<>(fallbackTocTitles)
                    : collectTocTitlesFromBookData(data);
            return;
        }
        if (".md".equals(ext) || ".txt".equals(ext)) {
            BookData data = parseStructuredLines(content.split("\\R"), evidence.bookTitle);
            evidence.bookTitle = firstNonBlank(data.title, evidence.bookTitle);
            evidence.tocTitles = !fallbackTocTitles.isEmpty()
                    ? new ArrayList<>(fallbackTocTitles)
                    : collectTocTitlesFromBookData(data);
        }
    }

    private BookData parsePlainText(Path source) throws Exception {
        String content = readText(source);
        String fallbackTitle = stripExt(source.getFileName().toString());
        return parseStructuredLines(content.split("\\R"), fallbackTitle);
    }

    private BookData parsePdf(
            String taskId,
            Path source,
            Path outputRoot,
            Path imageAssetsRoot,
            BookProcessingOptions options
    ) throws Exception {
        try (PDDocument pdf = PDDocument.load(source.toFile())) {
            Integer pageOffset = options != null ? options.pageOffset : null;
            BookData data = buildPdfBookStructure(pdf, source, pageOffset);
            annotateSelectors(data);
            BookData selected = applyBookSelectors(data, options);
            if (selected.chapters.isEmpty()) {
                return selected;
            }
            extractPdfContentForSelections(taskId, source, pdf, selected, outputRoot, imageAssetsRoot);
            return selected;
        }
    }

    private BookData parseEpub(Path source, Path outputRoot, Path imageAssetsRoot) throws Exception {
        BookData data = new BookData();
        data.title = stripExt(source.getFileName().toString());

        try (ZipFile zip = new ZipFile(source.toFile())) {
            String opfPath = resolveOpfPath(zip);
            byte[] opfBytes = readZipEntry(zip, opfPath);
            if (opfBytes == null) {
                throw new IllegalArgumentException("Invalid EPUB: OPF missing");
            }

            Document opfDoc = Jsoup.parse(new ByteArrayInputStream(opfBytes), "UTF-8", "", Parser.xmlParser());
            String metadataTitle = extractEpubMetadataTitle(opfDoc);
            if (!metadataTitle.isBlank()) {
                data.title = metadataTitle;
            }

            Map<String, String> manifestIdToHref = new LinkedHashMap<>();
            for (Element item : opfDoc.select("manifest > item")) {
                String id = normalize(item.attr("id"));
                String href = normalize(item.attr("href"));
                if (!id.isBlank() && !href.isBlank()) {
                    manifestIdToHref.put(id, href);
                }
            }

            List<String> spineHrefs = new ArrayList<>();
            for (Element itemRef : opfDoc.select("spine > itemref")) {
                String idRef = normalize(itemRef.attr("idref"));
                if (!idRef.isBlank() && manifestIdToHref.containsKey(idRef)) {
                    spineHrefs.add(manifestIdToHref.get(idRef));
                }
            }
            if (spineHrefs.isEmpty()) {
                for (String href : manifestIdToHref.values()) {
                    String lower = href.toLowerCase(Locale.ROOT);
                    if (lower.endsWith(".xhtml") || lower.endsWith(".html") || lower.endsWith(".htm")) {
                        spineHrefs.add(href);
                    }
                }
            }
            if (spineHrefs.isEmpty()) {
                throw new IllegalArgumentException("Invalid EPUB: no readable spine");
            }

            int chapterIndex = 0;
            for (String href : spineHrefs) {
                String spinePath = resolveRelativePath(opfPath, href);
                byte[] htmlBytes = readZipEntry(zip, spinePath);
                if (htmlBytes == null) {
                    continue;
                }
                Document htmlDoc = Jsoup.parse(new ByteArrayInputStream(htmlBytes), "UTF-8", "");
                Chapter chapter = parseEpubChapter(
                        zip,
                        htmlDoc,
                        spinePath,
                        outputRoot,
                        imageAssetsRoot,
                        chapterIndex + 1
                );
                if (chapter != null) {
                    data.chapters.add(chapter);
                    chapterIndex += 1;
                }
            }
        }

        if (data.chapters.isEmpty()) {
            Chapter fallback = new Chapter("Chapter 1");
            fallback.sections.add(new Section("Body"));
            data.chapters.add(fallback);
        }
        return data;
    }

    private BookData parseEpubStructure(Path source) throws Exception {
        return parseEpub(source, null, null);
    }

    private BookData buildPdfBookStructure(PDDocument pdf, Path source, Integer manualPageOffset) throws Exception {
        String title = "";
        if (pdf.getDocumentInformation() != null) {
            title = normalize(pdf.getDocumentInformation().getTitle());
        }
        String fallbackTitle = firstNonBlank(title, stripExt(source.getFileName().toString()));
        int totalPages = pdf.getNumberOfPages();
        List<PdfTocEntry> frontTocEntries = new ArrayList<>();
        Integer detectedOffsetFromToc = null;
        try {
            frontTocEntries = parsePdfTocEntries(pdf, totalPages);
            detectedOffsetFromToc = estimatePdfPageOffset(pdf, frontTocEntries, totalPages);
        } catch (Exception tocError) {
            logger.warn("Parse PDF TOC failed, fallback to outline/text strategy, title={}, err={}", fallbackTitle, tocError.getMessage());
        }

        BookData tocData = buildPdfBookStructureFromToc(
                pdf,
                fallbackTitle,
                totalPages,
                manualPageOffset,
                frontTocEntries,
                detectedOffsetFromToc
        );
        if (hasPageAnchoredStructure(tocData)) {
            return tocData;
        }

        BookData outlineData = buildPdfBookStructureFromOutline(pdf, fallbackTitle, totalPages);
        if (hasPageAnchoredStructure(outlineData)) {
            outlineData.pageMapStrategy = "outline";
            Integer detectedOffsetForOutline = resolveDetectedOffsetForOutline(outlineData, detectedOffsetFromToc);
            outlineData.detectedPageOffset = detectedOffsetForOutline;
            int appliedOffset = manualPageOffset != null
                    ? manualPageOffset
                    : (detectedOffsetForOutline != null ? detectedOffsetForOutline : 0);
            if (manualPageOffset != null && appliedOffset != 0) {
                shiftPdfPageRanges(outlineData, totalPages, appliedOffset);
                logger.info("Applied manual PDF page offset={} for outline mapping, title={}", manualPageOffset, fallbackTitle);
            }
            outlineData.appliedPageOffset = appliedOffset;
            List<PdfTocEntry> normalizedTocEntries = normalizePdfTocEntries(frontTocEntries, totalPages, appliedOffset);
            outlineData.leafSections = buildPdfTocLeafSections(normalizedTocEntries, totalPages, outlineData);
            return outlineData;
        }

        PDFTextStripper textStripper = new PDFTextStripper();
        textStripper.setSortByPosition(true);
        String allText = textStripper.getText(pdf);
        BookData textData = parseStructuredLines(allText.split("\\R"), fallbackTitle);
        textData.pageMapStrategy = "text_heuristic";
        textData.detectedPageOffset = detectedOffsetFromToc;
        textData.appliedPageOffset = manualPageOffset != null ? manualPageOffset : 0;
        return textData;
    }

    private BookData buildPdfBookStructureFromOutline(PDDocument pdf, String fallbackTitle, int totalPages) {
        PDDocumentOutline outline = pdf.getDocumentCatalog() != null
                ? pdf.getDocumentCatalog().getDocumentOutline()
                : null;
        if (outline == null) {
            return null;
        }
        List<PdfOutlineNode> topLevel = new ArrayList<>();
        for (PDOutlineItem item = outline.getFirstChild(); item != null; item = item.getNextSibling()) {
            PdfOutlineNode node = readPdfOutlineNode(pdf, item);
            if (node != null && node.pageNo > 0) {
                topLevel.add(node);
            }
        }
        if (topLevel.isEmpty()) {
            return null;
        }
        List<PdfOutlineNode> filtered = new ArrayList<>();
        for (PdfOutlineNode node : topLevel) {
            String title = normalize(node.title).toLowerCase(Locale.ROOT);
            if (title.length() <= 1) {
                continue;
            }
            if (isBackMatterTitle(title)) {
                continue;
            }
            filtered.add(node);
        }
        if (filtered.isEmpty()) {
            filtered = topLevel;
        }

        BookData data = new BookData();
        data.title = firstNonBlank(fallbackTitle, "Book");
        for (PdfOutlineNode node : filtered) {
            Chapter chapter = new Chapter(firstNonBlank(normalize(node.title), "Chapter " + (data.chapters.size() + 1)));
            chapter.startPage = node.pageNo;
            List<PdfOutlineNode> sectionNodes = new ArrayList<>();
            for (PdfOutlineNode child : node.children) {
                if (child.pageNo > 0 && !normalize(child.title).isBlank()) {
                    sectionNodes.add(child);
                }
            }
            if (sectionNodes.isEmpty()) {
                Section body = new Section("Body");
                body.startPage = chapter.startPage;
                chapter.sections.add(body);
            } else {
                for (PdfOutlineNode sectionNode : sectionNodes) {
                    Section section = new Section(firstNonBlank(normalize(sectionNode.title), "Section " + (chapter.sections.size() + 1)));
                    section.startPage = sectionNode.pageNo;
                    chapter.sections.add(section);
                }
            }
            data.chapters.add(chapter);
        }
        finalizePdfPageRanges(data, totalPages);
        return data;
    }

    private PdfOutlineNode readPdfOutlineNode(PDDocument pdf, PDOutlineItem item) {
        if (item == null) {
            return null;
        }
        String title = normalize(item.getTitle());
        if (title.isBlank()) {
            return null;
        }
        int pageNo = resolveOutlinePageNo(pdf, item);
        if (pageNo <= 0) {
            return null;
        }
        PdfOutlineNode node = new PdfOutlineNode();
        node.title = title;
        node.pageNo = pageNo;
        for (PDOutlineItem child = item.getFirstChild(); child != null; child = child.getNextSibling()) {
            PdfOutlineNode childNode = readPdfOutlineNode(pdf, child);
            if (childNode != null) {
                node.children.add(childNode);
            }
        }
        return node;
    }

    private int resolveOutlinePageNo(PDDocument pdf, PDOutlineItem item) {
        try {
            PDPage destinationPage = item.findDestinationPage(pdf);
            if (destinationPage == null) {
                return -1;
            }
            int index = pdf.getPages().indexOf(destinationPage);
            return index >= 0 ? index + 1 : -1;
        } catch (Exception ignored) {
            return -1;
        }
    }

    private BookData buildPdfBookStructureFromToc(
            PDDocument pdf,
            String fallbackTitle,
            int totalPages,
            Integer manualPageOffset,
            List<PdfTocEntry> parsedEntries,
            Integer detectedOffsetHint
    ) throws Exception {
        List<PdfTocEntry> entries = parsedEntries != null ? parsedEntries : parsePdfTocEntries(pdf, totalPages);
        if (entries.isEmpty()) {
            return null;
        }
        Integer detectedOffset = detectedOffsetHint != null
                ? detectedOffsetHint
                : estimatePdfPageOffset(pdf, entries, totalPages);
        int appliedOffset = manualPageOffset != null
                ? manualPageOffset
                : (detectedOffset != null ? detectedOffset : 0);
        if (manualPageOffset != null) {
            logger.info(
                    "Use manual PDF page offset={}, detectedOffset={}, title={}",
                    manualPageOffset,
                    detectedOffset,
                    fallbackTitle
            );
        } else if (detectedOffset != null && detectedOffset != 0) {
            logger.info("Detected PDF page offset={}, fallbackTitle={}", detectedOffset, fallbackTitle);
        }

        List<PdfTocEntry> normalized = normalizePdfTocEntries(entries, totalPages, appliedOffset);

        BookData data = new BookData();
        data.title = firstNonBlank(fallbackTitle, "Book");
        data.pageMapStrategy = "toc";
        data.detectedPageOffset = detectedOffset;
        data.appliedPageOffset = appliedOffset;
        List<Integer> chapterEntryIndexes = new ArrayList<>();
        for (int i = 0; i < normalized.size(); i++) {
            if (normalized.get(i).level == 1) {
                chapterEntryIndexes.add(i);
            }
        }
        if (chapterEntryIndexes.isEmpty()) {
            return null;
        }

        for (int chapterPos = 0; chapterPos < chapterEntryIndexes.size(); chapterPos++) {
            int entryIndex = chapterEntryIndexes.get(chapterPos);
            PdfTocEntry chapterEntry = normalized.get(entryIndex);
            String chapterTitle = firstNonBlank(
                    chapterEntry.displayTitle,
                    firstNonBlank(chapterEntry.title, "Chapter " + (chapterPos + 1))
            );
            Chapter chapter = new Chapter(chapterTitle);
            chapter.startPage = chapterEntry.pageNo;

            int nextChapterEntryIndex = chapterPos + 1 < chapterEntryIndexes.size()
                    ? chapterEntryIndexes.get(chapterPos + 1)
                    : normalized.size();
            for (int i = entryIndex + 1; i < nextChapterEntryIndex; i++) {
                PdfTocEntry sectionEntry = normalized.get(i);
                if (sectionEntry.level != 2) {
                    continue;
                }
                Section section = new Section(firstNonBlank(
                        sectionEntry.displayTitle,
                        firstNonBlank(sectionEntry.title, "Section " + (chapter.sections.size() + 1))
                ));
                section.startPage = sectionEntry.pageNo;
                chapter.sections.add(section);
            }
            if (chapter.sections.isEmpty()) {
                Section body = new Section("Body");
                body.startPage = chapter.startPage;
                chapter.sections.add(body);
            }
            data.chapters.add(chapter);
        }

        finalizePdfPageRanges(data, totalPages);
        data.leafSections = buildPdfTocLeafSections(normalized, totalPages, data);
        return data;
    }

    private List<PdfTocEntry> normalizePdfTocEntries(List<PdfTocEntry> entries, int totalPages, int appliedOffset) {
        if (entries == null || entries.isEmpty()) {
            return List.of();
        }
        List<PdfTocEntry> normalized = new ArrayList<>();
        int lastPageNo = 1;
        for (PdfTocEntry entry : entries) {
            if (entry == null) {
                continue;
            }
            int pageNo = clampPage(entry.pageNo + appliedOffset, totalPages);
            if (pageNo < lastPageNo) {
                pageNo = lastPageNo;
            }
            PdfTocEntry adjusted = new PdfTocEntry();
            adjusted.title = entry.title;
            adjusted.displayTitle = entry.displayTitle;
            adjusted.level = entry.level;
            adjusted.pageNo = pageNo;
            adjusted.sourcePageNo = entry.sourcePageNo;
            adjusted.outlineIndex = entry.outlineIndex;
            adjusted.chapterNo = entry.chapterNo;
            adjusted.sectionNo = entry.sectionNo;
            adjusted.leafNo = entry.leafNo;
            normalized.add(adjusted);
            lastPageNo = pageNo;
        }
        return normalized;
    }

    private List<Map<String, Object>> buildPdfTocLeafSections(
            List<PdfTocEntry> normalizedEntries,
            int totalPages,
            BookData data
    ) {
        List<Map<String, Object>> leafSections = new ArrayList<>();
        if (normalizedEntries == null || normalizedEntries.isEmpty()) {
            return leafSections;
        }

        Map<Integer, String> chapterTitleByIndex = new LinkedHashMap<>();
        Map<String, String> sectionTitleByKey = new LinkedHashMap<>();
        Map<String, int[]> sectionRangeByKey = buildPdfSectionRangeLookup(data, totalPages);
        Map<Integer, String> outlineChapterTitleByIndex = buildPdfChapterTitleLookup(data);
        Map<String, String> outlineSectionTitleByKey = buildPdfSectionTitleLookup(data);
        List<PdfTocEntry> leafEntries = new ArrayList<>();
        int fallbackChapterIndex = 1;
        int fallbackSectionIndex = 1;

        for (PdfTocEntry entry : normalizedEntries) {
            if (entry == null) {
                continue;
            }
            if (entry.level == 1) {
                int chapterIndex = entry.chapterNo != null && entry.chapterNo > 0 ? entry.chapterNo : fallbackChapterIndex;
                fallbackChapterIndex = chapterIndex;
                fallbackSectionIndex = 1;
                chapterTitleByIndex.put(
                        chapterIndex,
                        firstNonBlank(entry.displayTitle, firstNonBlank(entry.title, "Chapter " + chapterIndex))
                );
                continue;
            }
            if (entry.level == 2) {
                int chapterIndex = entry.chapterNo != null && entry.chapterNo > 0 ? entry.chapterNo : fallbackChapterIndex;
                int sectionIndex = entry.sectionNo != null && entry.sectionNo > 0 ? entry.sectionNo : fallbackSectionIndex;
                fallbackChapterIndex = chapterIndex;
                fallbackSectionIndex = sectionIndex;
                String sectionKey = chapterIndex + ":" + sectionIndex;
                sectionTitleByKey.put(
                        sectionKey,
                        firstNonBlank(entry.displayTitle, firstNonBlank(entry.title, "Section " + sectionIndex))
                );
                continue;
            }
            if (entry.level >= 3) {
                leafEntries.add(entry);
            }
        }
        if (leafEntries.isEmpty()) {
            return leafSections;
        }

        for (int i = 0; i < leafEntries.size(); i++) {
            PdfTocEntry entry = leafEntries.get(i);
            if (entry == null) {
                continue;
            }
            int chapterIndex = entry.chapterNo != null && entry.chapterNo > 0 ? entry.chapterNo : fallbackChapterIndex;
            int sectionIndex = entry.sectionNo != null && entry.sectionNo > 0 ? entry.sectionNo : fallbackSectionIndex;
            int subSectionIndex = entry.leafNo != null && entry.leafNo > 0 ? entry.leafNo : (i + 1);
            fallbackChapterIndex = chapterIndex;
            fallbackSectionIndex = sectionIndex;

            int startPage = clampPage(entry.pageNo, totalPages);
            int endPage = totalPages > 0 ? totalPages : startPage;
            if (i + 1 < leafEntries.size()) {
                PdfTocEntry nextEntry = leafEntries.get(i + 1);
                if (nextEntry != null) {
                    int nextStartPage = clampPage(nextEntry.pageNo, totalPages);
                    endPage = nextStartPage > startPage ? nextStartPage - 1 : startPage;
                }
            }
            String tocSectionKey = chapterIndex + ":" + sectionIndex;
            int selectorChapterIndex = chapterIndex;
            int selectorSectionIndex = sectionIndex;
            boolean hasExplicitTocCoordinates = entry.chapterNo != null
                    && entry.chapterNo > 0
                    && entry.sectionNo != null
                    && entry.sectionNo > 0;
            // 叶子 selector 应该优先代表目录里的 x.y.z，而不是回贴到正文解析出的 section。
            // 否则像 1.2.3 与 1.1.3 落在同页时，会被错误压成同一个 c1s1t3。
            if (!hasExplicitTocCoordinates) {
                String matchedSectionKey = resolveSectionKeyByPage(sectionRangeByKey, startPage);
                if (matchedSectionKey != null) {
                    String[] matchedParts = matchedSectionKey.split(":");
                    if (matchedParts.length == 2) {
                        Integer parsedChapter = parsePositiveIntOrNull(matchedParts[0]);
                        Integer parsedSection = parsePositiveIntOrNull(matchedParts[1]);
                        if (parsedChapter != null && parsedSection != null) {
                            selectorChapterIndex = parsedChapter;
                            selectorSectionIndex = parsedSection;
                        }
                    }
                }
            }
            String selectorSectionKey = selectorChapterIndex + ":" + selectorSectionIndex;
            int[] sectionRange = sectionRangeByKey.get(selectorSectionKey);
            if (sectionRange == null) {
                sectionRange = sectionRangeByKey.get(tocSectionKey);
            }
            if (sectionRange != null && sectionRange.length >= 2) {
                startPage = clampPage(Math.max(startPage, sectionRange[0]), totalPages);
                endPage = clampPage(Math.min(endPage, sectionRange[1]), totalPages);
            }
            if (endPage < startPage) {
                endPage = startPage;
            }

            String chapterTitle = firstNonBlank(
                    chapterTitleByIndex.get(chapterIndex),
                    firstNonBlank(outlineChapterTitleByIndex.get(selectorChapterIndex), "Chapter " + chapterIndex)
            );
            String sectionTitle = firstNonBlank(
                    sectionTitleByKey.get(tocSectionKey),
                    firstNonBlank(outlineSectionTitleByKey.get(selectorSectionKey), "Section " + sectionIndex)
            );
            String leafTitle = firstNonBlank(entry.displayTitle, firstNonBlank(entry.title, sectionTitle));
            String outlineIndex = chapterIndex + "." + sectionIndex + "." + subSectionIndex;
            String baseSectionSelector = "c" + selectorChapterIndex + "s" + selectorSectionIndex;
            String sectionSelector = baseSectionSelector + "t" + subSectionIndex;

            Map<String, Object> leafPayload = new LinkedHashMap<>();
            leafPayload.put("flatIndex", leafSections.size() + 1);
            leafPayload.put("chapterIndex", selectorChapterIndex);
            leafPayload.put("sectionIndex", selectorSectionIndex);
            leafPayload.put("subSectionIndex", subSectionIndex);
            leafPayload.put("chapterTitle", chapterTitle);
            leafPayload.put("sectionTitle", sectionTitle);
            leafPayload.put("title", leafTitle);
            leafPayload.put("outlineIndex", outlineIndex);
            leafPayload.put("startPage", startPage);
            leafPayload.put("endPage", endPage);
            leafPayload.put("baseSectionSelector", baseSectionSelector);
            leafPayload.put("sectionSelector", sectionSelector);
            leafSections.add(leafPayload);
        }
        return leafSections;
    }

    private Map<String, int[]> buildPdfSectionRangeLookup(BookData data, int totalPages) {
        Map<String, int[]> lookup = new LinkedHashMap<>();
        if (data == null || data.chapters == null || data.chapters.isEmpty()) {
            return lookup;
        }
        for (int chapterPos = 0; chapterPos < data.chapters.size(); chapterPos++) {
            Chapter chapter = data.chapters.get(chapterPos);
            if (chapter == null || chapter.sections == null || chapter.sections.isEmpty()) {
                continue;
            }
            for (int sectionPos = 0; sectionPos < chapter.sections.size(); sectionPos++) {
                Section section = chapter.sections.get(sectionPos);
                if (section == null) {
                    continue;
                }
                int sectionStart = section.startPage > 0 ? section.startPage : chapter.startPage;
                int sectionEnd = section.endPage > 0 ? section.endPage : chapter.endPage;
                if (sectionStart <= 0) {
                    continue;
                }
                if (sectionEnd < sectionStart) {
                    sectionEnd = sectionStart;
                }
                String key = (chapterPos + 1) + ":" + (sectionPos + 1);
                lookup.put(key, new int[]{
                        clampPage(sectionStart, totalPages),
                        clampPage(sectionEnd, totalPages)
                });
            }
        }
        return lookup;
    }

    private Map<Integer, String> buildPdfChapterTitleLookup(BookData data) {
        Map<Integer, String> lookup = new LinkedHashMap<>();
        if (data == null || data.chapters == null || data.chapters.isEmpty()) {
            return lookup;
        }
        for (int chapterPos = 0; chapterPos < data.chapters.size(); chapterPos++) {
            Chapter chapter = data.chapters.get(chapterPos);
            if (chapter == null) {
                continue;
            }
            lookup.put(chapterPos + 1, firstNonBlank(chapter.title, "Chapter " + (chapterPos + 1)));
        }
        return lookup;
    }

    private Map<String, String> buildPdfSectionTitleLookup(BookData data) {
        Map<String, String> lookup = new LinkedHashMap<>();
        if (data == null || data.chapters == null || data.chapters.isEmpty()) {
            return lookup;
        }
        for (int chapterPos = 0; chapterPos < data.chapters.size(); chapterPos++) {
            Chapter chapter = data.chapters.get(chapterPos);
            if (chapter == null || chapter.sections == null || chapter.sections.isEmpty()) {
                continue;
            }
            for (int sectionPos = 0; sectionPos < chapter.sections.size(); sectionPos++) {
                Section section = chapter.sections.get(sectionPos);
                if (section == null) {
                    continue;
                }
                String key = (chapterPos + 1) + ":" + (sectionPos + 1);
                lookup.put(key, firstNonBlank(section.title, "Section " + (sectionPos + 1)));
            }
        }
        return lookup;
    }

    private String resolveSectionKeyByPage(Map<String, int[]> sectionRangeByKey, int pageNo) {
        if (sectionRangeByKey == null || sectionRangeByKey.isEmpty() || pageNo <= 0) {
            return null;
        }
        String bestKey = null;
        int bestSpan = Integer.MAX_VALUE;
        for (Map.Entry<String, int[]> entry : sectionRangeByKey.entrySet()) {
            if (entry == null || entry.getValue() == null || entry.getValue().length < 2) {
                continue;
            }
            int start = entry.getValue()[0];
            int end = entry.getValue()[1];
            if (start <= 0 || end < start) {
                continue;
            }
            if (pageNo < start || pageNo > end) {
                continue;
            }
            int span = end - start;
            if (bestKey == null || span < bestSpan) {
                bestKey = entry.getKey();
                bestSpan = span;
            }
        }
        return bestKey;
    }

    private List<PdfTocEntry> parsePdfTocEntries(PDDocument pdf, int totalPages) throws Exception {
        List<PdfTocEntry> entries = new ArrayList<>();
        if (pdf == null || totalPages <= 0) {
            return entries;
        }
        PDFTextStripper pageStripper = new PDFTextStripper();
        pageStripper.setSortByPosition(true);
        int maxScanPages = Math.min(totalPages, PDF_TOC_MAX_SCAN_PAGES);
        int priorityScanPages = Math.min(maxScanPages, PDF_TOC_PRIORITY_SCAN_PAGES);
        boolean tocStarted = false;
        int tailNoHitPages = 0;
        Set<String> dedupe = new LinkedHashSet<>();

        for (int pageNo = 1; pageNo <= maxScanPages; pageNo++) {
            pageStripper.setStartPage(pageNo);
            pageStripper.setEndPage(pageNo);
            String pageText = pageStripper.getText(pdf);
            String normalizedPage = normalize(pageText).toLowerCase(Locale.ROOT);
            String[] lines = pageText.split("\\R");
            List<PdfTocEntry> pageEntries = new ArrayList<>();
            for (String rawLine : lines) {
                PdfTocEntry entry = parsePdfTocEntry(rawLine);
                if (entry == null) {
                    continue;
                }
                entry.sourcePageNo = pageNo;
                String dedupeKey = entry.level + "|" + normalize(entry.title).toLowerCase(Locale.ROOT) + "|" + entry.pageNo;
                if (dedupe.contains(dedupeKey)) {
                    continue;
                }
                dedupe.add(dedupeKey);
                pageEntries.add(entry);
            }

            boolean hasKeyword = looksLikeTocPage(normalizedPage);
            boolean hasStructuredTocLines = pageEntries.size() >= PDF_TOC_MIN_LINES_PER_PAGE;
            boolean inPriorityWindow = pageNo <= priorityScanPages;
            if (!tocStarted) {
                if (hasKeyword || (inPriorityWindow && hasStructuredTocLines)) {
                    tocStarted = true;
                } else {
                    continue;
                }
            }

            if (!pageEntries.isEmpty()) {
                entries.addAll(pageEntries);
                tailNoHitPages = 0;
            } else {
                tailNoHitPages += 1;
                if (entries.size() >= 12 && tailNoHitPages >= 3) {
                    break;
                }
            }
        }
        return entries;
    }

    private boolean looksLikeTocPage(String normalizedPageText) {
        if (normalizedPageText == null || normalizedPageText.isBlank()) {
            return false;
        }
        return normalizedPageText.contains("contents")
                || normalizedPageText.contains("table of contents")
                || normalizedPageText.contains("\u76EE\u5F55");
    }

    private PdfTocEntry parsePdfTocEntry(String rawLine) {
        String line = normalize(rawLine);
        if (line.isBlank() || line.length() > 220) {
            return null;
        }
        Matcher matcher = TOC_TRAILING_PAGE_PATTERN.matcher(line);
        if (!matcher.matches()) {
            return null;
        }
        String title = normalize(matcher.group(1)).replaceAll("\\.{2,}$", "");
        if (title.isBlank()) {
            return null;
        }
        int pageNo;
        try {
            pageNo = Integer.parseInt(matcher.group(2));
        } catch (Exception ignored) {
            return null;
        }
        if (pageNo <= 0) {
            return null;
        }

        String displayTitle = title;
        String outlineIndex = "";
        Integer chapterNo = null;
        Integer sectionNo = null;
        Integer leafNo = null;
        int level;
        Matcher numericTitleMatcher = TOC_NUMERIC_TITLE_PATTERN.matcher(title);
        if (numericTitleMatcher.matches()) {
            outlineIndex = firstNonBlank(numericTitleMatcher.group(1), "");
            displayTitle = firstNonBlank(normalize(numericTitleMatcher.group(2)), title);
            String[] indexParts = outlineIndex.split("\\.");
            chapterNo = indexParts.length >= 1 ? parsePositiveIntOrNull(indexParts[0]) : null;
            sectionNo = indexParts.length >= 2 ? parsePositiveIntOrNull(indexParts[1]) : null;
            leafNo = indexParts.length >= 3 ? parsePositiveIntOrNull(indexParts[2]) : null;
            if (indexParts.length == 1) {
                level = 1;
            } else if (indexParts.length == 2) {
                level = 2;
            } else {
                level = 3;
            }
        } else if (title.matches("^\\d+\\.\\d+(?:\\.\\d+){0,6}\\s+.+") || PDF_SECTION_TITLE_PATTERN.matcher(title).matches()) {
            level = 2;
        } else if (title.matches("^\\d+\\s+.+") || PDF_CHAPTER_TITLE_PATTERN.matcher(title).matches()) {
            level = 1;
        } else {
            return null;
        }

        PdfTocEntry entry = new PdfTocEntry();
        entry.title = title;
        entry.displayTitle = displayTitle;
        entry.level = level;
        entry.pageNo = pageNo;
        entry.outlineIndex = outlineIndex;
        entry.chapterNo = chapterNo;
        entry.sectionNo = sectionNo;
        entry.leafNo = leafNo;
        return entry;
    }

    private Integer estimatePdfPageOffset(PDDocument pdf, List<PdfTocEntry> entries, int totalPages) throws Exception {
        List<Integer> offsets = new ArrayList<>();
        if (entries == null || entries.isEmpty()) {
            return null;
        }
        for (PdfTocEntry entry : entries) {
            if (entry.level != 1) {
                continue;
            }
            int minSearchPage = entry.sourcePageNo > 0 ? Math.min(totalPages, entry.sourcePageNo + 1) : 1;
            int foundPage = findPdfPageContainingHeading(pdf, entry.title, totalPages, minSearchPage);
            if (foundPage > 0) {
                offsets.add(foundPage - entry.pageNo);
            }
            if (offsets.size() >= 6) {
                break;
            }
        }
        if (offsets.isEmpty()) {
            return null;
        }
        offsets.sort(Integer::compareTo);
        return offsets.get(offsets.size() / 2);
    }

    private int findPdfPageContainingHeading(PDDocument pdf, String heading, int totalPages, int minSearchPageNo) throws Exception {
        String normalizedHeading = normalize(heading).toLowerCase(Locale.ROOT);
        if (normalizedHeading.isBlank()) {
            return -1;
        }
        PDFTextStripper pageStripper = new PDFTextStripper();
        pageStripper.setSortByPosition(true);
        int maxScanPages = Math.min(totalPages, 240);
        int startPage = Math.max(1, minSearchPageNo);
        for (int pageNo = startPage; pageNo <= maxScanPages; pageNo++) {
            pageStripper.setStartPage(pageNo);
            pageStripper.setEndPage(pageNo);
            String pageText = normalize(pageStripper.getText(pdf)).toLowerCase(Locale.ROOT);
            if (pageText.contains(normalizedHeading)) {
                return pageNo;
            }
        }
        return -1;
    }

    private void finalizePdfPageRanges(BookData data, int totalPages) {
        if (data == null || data.chapters == null || data.chapters.isEmpty()) {
            return;
        }
        for (int chapterIndex = 0; chapterIndex < data.chapters.size(); chapterIndex++) {
            Chapter chapter = data.chapters.get(chapterIndex);
            int chapterStart = chapter.startPage > 0 ? chapter.startPage : 1;
            int chapterEnd = totalPages;
            if (chapterIndex + 1 < data.chapters.size()) {
                Chapter nextChapter = data.chapters.get(chapterIndex + 1);
                if (nextChapter.startPage > 0) {
                    chapterEnd = Math.max(chapterStart, nextChapter.startPage - 1);
                }
            }
            chapter.startPage = clampPage(chapterStart, totalPages);
            chapter.endPage = clampPage(chapterEnd, totalPages);

            if (chapter.sections == null || chapter.sections.isEmpty()) {
                Section body = new Section("Body");
                body.startPage = chapter.startPage;
                body.endPage = chapter.endPage;
                chapter.sections = new ArrayList<>();
                chapter.sections.add(body);
                continue;
            }

            for (int sectionIndex = 0; sectionIndex < chapter.sections.size(); sectionIndex++) {
                Section section = chapter.sections.get(sectionIndex);
                int sectionStart = section.startPage > 0 ? section.startPage : chapter.startPage;
                int sectionEnd = chapter.endPage;
                if (sectionIndex + 1 < chapter.sections.size()) {
                    Section nextSection = chapter.sections.get(sectionIndex + 1);
                    if (nextSection.startPage > 0) {
                        sectionEnd = Math.max(sectionStart, nextSection.startPage - 1);
                    }
                }
                section.startPage = clampPage(Math.max(chapter.startPage, sectionStart), totalPages);
                section.endPage = clampPage(Math.min(chapter.endPage, sectionEnd), totalPages);
            }
        }
    }

    private void shiftPdfPageRanges(BookData data, int totalPages, int pageOffset) {
        if (data == null || data.chapters == null || data.chapters.isEmpty() || pageOffset == 0) {
            return;
        }
        for (Chapter chapter : data.chapters) {
            if (chapter == null) {
                continue;
            }
            if (chapter.startPage > 0) {
                chapter.startPage = clampPage(chapter.startPage + pageOffset, totalPages);
            }
            if (chapter.endPage > 0) {
                chapter.endPage = clampPage(chapter.endPage + pageOffset, totalPages);
            }
            if (chapter.startPage > 0 && chapter.endPage > 0 && chapter.endPage < chapter.startPage) {
                chapter.endPage = chapter.startPage;
            }
            if (chapter.sections == null) {
                continue;
            }
            for (Section section : chapter.sections) {
                if (section == null) {
                    continue;
                }
                if (section.startPage > 0) {
                    section.startPage = clampPage(section.startPage + pageOffset, totalPages);
                }
                if (section.endPage > 0) {
                    section.endPage = clampPage(section.endPage + pageOffset, totalPages);
                }
                if (section.startPage > 0 && section.endPage > 0 && section.endPage < section.startPage) {
                    section.endPage = section.startPage;
                }
            }
        }
    }

    private Integer resolveDetectedOffsetForOutline(BookData outlineData, Integer fallbackOffset) {
        int firstAnchoredStart = findFirstAnchoredSectionStartPage(outlineData);
        if (firstAnchoredStart > 0) {
            return Math.max(0, firstAnchoredStart - 1);
        }
        return fallbackOffset;
    }

    private int findFirstAnchoredSectionStartPage(BookData data) {
        if (data == null || data.chapters == null || data.chapters.isEmpty()) {
            return -1;
        }
        int minStart = Integer.MAX_VALUE;
        for (Chapter chapter : data.chapters) {
            if (chapter == null || chapter.sections == null || chapter.sections.isEmpty()) {
                continue;
            }
            for (Section section : chapter.sections) {
                if (section == null || section.startPage <= 0) {
                    continue;
                }
                minStart = Math.min(minStart, section.startPage);
            }
        }
        return minStart == Integer.MAX_VALUE ? -1 : minStart;
    }

    private int clampPage(int pageNo, int totalPages) {
        if (totalPages <= 0) {
            return pageNo;
        }
        if (pageNo < 1) {
            return 1;
        }
        if (pageNo > totalPages) {
            return totalPages;
        }
        return pageNo;
    }

    private boolean hasPageAnchoredStructure(BookData data) {
        if (data == null || data.chapters == null || data.chapters.isEmpty()) {
            return false;
        }
        int anchoredSections = 0;
        for (Chapter chapter : data.chapters) {
            if (chapter == null || chapter.sections == null) {
                continue;
            }
            for (Section section : chapter.sections) {
                if (section != null && section.startPage > 0) {
                    anchoredSections += 1;
                }
            }
        }
        return anchoredSections > 0;
    }

    private boolean isBackMatterTitle(String normalizedLowerTitle) {
        if (normalizedLowerTitle == null || normalizedLowerTitle.isBlank()) {
            return false;
        }
        return normalizedLowerTitle.contains("index")
                || normalizedLowerTitle.contains("bibliography")
                || normalizedLowerTitle.contains("glossary");
    }

    private String extractEpubMetadataTitle(Document opfDoc) {
        if (opfDoc == null) {
            return "";
        }
        for (Element titleNode : opfDoc.select("metadata > title, metadata > dc|title, metadata > *|title")) {
            String title = normalize(titleNode.text());
            if (!title.isBlank()) {
                return title;
            }
        }
        return "";
    }

    private Chapter parseEpubChapter(
            ZipFile zip,
            Document htmlDoc,
            String spinePath,
            Path outputRoot,
            Path imageAssetsRoot,
            int chapterIndex
    ) throws Exception {
        Element body = htmlDoc.body();
        if (body == null) {
            return null;
        }
        String chapterTitle = normalize(body.select("h1").stream()
                .findFirst()
                .map(Element::text)
                .orElse(""));
        chapterTitle = firstNonBlank(chapterTitle, normalize(htmlDoc.title()));
        chapterTitle = firstNonBlank(chapterTitle, "Chapter " + chapterIndex);

        Chapter chapter = new Chapter(chapterTitle);
        Section currentSection = new Section("Body");
        chapter.sections.add(currentSection);

        int imageIndex = 0;
        for (Element node : body.select("h1, h2, h3, p, li, blockquote, pre, img[src], table")) {
            String tag = node.tagName().toLowerCase(Locale.ROOT);
            if (("p".equals(tag) || "li".equals(tag) || "blockquote".equals(tag) || "pre".equals(tag))
                    && node.closest("table") != null) {
                continue;
            }

            if ("h1".equals(tag) || "h2".equals(tag) || "h3".equals(tag)) {
                String heading = normalize(node.text());
                if (!heading.isBlank()) {
                    if ("h1".equals(tag)
                            && chapter.title.equalsIgnoreCase("Chapter " + chapterIndex)
                            && !hasSectionContent(currentSection)) {
                        chapter.title = heading;
                    } else if (!heading.equals(chapter.title)) {
                        currentSection = new Section(heading);
                        chapter.sections.add(currentSection);
                    }
                }
                continue;
            }

            if ("img".equals(tag)) {
                imageIndex += 1;
                String copiedImage = copyEpubImage(
                        zip,
                        spinePath,
                        node.attr("src"),
                        outputRoot,
                        imageAssetsRoot,
                        chapterIndex,
                        imageIndex
                );
                if (copiedImage != null) {
                    currentSection.images.add(copiedImage);
                    SectionBlock imageBlock = new SectionBlock();
                    imageBlock.type = SectionBlock.BlockType.IMAGE;
                    imageBlock.imagePath = copiedImage;
                    currentSection.blocks.add(imageBlock);
                }
                continue;
            }

            if ("table".equals(tag)) {
                List<List<String>> tableRows = parseHtmlTable(node);
                if (!tableRows.isEmpty()) {
                    currentSection.tables.add(tableRows);
                    SectionBlock tableBlock = new SectionBlock();
                    tableBlock.type = SectionBlock.BlockType.TABLE;
                    tableBlock.table = tableRows;
                    currentSection.blocks.add(tableBlock);
                }
                continue;
            }

            String text = normalize(node.text());
            if (!text.isBlank()) {
                currentSection.paragraphs.add(text);
                SectionBlock paragraphBlock = new SectionBlock();
                paragraphBlock.type = SectionBlock.BlockType.PARAGRAPH;
                paragraphBlock.text = text;
                currentSection.blocks.add(paragraphBlock);
            }
        }

        if (!hasChapterContent(chapter)) {
            return null;
        }
        return chapter;
    }

    private BookData parseStructuredLines(String[] lines, String fallbackTitle) {
        BookData data = new BookData();
        data.title = fallbackTitle;

        Chapter currentChapter = new Chapter("Chapter 1");
        Section currentSection = new Section("Body");
        currentChapter.sections.add(currentSection);

        String detectedTitle = "";
        for (String rawLine : lines) {
            String line = normalize(rawLine);
            if (line.isBlank()) {
                continue;
            }

            boolean chapterHeading = CHAPTER_PATTERN.matcher(line).matches();
            boolean sectionHeading = SECTION_PATTERN.matcher(line).matches();
            if (detectedTitle.isBlank() && !chapterHeading && !sectionHeading) {
                detectedTitle = line;
            }

            if (chapterHeading) {
                finalizeChapter(data.chapters, currentChapter);
                currentChapter = new Chapter(line);
                currentSection = new Section("Body");
                currentChapter.sections.add(currentSection);
                continue;
            }
            if (sectionHeading) {
                currentSection = new Section(line);
                currentChapter.sections.add(currentSection);
                continue;
            }
            currentSection.paragraphs.add(line);
        }

        finalizeChapter(data.chapters, currentChapter);
        data.title = firstNonBlank(detectedTitle, fallbackTitle);
        if (data.chapters.isEmpty()) {
            Chapter chapter = new Chapter("Chapter 1");
            chapter.sections.add(new Section("Body"));
            data.chapters.add(chapter);
        }
        return data;
    }

    private Path resolveBookSourcePath(String sourcePath) throws Exception {
        String normalized = firstNonBlank(sourcePath, "").trim();
        if (normalized.isBlank()) {
            throw new IllegalArgumentException("sourcePath cannot be empty");
        }
        if (normalized.startsWith("file://")) {
            return Paths.get(URI.create(normalized)).toAbsolutePath().normalize();
        }
        return Paths.get(normalized).toAbsolutePath().normalize();
    }

    private void annotateSelectors(BookData data) {
        if (data == null || data.chapters == null) {
            return;
        }
        for (int chapterIndex = 0; chapterIndex < data.chapters.size(); chapterIndex++) {
            Chapter chapter = data.chapters.get(chapterIndex);
            if (chapter == null) {
                continue;
            }
            chapter.selector = "c" + (chapterIndex + 1);
            if (chapter.sections == null) {
                chapter.sections = new ArrayList<>();
            }
            for (int sectionIndex = 0; sectionIndex < chapter.sections.size(); sectionIndex++) {
                Section section = chapter.sections.get(sectionIndex);
                if (section == null) {
                    continue;
                }
                section.selector = "c" + (chapterIndex + 1) + "s" + (sectionIndex + 1);
            }
        }
    }

    private void fillProbeResult(
            BookProbeResult result,
            Path source,
            String ext,
            BookData data,
            int totalPages
    ) {
        result.sourcePath = source.toString();
        result.format = firstNonBlank(ext, "");
        result.totalPages = totalPages;
        result.bookTitle = firstNonBlank(
                data != null ? data.title : "",
                stripExt(source.getFileName() != null ? source.getFileName().toString() : source.toString())
        );
        result.appliedPageOffset = data != null ? data.appliedPageOffset : null;
        result.detectedPageOffset = data != null ? data.detectedPageOffset : null;
        result.pageMapStrategy = data != null ? firstNonBlank(data.pageMapStrategy, "") : "";
        result.leafSections = data != null && data.leafSections != null ? new ArrayList<>(data.leafSections) : new ArrayList<>();
        Map<Integer, String> leafChapterTitleByIndex = buildLeafChapterTitleLookup(result.leafSections);
        Map<String, String> leafSectionTitleByKey = buildLeafSectionTitleLookup(result.leafSections);
        int sectionGlobalIndex = 0;
        if (data == null || data.chapters == null) {
            result.chapterCount = 0;
            result.sectionCount = 0;
            return;
        }
        result.chapterCount = data.chapters.size();
        for (int chapterIndex = 0; chapterIndex < data.chapters.size(); chapterIndex++) {
            Chapter chapter = data.chapters.get(chapterIndex);
            if (chapter == null) {
                continue;
            }
            int chapterHumanIndex = chapterIndex + 1;
            String chapterTitle = firstNonBlank(
                    leafChapterTitleByIndex.get(chapterHumanIndex),
                    firstNonBlank(chapter.title, "Chapter " + chapterHumanIndex)
            );
            Map<String, Object> chapterPayload = new LinkedHashMap<>();
            chapterPayload.put("chapterIndex", chapterHumanIndex);
            chapterPayload.put("chapterSelector", firstNonBlank(chapter.selector, "c" + chapterHumanIndex));
            chapterPayload.put("title", chapterTitle);
            chapterPayload.put("startPage", chapter.startPage);
            chapterPayload.put("endPage", chapter.endPage);
            List<Map<String, Object>> sectionPayload = new ArrayList<>();
            chapterPayload.put("sections", sectionPayload);
            result.chapters.add(chapterPayload);

            if (chapter.sections == null) {
                continue;
            }
            for (int sectionIndex = 0; sectionIndex < chapter.sections.size(); sectionIndex++) {
                Section section = chapter.sections.get(sectionIndex);
                if (section == null) {
                    continue;
                }
                sectionGlobalIndex += 1;
                int sectionHumanIndex = sectionIndex + 1;
                String sectionTitle = firstNonBlank(
                        leafSectionTitleByKey.get(chapterHumanIndex + ":" + sectionHumanIndex),
                        firstNonBlank(section.title, "Section " + sectionHumanIndex)
                );
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("flatIndex", sectionGlobalIndex);
                item.put("chapterIndex", chapterHumanIndex);
                item.put("sectionIndex", sectionHumanIndex);
                item.put("chapterSelector", firstNonBlank(chapter.selector, "c" + chapterHumanIndex));
                item.put("sectionSelector", firstNonBlank(section.selector, "c" + chapterHumanIndex + "s" + sectionHumanIndex));
                item.put("chapterTitle", chapterTitle);
                item.put("title", sectionTitle);
                item.put("startPage", section.startPage);
                item.put("endPage", section.endPage);
                sectionPayload.add(item);
                result.sections.add(item);
            }
        }
        result.sectionCount = sectionGlobalIndex;
    }

    private List<Map<String, Object>> buildBookSectionTreePayload(BookData data) {
        List<Map<String, Object>> tree = new ArrayList<>();
        if (data == null || data.chapters == null || data.chapters.isEmpty()) {
            return tree;
        }

        Map<Integer, String> leafChapterTitleByIndex = buildLeafChapterTitleLookup(data.leafSections);
        Map<String, String> leafSectionTitleByKey = buildLeafSectionTitleLookup(data.leafSections);
        Map<String, List<Map<String, Object>>> leafBySectionKey = new LinkedHashMap<>();
        if (data.leafSections != null) {
            for (Map<String, Object> leaf : data.leafSections) {
                if (leaf == null) {
                    continue;
                }
                int chapterIndex = intValue(leaf.get("chapterIndex"), -1);
                int sectionIndex = intValue(leaf.get("sectionIndex"), -1);
                if (chapterIndex <= 0 || sectionIndex <= 0) {
                    continue;
                }
                String sectionKey = chapterIndex + ":" + sectionIndex;
                leafBySectionKey.computeIfAbsent(sectionKey, key -> new ArrayList<>()).add(leaf);
            }
        }
        for (List<Map<String, Object>> leaves : leafBySectionKey.values()) {
            leaves.sort(
                    Comparator.comparingInt((Map<String, Object> leaf) -> intValue(leaf.get("subSectionIndex"), Integer.MAX_VALUE))
                            .thenComparingInt((Map<String, Object> leaf) -> intValue(leaf.get("flatIndex"), Integer.MAX_VALUE))
            );
        }

        int episodeNo = 0;
        for (int chapterPos = 0; chapterPos < data.chapters.size(); chapterPos++) {
            Chapter chapter = data.chapters.get(chapterPos);
            if (chapter == null) {
                continue;
            }
            int chapterIndex = chapterPos + 1;
            String chapterTitle = firstNonBlank(
                    leafChapterTitleByIndex.get(chapterIndex),
                    firstNonBlank(chapter.title, "Chapter " + chapterIndex)
            );

            Map<String, Object> chapterNode = new LinkedHashMap<>();
            chapterNode.put("nodeType", "chapter");
            chapterNode.put("chapterIndex", chapterIndex);
            chapterNode.put("title", chapterTitle);
            List<Map<String, Object>> sectionNodes = new ArrayList<>();
            chapterNode.put("children", sectionNodes);
            tree.add(chapterNode);

            List<Section> sections = chapter.sections != null ? chapter.sections : Collections.emptyList();
            for (int sectionPos = 0; sectionPos < sections.size(); sectionPos++) {
                Section section = sections.get(sectionPos);
                if (section == null) {
                    continue;
                }
                int sectionIndex = sectionPos + 1;
                String sectionKey = chapterIndex + ":" + sectionIndex;
                String sectionTitle = firstNonBlank(
                        leafSectionTitleByKey.get(sectionKey),
                        firstNonBlank(section.title, "Section " + sectionIndex)
                );

                Map<String, Object> sectionNode = new LinkedHashMap<>();
                sectionNode.put("nodeType", "section");
                sectionNode.put("chapterIndex", chapterIndex);
                sectionNode.put("sectionIndex", sectionIndex);
                sectionNode.put("title", sectionTitle);
                List<Map<String, Object>> leafNodes = new ArrayList<>();
                sectionNode.put("children", leafNodes);
                sectionNodes.add(sectionNode);

                List<Map<String, Object>> matchedLeaves = leafBySectionKey.getOrDefault(sectionKey, List.of());
                if (matchedLeaves.isEmpty()) {
                    episodeNo += 1;
                    Map<String, Object> leafNode = new LinkedHashMap<>();
                    leafNode.put("nodeType", "leaf");
                    leafNode.put("episodeNo", episodeNo);
                    leafNode.put("chapterIndex", chapterIndex);
                    leafNode.put("sectionIndex", sectionIndex);
                    leafNode.put("subSectionIndex", 1);
                    leafNode.put("outlineIndex", chapterIndex + "." + sectionIndex + ".1");
                    leafNode.put("title", sectionTitle);
                    leafNode.put("startPage", section.startPage);
                    leafNode.put("endPage", section.endPage);
                    String sectionSelector = firstNonBlank(section.selector, "");
                    if (!sectionSelector.isBlank()) {
                        leafNode.put("sectionSelector", sectionSelector);
                    }
                    leafNodes.add(leafNode);
                    continue;
                }

                for (int leafPos = 0; leafPos < matchedLeaves.size(); leafPos++) {
                    Map<String, Object> leaf = matchedLeaves.get(leafPos);
                    episodeNo += 1;
                    int subSectionIndex = intValue(leaf.get("subSectionIndex"), leafPos + 1);
                    if (subSectionIndex <= 0) {
                        subSectionIndex = leafPos + 1;
                    }
                    int startPage = intValue(leaf.get("startPage"), section.startPage);
                    int endPage = intValue(leaf.get("endPage"), section.endPage);
                    if (startPage > 0 && endPage > 0 && endPage < startPage) {
                        endPage = startPage;
                    }
                    String outlineIndex = firstNonBlank(stringValue(leaf.get("outlineIndex")), "");
                    if (outlineIndex.isBlank()) {
                        outlineIndex = chapterIndex + "." + sectionIndex + "." + subSectionIndex;
                    }
                    String leafTitle = firstNonBlank(stringValue(leaf.get("title")), sectionTitle);

                    Map<String, Object> leafNode = new LinkedHashMap<>();
                    leafNode.put("nodeType", "leaf");
                    leafNode.put("episodeNo", episodeNo);
                    leafNode.put("chapterIndex", chapterIndex);
                    leafNode.put("sectionIndex", sectionIndex);
                    leafNode.put("subSectionIndex", subSectionIndex);
                    leafNode.put("outlineIndex", outlineIndex);
                    leafNode.put("title", leafTitle);
                    leafNode.put("startPage", startPage);
                    leafNode.put("endPage", endPage);
                    String sectionSelector = firstNonBlank(stringValue(leaf.get("sectionSelector")), "");
                    if (!sectionSelector.isBlank()) {
                        leafNode.put("sectionSelector", sectionSelector);
                    } else if (section.selector != null && !section.selector.isBlank()) {
                        leafNode.put("sectionSelector", section.selector);
                    }
                    leafNodes.add(leafNode);
                }
            }
        }
        return tree;
    }

    private Map<Integer, String> buildLeafChapterTitleLookup(List<Map<String, Object>> leafSections) {
        Map<Integer, String> lookup = new LinkedHashMap<>();
        if (leafSections == null || leafSections.isEmpty()) {
            return lookup;
        }
        for (Map<String, Object> leaf : leafSections) {
            if (leaf == null) {
                continue;
            }
            int chapterIndex = intValue(leaf.get("chapterIndex"), -1);
            String chapterTitle = firstNonBlank(stringValue(leaf.get("chapterTitle")), "");
            if (chapterIndex <= 0 || chapterTitle.isBlank() || lookup.containsKey(chapterIndex)) {
                continue;
            }
            lookup.put(chapterIndex, chapterTitle);
        }
        return lookup;
    }

    private Map<String, String> buildLeafSectionTitleLookup(List<Map<String, Object>> leafSections) {
        Map<String, String> lookup = new LinkedHashMap<>();
        if (leafSections == null || leafSections.isEmpty()) {
            return lookup;
        }
        for (Map<String, Object> leaf : leafSections) {
            if (leaf == null) {
                continue;
            }
            int chapterIndex = intValue(leaf.get("chapterIndex"), -1);
            int sectionIndex = intValue(leaf.get("sectionIndex"), -1);
            String sectionTitle = firstNonBlank(stringValue(leaf.get("sectionTitle")), "");
            if (chapterIndex <= 0 || sectionIndex <= 0 || sectionTitle.isBlank()) {
                continue;
            }
            lookup.putIfAbsent(chapterIndex + ":" + sectionIndex, sectionTitle);
        }
        return lookup;
    }

    private int intValue(Object rawValue, int fallback) {
        if (rawValue == null) {
            return fallback;
        }
        if (rawValue instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(rawValue).trim());
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private String stringValue(Object rawValue) {
        if (rawValue == null) {
            return "";
        }
        return String.valueOf(rawValue).trim();
    }

    private BookData applyBookSelectors(BookData data, BookProcessingOptions options) {
        BookData safe = data != null ? data : new BookData();
        List<Chapter> source = safe.chapters != null ? safe.chapters : Collections.emptyList();

        if (options != null) {
            ContinuousLeafSelection continuousLeafSelection =
                    resolveContinuousLeafSelection(options.sectionSelector, safe.leafSections);
            if (continuousLeafSelection != null && continuousLeafSelection.leafCount > 1) {
                return buildContinuousLeafRangeBookData(safe, continuousLeafSelection);
            }
        }

        List<Chapter> chapterFiltered = source;
        if (options != null) {
            chapterFiltered = filterChapters(chapterFiltered, options.chapterSelector);
        }
        if (chapterFiltered.isEmpty()) {
            BookData empty = new BookData();
            empty.title = safe.title;
            empty.chapters = new ArrayList<>();
            return empty;
        }

        List<Chapter> finalChapters = chapterFiltered;
        if (options != null) {
            finalChapters = filterSections(chapterFiltered, options.sectionSelector, safe.leafSections);
        }

        BookData selected = new BookData();
        selected.title = safe.title;
        selected.pageMapStrategy = safe.pageMapStrategy;
        selected.detectedPageOffset = safe.detectedPageOffset;
        selected.appliedPageOffset = safe.appliedPageOffset;
        selected.chapters = finalChapters != null ? finalChapters : new ArrayList<>();
        selected.leafSections = safe.leafSections != null ? new ArrayList<>(safe.leafSections) : new ArrayList<>();
        return selected;
    }

    private List<Chapter> filterChapters(List<Chapter> chapters, String chapterSelector) {
        List<Chapter> source = chapters != null ? chapters : Collections.emptyList();
        if (source.isEmpty()) {
            return new ArrayList<>();
        }
        String selector = normalize(chapterSelector);
        if (selector.isBlank()) {
            return new ArrayList<>(source);
        }

        Set<Integer> selectedIndexes = new LinkedHashSet<>();
        List<String> titleTokens = new ArrayList<>();
        for (String rawToken : selector.split(SELECTOR_TOKEN_SPLIT_REGEX)) {
            String token = normalize(rawToken);
            if (token.isBlank()) {
                continue;
            }
            Matcher rangeMatcher = RANGE_PATTERN.matcher(token);
            if (rangeMatcher.matches()) {
                int start = Integer.parseInt(rangeMatcher.group(1));
                int end = Integer.parseInt(rangeMatcher.group(2));
                int low = Math.min(start, end);
                int high = Math.max(start, end);
                for (int idx = low; idx <= high; idx++) {
                    addSelectorIndex(selectedIndexes, idx, source.size());
                }
                continue;
            }
            if (token.chars().allMatch(Character::isDigit)) {
                addSelectorIndex(selectedIndexes, Integer.parseInt(token), source.size());
                continue;
            }
            titleTokens.add(token.toLowerCase(Locale.ROOT));
        }

        if (!titleTokens.isEmpty()) {
            for (int i = 0; i < source.size(); i++) {
                String normalizedTitle = normalize(source.get(i).title).toLowerCase(Locale.ROOT);
                for (String token : titleTokens) {
                    if (!token.isBlank() && normalizedTitle.contains(token)) {
                        selectedIndexes.add(i);
                        break;
                    }
                }
            }
        }

        if (selectedIndexes.isEmpty()) {
            return new ArrayList<>();
        }

        List<Chapter> filtered = new ArrayList<>();
        for (Integer index : selectedIndexes) {
            if (index != null && index >= 0 && index < source.size()) {
                filtered.add(source.get(index));
            }
        }
        return filtered;
    }

    private void addSelectorIndex(Set<Integer> selectedIndexes, int humanIndex, int chapterSize) {
        int zeroBasedIndex = humanIndex - 1;
        if (zeroBasedIndex >= 0 && zeroBasedIndex < chapterSize) {
            selectedIndexes.add(zeroBasedIndex);
        }
    }

    private List<Chapter> filterSections(
            List<Chapter> chapters,
            String sectionSelector,
            List<Map<String, Object>> leafSections
    ) {
        if (leafSections == null || leafSections.isEmpty()) {
            return filterSections(chapters, sectionSelector);
        }
        Map<String, Map<String, Object>> leafBySelector = new LinkedHashMap<>();
        Map<String, String> leafTitleBySelector = new LinkedHashMap<>();
        List<String> orderedLeafSelectors = new ArrayList<>();
        for (Map<String, Object> leaf : leafSections) {
            if (leaf == null) {
                continue;
            }
            String rawSelector = normalize(String.valueOf(leaf.get("sectionSelector")));
            if (rawSelector.isBlank()) {
                continue;
            }
            String canonicalLeafSelector = canonicalizeLeafSelector(rawSelector);
            String selector = canonicalLeafSelector.isBlank() ? rawSelector : canonicalLeafSelector;
            leafBySelector.putIfAbsent(selector, leaf);
            if (!canonicalLeafSelector.isBlank() && !orderedLeafSelectors.contains(canonicalLeafSelector)) {
                orderedLeafSelectors.add(canonicalLeafSelector);
            }
            String leafTitle = normalize(String.valueOf(leaf.get("title")));
            if (!leafTitle.isBlank()) {
                leafTitleBySelector.putIfAbsent(selector, leafTitle);
                if (!canonicalLeafSelector.isBlank()) {
                    leafTitleBySelector.putIfAbsent(canonicalLeafSelector, leafTitle);
                }
            }
        }
        List<Chapter> selected = filterSectionsBySelector(chapters, sectionSelector, leafBySelector);
        if (selected.isEmpty() || leafBySelector.isEmpty()) {
            return selected;
        }
        List<String> selectedLeafSelectors = parseOrderedLeafSelectors(sectionSelector);
        String firstLeafSelector = selectedLeafSelectors.isEmpty() ? "" : selectedLeafSelectors.get(0);
        String lastLeafSelector = selectedLeafSelectors.isEmpty()
                ? ""
                : selectedLeafSelectors.get(selectedLeafSelectors.size() - 1);
        String startTrimAnchor = resolveLeafTitle(firstLeafSelector, leafTitleBySelector, leafBySelector);
        String endTrimAnchor = findNextLeafTitle(lastLeafSelector, orderedLeafSelectors, leafTitleBySelector, leafBySelector);

        for (Chapter chapter : selected) {
            if (chapter == null || chapter.sections == null || chapter.sections.isEmpty()) {
                continue;
            }
            for (Section section : chapter.sections) {
                if (section == null) {
                    continue;
                }
                String rawSelector = normalize(section.selector);
                String canonicalLeafSelector = canonicalizeLeafSelector(rawSelector);
                String selector = canonicalLeafSelector.isBlank() ? rawSelector : canonicalLeafSelector;
                if (selector.isBlank()) {
                    continue;
                }
                Map<String, Object> leaf = leafBySelector.get(selector);
                if (leaf == null) {
                    continue;
                }
                int startPage = readObjectInt(leaf.get("startPage"), section.startPage);
                int endPage = readObjectInt(leaf.get("endPage"), section.endPage);
                if (startPage > 0) {
                    section.startPage = startPage;
                    section.endPage = endPage >= startPage ? endPage : startPage;
                }
                String leafTitle = normalize(String.valueOf(leaf.get("title")));
                if (!leafTitle.isBlank()) {
                    section.title = leafTitle;
                }
                if (!firstLeafSelector.isBlank() && firstLeafSelector.equals(selector) && !startTrimAnchor.isBlank()) {
                    section.trimStartAnchorTitle = startTrimAnchor;
                }
                if (!lastLeafSelector.isBlank() && lastLeafSelector.equals(selector) && !endTrimAnchor.isBlank()) {
                    section.trimEndBeforeTitle = endTrimAnchor;
                }
            }
        }
        return selected;
    }

    private List<Chapter> filterSections(List<Chapter> chapters, String sectionSelector) {
        return filterSectionsBySelector(chapters, sectionSelector, Collections.emptyMap());
    }

    private List<Chapter> filterSectionsBySelector(
            List<Chapter> chapters,
            String sectionSelector,
            Map<String, Map<String, Object>> leafBySelector
    ) {
        List<Chapter> source = chapters != null ? chapters : Collections.emptyList();
        String selector = normalize(sectionSelector);
        if (source.isEmpty() || selector.isBlank()) {
            return new ArrayList<>(source);
        }

        List<SectionSelectorRef> refs = new ArrayList<>();
        int global = 0;
        for (int chapterIndex = 0; chapterIndex < source.size(); chapterIndex++) {
            Chapter chapter = source.get(chapterIndex);
            if (chapter == null || chapter.sections == null) {
                continue;
            }
            for (int sectionIndex = 0; sectionIndex < chapter.sections.size(); sectionIndex++) {
                Section section = chapter.sections.get(sectionIndex);
                if (section == null) {
                    continue;
                }
                global += 1;
                SectionSelectorRef ref = new SectionSelectorRef();
                ref.chapter = chapter;
                ref.section = section;
                ref.chapterIndex = chapterIndex;
                ref.sectionIndex = sectionIndex;
                ref.globalIndex = global;
                ref.key = chapterIndex + ":" + sectionIndex;
                refs.add(ref);
            }
        }
        if (refs.isEmpty()) {
            return new ArrayList<>();
        }

        Set<String> selectedKeys = new LinkedHashSet<>();
        Map<String, Set<Integer>> selectedLeafIndexesByKey = new LinkedHashMap<>();
        List<String> titleTokens = new ArrayList<>();
        for (String rawToken : selector.split(SELECTOR_TOKEN_SPLIT_REGEX)) {
            String token = normalize(rawToken);
            if (token.isBlank()) {
                continue;
            }
            Matcher rangeMatcher = RANGE_PATTERN.matcher(token);
            if (rangeMatcher.matches()) {
                int start = Integer.parseInt(rangeMatcher.group(1));
                int end = Integer.parseInt(rangeMatcher.group(2));
                int low = Math.min(start, end);
                int high = Math.max(start, end);
                for (int idx = low; idx <= high; idx++) {
                    addSectionSelectorIndex(selectedKeys, refs, idx);
                }
                continue;
            }
            Matcher leafKeyMatcher = CHAPTER_SECTION_LEAF_KEY_PATTERN.matcher(token);
            if (leafKeyMatcher.matches()) {
                int chapterIndex = Integer.parseInt(leafKeyMatcher.group(1));
                int sectionIndex = Integer.parseInt(leafKeyMatcher.group(2));
                int leafIndex = Integer.parseInt(leafKeyMatcher.group(3));
                String refKey = (chapterIndex - 1) + ":" + (sectionIndex - 1);
                selectedKeys.add(refKey);
                selectedLeafIndexesByKey.computeIfAbsent(refKey, ignored -> new LinkedHashSet<>()).add(Math.max(1, leafIndex));
                continue;
            }
            Matcher keyMatcher = CHAPTER_SECTION_KEY_PATTERN.matcher(token);
            if (keyMatcher.matches()) {
                int chapterIndex = Integer.parseInt(keyMatcher.group(1));
                int sectionIndex = Integer.parseInt(keyMatcher.group(2));
                selectedKeys.add((chapterIndex - 1) + ":" + (sectionIndex - 1));
                continue;
            }
            Matcher leafDotMatcher = CHAPTER_SECTION_LEAF_DOT_PATTERN.matcher(token);
            if (leafDotMatcher.matches()) {
                int chapterIndex = Integer.parseInt(leafDotMatcher.group(1));
                int sectionIndex = Integer.parseInt(leafDotMatcher.group(2));
                int leafIndex = Integer.parseInt(leafDotMatcher.group(3));
                String refKey = (chapterIndex - 1) + ":" + (sectionIndex - 1);
                selectedKeys.add(refKey);
                selectedLeafIndexesByKey.computeIfAbsent(refKey, ignored -> new LinkedHashSet<>()).add(Math.max(1, leafIndex));
                continue;
            }
            Matcher dotMatcher = CHAPTER_SECTION_DOT_PATTERN.matcher(token);
            if (dotMatcher.matches()) {
                int chapterIndex = Integer.parseInt(dotMatcher.group(1));
                int sectionIndex = Integer.parseInt(dotMatcher.group(2));
                selectedKeys.add((chapterIndex - 1) + ":" + (sectionIndex - 1));
                continue;
            }
            if (token.chars().allMatch(Character::isDigit)) {
                addSectionSelectorIndex(selectedKeys, refs, Integer.parseInt(token));
                continue;
            }
            titleTokens.add(token.toLowerCase(Locale.ROOT));
        }

        if (!titleTokens.isEmpty()) {
            for (SectionSelectorRef ref : refs) {
                String sectionTitle = normalize(ref.section.title).toLowerCase(Locale.ROOT);
                String chapterTitle = normalize(ref.chapter.title).toLowerCase(Locale.ROOT);
                for (String token : titleTokens) {
                    if (sectionTitle.contains(token) || chapterTitle.contains(token)) {
                        selectedKeys.add(ref.key);
                        break;
                    }
                }
            }
        }

        if (selectedKeys.isEmpty()) {
            return new ArrayList<>();
        }

        Map<Integer, Chapter> selectedByChapter = new LinkedHashMap<>();
        for (SectionSelectorRef ref : refs) {
            if (!selectedKeys.contains(ref.key)) {
                continue;
            }
            Chapter chapter = selectedByChapter.computeIfAbsent(ref.chapterIndex, idx -> {
                Chapter shell = new Chapter(ref.chapter.title);
                shell.selector = ref.chapter.selector;
                shell.startPage = ref.chapter.startPage;
                shell.endPage = ref.chapter.endPage;
                shell.sections = new ArrayList<>();
                return shell;
            });
            Set<Integer> leafIndexes = selectedLeafIndexesByKey.get(ref.key);
            if (leafIndexes == null || leafIndexes.isEmpty()) {
                chapter.sections.add(ref.section);
                continue;
            }
            chapter.sections.addAll(splitSectionByLeafIndexes(ref.section, leafIndexes, ref.chapter.startPage, leafBySelector));
        }
        return new ArrayList<>(selectedByChapter.values());
    }

    private void addSectionSelectorIndex(Set<String> selectedKeys, List<SectionSelectorRef> refs, int humanIndex) {
        int zeroBasedIndex = humanIndex - 1;
        if (zeroBasedIndex < 0 || zeroBasedIndex >= refs.size()) {
            return;
        }
        SectionSelectorRef ref = refs.get(zeroBasedIndex);
        if (ref == null || ref.key == null || ref.key.isBlank()) {
            return;
        }
        selectedKeys.add(ref.key);
    }

    private String canonicalizeLeafSelector(String rawToken) {
        String token = normalize(rawToken);
        if (token.isBlank()) {
            return "";
        }
        Matcher keyMatcher = CHAPTER_SECTION_LEAF_KEY_PATTERN.matcher(token);
        if (keyMatcher.matches()) {
            return buildLeafSelectorToken(
                    Integer.parseInt(keyMatcher.group(1)),
                    Integer.parseInt(keyMatcher.group(2)),
                    Integer.parseInt(keyMatcher.group(3))
            );
        }
        Matcher dotMatcher = CHAPTER_SECTION_LEAF_DOT_PATTERN.matcher(token);
        if (dotMatcher.matches()) {
            return buildLeafSelectorToken(
                    Integer.parseInt(dotMatcher.group(1)),
                    Integer.parseInt(dotMatcher.group(2)),
                    Integer.parseInt(dotMatcher.group(3))
            );
        }
        return "";
    }

    private String buildLeafSelectorToken(int chapterIndex, int sectionIndex, int leafIndex) {
        int chapter = Math.max(1, chapterIndex);
        int section = Math.max(1, sectionIndex);
        int leaf = Math.max(1, leafIndex);
        return "c" + chapter + "s" + section + "t" + leaf;
    }

    private List<String> parseOrderedLeafSelectors(String sectionSelector) {
        List<String> ordered = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        String selector = normalize(sectionSelector);
        if (selector.isBlank()) {
            return ordered;
        }
        for (String rawToken : selector.split(SELECTOR_TOKEN_SPLIT_REGEX)) {
            String token = normalize(rawToken);
            if (token.isBlank()) {
                continue;
            }
            String canonical = canonicalizeLeafSelector(token);
            if (!canonical.isBlank()) {
                addOrderedLeafSelector(ordered, seen, canonical);
                continue;
            }

            Matcher keyRangeMatcher = CHAPTER_SECTION_LEAF_KEY_RANGE_PATTERN.matcher(token);
            if (keyRangeMatcher.matches()) {
                addOrderedLeafRange(
                        ordered,
                        seen,
                        Integer.parseInt(keyRangeMatcher.group(1)),
                        Integer.parseInt(keyRangeMatcher.group(2)),
                        Integer.parseInt(keyRangeMatcher.group(3)),
                        Integer.parseInt(keyRangeMatcher.group(4)),
                        Integer.parseInt(keyRangeMatcher.group(5)),
                        Integer.parseInt(keyRangeMatcher.group(6))
                );
                continue;
            }
            Matcher dotRangeMatcher = CHAPTER_SECTION_LEAF_DOT_RANGE_PATTERN.matcher(token);
            if (dotRangeMatcher.matches()) {
                addOrderedLeafRange(
                        ordered,
                        seen,
                        Integer.parseInt(dotRangeMatcher.group(1)),
                        Integer.parseInt(dotRangeMatcher.group(2)),
                        Integer.parseInt(dotRangeMatcher.group(3)),
                        Integer.parseInt(dotRangeMatcher.group(4)),
                        Integer.parseInt(dotRangeMatcher.group(5)),
                        Integer.parseInt(dotRangeMatcher.group(6))
                );
            }
        }
        return ordered;
    }

    private void addOrderedLeafRange(
            List<String> ordered,
            Set<String> seen,
            int startChapter,
            int startSection,
            int startLeaf,
            int endChapter,
            int endSection,
            int endLeaf
    ) {
        if (startChapter != endChapter || startSection != endSection) {
            addOrderedLeafSelector(ordered, seen, buildLeafSelectorToken(startChapter, startSection, startLeaf));
            addOrderedLeafSelector(ordered, seen, buildLeafSelectorToken(endChapter, endSection, endLeaf));
            return;
        }
        int low = Math.min(startLeaf, endLeaf);
        int high = Math.max(startLeaf, endLeaf);
        for (int leaf = low; leaf <= high; leaf++) {
            addOrderedLeafSelector(ordered, seen, buildLeafSelectorToken(startChapter, startSection, leaf));
        }
    }

    private void addOrderedLeafSelector(List<String> ordered, Set<String> seen, String selector) {
        if (selector == null || selector.isBlank()) {
            return;
        }
        if (!seen.add(selector)) {
            return;
        }
        ordered.add(selector);
    }

    private ContinuousLeafSelection resolveContinuousLeafSelection(
            String sectionSelector,
            List<Map<String, Object>> leafSections
    ) {
        String selector = normalize(sectionSelector);
        if (selector.isBlank() || leafSections == null || leafSections.isEmpty()) {
            return null;
        }

        List<OrderedLeafRef> orderedLeafs = buildOrderedLeafRefs(leafSections);
        if (orderedLeafs.isEmpty()) {
            return null;
        }
        Map<String, Integer> orderBySelector = new LinkedHashMap<>();
        for (int index = 0; index < orderedLeafs.size(); index++) {
            OrderedLeafRef leaf = orderedLeafs.get(index);
            if (leaf == null || leaf.selector == null || leaf.selector.isBlank()) {
                continue;
            }
            orderBySelector.putIfAbsent(leaf.selector, index);
        }
        if (orderBySelector.isEmpty()) {
            return null;
        }

        int minIndex = Integer.MAX_VALUE;
        int maxIndex = Integer.MIN_VALUE;
        boolean matchedLeafToken = false;
        for (String rawToken : selector.split(SELECTOR_TOKEN_SPLIT_REGEX)) {
            String token = normalize(rawToken);
            if (token.isBlank()) {
                continue;
            }
            String canonicalLeaf = canonicalizeLeafSelector(token);
            if (!canonicalLeaf.isBlank()) {
                Integer order = orderBySelector.get(canonicalLeaf);
                if (order != null) {
                    minIndex = Math.min(minIndex, order);
                    maxIndex = Math.max(maxIndex, order);
                    matchedLeafToken = true;
                }
                continue;
            }

            Matcher keyRangeMatcher = CHAPTER_SECTION_LEAF_KEY_RANGE_PATTERN.matcher(token);
            if (keyRangeMatcher.matches()) {
                String startLeafSelector = buildLeafSelectorToken(
                        Integer.parseInt(keyRangeMatcher.group(1)),
                        Integer.parseInt(keyRangeMatcher.group(2)),
                        Integer.parseInt(keyRangeMatcher.group(3))
                );
                String endLeafSelector = buildLeafSelectorToken(
                        Integer.parseInt(keyRangeMatcher.group(4)),
                        Integer.parseInt(keyRangeMatcher.group(5)),
                        Integer.parseInt(keyRangeMatcher.group(6))
                );
                Integer startOrder = orderBySelector.get(startLeafSelector);
                Integer endOrder = orderBySelector.get(endLeafSelector);
                if (startOrder != null) {
                    minIndex = Math.min(minIndex, startOrder);
                    maxIndex = Math.max(maxIndex, startOrder);
                    matchedLeafToken = true;
                }
                if (endOrder != null) {
                    minIndex = Math.min(minIndex, endOrder);
                    maxIndex = Math.max(maxIndex, endOrder);
                    matchedLeafToken = true;
                }
                continue;
            }

            Matcher dotRangeMatcher = CHAPTER_SECTION_LEAF_DOT_RANGE_PATTERN.matcher(token);
            if (dotRangeMatcher.matches()) {
                String startLeafSelector = buildLeafSelectorToken(
                        Integer.parseInt(dotRangeMatcher.group(1)),
                        Integer.parseInt(dotRangeMatcher.group(2)),
                        Integer.parseInt(dotRangeMatcher.group(3))
                );
                String endLeafSelector = buildLeafSelectorToken(
                        Integer.parseInt(dotRangeMatcher.group(4)),
                        Integer.parseInt(dotRangeMatcher.group(5)),
                        Integer.parseInt(dotRangeMatcher.group(6))
                );
                Integer startOrder = orderBySelector.get(startLeafSelector);
                Integer endOrder = orderBySelector.get(endLeafSelector);
                if (startOrder != null) {
                    minIndex = Math.min(minIndex, startOrder);
                    maxIndex = Math.max(maxIndex, startOrder);
                    matchedLeafToken = true;
                }
                if (endOrder != null) {
                    minIndex = Math.min(minIndex, endOrder);
                    maxIndex = Math.max(maxIndex, endOrder);
                    matchedLeafToken = true;
                }
            }
        }
        if (!matchedLeafToken || minIndex == Integer.MAX_VALUE || maxIndex == Integer.MIN_VALUE) {
            return null;
        }

        OrderedLeafRef startLeaf = orderedLeafs.get(Math.max(0, minIndex));
        OrderedLeafRef endLeaf = orderedLeafs.get(Math.max(0, maxIndex));
        if (startLeaf == null || endLeaf == null || startLeaf.startPage <= 0) {
            return null;
        }

        ContinuousLeafSelection selection = new ContinuousLeafSelection();
        selection.startSelector = firstNonBlank(startLeaf.selector, "");
        selection.endSelector = firstNonBlank(endLeaf.selector, selection.startSelector);
        selection.startTitle = firstNonBlank(startLeaf.title, "");
        selection.endTitle = firstNonBlank(endLeaf.title, selection.startTitle);
        selection.startOutlineIndex = firstNonBlank(startLeaf.outlineIndex, selection.startSelector);
        selection.endOutlineIndex = firstNonBlank(endLeaf.outlineIndex, selection.endSelector);
        selection.startPage = startLeaf.startPage;
        selection.endPage = endLeaf.endPage >= startLeaf.startPage ? endLeaf.endPage : startLeaf.startPage;
        selection.leafCount = Math.max(1, maxIndex - minIndex + 1);
        selection.trimStartAnchorTitle = selection.startTitle;
        if (maxIndex + 1 < orderedLeafs.size()) {
            OrderedLeafRef nextLeaf = orderedLeafs.get(maxIndex + 1);
            selection.trimEndBeforeTitle = nextLeaf != null ? firstNonBlank(nextLeaf.title, "") : "";
        }
        return selection;
    }

    private List<OrderedLeafRef> buildOrderedLeafRefs(List<Map<String, Object>> leafSections) {
        List<Map<String, Object>> sortedLeafSections = new ArrayList<>();
        for (Map<String, Object> leaf : leafSections) {
            if (leaf != null) {
                sortedLeafSections.add(leaf);
            }
        }
        sortedLeafSections.sort(
                Comparator.comparingInt((Map<String, Object> leaf) -> readObjectInt(leaf.get("flatIndex"), Integer.MAX_VALUE))
                        .thenComparingInt(leaf -> readObjectInt(leaf.get("startPage"), Integer.MAX_VALUE))
                        .thenComparing(leaf -> normalize(String.valueOf(leaf.get("sectionSelector"))))
        );

        List<OrderedLeafRef> orderedLeafs = new ArrayList<>();
        for (Map<String, Object> leaf : sortedLeafSections) {
            OrderedLeafRef orderedLeaf = toOrderedLeafRef(leaf);
            if (orderedLeaf != null) {
                orderedLeafs.add(orderedLeaf);
            }
        }
        return orderedLeafs;
    }

    private OrderedLeafRef toOrderedLeafRef(Map<String, Object> leaf) {
        if (leaf == null || leaf.isEmpty()) {
            return null;
        }
        String rawSelector = normalize(String.valueOf(leaf.get("sectionSelector")));
        String selector = canonicalizeLeafSelector(rawSelector);
        if (selector.isBlank()) {
            int chapterIndex = readObjectInt(leaf.get("chapterIndex"), -1);
            int sectionIndex = readObjectInt(leaf.get("sectionIndex"), -1);
            int subSectionIndex = readObjectInt(leaf.get("subSectionIndex"), -1);
            if (chapterIndex > 0 && sectionIndex > 0 && subSectionIndex > 0) {
                selector = buildLeafSelectorToken(chapterIndex, sectionIndex, subSectionIndex);
            }
        }
        if (selector.isBlank()) {
            return null;
        }
        OrderedLeafRef orderedLeaf = new OrderedLeafRef();
        orderedLeaf.selector = selector;
        orderedLeaf.title = normalize(String.valueOf(leaf.get("title")));
        orderedLeaf.outlineIndex = normalize(String.valueOf(leaf.get("outlineIndex")));
        orderedLeaf.startPage = readObjectInt(leaf.get("startPage"), -1);
        orderedLeaf.endPage = readObjectInt(leaf.get("endPage"), orderedLeaf.startPage);
        if (orderedLeaf.endPage < orderedLeaf.startPage) {
            orderedLeaf.endPage = orderedLeaf.startPage;
        }
        return orderedLeaf;
    }

    private BookData buildContinuousLeafRangeBookData(BookData source, ContinuousLeafSelection selection) {
        if (source == null || selection == null || selection.startPage <= 0) {
            return source != null ? source : new BookData();
        }
        BookData selected = new BookData();
        selected.title = source.title;
        selected.pageMapStrategy = source.pageMapStrategy;
        selected.detectedPageOffset = source.detectedPageOffset;
        selected.appliedPageOffset = source.appliedPageOffset;
        selected.leafSections = new ArrayList<>();

        String rangeSelector = buildContinuousLeafRangeSelector(selection);
        String rangeTitle = buildContinuousLeafRangeTitle(selection);
        Chapter chapter = new Chapter(rangeTitle);
        chapter.selector = rangeSelector;
        chapter.startPage = selection.startPage;
        chapter.endPage = selection.endPage;

        Section section = new Section(rangeTitle);
        section.selector = rangeSelector;
        section.startPage = selection.startPage;
        section.endPage = selection.endPage;
        section.trimStartAnchorTitle = firstNonBlank(selection.trimStartAnchorTitle, "");
        section.trimEndBeforeTitle = firstNonBlank(selection.trimEndBeforeTitle, "");
        chapter.sections.add(section);

        selected.chapters = new ArrayList<>();
        selected.chapters.add(chapter);
        return selected;
    }

    private String buildContinuousLeafRangeSelector(ContinuousLeafSelection selection) {
        if (selection == null) {
            return "";
        }
        String startSelector = firstNonBlank(selection.startSelector, "");
        String endSelector = firstNonBlank(selection.endSelector, startSelector);
        if (startSelector.isBlank()) {
            return "";
        }
        return startSelector.equals(endSelector) ? startSelector : startSelector + "-" + endSelector;
    }

    private String buildContinuousLeafRangeTitle(ContinuousLeafSelection selection) {
        if (selection == null) {
            return "章节区间";
        }
        String startLabel = firstNonBlank(selection.startOutlineIndex, selection.startSelector);
        String endLabel = firstNonBlank(selection.endOutlineIndex, selection.endSelector);
        if (startLabel.isBlank() && endLabel.isBlank()) {
            return "章节区间";
        }
        if (startLabel.equals(endLabel)) {
            return "章节 " + startLabel;
        }
        return "章节区间 " + startLabel + "-" + endLabel;
    }

    private String resolveLeafTitle(
            String selector,
            Map<String, String> leafTitleBySelector,
            Map<String, Map<String, Object>> leafBySelector
    ) {
        if (selector == null || selector.isBlank()) {
            return "";
        }
        String title = normalize(leafTitleBySelector.get(selector));
        if (!title.isBlank()) {
            return title;
        }
        Map<String, Object> leaf = leafBySelector.get(selector);
        if (leaf == null) {
            return "";
        }
        return normalize(String.valueOf(leaf.get("title")));
    }

    private String findNextLeafTitle(
            String selector,
            List<String> orderedLeafSelectors,
            Map<String, String> leafTitleBySelector,
            Map<String, Map<String, Object>> leafBySelector
    ) {
        if (selector == null || selector.isBlank() || orderedLeafSelectors == null || orderedLeafSelectors.isEmpty()) {
            return "";
        }
        int currentIndex = orderedLeafSelectors.indexOf(selector);
        if (currentIndex < 0 || currentIndex + 1 >= orderedLeafSelectors.size()) {
            return "";
        }
        String nextSelector = orderedLeafSelectors.get(currentIndex + 1);
        return resolveLeafTitle(nextSelector, leafTitleBySelector, leafBySelector);
    }

    private List<Section> splitSectionByLeafIndexes(
            Section sourceSection,
            Set<Integer> leafIndexes,
            int chapterStartPage,
            Map<String, Map<String, Object>> leafBySelector
    ) {
        if (sourceSection == null || leafIndexes == null || leafIndexes.isEmpty()) {
            return List.of();
        }
        List<Integer> sortedLeafs = new ArrayList<>(leafIndexes);
        sortedLeafs.sort(Integer::compareTo);
        int baseStart = sourceSection.startPage > 0 ? sourceSection.startPage : Math.max(1, chapterStartPage);
        int baseEnd = sourceSection.endPage > 0 ? Math.max(baseStart, sourceSection.endPage) : baseStart;
        List<Section> narrowed = new ArrayList<>();
        String baseSelector = firstNonBlank(sourceSection.selector, "c1s1");
        for (Integer leafIndex : sortedLeafs) {
            if (leafIndex == null || leafIndex <= 0) {
                continue;
            }
            Section copy = cloneSectionForSelector(sourceSection);
            String leafSelector = baseSelector + "t" + leafIndex;
            copy.selector = leafSelector;
            boolean appliedLeafRange = false;
            Map<String, Object> leaf = leafBySelector != null ? leafBySelector.get(leafSelector) : null;
            if (leaf != null) {
                int leafStartPage = readObjectInt(leaf.get("startPage"), -1);
                int leafEndPage = readObjectInt(leaf.get("endPage"), leafStartPage);
                if (leafStartPage > 0) {
                    copy.startPage = leafStartPage;
                    copy.endPage = leafEndPage >= leafStartPage ? leafEndPage : leafStartPage;
                    appliedLeafRange = true;
                }
                String leafTitle = normalize(String.valueOf(leaf.get("title")));
                if (!leafTitle.isBlank()) {
                    copy.title = leafTitle;
                }
            }
            if (!appliedLeafRange) {
                int pageNo = baseStart + leafIndex - 1;
                if (pageNo < baseStart || pageNo > baseEnd) {
                    continue;
                }
                copy.startPage = pageNo;
                copy.endPage = pageNo;
                copy.title = firstNonBlank(sourceSection.title, "Section") + " - page " + pageNo;
            }
            narrowed.add(copy);
        }
        if (narrowed.isEmpty()) {
            narrowed.add(sourceSection);
        }
        return narrowed;
    }

    private Section cloneSectionForSelector(Section sourceSection) {
        Section copy = new Section(sourceSection != null ? sourceSection.title : "");
        if (sourceSection == null) {
            return copy;
        }
        copy.selector = sourceSection.selector;
        copy.startPage = sourceSection.startPage;
        copy.endPage = sourceSection.endPage;
        copy.markdownBody = sourceSection.markdownBody;
        copy.paragraphs = sourceSection.paragraphs != null ? new ArrayList<>(sourceSection.paragraphs) : new ArrayList<>();
        copy.images = sourceSection.images != null ? new ArrayList<>(sourceSection.images) : new ArrayList<>();
        copy.tables = sourceSection.tables != null ? new ArrayList<>(sourceSection.tables) : new ArrayList<>();
        copy.blocks = sourceSection.blocks != null ? new ArrayList<>(sourceSection.blocks) : new ArrayList<>();
        return copy;
    }

    private void extractPdfContentForSelections(
            String taskId,
            Path source,
            PDDocument pdf,
            BookData data,
            Path outputRoot,
            Path imageAssetsRoot
    ) throws Exception {
        if (pdf == null || data == null || data.chapters == null) {
            return;
        }
        PDFTextStripper pageStripper = new PDFTextStripper();
        pageStripper.setSortByPosition(true);
        int totalPages = pdf.getNumberOfPages();
        int totalSelectedSections = 0;
        for (Chapter chapter : data.chapters) {
            if (chapter == null || chapter.sections == null) {
                continue;
            }
            totalSelectedSections += chapter.sections.size();
        }
        int sectionCursor = 0;

        for (Chapter chapter : data.chapters) {
            if (chapter == null || chapter.sections == null) {
                continue;
            }
            for (Section section : chapter.sections) {
                if (section == null) {
                    continue;
                }
                sectionCursor += 1;
                int startPage = section.startPage > 0 ? section.startPage : chapter.startPage;
                int endPage = section.endPage > 0 ? section.endPage : chapter.endPage;
                startPage = clampPage(startPage > 0 ? startPage : 1, totalPages);
                endPage = clampPage(endPage >= startPage ? endPage : startPage, totalPages);
                section.startPage = startPage;
                section.endPage = endPage;
                logger.info(
                        "[{}] Book PDF section extract start {}/{}, section={}, pages={}~{}",
                        firstNonBlank(taskId, "book_pdf_extract"),
                        sectionCursor,
                        Math.max(1, totalSelectedSections),
                        firstNonBlank(firstNonBlank(section.selector, section.title), "section"),
                        startPage,
                        endPage
                );
                section.paragraphs = new ArrayList<>();
                section.images = new ArrayList<>();
                section.tables = new ArrayList<>();
                section.blocks = new ArrayList<>();
                section.markdownBody = "";

                boolean extractedByGrpc = tryExtractPdfSectionWithGrpc(
                        taskId,
                        source,
                        outputRoot,
                        imageAssetsRoot,
                        section,
                        startPage,
                        endPage
                );
                if (extractedByGrpc) {
                    continue;
                }

                for (int pageNo = startPage; pageNo <= endPage; pageNo++) {
                    String pageText = extractPdfPageText(pdf, pageStripper, pageNo);
                    String[] lines = pageText.split("\\R");
                    List<String> paragraphs = parsePdfPageParagraphs(lines);
                    List<ExtractedPdfImage> images = extractPdfImagesWithPosition(
                            pdf.getPage(pageNo - 1),
                            pageNo,
                            outputRoot,
                            imageAssetsRoot
                    );

                    List<SectionBlock> pageBlocks = new ArrayList<>();
                    for (String paragraph : paragraphs) {
                        String text = normalize(paragraph);
                        if (text.isBlank()) {
                            continue;
                        }
                        SectionBlock block = new SectionBlock();
                        block.type = SectionBlock.BlockType.PARAGRAPH;
                        block.text = text;
                        block.pageNo = pageNo;
                        pageBlocks.add(block);
                    }
                    insertImageBlocks(pageBlocks, images, pageNo);
                    section.blocks.addAll(pageBlocks);
                    for (SectionBlock block : pageBlocks) {
                        if (block.type == SectionBlock.BlockType.PARAGRAPH && block.text != null && !block.text.isBlank()) {
                            section.paragraphs.add(block.text);
                        } else if (block.type == SectionBlock.BlockType.IMAGE && block.imagePath != null && !block.imagePath.isBlank()) {
                            section.images.add(block.imagePath);
                        }
                    }

                    Section tableCollector = new Section("table_collector");
                    extractTablesFromLines(lines, tableCollector);
                    for (List<List<String>> table : tableCollector.tables) {
                        section.tables.add(table);
                        SectionBlock tableBlock = new SectionBlock();
                        tableBlock.type = SectionBlock.BlockType.TABLE;
                        tableBlock.table = table;
                        tableBlock.pageNo = pageNo;
                        section.blocks.add(tableBlock);
                    }
                }
            }
        }
    }

    private boolean tryExtractPdfSectionWithGrpc(
            String taskId,
            Path source,
            Path outputRoot,
            Path imageAssetsRoot,
            Section section,
            int startPage,
            int endPage
    ) {
        String strategy = normalizePdfExtractorStrategy();
        if ("pdfbox".equals(strategy)) {
            return false;
        }
        if (grpcClient == null) {
            logger.debug("Book PDF grpc extractor unavailable: grpcClient not ready");
            return false;
        }
        try {
            String safeTaskId = firstNonBlank(taskId, "book_pdf_extract");
            String sectionId = firstNonBlank(section.selector, "p" + startPage + "_" + endPage);
            boolean preferMineru = "mineru".equals(strategy) || preferMineruExtractor;
            int pageCount = Math.max(1, endPage - startPage + 1);
            long timeoutByPagesLong = (long) pageCount * (long) Math.max(1, bookPdfExtractorTimeoutPerPageSec);
            int timeoutByPages = (int) Math.max(30L, Math.min(Integer.MAX_VALUE, timeoutByPagesLong));
            int timeoutSec = Math.max(Math.max(30, bookPdfExtractorTimeoutSec), timeoutByPages);
            PythonGrpcClient.ExtractBookPdfResult grpcResult = grpcClient.extractBookPdf(
                    safeTaskId,
                    source != null ? source.toString() : "",
                    outputRoot != null ? outputRoot.toString() : "",
                    startPage,
                    endPage,
                    imageAssetsRoot != null ? imageAssetsRoot.toString() : "",
                    outputRoot != null ? outputRoot.toString() : "",
                    sectionId,
                    preferMineru,
                    timeoutSec
            );
            if (grpcResult == null || !grpcResult.success) {
                String errorMessage = grpcResult != null ? grpcResult.errorMsg : "empty grpc result";
                logger.info(
                        "Book PDF grpc extractor fallback to PDFBox, section={}, pages={}~{}, strategy={}, err={}",
                        sectionId,
                        startPage,
                        endPage,
                        strategy,
                        firstNonBlank(errorMessage, "unknown")
                );
                return false;
            }
            String markdown = grpcResult.markdown != null ? grpcResult.markdown.trim() : "";
            if (markdown.isBlank()) {
                logger.info(
                        "Book PDF grpc extractor returned empty markdown, fallback to PDFBox, section={}, pages={}~{}",
                        sectionId,
                        startPage,
                        endPage
                );
                return false;
            }

            String clippedMarkdown = trimMarkdownByLeafAnchorWindow(
                    markdown,
                    section != null ? section.trimStartAnchorTitle : "",
                    section != null ? section.trimEndBeforeTitle : ""
            );
            List<String> images = normalizeGrpcImagePaths(grpcResult.imagePaths, clippedMarkdown, outputRoot);
            section.markdownBody = clippedMarkdown;
            section.images = images;
            section.paragraphs = new ArrayList<>();
            String metadataText = stripMarkdownForMetadata(clippedMarkdown);
            if (!metadataText.isBlank()) {
                section.paragraphs.add(metadataText);
            }
            section.tables = new ArrayList<>();
            section.blocks = new ArrayList<>();
            logger.info(
                    "Book PDF section extracted via grpc, section={}, pages={}~{}, extractor={}, images={}, tables={}, code={}, formula={}",
                    sectionId,
                    startPage,
                    endPage,
                    firstNonBlank(grpcResult.extractor, "python"),
                    grpcResult.imageCount,
                    grpcResult.tableCount,
                    grpcResult.codeBlockCount,
                    grpcResult.formulaBlockCount
            );
            return true;
        } catch (Exception error) {
            logger.warn(
                    "Book PDF grpc extractor failed, fallback to PDFBox, section={}, pages={}~{}, err={}",
                    section != null ? section.selector : "",
                    startPage,
                    endPage,
                    error.getMessage()
            );
            return false;
        }
    }

    private String trimMarkdownByLeafAnchorWindow(String markdown, String startAnchorTitle, String endAnchorTitleExclusive) {
        String original = markdown != null ? markdown.trim() : "";
        if (original.isBlank()) {
            return original;
        }
        String trimmed = original;
        if (startAnchorTitle != null && !startAnchorTitle.isBlank()) {
            trimmed = trimMarkdownFromAnchorLine(trimmed, startAnchorTitle);
        }
        if (endAnchorTitleExclusive != null && !endAnchorTitleExclusive.isBlank()) {
            trimmed = trimMarkdownBeforeAnchorLine(trimmed, endAnchorTitleExclusive);
        }
        return trimmed.isBlank() ? original : trimmed;
    }

    private String trimMarkdownFromAnchorLine(String markdown, String anchorTitle) {
        List<String> lines = splitMarkdownLines(markdown);
        int anchorLine = findMarkdownAnchorLine(lines, anchorTitle);
        if (anchorLine <= 0 || anchorLine >= lines.size()) {
            return markdown;
        }
        String trimmed = String.join("\n", lines.subList(anchorLine, lines.size())).trim();
        return trimmed.isBlank() ? markdown : trimmed;
    }

    private String trimMarkdownBeforeAnchorLine(String markdown, String anchorTitle) {
        List<String> lines = splitMarkdownLines(markdown);
        int anchorLine = findMarkdownAnchorLine(lines, anchorTitle);
        if (anchorLine <= 0 || anchorLine > lines.size()) {
            return markdown;
        }
        String trimmed = String.join("\n", lines.subList(0, anchorLine)).trim();
        return trimmed.isBlank() ? markdown : trimmed;
    }

    private List<String> splitMarkdownLines(String markdown) {
        String normalized = markdown != null ? markdown.replace("\r\n", "\n").replace('\r', '\n') : "";
        return new ArrayList<>(List.of(normalized.split("\n", -1)));
    }

    private int findMarkdownAnchorLine(List<String> lines, String anchorTitle) {
        if (lines == null || lines.isEmpty()) {
            return -1;
        }
        String normalizedAnchor = normalizeAnchorText(anchorTitle);
        if (normalizedAnchor.isBlank()) {
            return -1;
        }
        String compactAnchor = normalizedAnchor.replace(" ", "");
        int fallback = -1;
        for (int i = 0; i < lines.size(); i++) {
            String rawLine = lines.get(i);
            String normalizedLine = normalizeAnchorText(rawLine);
            if (normalizedLine.isBlank()) {
                continue;
            }
            String compactLine = normalizedLine.replace(" ", "");
            boolean matched = normalizedLine.contains(normalizedAnchor) || compactLine.contains(compactAnchor);
            if (!matched) {
                continue;
            }
            if (rawLine != null && rawLine.stripLeading().startsWith("#")) {
                return i;
            }
            if (fallback < 0) {
                fallback = i;
            }
        }
        return fallback;
    }

    private String normalizeAnchorText(String text) {
        String normalized = normalize(text).toLowerCase(Locale.ROOT);
        if (normalized.isBlank()) {
            return "";
        }
        normalized = normalized.replaceAll("^[#>*\\-+\\d\\s\\.()]+", "");
        normalized = normalized.replace("`", " ");
        normalized = normalized.replaceAll("[\\[\\]_*~|]", " ");
        normalized = normalized.replaceAll("[,;:\\uFF0C\\u3002\\uFF1B\\uFF1A!?\\uFF01\\uFF1F\\u3001]", " ");
        normalized = normalized.replaceAll("\\s+", " ").trim();
        return normalized;
    }

    private String normalizePdfExtractorStrategy() {
        String value = normalize(pdfExtractorStrategy).toLowerCase(Locale.ROOT);
        if ("pdfbox".equals(value) || "mineru".equals(value) || "auto".equals(value)) {
            return value;
        }
        return "auto";
    }

    private List<String> normalizeGrpcImagePaths(List<String> grpcImagePaths, String markdown, Path outputRoot) {
        List<String> markdownImagePaths = collectMarkdownImagePaths(markdown);
        List<String> normalized = new ArrayList<>();
        if (grpcImagePaths != null) {
            for (String rawPath : grpcImagePaths) {
                String mapped = normalizeGrpcImagePath(rawPath, outputRoot);
                if (mapped.isBlank() || normalized.contains(mapped)) {
                    continue;
                }
                normalized.add(mapped);
            }
        }
        if (normalized.isEmpty()) {
            return markdownImagePaths;
        }
        if (markdownImagePaths.isEmpty()) {
            return normalized;
        }
        Set<String> markdownSet = new LinkedHashSet<>(markdownImagePaths);
        List<String> filtered = new ArrayList<>();
        for (String imagePath : normalized) {
            if (markdownSet.contains(normalizeZipPath(imagePath))) {
                filtered.add(imagePath);
            }
        }
        return filtered.isEmpty() ? markdownImagePaths : filtered;
    }

    private String normalizeGrpcImagePath(String rawPath, Path outputRoot) {
        String normalized = normalizeZipPath(rawPath);
        if (normalized.isBlank() || isExternalAssetPath(normalized)) {
            return normalized;
        }
        try {
            Path asPath = Paths.get(normalized);
            if (asPath.isAbsolute()) {
                if (outputRoot != null) {
                    return toRelative(outputRoot, asPath);
                }
                return normalizeZipPath(asPath.toString());
            }
        } catch (InvalidPathException ignored) {
            return normalized;
        }
        return normalized;
    }

    private List<String> collectMarkdownImagePaths(String markdown) {
        List<String> paths = new ArrayList<>();
        if (markdown == null || markdown.isBlank()) {
            return paths;
        }
        Matcher matcher = MARKDOWN_IMAGE_PATTERN.matcher(markdown);
        while (matcher.find()) {
            String path = normalizeZipPath(matcher.group(1));
            if (path.isBlank() || paths.contains(path)) {
                continue;
            }
            paths.add(path);
        }
        return paths;
    }

    private boolean isExternalAssetPath(String value) {
        String path = value != null ? value.trim().toLowerCase(Locale.ROOT) : "";
        return path.startsWith("http://") || path.startsWith("https://") || path.startsWith("data:");
    }

    private String stripMarkdownForMetadata(String markdown) {
        if (markdown == null || markdown.isBlank()) {
            return "";
        }
        String text = markdown
                .replaceAll("!\\[[^\\]]*]\\(([^)]+)\\)", " ")
                .replaceAll("(?m)^\\|.*\\|\\s*$", " ")
                .replaceAll("(?m)^```.*$", " ")
                .replace("`", " ")
                .replace("$$", " ")
                .replaceAll("(?m)^#{1,6}\\s*", " ")
                .replaceAll("\\s+", " ")
                .trim();
        return normalize(text);
    }

    private String extractPdfPageText(PDDocument pdf, PDFTextStripper pageStripper, int pageNo) throws Exception {
        pageStripper.setStartPage(pageNo);
        pageStripper.setEndPage(pageNo);
        return pageStripper.getText(pdf);
    }

    private String extractPdfPageRangeText(
            PDDocument pdf,
            PDFTextStripper pageStripper,
            int startPage,
            int endPage
    ) throws Exception {
        if (pdf == null || pageStripper == null || startPage <= 0 || endPage < startPage) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        int totalPages = pdf.getNumberOfPages();
        for (int pageNo = startPage; pageNo <= endPage && pageNo <= totalPages; pageNo++) {
            String pageText = normalize(extractPdfPageText(pdf, pageStripper, pageNo));
            if (pageText.isBlank()) {
                continue;
            }
            if (builder.length() > 0) {
                builder.append("\n\n");
            }
            builder.append("[Page ").append(pageNo).append("]\n").append(pageText);
        }
        return builder.toString();
    }

    private String extractPdfPrefaceText(PDDocument pdf, PDFTextStripper pageStripper, int totalPages) throws Exception {
        if (pdf == null || pageStripper == null || totalPages <= 0) {
            return "";
        }
        Pattern prefaceHeadingPattern = Pattern.compile(
                "(?i)\\b(preface|foreword|introduction|prologue)\\b|前言|序言|序|导言|引言"
        );
        int maxScanPages = Math.min(totalPages, 12);
        for (int pageNo = 1; pageNo <= maxScanPages; pageNo++) {
            String pageText = normalize(extractPdfPageText(pdf, pageStripper, pageNo));
            if (pageText.isBlank()) {
                continue;
            }
            if (!prefaceHeadingPattern.matcher(pageText).find()) {
                continue;
            }
            return extractPdfPageRangeText(pdf, pageStripper, pageNo, Math.min(totalPages, pageNo + 2));
        }
        return "";
    }

    private String extractPrefaceFromText(String content) {
        String normalized = normalize(content);
        if (normalized.isBlank()) {
            return "";
        }
        Pattern prefacePattern = Pattern.compile(
                "(?is)(preface|foreword|introduction|前言|序言|导言|引言)\\s*[\\r\\n]+(.{0,3000})"
        );
        Matcher matcher = prefacePattern.matcher(normalized);
        if (matcher.find()) {
            return firstNonBlank(matcher.group(0), "");
        }
        return "";
    }

    private List<String> collectTocTitlesFromBookData(BookData data) {
        List<String> titles = new ArrayList<>();
        if (data == null || data.chapters == null) {
            return titles;
        }
        for (Chapter chapter : data.chapters) {
            if (chapter == null) {
                continue;
            }
            String chapterTitle = normalize(chapter.title);
            if (!chapterTitle.isBlank()) {
                titles.add(chapterTitle);
            }
            if (chapter.sections == null) {
                continue;
            }
            for (Section section : chapter.sections) {
                if (section == null) {
                    continue;
                }
                String sectionTitle = normalize(section.title);
                if (!sectionTitle.isBlank()) {
                    titles.add(sectionTitle);
                }
                if (titles.size() >= 40) {
                    return normalizeDistinctTitles(titles);
                }
            }
        }
        return normalizeDistinctTitles(titles);
    }

    private List<String> collectTocTitlesFromTree(List<Map<String, Object>> bookSectionTree) {
        List<String> titles = new ArrayList<>();
        collectTocTitlesFromTreeNodes(bookSectionTree, titles);
        return normalizeDistinctTitles(titles);
    }

    private void collectTocTitlesFromTreeNodes(List<Map<String, Object>> nodes, List<String> titles) {
        if (nodes == null || nodes.isEmpty() || titles == null || titles.size() >= 40) {
            return;
        }
        for (Map<String, Object> node : nodes) {
            if (node == null) {
                continue;
            }
            String title = normalize(stringValue(node.get("title")));
            if (!title.isBlank()) {
                titles.add(title);
                if (titles.size() >= 40) {
                    return;
                }
            }
            Object children = node.get("children");
            if (!(children instanceof List<?> childList)) {
                continue;
            }
            List<Map<String, Object>> safeChildren = new ArrayList<>();
            for (Object child : childList) {
                if (!(child instanceof Map<?, ?> childMap)) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> normalizedChild = (Map<String, Object>) childMap;
                safeChildren.add(normalizedChild);
            }
            collectTocTitlesFromTreeNodes(safeChildren, titles);
            if (titles.size() >= 40) {
                return;
            }
        }
    }

    private List<String> normalizeDistinctTitles(List<String> rawTitles) {
        List<String> normalizedTitles = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        if (rawTitles == null) {
            return normalizedTitles;
        }
        for (String rawTitle : rawTitles) {
            String title = normalize(rawTitle);
            if (title.isBlank()) {
                continue;
            }
            String dedupeKey = title.toLowerCase(Locale.ROOT);
            if (!seen.add(dedupeKey)) {
                continue;
            }
            normalizedTitles.add(title);
            if (normalizedTitles.size() >= 40) {
                break;
            }
        }
        return normalizedTitles;
    }

    private String trimEvidenceText(String rawText, int limit) {
        String normalized = normalize(rawText);
        if (normalized.isBlank()) {
            return "";
        }
        int safeLimit = Math.max(256, limit);
        return normalized.length() <= safeLimit ? normalized : normalized.substring(0, safeLimit);
    }

    private List<String> parsePdfPageParagraphs(String[] lines) {
        List<String> paragraphs = new ArrayList<>();
        if (lines == null || lines.length == 0) {
            return paragraphs;
        }
        StringBuilder current = new StringBuilder();
        for (String rawLine : lines) {
            String line = normalize(rawLine);
            if (line.isBlank()) {
                if (current.length() > 0) {
                    paragraphs.add(current.toString().trim());
                    current.setLength(0);
                }
                continue;
            }
            if (current.length() == 0) {
                current.append(line);
                continue;
            }
            if (current.charAt(current.length() - 1) == '-') {
                current.setLength(current.length() - 1);
                current.append(line);
            } else {
                current.append(' ').append(line);
            }
        }
        if (current.length() > 0) {
            paragraphs.add(current.toString().trim());
        }
        return paragraphs;
    }

    private void insertImageBlocks(List<SectionBlock> pageBlocks, List<ExtractedPdfImage> images, int pageNo) {
        if (images == null || images.isEmpty()) {
            return;
        }
        int paragraphCount = 0;
        for (SectionBlock block : pageBlocks) {
            if (block.type == SectionBlock.BlockType.PARAGRAPH) {
                paragraphCount += 1;
            }
        }

        for (ExtractedPdfImage image : images) {
            if (image == null || image.relativePath == null || image.relativePath.isBlank()) {
                continue;
            }
            int targetAfterParagraph = paragraphCount == 0
                    ? 0
                    : (int) Math.round(image.topRatio * (double) paragraphCount);
            if (targetAfterParagraph < 0) {
                targetAfterParagraph = 0;
            }
            if (targetAfterParagraph > paragraphCount) {
                targetAfterParagraph = paragraphCount;
            }

            int insertAt = pageBlocks.size();
            if (paragraphCount > 0) {
                int seenParagraphs = 0;
                for (int i = 0; i < pageBlocks.size(); i++) {
                    SectionBlock block = pageBlocks.get(i);
                    if (block.type != SectionBlock.BlockType.PARAGRAPH) {
                        continue;
                    }
                    seenParagraphs += 1;
                    if (seenParagraphs >= targetAfterParagraph) {
                        insertAt = i + 1;
                        break;
                    }
                }
                if (targetAfterParagraph == 0) {
                    insertAt = 0;
                }
            }

            SectionBlock imageBlock = new SectionBlock();
            imageBlock.type = SectionBlock.BlockType.IMAGE;
            imageBlock.imagePath = image.relativePath;
            imageBlock.pageNo = pageNo;
            pageBlocks.add(insertAt, imageBlock);
        }
    }

    private List<ExtractedPdfImage> extractPdfImagesWithPosition(
            PDPage page,
            int pageNo,
            Path outputRoot,
            Path imageAssetsRoot
    ) {
        List<ExtractedPdfImage> extracted = new ArrayList<>();
        if (page == null || outputRoot == null || imageAssetsRoot == null) {
            return extracted;
        }
        try {
            List<PdfImagePlacement> placements = collectPdfImagePlacements(page);
            int imageIndex = 0;
            for (PdfImagePlacement placement : placements) {
                if (placement == null || placement.image == null) {
                    continue;
                }
                var buffered = placement.image.getImage();
                if (buffered == null) {
                    continue;
                }
                if (buffered.getWidth() < 24 || buffered.getHeight() < 24) {
                    continue;
                }
                imageIndex += 1;
                String fileName = String.format(Locale.ROOT, "pdf-page-%03d-img-%03d.png", pageNo, imageIndex);
                Path target = ensureUniqueFile(imageAssetsRoot.resolve(fileName));
                ImageIO.write(buffered, "png", target.toFile());
                String relativePath = toRelative(outputRoot, target);
                extracted.add(new ExtractedPdfImage(relativePath, placement.topRatio));
            }
        } catch (Exception error) {
            logger.warn("Extract PDF images failed, pageNo={}, err={}", pageNo, error.getMessage());
        }
        return extracted;
    }

    private List<PdfImagePlacement> collectPdfImagePlacements(PDPage page) {
        List<PdfImagePlacement> placements = new ArrayList<>();
        if (page == null || page.getResources() == null) {
            return placements;
        }
        PDRectangle mediaBox = page.getMediaBox();
        float pageHeight = mediaBox != null ? mediaBox.getHeight() : 0f;
        PDFStreamEngine engine = new PDFStreamEngine() {
            @Override
            protected void processOperator(Operator operator, List<COSBase> operands) throws IOException {
                String name = operator != null ? operator.getName() : "";
                if ("Do".equals(name) && operands != null && !operands.isEmpty() && operands.get(0) instanceof COSName) {
                    COSName objectName = (COSName) operands.get(0);
                    PDXObject xObject = getResources().getXObject(objectName);
                    if (xObject instanceof PDImageXObject) {
                        Matrix ctm = getGraphicsState() != null
                                ? getGraphicsState().getCurrentTransformationMatrix()
                                : null;
                        float topRatio = 0.5f;
                        if (ctm != null && pageHeight > 0f) {
                            float topY = ctm.getTranslateY() + Math.abs(ctm.getScalingFactorY());
                            float ratio = 1f - (topY / pageHeight);
                            if (ratio < 0f) {
                                ratio = 0f;
                            } else if (ratio > 1f) {
                                ratio = 1f;
                            }
                            topRatio = ratio;
                        }
                        placements.add(new PdfImagePlacement((PDImageXObject) xObject, topRatio));
                        return;
                    }
                }
                super.processOperator(operator, operands);
            }
        };
        try {
            engine.processPage(page);
        } catch (Exception error) {
            logger.debug("collectPdfImagePlacements failed: {}", error.getMessage());
        }
        placements.sort(Comparator.comparingDouble(p -> p.topRatio));
        return placements;
    }

    private LeafTaskDescriptor resolveLeafTaskDescriptor(BookData data, Path source, BookProcessingOptions options) {
        int sectionCount = countSections(data);
        if (sectionCount <= 0) {
            return null;
        }
        String requestedSelector = normalize(options != null ? options.sectionSelector : null);
        List<String> selectedLeafSelectors = parseOrderedLeafSelectors(requestedSelector);
        boolean singleLeafSelection = selectedLeafSelectors.size() == 1;
        if (!singleLeafSelection && sectionCount != 1) {
            return null;
        }

        Section onlySection = findFirstSection(data);
        String sectionSelector = firstNonBlank(
                singleLeafSelection ? selectedLeafSelectors.get(0) : null,
                onlySection != null ? normalize(onlySection.selector) : null
        );
        String leafTitle = firstNonBlank(
                normalize(options != null ? options.leafTitle : null),
                firstNonBlank(
                        findLeafField(data.leafSections, sectionSelector, "title"),
                        firstNonBlank(onlySection != null ? normalize(onlySection.title) : null, "leaf")
                )
        );
        String outlineIndex = firstNonBlank(
                normalize(options != null ? options.leafOutlineIndex : null),
                findLeafField(data.leafSections, sectionSelector, "outlineIndex")
        );

        LeafTaskDescriptor descriptor = new LeafTaskDescriptor();
        descriptor.bookTitle = firstNonBlank(
                normalize(options != null ? options.bookTitle : null),
                firstNonBlank(data != null ? data.title : null, stripExt(source.getFileName().toString()))
        );
        descriptor.leafTitle = leafTitle;
        descriptor.outlineIndex = outlineIndex;
        descriptor.sectionSelector = sectionSelector;
        descriptor.storageKey = normalize(options != null ? options.storageKey : null);
        descriptor.markdownFileName = sanitizeLeafMarkdownFileName(leafTitle);
        return descriptor;
    }

    private int countSections(BookData data) {
        if (data == null || data.chapters == null) {
            return 0;
        }
        int count = 0;
        for (Chapter chapter : data.chapters) {
            if (chapter == null || chapter.sections == null) {
                continue;
            }
            count += chapter.sections.size();
        }
        return count;
    }

    private Section findFirstSection(BookData data) {
        if (data == null || data.chapters == null) {
            return null;
        }
        for (Chapter chapter : data.chapters) {
            if (chapter == null || chapter.sections == null || chapter.sections.isEmpty()) {
                continue;
            }
            return chapter.sections.get(0);
        }
        return null;
    }

    private String findLeafField(List<Map<String, Object>> leafSections, String sectionSelector, String fieldName) {
        if (leafSections == null || leafSections.isEmpty()) {
            return "";
        }
        String normalizedSelector = canonicalizeLeafSelector(normalize(sectionSelector));
        for (Map<String, Object> leaf : leafSections) {
            if (leaf == null) {
                continue;
            }
            String leafSelector = canonicalizeLeafSelector(normalize(String.valueOf(leaf.get("sectionSelector"))));
            if (normalizedSelector.isBlank() || !normalizedSelector.equals(leafSelector)) {
                continue;
            }
            return normalize(String.valueOf(leaf.get(fieldName)));
        }
        return "";
    }

    private String sanitizeLeafMarkdownFileName(String leafTitle) {
        String rawTitle = firstNonBlank(leafTitle, "leaf");
        StringBuilder safe = new StringBuilder();
        for (int index = 0; index < rawTitle.length(); index++) {
            char current = rawTitle.charAt(index);
            if (current == '<' || current == '>' || current == ':' || current == '"' || current == '/'
                    || current == '\\' || current == '|' || current == '?' || current == '*') {
                safe.append('_');
                continue;
            }
            if (Character.isISOControl(current)) {
                continue;
            }
            safe.append(current);
        }
        String normalized = safe.toString().trim();
        normalized = normalized.replaceAll("[.\\s]+$", "");
        if (normalized.isBlank()) {
            normalized = "leaf";
        }
        return normalized + ".md";
    }

    private Path writeSelectedPdfOutput(
            Path source,
            Path outputRoot,
            BookData data,
            Path markdownPath
    ) throws Exception {
        if (source == null || outputRoot == null || data == null || markdownPath == null) {
            return null;
        }
        if (!".pdf".equals(lowerExt(source.getFileName().toString()))) {
            return null;
        }

        TreeSet<Integer> selectedPages = collectSelectedPdfPages(data);
        if (selectedPages.isEmpty()) {
            return null;
        }

        String markdownFileName = markdownPath.getFileName() != null
                ? markdownPath.getFileName().toString()
                : "book.md";
        String pdfFileName = stripExt(markdownFileName) + "_pages.pdf";
        Path selectedPdfPath = outputRoot.resolve(pdfFileName).toAbsolutePath().normalize();
        Path normalizedOutputRoot = outputRoot.toAbsolutePath().normalize();
        if (!selectedPdfPath.startsWith(normalizedOutputRoot)) {
            throw new IllegalArgumentException("selected pdf path escapes output root: " + selectedPdfPath);
        }
        Files.createDirectories(normalizedOutputRoot);

        // 只保留最终 Markdown 真正覆盖到的页，避免任务目录继续滞留整本原始 PDF。
        Path tempPdfPath = Files.createTempFile(normalizedOutputRoot, stripExt(pdfFileName) + "_", ".pdf");
        boolean written = false;
        try (PDDocument sourcePdf = PDDocument.load(source.toFile()); PDDocument selectedPdf = new PDDocument()) {
            int totalPages = sourcePdf.getNumberOfPages();
            for (Integer pageNo : selectedPages) {
                if (pageNo == null || pageNo < 1 || pageNo > totalPages) {
                    continue;
                }
                selectedPdf.importPage(sourcePdf.getPage(pageNo - 1));
            }
            if (selectedPdf.getNumberOfPages() <= 0) {
                return null;
            }
            selectedPdf.save(tempPdfPath.toFile());
            written = true;
        } finally {
            if (!written) {
                Files.deleteIfExists(tempPdfPath);
            }
        }
        Files.move(tempPdfPath, selectedPdfPath, StandardCopyOption.REPLACE_EXISTING);
        logger.info(
                "Book selected-page pdf saved: source={} target={} pages={}",
                source,
                selectedPdfPath,
                selectedPages.size()
        );
        return selectedPdfPath;
    }

    private TreeSet<Integer> collectSelectedPdfPages(BookData data) {
        TreeSet<Integer> pages = new TreeSet<>();
        if (data == null || data.chapters == null) {
            return pages;
        }
        for (Chapter chapter : data.chapters) {
            if (chapter == null) {
                continue;
            }
            if (chapter.sections == null || chapter.sections.isEmpty()) {
                appendPageRange(pages, chapter.startPage, chapter.endPage);
                continue;
            }
            for (Section section : chapter.sections) {
                if (section == null) {
                    continue;
                }
                int startPage = section.startPage > 0 ? section.startPage : chapter.startPage;
                int endPage = section.endPage >= startPage ? section.endPage : startPage;
                appendPageRange(pages, startPage, endPage);
            }
        }
        return pages;
    }

    private void appendPageRange(Set<Integer> pages, int startPage, int endPage) {
        if (pages == null || startPage <= 0) {
            return;
        }
        int normalizedEnd = endPage >= startPage ? endPage : startPage;
        for (int pageNo = startPage; pageNo <= normalizedEnd; pageNo++) {
            pages.add(pageNo);
        }
    }

    private void cleanupOriginalTaskPdf(Path source, Path outputRoot, Path selectedPdfPath) {
        if (source == null || outputRoot == null) {
            return;
        }
        if (!".pdf".equals(lowerExt(source.getFileName().toString()))) {
            return;
        }
        Path normalizedSource = source.toAbsolutePath().normalize();
        Path normalizedOutputRoot = outputRoot.toAbsolutePath().normalize();
        if (!normalizedSource.startsWith(normalizedOutputRoot)) {
            return;
        }
        if (selectedPdfPath != null && normalizedSource.equals(selectedPdfPath.toAbsolutePath().normalize())) {
            return;
        }
        try {
            // 只清理任务目录内部的原始 PDF，外部源文件由原有来源路径自行负责。
            if (Files.deleteIfExists(normalizedSource)) {
                logger.info("Book original pdf removed from task directory: {}", normalizedSource);
            }
        } catch (Exception error) {
            logger.warn(
                    "Delete original task pdf failed: source={} outputRoot={} err={}",
                    normalizedSource,
                    normalizedOutputRoot,
                    error.getMessage()
            );
        }
    }

    private Path writeMarkdownOutputs(
            BookData data,
            Path outputRoot,
            boolean splitByChapter,
            boolean splitBySection,
            LeafTaskDescriptor leafTask
    ) throws Exception {
        if (leafTask != null) {
            return writeLeafMarkdownOutput(data, outputRoot, leafTask);
        }
        Path mainMarkdownPath = outputRoot.resolve("book.md");
        StringBuilder main = new StringBuilder();
        main.append("# ").append(firstNonBlank(data.title, "Book")).append("\n\n");
        appendBookOverview(main, data);

        if (splitByChapter) {
            Path chaptersDir = outputRoot.resolve("chapters");
            Files.createDirectories(chaptersDir);
            for (int chapterIndex = 0; chapterIndex < data.chapters.size(); chapterIndex++) {
                Chapter chapter = data.chapters.get(chapterIndex);
                String chapterStem = String.format(
                        Locale.ROOT,
                        "chapter-%03d-%s",
                        chapterIndex + 1,
                        slugify(chapter.title)
                );
                Path chapterPath = chaptersDir.resolve(chapterStem + ".md");
                writeChapterFile(chapterPath, chapter, outputRoot, splitBySection, chapterStem);
                main.append(chapterIndex + 1)
                        .append(". [")
                        .append(firstNonBlank(chapter.title, "Chapter " + (chapterIndex + 1)))
                        .append("](./chapters/")
                        .append(chapterStem)
                        .append(".md)\n");
            }
            main.append("\n");
        } else if (splitBySection) {
            Path sectionsDir = outputRoot.resolve("sections");
            Files.createDirectories(sectionsDir);
            int globalSectionIndex = 1;
            for (int chapterIndex = 0; chapterIndex < data.chapters.size(); chapterIndex++) {
                Chapter chapter = data.chapters.get(chapterIndex);
                main.append("## ").append(firstNonBlank(chapter.title, "Chapter " + (chapterIndex + 1))).append("\n\n");
                for (int sectionIndex = 0; sectionIndex < chapter.sections.size(); sectionIndex++) {
                    Section section = chapter.sections.get(sectionIndex);
                    String sectionFileName = String.format(
                            Locale.ROOT,
                            "section-%03d-%03d-%s.md",
                            chapterIndex + 1,
                            sectionIndex + 1,
                            slugify(section.title)
                    );
                    Path sectionPath = sectionsDir.resolve(sectionFileName);
                    writeSectionFile(
                            sectionPath,
                            chapter,
                            section,
                            chapterIndex + 1,
                            sectionIndex + 1,
                            outputRoot
                    );
                    main.append(globalSectionIndex)
                            .append(". [")
                            .append(firstNonBlank(section.title, "Section " + (sectionIndex + 1)))
                            .append("](./sections/")
                            .append(sectionFileName)
                            .append(")\n");
                    globalSectionIndex += 1;
                }
                main.append("\n");
            }
        } else {
            for (int chapterIndex = 0; chapterIndex < data.chapters.size(); chapterIndex++) {
                appendChapterInline(main, data.chapters.get(chapterIndex), 2, mainMarkdownPath, outputRoot);
            }
        }

        Files.writeString(mainMarkdownPath, main.toString(), StandardCharsets.UTF_8);
        return mainMarkdownPath;
    }

    private Path writeLeafMarkdownOutput(BookData data, Path outputRoot, LeafTaskDescriptor leafTask) throws Exception {
        Path markdownPath = outputRoot.resolve(firstNonBlank(leafTask.markdownFileName, "leaf.md"));
        StringBuilder builder = new StringBuilder();
        builder.append("# ").append(firstNonBlank(leafTask.leafTitle, "Leaf")).append("\n\n");
        builder.append("- Book: ").append(firstNonBlank(leafTask.bookTitle, firstNonBlank(data.title, "Book"))).append("\n");
        if (!normalize(leafTask.outlineIndex).isBlank()) {
            builder.append("- Outline: ").append(leafTask.outlineIndex).append("\n");
        }
        if (!normalize(leafTask.sectionSelector).isBlank()) {
            builder.append("- Selector: ").append(leafTask.sectionSelector).append("\n");
        }
        builder.append("\n");
        for (Chapter chapter : data.chapters) {
            if (chapter == null || chapter.sections == null) {
                continue;
            }
            for (Section section : chapter.sections) {
                if (section == null) {
                    continue;
                }
                appendSectionBody(builder, section, markdownPath, outputRoot);
            }
        }
        Files.writeString(markdownPath, builder.toString(), StandardCharsets.UTF_8);
        return markdownPath;
    }

    private void appendBookOverview(StringBuilder builder, BookData data) {
        int sectionCount = 0;
        for (Chapter chapter : data.chapters) {
            sectionCount += chapter.sections.size();
        }
        builder.append("- Chapters: ").append(data.chapters.size()).append("\n");
        builder.append("- Sections: ").append(sectionCount).append("\n\n");
        builder.append("## Contents\n\n");
    }

    private void writeChapterFile(
            Path chapterPath,
            Chapter chapter,
            Path outputRoot,
            boolean splitBySection,
            String chapterStem
    ) throws Exception {
        StringBuilder chapterMd = new StringBuilder();
        chapterMd.append("# ").append(firstNonBlank(chapter.title, "Chapter")).append("\n\n");

        if (!splitBySection) {
            for (Section section : chapter.sections) {
                appendSectionInline(chapterMd, section, 2, chapterPath, outputRoot);
            }
            Files.writeString(chapterPath, chapterMd.toString(), StandardCharsets.UTF_8);
            return;
        }

        Path sectionDir = chapterPath.getParent().resolve(chapterStem);
        Files.createDirectories(sectionDir);
        chapterMd.append("## Sections\n\n");
        for (int sectionIndex = 0; sectionIndex < chapter.sections.size(); sectionIndex++) {
            Section section = chapter.sections.get(sectionIndex);
            String sectionFileName = String.format(
                    Locale.ROOT,
                    "section-%03d-%s.md",
                    sectionIndex + 1,
                    slugify(section.title)
            );
            Path sectionPath = sectionDir.resolve(sectionFileName);
            writeSectionFile(
                    sectionPath,
                    chapter,
                    section,
                    -1,
                    sectionIndex + 1,
                    outputRoot
            );
            chapterMd.append(sectionIndex + 1)
                    .append(". [")
                    .append(firstNonBlank(section.title, "Section " + (sectionIndex + 1)))
                    .append("](")
                    .append("./")
                    .append(chapterStem)
                    .append("/")
                    .append(sectionFileName)
                    .append(")\n");
        }
        chapterMd.append("\n");
        Files.writeString(chapterPath, chapterMd.toString(), StandardCharsets.UTF_8);
    }

    private void writeSectionFile(
            Path sectionPath,
            Chapter chapter,
            Section section,
            int chapterIndex,
            int sectionIndex,
            Path outputRoot
    ) throws Exception {
        StringBuilder sectionMd = new StringBuilder();
        sectionMd.append("# ").append(firstNonBlank(section.title, "Section " + sectionIndex)).append("\n\n");
        sectionMd.append("- Chapter: ").append(firstNonBlank(chapter.title, "Chapter")).append("\n");
        if (chapterIndex > 0) {
            sectionMd.append("- Chapter Index: ").append(chapterIndex).append("\n");
        }
        sectionMd.append("- Section Index: ").append(sectionIndex).append("\n\n");
        if (section.startPage > 0) {
            sectionMd.append("- Start Page: ").append(section.startPage).append("\n");
        }
        if (section.endPage > 0) {
            sectionMd.append("- End Page: ").append(section.endPage).append("\n");
        }
        if (section.startPage > 0 || section.endPage > 0) {
            sectionMd.append("\n");
        }
        appendSectionBody(sectionMd, section, sectionPath, outputRoot);
        Files.writeString(sectionPath, sectionMd.toString(), StandardCharsets.UTF_8);
    }

    private void appendChapterInline(
            StringBuilder builder,
            Chapter chapter,
            int headingLevel,
            Path markdownPath,
            Path outputRoot
    ) {
        builder.append("#".repeat(Math.max(1, headingLevel)))
                .append(" ")
                .append(firstNonBlank(chapter.title, "Chapter"))
                .append("\n\n");
        for (Section section : chapter.sections) {
            appendSectionInline(builder, section, headingLevel + 1, markdownPath, outputRoot);
        }
    }

    private void appendSectionInline(
            StringBuilder builder,
            Section section,
            int headingLevel,
            Path markdownPath,
            Path outputRoot
    ) {
        builder.append("#".repeat(Math.max(1, headingLevel)))
                .append(" ")
                .append(firstNonBlank(section.title, "Body"))
                .append("\n\n");
        appendSectionBody(builder, section, markdownPath, outputRoot);
    }

    private void appendSectionBody(
            StringBuilder builder,
            Section section,
            Path markdownPath,
            Path outputRoot
    ) {
        String markdownBody = section.markdownBody != null ? section.markdownBody.trim() : "";
        if (!markdownBody.isBlank()) {
            String resolvedMarkdown = rewriteSectionMarkdownAssets(markdownBody, markdownPath, outputRoot);
            builder.append(resolvedMarkdown);
            if (!resolvedMarkdown.endsWith("\n")) {
                builder.append("\n");
            }
            builder.append("\n");
            return;
        }

        boolean hasBodyContent = false;
        if (section.blocks != null && !section.blocks.isEmpty()) {
            int imageIndex = 0;
            for (SectionBlock block : section.blocks) {
                if (block == null || block.type == null) {
                    continue;
                }
                if (block.type == SectionBlock.BlockType.PARAGRAPH) {
                    String text = normalize(block.text);
                    if (text.isBlank()) {
                        continue;
                    }
                    builder.append(text).append("\n\n");
                    hasBodyContent = true;
                    continue;
                }
                if (block.type == SectionBlock.BlockType.IMAGE) {
                    String imagePath = normalize(block.imagePath);
                    if (imagePath.isBlank()) {
                        continue;
                    }
                    imageIndex += 1;
                    String markdownPathLink = resolveAssetMarkdownPath(markdownPath, outputRoot, imagePath);
                    builder.append("![image-")
                            .append(imageIndex)
                            .append("](")
                            .append(markdownPathLink)
                            .append(")\n\n");
                    hasBodyContent = true;
                    continue;
                }
                if (block.type == SectionBlock.BlockType.TABLE && block.table != null && !block.table.isEmpty()) {
                    appendMarkdownTable(builder, block.table);
                    hasBodyContent = true;
                }
            }
            if (!hasBodyContent) {
                builder.append("_No content extracted._\n\n");
            }
            return;
        }

        for (String paragraph : section.paragraphs) {
            String text = normalize(paragraph);
            if (text.isBlank()) {
                continue;
            }
            builder.append(text).append("\n\n");
            hasBodyContent = true;
        }

        for (int imageIndex = 0; imageIndex < section.images.size(); imageIndex++) {
            String imagePath = section.images.get(imageIndex);
            if (imagePath == null || imagePath.isBlank()) {
                continue;
            }
            String markdownPathLink = resolveAssetMarkdownPath(markdownPath, outputRoot, imagePath);
            builder.append("![image-")
                    .append(imageIndex + 1)
                    .append("](")
                    .append(markdownPathLink)
                    .append(")\n\n");
            hasBodyContent = true;
        }

        for (List<List<String>> table : section.tables) {
            if (table == null || table.isEmpty()) {
                continue;
            }
            appendMarkdownTable(builder, table);
            hasBodyContent = true;
        }

        if (!hasBodyContent) {
            builder.append("_No content extracted._\n\n");
        }
    }

    private void appendMarkdownTable(StringBuilder builder, List<List<String>> tableRows) {
        int maxColumns = 0;
        for (List<String> row : tableRows) {
            maxColumns = Math.max(maxColumns, row != null ? row.size() : 0);
        }
        if (maxColumns < 2) {
            return;
        }

        List<String> header = tableRows.get(0);
        builder.append(buildTableRow(header, maxColumns)).append("\n");

        List<String> divider = new ArrayList<>();
        for (int i = 0; i < maxColumns; i++) {
            divider.add("---");
        }
        builder.append(buildTableRow(divider, maxColumns)).append("\n");

        for (int i = 1; i < tableRows.size(); i++) {
            builder.append(buildTableRow(tableRows.get(i), maxColumns)).append("\n");
        }
        builder.append("\n");
    }

    private String buildTableRow(List<String> row, int width) {
        List<String> values = row != null ? row : Collections.emptyList();
        StringBuilder line = new StringBuilder("|");
        for (int col = 0; col < width; col++) {
            String cell = col < values.size() ? normalize(values.get(col)) : "";
            line.append(" ").append(cell.replace("|", "\\|")).append(" |");
        }
        return line.toString();
    }

    private Path writeAbstractMetadata(
            String taskId,
            Path source,
            Path outputRoot,
            BookData data,
            LeafTaskDescriptor leafTask
    ) throws Exception {
        List<Map<String, Object>> units = new ArrayList<>();
        for (int chapterIndex = 0; chapterIndex < data.chapters.size(); chapterIndex++) {
            Chapter chapter = data.chapters.get(chapterIndex);
            for (int sectionIndex = 0; sectionIndex < chapter.sections.size(); sectionIndex++) {
                Section section = chapter.sections.get(sectionIndex);
                Map<String, Object> unit = new LinkedHashMap<>();
                String unitId = String.format(Locale.ROOT, "book_unit_%03d_%03d", chapterIndex + 1, sectionIndex + 1);
                if (leafTask != null && !normalize(leafTask.outlineIndex).isBlank()) {
                    unitId = "book_leaf_" + leafTask.outlineIndex.replace('.', '_');
                }
                unit.put("unit_id", unitId);
                unit.put("unit_type", "abstract");
                unit.put("chapter_index", chapterIndex + 1);
                unit.put("chapter_title", firstNonBlank(chapter.title, ""));
                unit.put("chapter_selector", firstNonBlank(chapter.selector, ""));
                unit.put("section_index", sectionIndex + 1);
                unit.put("section_title", firstNonBlank(section.title, ""));
                unit.put("section_selector", firstNonBlank(section.selector, ""));
                unit.put("source_book_title", firstNonBlank(
                        leafTask != null ? leafTask.bookTitle : null,
                        firstNonBlank(data.title, stripExt(source.getFileName().toString()))
                ));
                unit.put("leaf_title", firstNonBlank(
                        leafTask != null ? leafTask.leafTitle : null,
                        section.title
                ));
                unit.put("leaf_outline_index", firstNonBlank(leafTask != null ? leafTask.outlineIndex : null, ""));
                unit.put("start_page", section.startPage);
                unit.put("end_page", section.endPage);
                String metadataText = String.join("\n\n", section.paragraphs);
                if (metadataText.isBlank()) {
                    metadataText = stripMarkdownForMetadata(section.markdownBody);
                }
                unit.put("text", metadataText);
                unit.put("markdown_body", firstNonBlank(section.markdownBody, ""));
                unit.put("images", new ArrayList<>(section.images));
                unit.put("tables", new ArrayList<>(section.tables));
                units.add(unit);
            }
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("version", "book-abstract-v2");
        payload.put("generated_at", Instant.now().toString());
        payload.put("task_id", firstNonBlank(taskId, ""));
        payload.put("source_path", source.toString());
        payload.put("book_title", firstNonBlank(data.title, stripExt(source.getFileName().toString())));
        payload.put("leaf_title", firstNonBlank(leafTask != null ? leafTask.leafTitle : null, ""));
        payload.put("leaf_outline_index", firstNonBlank(leafTask != null ? leafTask.outlineIndex : null, ""));
        payload.put("leaf_selector", firstNonBlank(leafTask != null ? leafTask.sectionSelector : null, ""));
        payload.put("storage_key", firstNonBlank(leafTask != null ? leafTask.storageKey : null, ""));
        payload.put("page_map_strategy", firstNonBlank(data.pageMapStrategy, ""));
        payload.put("detected_page_offset", data.detectedPageOffset);
        payload.put("applied_page_offset", data.appliedPageOffset);
        payload.put("semantic_units", units);
        payload.put("semantic_unit_count", units.size());

        Path metadataPath = outputRoot.resolve("book_semantic_units.json");
        OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(metadataPath.toFile(), payload);
        return metadataPath;
    }

    private void extractPdfImages(PDPage page, int pageNo, Path outputRoot, Path imageAssetsRoot, Section section) {
        if (page == null || section == null) {
            return;
        }
        try {
            if (page.getResources() == null) {
                return;
            }
            int imageIndex = 0;
            for (COSName name : page.getResources().getXObjectNames()) {
                PDXObject xObject = page.getResources().getXObject(name);
                if (!(xObject instanceof PDImageXObject)) {
                    continue;
                }
                imageIndex += 1;
                String fileName = String.format(Locale.ROOT, "pdf-page-%03d-img-%02d.png", pageNo, imageIndex);
                Path target = ensureUniqueFile(imageAssetsRoot.resolve(fileName));
                ImageIO.write(((PDImageXObject) xObject).getImage(), "png", target.toFile());
                section.images.add(toRelative(outputRoot, target));
            }
        } catch (Exception error) {
            logger.warn("Extract PDF images failed, pageNo={}, err={}", pageNo, error.getMessage());
        }
    }

    private void extractTablesFromLines(String[] lines, Section section) {
        if (lines == null || section == null) {
            return;
        }

        List<List<String>> currentTable = new ArrayList<>();
        for (String rawLine : lines) {
            String line = normalize(rawLine);
            if (line.isBlank()) {
                if (!currentTable.isEmpty()) {
                    section.tables.add(new ArrayList<>(currentTable));
                    currentTable.clear();
                }
                continue;
            }

            String[] parts = line.contains("|") ? line.split("\\|") : line.split("\\s{2,}");
            List<String> cells = new ArrayList<>();
            for (String part : parts) {
                String cell = normalize(part);
                if (!cell.isBlank()) {
                    cells.add(cell);
                }
            }

            if (cells.size() >= 2) {
                currentTable.add(cells);
            } else if (!currentTable.isEmpty()) {
                section.tables.add(new ArrayList<>(currentTable));
                currentTable.clear();
            }
        }

        if (!currentTable.isEmpty()) {
            section.tables.add(new ArrayList<>(currentTable));
        }
    }

    private List<List<String>> parseHtmlTable(Element tableElement) {
        List<List<String>> rows = new ArrayList<>();
        if (tableElement == null) {
            return rows;
        }
        for (Element row : tableElement.select("tr")) {
            List<String> cells = new ArrayList<>();
            for (Element cell : row.select("th, td")) {
                String text = normalize(cell.text());
                if (!text.isBlank()) {
                    cells.add(text);
                }
            }
            if (!cells.isEmpty()) {
                rows.add(cells);
            }
        }
        return rows;
    }

    private String copyEpubImage(
            ZipFile zip,
            String spinePath,
            String sourcePath,
            Path outputRoot,
            Path imageAssetsRoot,
            int chapterIndex,
            int imageIndex
    ) {
        if (outputRoot == null || imageAssetsRoot == null) {
            return null;
        }
        String imageRef = normalize(sourcePath);
        if (imageRef.isBlank()) {
            return null;
        }
        String imageEntryPath = resolveRelativePath(spinePath, imageRef);
        byte[] imageBytes = readZipEntry(zip, imageEntryPath);
        if (imageBytes == null || imageBytes.length == 0) {
            return null;
        }

        String extension = lowerExt(removeQueryAndFragment(imageRef));
        if (extension.isBlank()) {
            extension = ".png";
        }
        String fileName = String.format(Locale.ROOT, "epub-c%03d-img-%03d%s", chapterIndex, imageIndex, extension);
        Path target = ensureUniqueFile(imageAssetsRoot.resolve(fileName));
        try {
            Files.write(target, imageBytes);
            return toRelative(outputRoot, target);
        } catch (Exception error) {
            logger.warn("Write EPUB image failed, sourcePath={}, target={}, err={}", sourcePath, target, error.getMessage());
            return null;
        }
    }

    private String resolveOpfPath(ZipFile zip) throws IOException {
        byte[] container = readZipEntry(zip, "META-INF/container.xml");
        if (container != null) {
            Document containerDoc = Jsoup.parse(
                    new ByteArrayInputStream(container),
                    "UTF-8",
                    "",
                    Parser.xmlParser()
            );
            for (Element rootFile : containerDoc.select("rootfile")) {
                String fullPath = normalize(rootFile.attr("full-path"));
                if (!fullPath.isBlank()) {
                    return normalizeZipPath(fullPath);
                }
            }
        }
        Enumeration<? extends ZipEntry> entries = zip.entries();
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            String name = normalizeZipPath(entry.getName());
            if (!entry.isDirectory() && name.toLowerCase(Locale.ROOT).endsWith(".opf")) {
                return name;
            }
        }
        throw new IllegalArgumentException("Invalid EPUB: cannot locate OPF");
    }

    private byte[] readZipEntry(ZipFile zip, String entryPath) {
        if (zip == null || entryPath == null || entryPath.isBlank()) {
            return null;
        }
        String normalizedPath = normalizeZipPath(entryPath);
        ZipEntry targetEntry = zip.getEntry(normalizedPath);
        if (targetEntry == null) {
            Enumeration<? extends ZipEntry> entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                String normalizedName = normalizeZipPath(entry.getName());
                if (normalizedName.equalsIgnoreCase(normalizedPath)) {
                    targetEntry = entry;
                    break;
                }
            }
        }
        if (targetEntry == null || targetEntry.isDirectory()) {
            return null;
        }
        try (InputStream in = zip.getInputStream(targetEntry)) {
            return in.readAllBytes();
        } catch (Exception error) {
            logger.warn("Read ZIP entry failed, entryPath={}, err={}", entryPath, error.getMessage());
            return null;
        }
    }

    private String resolveRelativePath(String basePath, String relativePath) {
        String base = normalizeZipPath(basePath);
        String rel = normalizeZipPath(removeQueryAndFragment(relativePath));
        if (rel.isBlank()) {
            return "";
        }
        try {
            Path relPath = Paths.get(rel);
            if (relPath.isAbsolute()) {
                return normalizeZipPath(relPath.toString());
            }
            Path baseParent = Paths.get(base).getParent();
            Path merged = baseParent == null ? relPath : baseParent.resolve(relPath);
            return normalizeZipPath(merged.normalize().toString());
        } catch (InvalidPathException error) {
            logger.warn("Resolve relative path failed, basePath={}, relativePath={}", basePath, relativePath);
            return rel;
        }
    }

    private String normalizeZipPath(String path) {
        if (path == null) {
            return "";
        }
        String normalized = path.replace('\\', '/').trim();
        while (normalized.startsWith("./")) {
            normalized = normalized.substring(2);
        }
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        return normalized;
    }

    private String removeQueryAndFragment(String path) {
        if (path == null) {
            return "";
        }
        String stripped = path;
        int queryIndex = stripped.indexOf('?');
        if (queryIndex >= 0) {
            stripped = stripped.substring(0, queryIndex);
        }
        int fragmentIndex = stripped.indexOf('#');
        if (fragmentIndex >= 0) {
            stripped = stripped.substring(0, fragmentIndex);
        }
        return stripped;
    }

    private Path ensureUniqueFile(Path desiredPath) {
        if (!Files.exists(desiredPath)) {
            return desiredPath;
        }
        String fileName = desiredPath.getFileName().toString();
        int dotIndex = fileName.lastIndexOf('.');
        String stem = dotIndex >= 0 ? fileName.substring(0, dotIndex) : fileName;
        String ext = dotIndex >= 0 ? fileName.substring(dotIndex) : "";
        int sequence = 2;
        while (true) {
            Path candidate = desiredPath.getParent().resolve(stem + "-" + sequence + ext);
            if (!Files.exists(candidate)) {
                return candidate;
            }
            sequence += 1;
        }
    }

    private String resolveAssetMarkdownPath(Path markdownPath, Path outputRoot, String outputRelativeAssetPath) {
        String normalizedAssetPath = normalizeZipPath(outputRelativeAssetPath);
        if (normalizedAssetPath.isBlank()) {
            return outputRelativeAssetPath;
        }
        try {
            Path markdownParent = markdownPath.getParent() == null
                    ? outputRoot.toAbsolutePath().normalize()
                    : markdownPath.getParent().toAbsolutePath().normalize();
            Path assetAbs = outputRoot.resolve(normalizedAssetPath).toAbsolutePath().normalize();
            return normalizeZipPath(markdownParent.relativize(assetAbs).toString());
        } catch (Exception error) {
            return normalizedAssetPath;
        }
    }

    private String rewriteSectionMarkdownAssets(String markdown, Path markdownPath, Path outputRoot) {
        if (markdown == null || markdown.isBlank()) {
            return "";
        }
        Matcher matcher = MARKDOWN_IMAGE_PATTERN.matcher(markdown);
        StringBuffer rewritten = new StringBuffer();
        while (matcher.find()) {
            String rawPath = matcher.group(1);
            String mappedPath = rawPath;
            if (!isExternalAssetPath(rawPath)) {
                mappedPath = resolveAssetMarkdownPath(markdownPath, outputRoot, rawPath);
            }
            String fullToken = matcher.group(0);
            int openIndex = fullToken.lastIndexOf('(');
            String replacedToken;
            if (openIndex >= 0 && fullToken.endsWith(")")) {
                replacedToken = fullToken.substring(0, openIndex + 1) + mappedPath + ")";
            } else {
                replacedToken = fullToken;
            }
            matcher.appendReplacement(rewritten, Matcher.quoteReplacement(replacedToken));
        }
        matcher.appendTail(rewritten);
        return rewritten.toString();
    }

    private String readText(Path source) throws IOException {
        byte[] bytes = Files.readAllBytes(source);
        if (bytes.length >= 3
                && (bytes[0] & 0xFF) == 0xEF
                && (bytes[1] & 0xFF) == 0xBB
                && (bytes[2] & 0xFF) == 0xBF) {
            return new String(bytes, 3, bytes.length - 3, StandardCharsets.UTF_8);
        }
        if (bytes.length >= 2
                && (bytes[0] & 0xFF) == 0xFF
                && (bytes[1] & 0xFF) == 0xFE) {
            return new String(bytes, 2, bytes.length - 2, StandardCharsets.UTF_16LE);
        }
        if (bytes.length >= 2
                && (bytes[0] & 0xFF) == 0xFE
                && (bytes[1] & 0xFF) == 0xFF) {
            return new String(bytes, 2, bytes.length - 2, StandardCharsets.UTF_16BE);
        }

        String utf8 = new String(bytes, StandardCharsets.UTF_8);
        if (!utf8.contains("\uFFFD")) {
            return utf8;
        }
        String gb = new String(bytes, GB18030);
        return gb.contains("\uFFFD") ? utf8 : gb;
    }

    private void finalizeChapter(List<Chapter> chapters, Chapter chapter) {
        if (chapter == null) {
            return;
        }
        if (chapter.sections == null) {
            chapter.sections = new ArrayList<>();
        }
        if (chapter.sections.isEmpty()) {
            chapter.sections.add(new Section("Body"));
        }
        if (!hasChapterContent(chapter)) {
            return;
        }
        chapter.title = firstNonBlank(chapter.title, "Chapter " + (chapters.size() + 1));
        chapters.add(chapter);
    }

    private boolean hasChapterContent(Chapter chapter) {
        if (chapter == null || chapter.sections == null) {
            return false;
        }
        for (Section section : chapter.sections) {
            if (hasSectionContent(section)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasSectionContent(Section section) {
        if (section == null) {
            return false;
        }
        return (section.paragraphs != null && !section.paragraphs.isEmpty())
                || (section.images != null && !section.images.isEmpty())
                || (section.tables != null && !section.tables.isEmpty())
                || (section.markdownBody != null && !section.markdownBody.isBlank());
    }

    private String normalize(String value) {
        if (value == null) {
            return "";
        }
        String normalized = value
                .replace('\u00A0', ' ')
                .replace('\u3000', ' ')
                .trim();
        normalized = normalized.replaceAll("\\s+", " ");
        return Normalizer.normalize(normalized, Normalizer.Form.NFKC);
    }

    private String toRelative(Path basePath, Path targetPath) {
        try {
            return normalizeZipPath(
                    basePath.toAbsolutePath().normalize()
                            .relativize(targetPath.toAbsolutePath().normalize())
                            .toString()
            );
        } catch (Exception error) {
            return normalizeZipPath(targetPath.getFileName() != null ? targetPath.getFileName().toString() : targetPath.toString());
        }
    }

    private String firstNonBlank(String primary, String fallback) {
        if (primary != null && !primary.isBlank()) {
            return primary.trim();
        }
        return fallback != null ? fallback.trim() : "";
    }

    private Integer parsePositiveIntOrNull(String rawValue) {
        String normalized = normalize(rawValue);
        if (normalized.isBlank()) {
            return null;
        }
        try {
            int parsed = Integer.parseInt(normalized);
            return parsed > 0 ? parsed : null;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private int readObjectInt(Object value, int fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        String normalized = normalize(String.valueOf(value));
        if (normalized.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(normalized);
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private String lowerExt(String fileName) {
        if (fileName == null) {
            return "";
        }
        int dot = fileName.lastIndexOf('.');
        if (dot < 0 || dot == fileName.length() - 1) {
            return "";
        }
        return fileName.substring(dot).toLowerCase(Locale.ROOT);
    }

    private String stripExt(String fileName) {
        if (fileName == null || fileName.isBlank()) {
            return "book";
        }
        int dot = fileName.lastIndexOf('.');
        if (dot <= 0) {
            return fileName;
        }
        return fileName.substring(0, dot);
    }

    private String slugify(String value) {
        String raw = normalize(value).toLowerCase(Locale.ROOT);
        raw = raw.replaceAll("[^a-z0-9]+", "-");
        raw = raw.replaceAll("^-+", "").replaceAll("-+$", "");
        if (raw.isBlank()) {
            return "item";
        }
        if (raw.length() > 48) {
            return raw.substring(0, 48);
        }
        return raw;
    }
}
