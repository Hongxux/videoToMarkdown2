package com.mvp.module2.fusion.service;

import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

class BookMarkdownServiceGrpcExtractorTest {

    @Test
    void processPdfUsesGrpcExtractorResultWhenAvailable() throws Exception {
        Path testRoot = Paths.get("var", "tmp_book_grpc_extractor_test").toAbsolutePath().normalize();
        deleteDirIfExists(testRoot);
        Files.createDirectories(testRoot);

        Path pdfPath = testRoot.resolve("sample.pdf");
        createSimplePdf(pdfPath, "grpc extractor sample text");
        Path outputDir = testRoot.resolve("out");
        Files.createDirectories(outputDir);

        BookMarkdownService service = new BookMarkdownService();
        PythonGrpcClient grpcClient = Mockito.mock(PythonGrpcClient.class);

        PythonGrpcClient.ExtractBookPdfResult grpcResult = new PythonGrpcClient.ExtractBookPdfResult();
        grpcResult.success = true;
        grpcResult.extractor = "mineru";
        grpcResult.markdown = "This content comes from grpc extractor.\n\n![image-1](assets/book_images/mock.png)\n";
        grpcResult.imagePaths = new ArrayList<>();
        grpcResult.imagePaths.add("assets/book_images/mock.png");

        Mockito.when(grpcClient.extractBookPdf(
                        Mockito.anyString(),
                        Mockito.eq(pdfPath.toString()),
                        Mockito.anyString(),
                        Mockito.anyInt(),
                        Mockito.anyInt(),
                        Mockito.anyString(),
                        Mockito.anyString(),
                        Mockito.anyString(),
                        Mockito.anyBoolean(),
                        Mockito.anyInt()
                ))
                .thenReturn(grpcResult);

        ReflectionTestUtils.setField(service, "grpcClient", grpcClient);
        ReflectionTestUtils.setField(service, "pdfExtractorStrategy", "auto");
        ReflectionTestUtils.setField(service, "preferMineruExtractor", true);
        ReflectionTestUtils.setField(service, "bookPdfExtractorTimeoutSec", 120);

        BookMarkdownService.BookProcessingResult result = service.processBook(
                "book_grpc_test",
                pdfPath.toString(),
                outputDir.toString(),
                null
        );

        Assertions.assertTrue(result.success, "processBook failed: " + result.errorMessage);
        String markdown = Files.readString(Paths.get(result.markdownPath), StandardCharsets.UTF_8);
        Assertions.assertTrue(markdown.contains("This content comes from grpc extractor."));
        Assertions.assertTrue(markdown.contains("![image-1]("));
        Mockito.verify(grpcClient, Mockito.atLeastOnce()).extractBookPdf(
                Mockito.anyString(),
                Mockito.eq(pdfPath.toString()),
                Mockito.anyString(),
                Mockito.anyInt(),
                Mockito.anyInt(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyBoolean(),
                Mockito.anyInt()
        );
    }

    @Test
    void trimMarkdownByLeafAnchorWindowKeepsOnlySelectedLeafRange() {
        BookMarkdownService service = new BookMarkdownService();
        String markdown = String.join(
                "\n",
                "# 1.1.1 Previous",
                "drop-a",
                "# 1.1.2 Selected",
                "keep-a",
                "keep-b",
                "# 1.1.3 Next",
                "drop-b"
        );

        String trimmed = ReflectionTestUtils.invokeMethod(
                service,
                "trimMarkdownByLeafAnchorWindow",
                markdown,
                "1.1.2 Selected",
                "1.1.3 Next"
        );

        Assertions.assertNotNull(trimmed);
        Assertions.assertTrue(trimmed.startsWith("# 1.1.2 Selected"));
        Assertions.assertTrue(trimmed.contains("keep-a"));
        Assertions.assertTrue(trimmed.contains("keep-b"));
        Assertions.assertFalse(trimmed.contains("drop-a"));
        Assertions.assertFalse(trimmed.contains("# 1.1.3 Next"));
        Assertions.assertFalse(trimmed.contains("drop-b"));
    }

    private void createSimplePdf(Path target, String text) throws Exception {
        try (PDDocument document = new PDDocument()) {
            PDPage page = new PDPage();
            document.addPage(page);
            try (PDPageContentStream stream = new PDPageContentStream(document, page)) {
                stream.beginText();
                stream.setFont(PDType1Font.HELVETICA, 12);
                stream.newLineAtOffset(72, 720);
                stream.showText(text);
                stream.endText();
            }
            document.save(target.toFile());
        }
    }

    private void deleteDirIfExists(Path root) throws Exception {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var stream = Files.walk(root)) {
            stream.sorted((a, b) -> b.compareTo(a)).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                }
            });
        }
    }
}
