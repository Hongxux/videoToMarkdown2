package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class BookMarkdownServiceLeafSectionMappingTest {

    @Test
    void buildPdfTocLeafSectionsKeepsTocSectionWhenPageRangesOverlap() throws Exception {
        BookMarkdownService service = new BookMarkdownService();
        Object bookData = newBookData();

        Object chapter = newChapter("Chapter 1", "c1", 24, 34);
        Object sectionOne = newSection("Section 1", "c1s1", 24, 30);
        Object sectionTwo = newSection("Section 2", "c1s2", 24, 34);
        ReflectionTestUtils.setField(chapter, "sections", new ArrayList<>(List.of(sectionOne, sectionTwo)));
        ReflectionTestUtils.setField(bookData, "chapters", new ArrayList<>(List.of(chapter)));

        List<Object> tocEntries = new ArrayList<>();
        tocEntries.add(newTocEntry("Chapter 1", "Chapter 1", 1, 24, 1, null, 1, null, null));
        tocEntries.add(newTocEntry("Section 1", "Section 1", 2, 24, 1, "1.1", 1, 1, null));
        tocEntries.add(newTocEntry("Section 2", "Section 2", 2, 24, 1, "1.2", 1, 2, null));
        tocEntries.add(newTocEntry("Leaf 1.1.1", "Leaf 1.1.1", 3, 29, 1, "1.1.1", 1, 1, 1));
        tocEntries.add(newTocEntry("Leaf 1.2.3", "Leaf 1.2.3", 3, 29, 1, "1.2.3", 1, 2, 3));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> leafSections = (List<Map<String, Object>>) ReflectionTestUtils.invokeMethod(
                service,
                "buildPdfTocLeafSections",
                tocEntries,
                40,
                bookData
        );

        Assertions.assertNotNull(leafSections);
        Assertions.assertEquals(2, leafSections.size());

        Map<String, Object> firstLeaf = leafSections.get(0);
        Map<String, Object> secondLeaf = leafSections.get(1);
        Assertions.assertEquals("c1s1t1", firstLeaf.get("sectionSelector"));
        Assertions.assertEquals("1.1.1", firstLeaf.get("outlineIndex"));
        Assertions.assertEquals("c1s2t3", secondLeaf.get("sectionSelector"));
        Assertions.assertEquals("1.2.3", secondLeaf.get("outlineIndex"));
        Assertions.assertEquals(2, readInt(secondLeaf.get("sectionIndex")));

        Map<String, Integer> selectorCounter = new LinkedHashMap<>();
        for (Map<String, Object> leaf : leafSections) {
            String selector = String.valueOf(leaf.get("sectionSelector"));
            selectorCounter.put(selector, selectorCounter.getOrDefault(selector, 0) + 1);
        }
        Assertions.assertTrue(
                selectorCounter.values().stream().allMatch(count -> count == 1),
                "leaf section selectors should remain unique when section page ranges overlap"
        );
    }

    private Object newBookData() throws Exception {
        Class<?> clazz = Class.forName("com.mvp.module2.fusion.service.BookMarkdownService$BookData");
        Constructor<?> constructor = clazz.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object instance = constructor.newInstance();
        ReflectionTestUtils.setField(instance, "leafSections", new ArrayList<>());
        return instance;
    }

    private Object newChapter(String title, String selector, int startPage, int endPage) throws Exception {
        Class<?> clazz = Class.forName("com.mvp.module2.fusion.service.BookMarkdownService$Chapter");
        Constructor<?> constructor = clazz.getDeclaredConstructor(String.class);
        constructor.setAccessible(true);
        Object instance = constructor.newInstance(title);
        ReflectionTestUtils.setField(instance, "selector", selector);
        ReflectionTestUtils.setField(instance, "startPage", startPage);
        ReflectionTestUtils.setField(instance, "endPage", endPage);
        ReflectionTestUtils.setField(instance, "sections", new ArrayList<>());
        return instance;
    }

    private Object newSection(String title, String selector, int startPage, int endPage) throws Exception {
        Class<?> clazz = Class.forName("com.mvp.module2.fusion.service.BookMarkdownService$Section");
        Constructor<?> constructor = clazz.getDeclaredConstructor(String.class);
        constructor.setAccessible(true);
        Object instance = constructor.newInstance(title);
        ReflectionTestUtils.setField(instance, "selector", selector);
        ReflectionTestUtils.setField(instance, "startPage", startPage);
        ReflectionTestUtils.setField(instance, "endPage", endPage);
        return instance;
    }

    private Object newTocEntry(
            String title,
            String displayTitle,
            int level,
            int pageNo,
            int sourcePageNo,
            String outlineIndex,
            Integer chapterNo,
            Integer sectionNo,
            Integer leafNo
    ) throws Exception {
        Class<?> clazz = Class.forName("com.mvp.module2.fusion.service.BookMarkdownService$PdfTocEntry");
        Constructor<?> constructor = clazz.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object instance = constructor.newInstance();
        ReflectionTestUtils.setField(instance, "title", title);
        ReflectionTestUtils.setField(instance, "displayTitle", displayTitle);
        ReflectionTestUtils.setField(instance, "level", level);
        ReflectionTestUtils.setField(instance, "pageNo", pageNo);
        ReflectionTestUtils.setField(instance, "sourcePageNo", sourcePageNo);
        ReflectionTestUtils.setField(instance, "outlineIndex", outlineIndex);
        ReflectionTestUtils.setField(instance, "chapterNo", chapterNo);
        ReflectionTestUtils.setField(instance, "sectionNo", sectionNo);
        ReflectionTestUtils.setField(instance, "leafNo", leafNo);
        return instance;
    }

    private int readInt(Object rawValue) {
        if (rawValue instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(String.valueOf(rawValue));
    }
}