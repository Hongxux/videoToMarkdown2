package com.hongxu.videoToMarkdownTest2

import io.noties.prism4j.annotations.PrismBundle

@PrismBundle(
    include = [
        "markup",
        "css",
        "clike",
        "javascript",
        "json",
        "java",
        "kotlin",
        "yaml",
        "sql",
        "python",
        "markdown",
        "go",
        "dart",
        "swift",
        "c",
        "cpp"
    ],
    grammarLocatorClassName = ".MarkdownGrammarLocator",
    includeAll = false
)
class MarkdownPrismBundle
