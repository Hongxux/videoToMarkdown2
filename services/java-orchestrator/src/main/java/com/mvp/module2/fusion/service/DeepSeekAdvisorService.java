package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.service.llm.LlmClient;
import com.mvp.module2.fusion.service.llm.LlmFallbackStrategy;
import com.mvp.module2.fusion.service.llm.LlmGateway;
import com.mvp.module2.fusion.service.llm.LlmGatewayResult;
import com.mvp.module2.fusion.service.llm.LlmPromptRequest;
import com.mvp.module2.fusion.service.llm.LlmProviderConfig;
import com.mvp.module2.fusion.service.llm.LlmResponse;
import com.mvp.module2.fusion.service.llm.LlmRetryPolicy;
import com.mvp.module2.fusion.service.llm.OpenAiCompatibleLlmClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

@Service
public class DeepSeekAdvisorService {

    private static final Logger logger = LoggerFactory.getLogger(DeepSeekAdvisorService.class);
    private static final String PROMPT_TEMPLATE_CONTEXT_EMPTY = "（无）";
    private static final int STRUCTURED_RETRY_MAX_ATTEMPTS = 3;
    private static final long NETWORK_RETRY_INITIAL_BACKOFF_MS = 500L;
    private static final long NETWORK_RETRY_MAX_BACKOFF_MS = 4000L;
    private static final int PHASE2B_PROVIDER_MAX_RETRIES_DEFAULT = 3;
    private static final long PHASE2B_PROVIDER_INITIAL_BACKOFF_MS_DEFAULT = 2000L;
    private static final long PHASE2B_PROVIDER_MAX_BACKOFF_MS_DEFAULT = 16000L;
    private static final double PHASE2B_PROVIDER_JITTER_RATIO_DEFAULT = 0.2d;
    private static final int PHASE2B_RAW_LOG_MAX_LINES = 400;
    private static final int PHASE2B_RAW_LOG_MAX_LINE_CHARS = 1200;
    private static final String PHASE2B_CHILD_INDENT = "    ";
    private static final String[] SECTION_KEYS_BACKGROUND = {
            "background", "bg", "background_knowledge", "背景", "背景知识"
    };
    private static final String[] SECTION_KEYS_CONTEXTUAL = {
            "contextual_explanations", "contextualExplanations", "contextual", "context", "语境化", "语境解释"
    };
    private static final String[] SECTION_KEYS_DEPTH = {
            "depth", "deep", "principles", "mechanism", "deep_analysis", "深度"
    };
    private static final String[] SECTION_KEYS_BREADTH = {
            "breadth", "width", "cross_domain", "industry", "crossDomain", "广度"
    };
    private static final String[] TERM_KEYS = {
            "term", "tag", "keyword", "insight_tag", "insightTerm", "insight_term", "name", "title", "术语", "概念"
    };
    private static final List<String> WRAPPER_KEYS = List.of(
            "result",
            "results",
            "output",
            "outputs",
            "data",
            "payload",
            "response",
            "responses",
            "answer",
            "answers",
            "advice",
            "analysis",
            "structured",
            "content",
            "json",
            "item",
            "items"
    );
    private static final List<String> LIST_CONTAINER_KEYS = List.of(
            "items",
            "results",
            "outputs",
            "list",
            "entries",
            "records",
            "rows"
    );
    private static final List<String> STRING_LIST_OBJECT_KEYS = List.of(
            "lines",
            "items",
            "list",
            "bullets",
            "points",
            "sentences",
            "entries",
            "values"
    );
    private static final List<String> STRING_VALUE_OBJECT_KEYS = List.of(
            "text",
            "content",
            "value",
            "line",
            "sentence",
            "desc",
            "description"
    );

    private static final String DEFAULT_SYSTEM_PROMPT = String.join("\n",
            "你是阅读场景下的语境解释助手。",
            "你的任务是解释“被选中的词或句子”在当前段落中的具体含义。",
            "禁止脱离段落语境给出词典式、百科式定义。",
            "必须基于上下文线索给出可验证的解释，并指出边界条件。",
            "输出要简洁，可直接用于笔记补充。");

    private static final String DEFAULT_USER_PROMPT = String.join("\n",
            "被解释文本：{term}",
            "解释模式：{scenario}",
            "段落上下文：",
            "{context_block}",
            "锚点句（优先参考）：",
            "{example_block}",
            "",
            "请严格输出 3 条中文 bullet：",
            "1) 本段含义：该词/句在当前段落具体指什么。",
            "2) 推理线索：你依据了哪些上下文证据，为什么这样解释。",
            "3) 边界提醒：在什么条件下该解释会失效或被误读。",
            "",
            "硬性约束：",
            "- 不要输出标题、前言、总结。",
            "- 不要写脱离语境的通用术语定义。",
            "- 优先控制在 120 字以内。");
    private static final String DEFAULT_STRUCTURED_SYSTEM_PROMPT = String.join("\n",
            "你是一个阅读语境术语解释助手。",
            "你必须只输出 JSON 对象，禁止输出 markdown、解释文本、代码围栏。",
            "JSON schema 固定为：",
            "{",
            "  \"background\": [\"...\"],",
            "  \"contextual_explanations\": [\"...\"],",
            "  \"depth\": [\"...\"],",
            "  \"breadth\": [\"...\"]",
            "}",
            "四个数组都必须存在，每个数组 1~3 条短句。"
    );

    private static final String DEFAULT_STRUCTURED_USER_PROMPT = String.join("\n",
            "术语：{term}",
            "模式：{scenario}",
            "语境段落：{context_block}",
            "锚点句：{example_block}",
            "",
            "请输出 JSON：",
            "1. background: 背景知识与落地背景（数组）",
            "2. contextual_explanations: 语境化解释（数组）",
            "3. depth: 第一性原理与机制（数组）",
            "4. breadth: 跨场景/行业广度（数组）",
            "",
            "要求：",
            "- 仅输出一个 JSON 对象",
            "- 不要输出任何额外文本"
    );
    private static final String DEFAULT_STRUCTURED_BATCH_SYSTEM_PROMPT = String.join("\n",
            "你是一个阅读语境术语解释助手。",
            "你必须只输出 JSON 对象，禁止输出 markdown、解释文本、代码围栏。",
            "JSON schema 固定为：",
            "{",
            "  \"items\": [",
            "    {",
            "      \"term\": \"术语原文\",",
            "      \"background\": [\"...\"],",
            "      \"contextual_explanations\": [\"...\"],",
            "      \"depth\": [\"...\"],",
            "      \"breadth\": [\"...\"]",
            "    }",
            "  ]",
            "}",
            "items 覆盖全部术语，且每个术语的四个数组都必须存在。"
    );
    private static final String DEFAULT_STRUCTURED_BATCH_USER_PROMPT = String.join("\n",
            "术语列表（同一语境）：",
            "{terms_block}",
            "模式：{scenario}",
            "语境段落：{context_block}",
            "锚点句（共享）：{example_block}",
            "",
            "请输出一个 JSON 对象，格式固定为：",
            "{",
            "  \"items\": [",
            "    {",
            "      \"term\": \"术语原文\",",
            "      \"background\": [\"...\"],",
            "      \"contextual_explanations\": [\"...\"],",
            "      \"depth\": [\"...\"],",
            "      \"breadth\": [\"...\"]",
            "    }",
            "  ]",
            "}",
            "",
            "要求：",
            "- items 覆盖全部术语，顺序与输入保持一致",
            "- term 必须与输入术语原文一致",
            "- background/contextual_explanations/depth/breadth 四个数组都必须存在，且每个数组 1~3 条短句",
            "- 仅输出 JSON，不得输出其他文本"
    );
    private static final String DEFAULT_PHASE2B_STRUCTURED_SYSTEM_PROMPT = String.join("\n",
            "你是文本结构化助手。",
            "请将输入的一段或多段文字，整理为清晰、逻辑严密的 Markdown 文本，适配 Obsidian 知识笔记。",
            "仅基于给定文本改写，不补充外部事实。",
            "直接输出 Markdown，不输出 JSON 或代码块。"
    );
    private static final String DEFAULT_PHASE2B_STRUCTURED_USER_PROMPT = String.join("\n",
            "## 原始文本",
            "{body_text}",
            "",
            "请输出结构化 Markdown。"
    );
    private static final String DEFAULT_PHASE2B_BLEND_SYSTEM_PROMPT = String.join("\n",
            "你是多源融合重写助手。",
            "你会收到两个输入流：输入流A是参考信源正文，输入流B是用户指令。",
            "你必须严格执行输入流B中的用户指令，并在需要时引用输入流A中的证据。",
            "若输入流A包含图片 Markdown 标记（如 ![...](...) 或 ![[...]]），必须保持其顺序和相对位置，不得改写路径。",
            "输出必须是结构化 Markdown，不输出 JSON 或代码块。"
    );

    private static final String PHASE2B_IMAGE_MARKER_CONSTRAINTS = String.join("\n",
            "## Image Marker Hard Constraints",
            "- If the input contains Markdown image markers such as `![alt](url)` or `![[path]]`, keep every marker exactly as-is.",
            "- Do not rewrite image path, alt text, filename, bracket shape, order, or relative position.",
            "- Do not delete, merge, duplicate, relocate, or convert image markers.",
            "- If exact preservation is difficult, leave the original image marker untouched and continue restructuring only the surrounding text."
    );

    @Value("${deepseek.advisor.enabled:true}")
    private boolean advisorEnabled;

    @Value("${deepseek.advisor.base-url:https://api.deepseek.com/v1}")
    private String advisorBaseUrl;

    @Value("${deepseek.advisor.model:deepseek-reasoner}")
    private String advisorModel;

    @Value("${deepseek.advisor.timeout-seconds:240}")
    private int timeoutSeconds;

    @Value("${deepseek.advisor.connect-timeout-seconds:20}")
    private int connectTimeoutSeconds;

    @Value("${deepseek.advisor.structured-max-tokens:8000}")
    private int structuredMaxTokens;

    @Value("${deepseek.advisor.phase2b-max-tokens:8000}")
    private int phase2bMaxTokens;

    @Value("${deepseek.advisor.phase2b-log-raw-markdown:false}")
    private boolean phase2bLogRawMarkdown;

    @Value("${deepseek.advisor.phase2b-provider-max-retries:3}")
    private int phase2bProviderMaxRetries = PHASE2B_PROVIDER_MAX_RETRIES_DEFAULT;

    @Value("${deepseek.advisor.phase2b-provider-initial-backoff-ms:2000}")
    private long phase2bProviderInitialBackoffMs = PHASE2B_PROVIDER_INITIAL_BACKOFF_MS_DEFAULT;

    @Value("${deepseek.advisor.phase2b-provider-max-backoff-ms:16000}")
    private long phase2bProviderMaxBackoffMs = PHASE2B_PROVIDER_MAX_BACKOFF_MS_DEFAULT;

    @Value("${deepseek.advisor.phase2b-provider-jitter-ratio:0.2}")
    private double phase2bProviderJitterRatio = PHASE2B_PROVIDER_JITTER_RATIO_DEFAULT;

    @Value("${deepseek.advisor.qwen-fallback.enabled:${MODULE2_DEEPSEEK_QWEN_FALLBACK_ENABLED:true}}")
    private boolean phase2bQwenFallbackEnabled = true;

    @Value("${deepseek.advisor.qwen-fallback.base-url:${MODULE2_DEEPSEEK_QWEN_FALLBACK_BASE_URL:https://dashscope.aliyuncs.com/compatible-mode/v1}}")
    private String phase2bQwenFallbackBaseUrl = "https://dashscope.aliyuncs.com/compatible-mode/v1";

    @Value("${deepseek.advisor.qwen-fallback.model:${MODULE2_DEEPSEEK_QWEN_FALLBACK_MODEL:qwen-plus}}")
    private String phase2bQwenFallbackModel = "qwen-plus";

    @Value("${deepseek.advisor.qwen-fallback.api-key-env:${MODULE2_DEEPSEEK_QWEN_FALLBACK_API_KEY_ENV:DASHSCOPE_API_KEY}}")
    private String phase2bQwenFallbackApiKeyEnv = "DASHSCOPE_API_KEY";

    @Value("${deepseek.advisor.qwen-fallback.api-key:${MODULE2_DEEPSEEK_QWEN_FALLBACK_API_KEY:}}")
    private String phase2bQwenFallbackApiKey = "";

    @Value("${DEEPSEEK_API_KEY:}")
    private String apiKey;

    @Value("${deepseek.advisor.prompt.system-resource:classpath:prompts/deepseek-advisor/system-zh.txt}")
    private Resource systemPromptResource;

    @Value("${deepseek.advisor.prompt.user-resource:classpath:prompts/deepseek-advisor/user-zh.txt}")
    private Resource userPromptResource;

    @Value("${deepseek.advisor.prompt.structured-system-resource:classpath:prompts/deepseek-advisor/structured-system-zh.txt}")
    private Resource structuredSystemPromptResource;

    @Value("${deepseek.advisor.prompt.structured-user-resource:classpath:prompts/deepseek-advisor/structured-user-zh.txt}")
    private Resource structuredUserPromptResource;

    @Value("${deepseek.advisor.prompt.phase2b-structured-system-resource:classpath:prompts/ai-structrued/structured_system.md}")
    private Resource phase2bStructuredSystemPromptResource;

    @Value("${deepseek.advisor.prompt.phase2b-structured-user-resource:classpath:prompts/ai-structrued/structured_user.md}")
    private Resource phase2bStructuredUserPromptResource;

    @Value("${deepseek.advisor.prompt.phase2b-blend-system-resource:classpath:prompts/ai-structrued/blend_system.md}")
    private Resource phase2bBlendSystemPromptResource;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Object httpClientLock = new Object();
    private final Object llmStackLock = new Object();
    private volatile HttpClient httpClient;
    private volatile LlmClient llmClient;
    private volatile LlmGateway llmGateway;
    private final Map<String, String> promptTemplateCache = new ConcurrentHashMap<>();

    public AdviceResult requestAdvice(String term, String context, boolean contextDependent) {
        return requestAdvice(term, context, "", contextDependent);
    }

    public AdviceResult requestAdvice(String term, String context, String contextExample, boolean contextDependent) {
        String safeTerm = String.valueOf(term == null ? "" : term).trim();
        if (safeTerm.isEmpty()) {
            throw new IllegalArgumentException("term cannot be empty");
        }
        String safeContext = String.valueOf(context == null ? "" : context).trim();
        String safeContextExample = String.valueOf(contextExample == null ? "" : contextExample).trim();
        ensureAdvisorPrimaryConfigured();
        LlmGatewayResult gatewayResult;
        try {
            gatewayResult = resolveLlmGateway().execute(
                    new LlmPromptRequest(
                            buildSystemPrompt(),
                            buildUserPrompt(safeTerm, safeContext, safeContextExample, contextDependent),
                            0.35,
                            320,
                            false
                    ),
                    buildAdvisorFallbackStrategy(),
                    buildAdvisorRetryPolicy(),
                    false,
                    null
            );
        } catch (Exception ex) {
            Throwable cause = resolveAdvisorFailureCause(ex);
            throw new IllegalStateException("DeepSeek advisor call failed: " + resolveAdvisorFailureMessage(ex), cause);
        }
        String content = String.valueOf(gatewayResult.content == null ? "" : gatewayResult.content).trim();
        if (!StringUtils.hasText(content)) {
            return AdviceResult.empty(resolveProviderSource(gatewayResult, "empty"));
        }
        return new AdviceResult(content, resolveProviderKey(gatewayResult));
    }

    public StructuredAdviceResult requestStructuredAdvice(String term, String context, String contextExample, boolean contextDependent) {
        String safeTerm = String.valueOf(term == null ? "" : term).trim();
        if (safeTerm.isEmpty()) {
            throw new IllegalArgumentException("term cannot be empty");
        }
        String safeContext = String.valueOf(context == null ? "" : context).trim();
        String safeContextExample = String.valueOf(contextExample == null ? "" : contextExample).trim();

        ensureAdvisorPrimaryConfigured();
        LlmGatewayResult callResult;
        String structuredSystemPrompt = buildStructuredSystemPrompt();
        String structuredUserPrompt = buildStructuredUserPrompt(safeTerm, safeContext, safeContextExample, contextDependent);
        try {
            callResult = callStructuredWithRetry(
                    structuredSystemPrompt,
                    structuredUserPrompt,
                    Math.max(256, structuredMaxTokens)
            );
        } catch (Exception ex) {
            Throwable cause = resolveAdvisorFailureCause(ex);
            throw new IllegalStateException("DeepSeek structured advisor call failed: " + resolveAdvisorFailureMessage(ex), cause);
        }
        String providerKey = resolveProviderKey(callResult);
        String raw = String.valueOf(callResult.content == null ? "" : callResult.content).trim();
        if (isFinishReasonLength(callResult.response.finishReason)) {
            return StructuredAdviceResult.empty(
                    providerKey + "-truncated",
                    raw,
                    callResult.response.requestPayloadJson,
                    callResult.response.responseBodyJson
            );
        }
        if (!StringUtils.hasText(raw)) {
            return StructuredAdviceResult.empty(
                    providerKey + "-empty",
                    "",
                    callResult.response.requestPayloadJson,
                    callResult.response.responseBodyJson
            );
        }

        StructuredAdviceResult parsed = parseStructuredAdvice(raw);
        if (parsed.hasContent()) {
            return new StructuredAdviceResult(
                    parsed.background,
                    parsed.contextualExplanations,
                    parsed.depth,
                    parsed.breadth,
                    providerKey,
                    raw,
                    callResult.response.requestPayloadJson,
                    callResult.response.responseBodyJson
            );
        }
        return StructuredAdviceResult.empty(
                providerKey + "-parse-empty",
                raw,
                callResult.response.requestPayloadJson,
                callResult.response.responseBodyJson
        );
    }

    public String requestPhase2bStructuredMarkdown(String bodyText, String filterRequirement) {
        return requestPhase2bStructuredMarkdownResult(bodyText, filterRequirement, false).markdown;
    }

    public String requestPhase2bStructuredMarkdown(String bodyText, String filterRequirement, boolean blendMode) {
        return requestPhase2bStructuredMarkdownResult(bodyText, filterRequirement, blendMode).markdown;
    }

    public Phase2bMarkdownResult requestPhase2bStructuredMarkdownResult(
            String bodyText,
            String filterRequirement,
            boolean blendMode
    ) {
        String safeBody = String.valueOf(bodyText == null ? "" : bodyText).trim();
        if (safeBody.isEmpty()) {
            throw new IllegalArgumentException("bodyText cannot be empty");
        }
        if (!advisorEnabled) {
            throw new IllegalStateException("deepseek.advisor.enabled=false");
        }
        return executePhase2bStructuredMarkdown(
                safeBody,
                blendMode,
                null,
                false
        );
    }

    public String requestPhase2bStructuredMarkdownStreamed(
            String bodyText,
            String filterRequirement,
            boolean blendMode,
            Consumer<String> onDelta
    ) {
        return requestPhase2bStructuredMarkdownStreamedResult(bodyText, filterRequirement, blendMode, onDelta).markdown;
    }

    public Phase2bMarkdownResult requestPhase2bStructuredMarkdownStreamedResult(
            String bodyText,
            String filterRequirement,
            boolean blendMode,
            Consumer<String> onDelta
    ) {
        String safeBody = String.valueOf(bodyText == null ? "" : bodyText).trim();
        if (safeBody.isEmpty()) {
            throw new IllegalArgumentException("bodyText cannot be empty");
        }
        if (!advisorEnabled) {
            throw new IllegalStateException("deepseek.advisor.enabled=false");
        }
        return executePhase2bStructuredMarkdown(
                safeBody,
                blendMode,
                onDelta,
                true
        );
    }

    private Phase2bMarkdownResult executePhase2bStructuredMarkdown(
            String safeBody,
            boolean blendMode,
            Consumer<String> onDelta,
            boolean streamRequested
    ) {
        String systemPrompt = blendMode
                ? buildPhase2bBlendSystemPrompt()
                : buildPhase2bStructuredSystemPrompt();
        String userPrompt = buildPhase2bStructuredUserPrompt(safeBody);
        LlmGatewayResult gatewayResult;
        try {
            gatewayResult = resolveLlmGateway().execute(
                    new LlmPromptRequest(systemPrompt, userPrompt, 0.2, Math.max(streamRequested ? 4096 : 512, phase2bMaxTokens), false),
                    new LlmFallbackStrategy(buildPrimaryLlmProvider(), resolvePhase2bQwenFallbackProvider()),
                    buildPhase2bRetryPolicy(),
                    streamRequested,
                    onDelta
            );
        } catch (Exception ex) {
            throw new IllegalStateException("Phase2b provider chain failed: " + ex.getMessage(), ex);
        }
        String rawMarkdown = String.valueOf(gatewayResult.content == null ? "" : gatewayResult.content).trim();
        String finalMarkdown = streamRequested ? rawMarkdown : normalizePhase2bListIndentation(rawMarkdown);
        logPhase2bRawMarkdownIfEnabled(
                (streamRequested ? "stream" : "sync") + "-" + gatewayResult.provider.resolveProviderKey(),
                finalMarkdown
        );
        return new Phase2bMarkdownResult(
                finalMarkdown,
                buildPhase2bSource(gatewayResult.provider.resolveProviderKey(), blendMode),
                gatewayResult.provider.resolveProviderKey(),
                gatewayResult.degraded
        );
    }

    private LlmProviderConfig buildPrimaryLlmProvider() {
        return new LlmProviderConfig(
                "DeepSeek",
                "deepseek",
                normalizeVersionedProviderBaseUrl(advisorBaseUrl),
                advisorModel,
                apiKey,
                DeepSeekModelRouter::resolveModel
        );
    }

    private LlmProviderConfig resolvePhase2bQwenFallbackProvider() {
        if (!phase2bQwenFallbackEnabled) {
            return null;
        }
        String fallbackApiKey = resolvePhase2bQwenFallbackApiKey();
        if (!StringUtils.hasText(fallbackApiKey)) {
            logger.warn(
                    "[phase2b-degrade] Qwen fallback is enabled but api key is missing: api_key_env={}",
                    StringUtils.hasText(phase2bQwenFallbackApiKeyEnv) ? phase2bQwenFallbackApiKeyEnv.trim() : "DASHSCOPE_API_KEY"
            );
            return null;
        }
        return new LlmProviderConfig(
                "Qwen",
                "qwen",
                phase2bQwenFallbackBaseUrl,
                phase2bQwenFallbackModel,
                fallbackApiKey,
                UnaryOperator.identity()
        );
    }

    private String resolvePhase2bQwenFallbackApiKey() {
        String explicitApiKey = String.valueOf(phase2bQwenFallbackApiKey == null ? "" : phase2bQwenFallbackApiKey).trim();
        if (StringUtils.hasText(explicitApiKey)) {
            return explicitApiKey;
        }
        String envName = StringUtils.hasText(phase2bQwenFallbackApiKeyEnv)
                ? phase2bQwenFallbackApiKeyEnv.trim()
                : "DASHSCOPE_API_KEY";
        String fromProperty = String.valueOf(System.getProperty(envName, "")).trim();
        if (StringUtils.hasText(fromProperty)) {
            return fromProperty;
        }
        String fromEnv = System.getenv(envName);
        return String.valueOf(fromEnv == null ? "" : fromEnv).trim();
    }

    private String buildPhase2bSource(String providerKey, boolean blendMode) {
        String prefix = StringUtils.hasText(providerKey) ? providerKey.trim() : "deepseek";
        return blendMode ? prefix + ".phase2b.blend" : prefix + ".phase2b";
    }

    private String normalizeVersionedProviderBaseUrl(String rawBaseUrl) {
        String endpoint = String.valueOf(rawBaseUrl == null ? "" : rawBaseUrl).trim();
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        if (endpoint.isEmpty()) {
            throw new IllegalStateException("deepseek.advisor.base-url is empty");
        }
        if (!endpoint.matches("(?i).*/v\\d+$")) {
            endpoint = endpoint + "/v1";
        }
        return endpoint;
    }

    private boolean isRetryablePhase2bException(Throwable ex) {
        Throwable cursor = ex;
        while (cursor != null) {
            if (cursor instanceof InterruptedException) {
                return false;
            }
            if (cursor instanceof HttpTimeoutException || cursor instanceof ConnectException || cursor instanceof IOException) {
                return true;
            }
            String text = String.valueOf(cursor.getMessage() == null ? "" : cursor.getMessage()).toLowerCase(Locale.ROOT);
            if (text.contains("http 400")
                    || text.contains("http 401")
                    || text.contains("http 403")
                    || text.contains("http 404")
                    || text.contains("api key is empty")
                    || text.contains("model is empty")
                    || text.contains("model_not_found")) {
                return false;
            }
            if (text.contains("http 408")
                    || text.contains("http 409")
                    || text.contains("http 425")
                    || text.contains("http 429")
                    || text.contains("http 500")
                    || text.contains("http 502")
                    || text.contains("http 503")
                    || text.contains("http 504")
                    || text.contains("timeout")
                    || text.contains("timed out")
                    || text.contains("connection reset")
                    || text.contains("broken pipe")
                    || text.contains("unexpected end")
                    || text.contains("eof")
                    || text.contains("closed")) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    public Map<String, StructuredAdviceResult> requestStructuredAdviceBatch(
            List<String> terms,
            String context,
            String contextExample,
            boolean contextDependent
    ) {
        List<String> safeTerms = normalizeTerms(terms);
        if (safeTerms.isEmpty()) {
            throw new IllegalArgumentException("terms cannot be empty");
        }
        if (safeTerms.size() == 1) {
            String term = safeTerms.get(0);
            StructuredAdviceResult single = requestStructuredAdvice(term, context, contextExample, contextDependent);
            return Map.of(term, single);
        }
        ensureAdvisorPrimaryConfigured();
        String safeContext = String.valueOf(context == null ? "" : context).trim();
        String safeContextExample = String.valueOf(contextExample == null ? "" : contextExample).trim();

        LlmGatewayResult callResult;
        String structuredSystemPrompt = buildStructuredBatchSystemPrompt();
        String structuredUserPrompt = buildStructuredBatchUserPrompt(
                safeTerms,
                safeContext,
                safeContextExample,
                contextDependent
        );
        try {
            callResult = callStructuredWithRetry(
                    structuredSystemPrompt,
                    structuredUserPrompt,
                    Math.max(256, structuredMaxTokens),
                    true
            );
        } catch (Exception ex) {
            Throwable cause = resolveAdvisorFailureCause(ex);
            throw new IllegalStateException("DeepSeek structured batch advisor call failed: " + resolveAdvisorFailureMessage(ex), cause);
        }
        String providerKey = resolveProviderKey(callResult);
        String raw = String.valueOf(callResult.content == null ? "" : callResult.content).trim();
        if (isFinishReasonLength(callResult.response.finishReason)) {
            return buildBatchFallbackResults(
                    safeTerms,
                    providerKey + "-batch-truncated",
                    raw,
                    callResult.response.requestPayloadJson,
                    callResult.response.responseBodyJson
            );
        }
        if (!StringUtils.hasText(raw)) {
            return buildBatchFallbackResults(
                    safeTerms,
                    providerKey + "-batch-empty",
                    "",
                    callResult.response.requestPayloadJson,
                    callResult.response.responseBodyJson
            );
        }
        Map<String, StructuredAdviceResult> parsed = parseStructuredAdviceBatch(
                raw,
                callResult.response.requestPayloadJson,
                callResult.response.responseBodyJson
        );
        if (parsed.isEmpty()) {
            return buildBatchFallbackResults(
                    safeTerms,
                    providerKey + "-batch-parse-empty",
                    raw,
                    callResult.response.requestPayloadJson,
                    callResult.response.responseBodyJson
            );
        }
        LinkedHashMap<String, StructuredAdviceResult> resolved = new LinkedHashMap<>();
        for (String term : safeTerms) {
            String key = normalizeTermKey(term);
            StructuredAdviceResult result = parsed.get(key);
            if (result == null || !result.hasContent()) {
                result = StructuredAdviceResult.empty(
                        providerKey + "-batch-miss",
                        raw,
                        callResult.response.requestPayloadJson,
                        callResult.response.responseBodyJson
                );
            }
            resolved.put(term, result);
        }
        return resolved;
    }

    private LlmGatewayResult callStructuredWithRetry(
            String systemPrompt,
            String userPrompt,
            int initialMaxTokens
    ) throws Exception {
        return callStructuredWithRetry(systemPrompt, userPrompt, initialMaxTokens, false);
    }

    private LlmGatewayResult callStructuredWithRetry(
            String systemPrompt,
            String userPrompt,
            int initialMaxTokens,
            boolean forceJsonObject
    ) throws Exception {
        int attempt = 0;
        int currentMaxTokens = Math.max(256, initialMaxTokens);
        LlmGatewayResult lastResult = null;
        while (attempt < STRUCTURED_RETRY_MAX_ATTEMPTS) {
            attempt += 1;
            lastResult = resolveLlmGateway().execute(
                    new LlmPromptRequest(systemPrompt, userPrompt, 0.2, currentMaxTokens, forceJsonObject),
                    buildAdvisorFallbackStrategy(),
                    buildAdvisorRetryPolicy(),
                    false,
                    null
            );
            if (!isFinishReasonLength(lastResult.response.finishReason)) {
                return lastResult;
            }
            int next = Math.min(16000, Math.max(currentMaxTokens * 2, currentMaxTokens + 1024));
            if (next <= currentMaxTokens) {
                break;
            }
            currentMaxTokens = next;
        }
        return lastResult != null ? lastResult : new LlmGatewayResult("", buildPrimaryLlmProvider(), new LlmResponse("", "", "", ""), false, false, 0);
    }

    private boolean isFinishReasonLength(String finishReason) {
        return "length".equalsIgnoreCase(String.valueOf(finishReason == null ? "" : finishReason).trim());
    }

    private HttpClient resolveHttpClient() {
        HttpClient client = httpClient;
        if (client != null) {
            return client;
        }
        synchronized (httpClientLock) {
            if (httpClient == null) {
                httpClient = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(resolveConnectTimeoutSeconds()))
                        .build();
            }
            return httpClient;
        }
    }

    private int resolveConnectTimeoutSeconds() {
        int value = connectTimeoutSeconds;
        if (value <= 0) {
            return 1;
        }
        return Math.min(value, 120);
    }

    private LlmClient resolveLlmClient() {
        LlmClient client = llmClient;
        if (client != null) {
            return client;
        }
        synchronized (llmStackLock) {
            if (llmClient == null) {
                llmClient = new OpenAiCompatibleLlmClient(
                        objectMapper,
                        this::resolveHttpClient,
                        () -> timeoutSeconds
                );
            }
            return llmClient;
        }
    }

    private LlmGateway resolveLlmGateway() {
        LlmGateway gateway = llmGateway;
        if (gateway != null) {
            return gateway;
        }
        synchronized (llmStackLock) {
            if (llmGateway == null) {
                llmGateway = new LlmGateway(resolveLlmClient());
            }
            return llmGateway;
        }
    }

    private LlmRetryPolicy buildPhase2bRetryPolicy() {
        return new LlmRetryPolicy(
                phase2bProviderMaxRetries,
                phase2bProviderInitialBackoffMs,
                phase2bProviderMaxBackoffMs,
                phase2bProviderJitterRatio,
                this::isRetryablePhase2bException
        );
    }

    private LlmRetryPolicy buildAdvisorRetryPolicy() {
        return new LlmRetryPolicy(
                Math.max(0, STRUCTURED_RETRY_MAX_ATTEMPTS - 1),
                NETWORK_RETRY_INITIAL_BACKOFF_MS,
                NETWORK_RETRY_MAX_BACKOFF_MS,
                0d,
                this::isRetryableNetworkException
        );
    }

    private LlmFallbackStrategy buildAdvisorFallbackStrategy() {
        return new LlmFallbackStrategy(buildPrimaryLlmProvider(), null);
    }

    private void ensureAdvisorPrimaryConfigured() {
        if (!advisorEnabled) {
            throw new IllegalStateException("deepseek.advisor.enabled=false");
        }
        if (!StringUtils.hasText(apiKey)) {
            throw new IllegalStateException("DEEPSEEK_API_KEY is empty");
        }
    }

    private boolean isRetryableNetworkException(Throwable ex) {
        Throwable cursor = ex;
        while (cursor != null) {
            if (cursor instanceof HttpTimeoutException || cursor instanceof ConnectException) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    private String resolveAdvisorFailureMessage(Throwable ex) {
        String lastNonBlankMessage = "";
        Throwable cursor = ex;
        while (cursor != null) {
            String message = String.valueOf(cursor.getMessage() == null ? "" : cursor.getMessage()).trim();
            if (StringUtils.hasText(message)) {
                lastNonBlankMessage = message;
            }
            cursor = cursor.getCause();
        }
        return lastNonBlankMessage;
    }

    private Throwable resolveAdvisorFailureCause(Throwable ex) {
        Throwable last = ex;
        Throwable cursor = ex;
        while (cursor != null) {
            last = cursor;
            cursor = cursor.getCause();
        }
        return last;
    }

    private String resolveProviderKey(LlmGatewayResult gatewayResult) {
        if (gatewayResult == null || gatewayResult.provider == null) {
            return "deepseek";
        }
        return gatewayResult.provider.resolveProviderKey();
    }

    private String resolveProviderSource(LlmGatewayResult gatewayResult, String suffix) {
        String providerKey = resolveProviderKey(gatewayResult);
        String normalizedSuffix = String.valueOf(suffix == null ? "" : suffix).trim();
        if (!StringUtils.hasText(normalizedSuffix)) {
            return providerKey;
        }
        return providerKey + "-" + normalizedSuffix;
    }

    /**
     * 兜底修复 Phase2B 列表缩进：若模型只输出 1 空格子级缩进，统一补齐为 4 空格。
     * 仅处理代码围栏外且“看起来是列表项”的行，避免影响正文与代码块。
     */
    private String normalizePhase2bListIndentation(String markdown) {
        String source = String.valueOf(markdown == null ? "" : markdown);
        if (!StringUtils.hasText(source)) {
            return source;
        }
        String normalized = source.replace("\r\n", "\n").replace('\r', '\n');
        String[] lines = normalized.split("\n", -1);
        boolean inFence = false;
        boolean previousLineIsListItem = false;
        boolean changed = false;

        for (int i = 0; i < lines.length; i += 1) {
            String line = lines[i];
            String trimmedLeft = trimLeadingWhitespace(line);
            if (isFenceLine(trimmedLeft)) {
                inFence = !inFence;
                previousLineIsListItem = false;
                continue;
            }
            if (inFence) {
                continue;
            }
            if (trimmedLeft.isEmpty()) {
                previousLineIsListItem = false;
                continue;
            }

            int leadingSpaces = countLeadingSpaces(line);
            boolean lineIsListItem = isLikelyMarkdownListItem(trimmedLeft);
            boolean lineIsListContinuation = previousLineIsListItem && leadingSpaces == 1;
            if (leadingSpaces == 1 && (lineIsListItem || lineIsListContinuation)) {
                lines[i] = PHASE2B_CHILD_INDENT + line.substring(1);
                changed = true;
                trimmedLeft = trimLeadingWhitespace(lines[i]);
                lineIsListItem = isLikelyMarkdownListItem(trimmedLeft);
            }
            previousLineIsListItem = lineIsListItem;
        }

        if (!changed) {
            return source;
        }
        return String.join("\n", lines);
    }

    private String trimLeadingWhitespace(String value) {
        String text = String.valueOf(value == null ? "" : value);
        int index = 0;
        while (index < text.length()) {
            char ch = text.charAt(index);
            if (ch != ' ' && ch != '\t') {
                break;
            }
            index += 1;
        }
        if (index <= 0) {
            return text;
        }
        return text.substring(index);
    }

    private boolean isFenceLine(String trimmedLeftLine) {
        String text = String.valueOf(trimmedLeftLine == null ? "" : trimmedLeftLine);
        return text.startsWith("```") || text.startsWith("~~~");
    }

    private boolean isLikelyMarkdownListItem(String trimmedLeftLine) {
        String text = String.valueOf(trimmedLeftLine == null ? "" : trimmedLeftLine);
        if (text.isEmpty()) {
            return false;
        }
        char first = text.charAt(0);
        if (first == '-' || first == '+' || first == '*') {
            return true;
        }
        if (!Character.isDigit(first)) {
            return false;
        }
        int index = 1;
        while (index < text.length() && Character.isDigit(text.charAt(index))) {
            index += 1;
        }
        if (index >= text.length()) {
            return false;
        }
        char separator = text.charAt(index);
        return separator == '.' || separator == ')';
    }

    private void logPhase2bRawMarkdownIfEnabled(String mode, String rawMarkdown) {
        if (!phase2bLogRawMarkdown || !logger.isDebugEnabled()) {
            return;
        }
        String source = String.valueOf(rawMarkdown == null ? "" : rawMarkdown);
        String normalized = source.replace("\r\n", "\n").replace('\r', '\n');
        String[] lines = normalized.split("\n", -1);
        int totalLines = lines.length;
        int emitLines = Math.min(PHASE2B_RAW_LOG_MAX_LINES, totalLines);
        StringBuilder builder = new StringBuilder(Math.min(131072, normalized.length() + 2048));
        builder.append("phase2b.llm.raw mode=")
                .append(String.valueOf(mode == null ? "" : mode))
                .append(" chars=")
                .append(source.length())
                .append(" lines=")
                .append(totalLines)
                .append('\n');
        builder.append("----- RAW BEGIN -----\n");
        builder.append(normalized);
        if (!normalized.endsWith("\n")) {
            builder.append('\n');
        }
        builder.append("----- RAW END -----\n");
        builder.append("----- INDENT PROBE BEGIN -----\n");
        for (int i = 0; i < emitLines; i += 1) {
            String line = lines[i];
            int leadingSpaces = countLeadingSpaces(line);
            int leadingTabs = countLeadingTabs(line);
            String visible = line.replace("\t", "\\t").replace(" ", "·");
            if (visible.length() > PHASE2B_RAW_LOG_MAX_LINE_CHARS) {
                visible = visible.substring(0, PHASE2B_RAW_LOG_MAX_LINE_CHARS) + "...(truncated)";
            }
            builder.append(String.format(
                    Locale.ROOT,
                    "%04d sp=%d tab=%d |%s|%n",
                    i + 1,
                    leadingSpaces,
                    leadingTabs,
                    visible
            ));
        }
        if (emitLines < totalLines) {
            builder.append("... indent probe truncated at ")
                    .append(emitLines)
                    .append(" lines\n");
        }
        builder.append("----- INDENT PROBE END -----");
        logger.debug(builder.toString());
    }

    private int countLeadingSpaces(String line) {
        String text = String.valueOf(line == null ? "" : line);
        int count = 0;
        for (int i = 0; i < text.length(); i += 1) {
            char ch = text.charAt(i);
            if (ch == ' ') {
                count += 1;
                continue;
            }
            if (ch == '\t') {
                continue;
            }
            break;
        }
        return count;
    }

    private int countLeadingTabs(String line) {
        String text = String.valueOf(line == null ? "" : line);
        int count = 0;
        for (int i = 0; i < text.length(); i += 1) {
            char ch = text.charAt(i);
            if (ch == '\t') {
                count += 1;
                continue;
            }
            if (ch == ' ') {
                continue;
            }
            break;
        }
        return count;
    }

    private String buildSystemPrompt() {
        return loadPromptTemplate("system", systemPromptResource, DEFAULT_SYSTEM_PROMPT);
    }

    private String buildUserPrompt(String term, String context, String contextExample, boolean contextDependent) {
        String safeContext = trimContext(context);
        String safeExample = trimContext(contextExample);
        String scenario = contextDependent ? "段落绑定" : "全局语境";
        Map<String, String> values = new LinkedHashMap<>();
        values.put("term", term);
        values.put("scenario", scenario);
        values.put("context_block", safeContext.isEmpty() ? PROMPT_TEMPLATE_CONTEXT_EMPTY : safeContext);
        values.put("example_block", safeExample.isEmpty() ? PROMPT_TEMPLATE_CONTEXT_EMPTY : safeExample);
        return applyTemplate(
                loadPromptTemplate("user", userPromptResource, DEFAULT_USER_PROMPT),
                values
        );
    }

    private String buildStructuredSystemPrompt() {
        return loadPromptTemplate("structured_system", structuredSystemPromptResource, DEFAULT_STRUCTURED_SYSTEM_PROMPT);
    }

    private String buildStructuredBatchSystemPrompt() {
        return DEFAULT_STRUCTURED_BATCH_SYSTEM_PROMPT;
    }

    private String buildStructuredUserPrompt(String term, String context, String contextExample, boolean contextDependent) {
        String safeContext = trimContext(context);
        String safeExample = trimContext(contextExample);
        String scenario = contextDependent ? "段落绑定" : "全局语境";
        Map<String, String> values = new LinkedHashMap<>();
        values.put("term", term);
        values.put("scenario", scenario);
        values.put("context_block", safeContext.isEmpty() ? PROMPT_TEMPLATE_CONTEXT_EMPTY : safeContext);
        values.put("example_block", safeExample.isEmpty() ? PROMPT_TEMPLATE_CONTEXT_EMPTY : safeExample);
        return applyTemplate(
                loadPromptTemplate("structured_user", structuredUserPromptResource, DEFAULT_STRUCTURED_USER_PROMPT),
                values
        );
    }

    private String buildStructuredBatchUserPrompt(
            List<String> terms,
            String context,
            String contextExample,
            boolean contextDependent
    ) {
        String safeContext = trimContext(context);
        String safeExample = trimContext(contextExample);
        String scenario = contextDependent ? "段落绑定" : "全局语境";
        String termsBlock = renderTermsBlock(terms);
        Map<String, String> values = new LinkedHashMap<>();
        values.put("terms_block", StringUtils.hasText(termsBlock) ? termsBlock : PROMPT_TEMPLATE_CONTEXT_EMPTY);
        values.put("scenario", scenario);
        values.put("context_block", safeContext.isEmpty() ? PROMPT_TEMPLATE_CONTEXT_EMPTY : safeContext);
        values.put("example_block", safeExample.isEmpty() ? PROMPT_TEMPLATE_CONTEXT_EMPTY : safeExample);
        return applyTemplate(DEFAULT_STRUCTURED_BATCH_USER_PROMPT, values);
    }

    private String buildPhase2bStructuredSystemPrompt() {
        return appendPhase2bImageMarkerConstraints(loadPromptTemplate(
                "phase2b_structured_system",
                phase2bStructuredSystemPromptResource,
                DEFAULT_PHASE2B_STRUCTURED_SYSTEM_PROMPT
        ));
    }

    private String buildPhase2bBlendSystemPrompt() {
        return appendPhase2bImageMarkerConstraints(loadPromptTemplate(
                "phase2b_blend_system",
                phase2bBlendSystemPromptResource,
                DEFAULT_PHASE2B_BLEND_SYSTEM_PROMPT
        ));
    }

    private String buildPhase2bStructuredUserPrompt(String bodyText) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("body_text", trimContext(bodyText));
        return applyTemplate(
                loadPromptTemplate(
                        "phase2b_structured_user",
                        phase2bStructuredUserPromptResource,
                        DEFAULT_PHASE2B_STRUCTURED_USER_PROMPT
                ),
                values
        );
    }

    private String appendPhase2bImageMarkerConstraints(String prompt) {
        String basePrompt = String.valueOf(prompt == null ? "" : prompt).trim();
        if (!StringUtils.hasText(basePrompt)) {
            return PHASE2B_IMAGE_MARKER_CONSTRAINTS;
        }
        if (basePrompt.contains("Image Marker Hard Constraints")) {
            return basePrompt;
        }
        return basePrompt + "\n\n" + PHASE2B_IMAGE_MARKER_CONSTRAINTS;
    }

    private String renderTermsBlock(List<String> terms) {
        if (terms == null || terms.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < terms.size(); index += 1) {
            String term = String.valueOf(terms.get(index) == null ? "" : terms.get(index)).trim();
            if (!StringUtils.hasText(term)) {
                continue;
            }
            builder.append(index + 1).append(". ").append(term).append('\n');
        }
        return builder.toString().trim();
    }

    private StructuredAdviceResult parseStructuredAdvice(String rawText) {
        String text = String.valueOf(rawText == null ? "" : rawText).trim();
        if (!StringUtils.hasText(text)) {
            return StructuredAdviceResult.empty("structured-empty", text);
        }
        boolean foundJsonCandidate = false;
        for (JsonNode node : parseJsonNodeCandidates(text)) {
            foundJsonCandidate = true;
            StructuredAdviceResult parsed = parseStructuredAdviceFromNode(node, text);
            if (parsed != null && parsed.hasContent()) {
                return parsed;
            }
        }
        List<String> background = parseLooseArrayByKey(text, SECTION_KEYS_BACKGROUND);
        List<String> contextual = parseLooseArrayByKey(text, SECTION_KEYS_CONTEXTUAL);
        List<String> depth = parseLooseArrayByKey(text, SECTION_KEYS_DEPTH);
        List<String> breadth = parseLooseArrayByKey(text, SECTION_KEYS_BREADTH);
        if (!background.isEmpty() || !contextual.isEmpty() || !depth.isEmpty() || !breadth.isEmpty()) {
            return new StructuredAdviceResult(background, contextual, depth, breadth, "deepseek", text, "", "");
        }
        return StructuredAdviceResult.empty(foundJsonCandidate ? "structured-parse-error" : "structured-no-json", text);
    }

    private Map<String, StructuredAdviceResult> parseStructuredAdviceBatch(
            String rawText,
            String requestPayloadJson,
            String responseBodyJson
    ) {
        String text = String.valueOf(rawText == null ? "" : rawText).trim();
        if (!StringUtils.hasText(text)) {
            return Map.of();
        }
        LinkedHashMap<String, StructuredAdviceResult> output = new LinkedHashMap<>();
        for (JsonNode node : parseJsonNodeCandidates(text)) {
            collectBatchStructuredAdvice(
                    output,
                    node,
                    "",
                    text,
                    requestPayloadJson,
                    responseBodyJson,
                    0
            );
        }
        return output;
    }

    private Object readAlias(Map<?, ?> source, String... aliases) {
        if (source == null || aliases == null || aliases.length == 0) {
            return null;
        }
        for (String alias : aliases) {
            if (!StringUtils.hasText(alias)) {
                continue;
            }
            if (source.containsKey(alias)) {
                return source.get(alias);
            }
        }
        return null;
    }

    private List<String> normalizeTerms(List<String> terms) {
        if (terms == null || terms.isEmpty()) {
            return List.of();
        }
        LinkedHashMap<String, String> ordered = new LinkedHashMap<>();
        for (String rawTerm : terms) {
            String term = normalizeTerm(rawTerm);
            if (!StringUtils.hasText(term)) {
                continue;
            }
            ordered.putIfAbsent(normalizeTermKey(term), term);
        }
        return new ArrayList<>(ordered.values());
    }

    private String normalizeTerm(String rawTerm) {
        return String.valueOf(rawTerm == null ? "" : rawTerm)
                .replace("\r\n", "\n")
                .replace('\r', '\n')
                .replace('\n', ' ')
                .replaceAll("\\s+", " ")
                .trim();
    }

    private String normalizeTermKey(String term) {
        return normalizeTerm(term).toLowerCase(Locale.ROOT);
    }

    private Map<String, StructuredAdviceResult> buildBatchFallbackResults(
            List<String> terms,
            String source,
            String raw,
            String requestPayloadJson,
            String responseBodyJson
    ) {
        if (terms == null || terms.isEmpty()) {
            return Map.of();
        }
        LinkedHashMap<String, StructuredAdviceResult> output = new LinkedHashMap<>();
        for (String term : terms) {
            String safeTerm = normalizeTerm(term);
            if (!StringUtils.hasText(safeTerm)) {
                continue;
            }
            output.put(
                    safeTerm,
                    StructuredAdviceResult.empty(source, raw, requestPayloadJson, responseBodyJson)
            );
        }
        return output;
    }

    private List<String> normalizeStringList(Object raw) {
        if (raw == null) {
            return List.of();
        }
        List<String> output = new ArrayList<>();
        if (raw instanceof List<?> list) {
            for (Object item : list) {
                String text = String.valueOf(item == null ? "" : item).trim();
                if (StringUtils.hasText(text)) {
                    output.add(trimContext(text));
                }
                if (output.size() >= 3) {
                    break;
                }
            }
            return output;
        }
        String text = String.valueOf(raw).trim();
        if (!StringUtils.hasText(text)) {
            return List.of();
        }
        if (text.startsWith("[") && text.endsWith("]")) {
            try {
                List<String> parsed = objectMapper.readValue(text, new TypeReference<List<String>>() {});
                return normalizeStringList(parsed);
            } catch (Exception ignored) {
                // noop
            }
        }
        output.add(trimContext(text));
        return output;
    }

    private StructuredAdviceResult parseStructuredAdviceFromNode(JsonNode rootNode, String rawText) {
        JsonNode sectionNode = resolveSectionCarrierNode(rootNode, 0);
        if (sectionNode == null || !sectionNode.isObject()) {
            return null;
        }
        List<String> background = readNodeListByAlias(sectionNode, SECTION_KEYS_BACKGROUND);
        List<String> contextual = readNodeListByAlias(sectionNode, SECTION_KEYS_CONTEXTUAL);
        List<String> depth = readNodeListByAlias(sectionNode, SECTION_KEYS_DEPTH);
        List<String> breadth = readNodeListByAlias(sectionNode, SECTION_KEYS_BREADTH);
        StructuredAdviceResult parsed = new StructuredAdviceResult(
                background,
                contextual,
                depth,
                breadth,
                "deepseek",
                rawText,
                "",
                ""
        );
        return parsed.hasContent() ? parsed : null;
    }

    private void collectBatchStructuredAdvice(
            Map<String, StructuredAdviceResult> output,
            JsonNode node,
            String termHint,
            String rawText,
            String requestPayloadJson,
            String responseBodyJson,
            int depth
    ) {
        if (output == null || node == null || node.isNull() || depth > 6) {
            return;
        }
        if (node.isTextual()) {
            String nestedRaw = String.valueOf(node.asText("")).trim();
            if (!StringUtils.hasText(nestedRaw) || (!nestedRaw.contains("{") && !nestedRaw.contains("["))) {
                return;
            }
            for (JsonNode nested : parseJsonNodeCandidates(nestedRaw)) {
                collectBatchStructuredAdvice(
                        output,
                        nested,
                        termHint,
                        rawText,
                        requestPayloadJson,
                        responseBodyJson,
                        depth + 1
                );
            }
            return;
        }
        if (node.isArray()) {
            for (JsonNode item : node) {
                collectBatchStructuredAdvice(
                        output,
                        item,
                        termHint,
                        rawText,
                        requestPayloadJson,
                        responseBodyJson,
                        depth + 1
                );
            }
            return;
        }
        if (!node.isObject()) {
            return;
        }
        StructuredAdviceResult itemResult = parseStructuredAdviceFromNode(node, rawText);
        String term = readNodeTextByAlias(node, TERM_KEYS);
        if (!StringUtils.hasText(term)) {
            term = normalizeTerm(termHint);
        }
        if (itemResult != null && itemResult.hasContent() && StringUtils.hasText(term)) {
            output.putIfAbsent(
                    normalizeTermKey(term),
                    StructuredAdviceResult.deepseek(
                            itemResult.background,
                            itemResult.contextualExplanations,
                            itemResult.depth,
                            itemResult.breadth,
                            rawText,
                            requestPayloadJson,
                            responseBodyJson
                    )
            );
        }

        for (String alias : LIST_CONTAINER_KEYS) {
            JsonNode listNode = findNodeByAliases(node, alias);
            if (listNode == null || listNode.isNull()) {
                continue;
            }
            collectBatchStructuredAdvice(
                    output,
                    listNode,
                    termHint,
                    rawText,
                    requestPayloadJson,
                    responseBodyJson,
                    depth + 1
            );
        }
        for (String alias : WRAPPER_KEYS) {
            JsonNode wrapped = findNodeByAliases(node, alias);
            if (wrapped == null || wrapped.isNull()) {
                continue;
            }
            collectBatchStructuredAdvice(
                    output,
                    wrapped,
                    termHint,
                    rawText,
                    requestPayloadJson,
                    responseBodyJson,
                    depth + 1
            );
        }

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            if (entry == null) {
                continue;
            }
            String key = normalizeTerm(entry.getKey());
            JsonNode value = entry.getValue();
            if (value == null || value.isNull()) {
                continue;
            }
            String nextHint = isWrapperKey(key) ? termHint : key;
            collectBatchStructuredAdvice(
                    output,
                    value,
                    nextHint,
                    rawText,
                    requestPayloadJson,
                    responseBodyJson,
                    depth + 1
            );
        }
    }

    private JsonNode resolveSectionCarrierNode(JsonNode node, int depth) {
        if (node == null || node.isNull() || depth > 6) {
            return null;
        }
        if (node.isTextual()) {
            String nestedRaw = String.valueOf(node.asText("")).trim();
            if (!StringUtils.hasText(nestedRaw) || (!nestedRaw.contains("{") && !nestedRaw.contains("["))) {
                return null;
            }
            for (JsonNode nested : parseJsonNodeCandidates(nestedRaw)) {
                JsonNode resolved = resolveSectionCarrierNode(nested, depth + 1);
                if (resolved != null) {
                    return resolved;
                }
            }
            return null;
        }
        if (node.isArray()) {
            for (JsonNode item : node) {
                JsonNode resolved = resolveSectionCarrierNode(item, depth + 1);
                if (resolved != null) {
                    return resolved;
                }
            }
            return null;
        }
        if (!node.isObject()) {
            return null;
        }
        if (containsSectionKey(node)) {
            return node;
        }

        for (String alias : WRAPPER_KEYS) {
            JsonNode wrapped = findNodeByAliases(node, alias);
            if (wrapped == null || wrapped.isNull()) {
                continue;
            }
            JsonNode resolved = resolveSectionCarrierNode(wrapped, depth + 1);
            if (resolved != null) {
                return resolved;
            }
        }

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            if (entry == null || entry.getValue() == null || entry.getValue().isNull()) {
                continue;
            }
            JsonNode resolved = resolveSectionCarrierNode(entry.getValue(), depth + 1);
            if (resolved != null) {
                return resolved;
            }
        }
        return null;
    }

    private boolean containsSectionKey(JsonNode node) {
        if (node == null || !node.isObject()) {
            return false;
        }
        return findNodeByAliases(node, SECTION_KEYS_BACKGROUND) != null
                || findNodeByAliases(node, SECTION_KEYS_CONTEXTUAL) != null
                || findNodeByAliases(node, SECTION_KEYS_DEPTH) != null
                || findNodeByAliases(node, SECTION_KEYS_BREADTH) != null;
    }

    private JsonNode findNodeByAliases(JsonNode source, String... aliases) {
        if (source == null || !source.isObject() || aliases == null || aliases.length == 0) {
            return null;
        }
        for (String alias : aliases) {
            if (!StringUtils.hasText(alias)) {
                continue;
            }
            JsonNode direct = source.get(alias);
            if (direct != null) {
                return direct;
            }
        }
        Iterator<Map.Entry<String, JsonNode>> fields = source.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            if (entry == null || !StringUtils.hasText(entry.getKey())) {
                continue;
            }
            for (String alias : aliases) {
                if (StringUtils.hasText(alias) && entry.getKey().equalsIgnoreCase(alias)) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    private String readNodeTextByAlias(JsonNode source, String... aliases) {
        JsonNode node = findNodeByAliases(source, aliases);
        if (node == null || node.isNull()) {
            return "";
        }
        return normalizeTerm(node.asText(""));
    }

    private List<String> readNodeListByAlias(JsonNode source, String... aliases) {
        JsonNode node = findNodeByAliases(source, aliases);
        if (node == null || node.isNull()) {
            return List.of();
        }
        return normalizeStringListFromJsonNode(node);
    }

    private List<String> normalizeStringListFromJsonNode(JsonNode node) {
        if (node == null || node.isNull()) {
            return List.of();
        }
        List<String> output = new ArrayList<>();
        if (node.isArray()) {
            for (JsonNode item : node) {
                if (output.size() >= 3) {
                    break;
                }
                if (item == null || item.isNull()) {
                    continue;
                }
                if (item.isTextual() || item.isNumber() || item.isBoolean()) {
                    appendStringLine(output, item.asText(""));
                    continue;
                }
                if (item.isObject()) {
                    List<String> nested = normalizeStringListFromJsonNode(item);
                    for (String line : nested) {
                        appendStringLine(output, line);
                        if (output.size() >= 3) {
                            break;
                        }
                    }
                }
            }
            return output;
        }
        if (node.isTextual()) {
            return normalizeStringList(node.asText(""));
        }
        if (node.isObject()) {
            for (String key : STRING_LIST_OBJECT_KEYS) {
                JsonNode listNode = findNodeByAliases(node, key);
                if (listNode == null || listNode.isNull() || listNode == node) {
                    continue;
                }
                List<String> nested = normalizeStringListFromJsonNode(listNode);
                if (!nested.isEmpty()) {
                    return nested;
                }
            }
            for (String key : STRING_VALUE_OBJECT_KEYS) {
                String text = readNodeTextByAlias(node, key);
                if (StringUtils.hasText(text)) {
                    return List.of(trimContext(text));
                }
            }
            return List.of();
        }
        appendStringLine(output, node.asText(""));
        return output;
    }

    private void appendStringLine(List<String> output, String rawLine) {
        if (output == null || output.size() >= 3) {
            return;
        }
        String line = trimContext(String.valueOf(rawLine == null ? "" : rawLine));
        if (!StringUtils.hasText(line)) {
            return;
        }
        output.add(line);
    }

    private List<JsonNode> parseJsonNodeCandidates(String rawText) {
        String text = String.valueOf(rawText == null ? "" : rawText).trim();
        if (!StringUtils.hasText(text)) {
            return List.of();
        }
        LinkedHashSet<String> candidates = extractJsonCandidates(text);
        if (candidates.isEmpty()) {
            return List.of();
        }
        List<JsonNode> output = new ArrayList<>();
        for (String candidate : candidates) {
            try {
                output.add(objectMapper.readTree(candidate));
            } catch (Exception ignored) {
                // noop
            }
        }
        return output;
    }

    private LinkedHashSet<String> extractJsonCandidates(String rawText) {
        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        String text = String.valueOf(rawText == null ? "" : rawText).trim();
        if (!StringUtils.hasText(text)) {
            return candidates;
        }
        addJsonCandidate(candidates, text);
        collectBalancedJsonCandidates(candidates, text);
        String stripped = stripJsonFence(text);
        if (!stripped.equals(text)) {
            addJsonCandidate(candidates, stripped);
            collectBalancedJsonCandidates(candidates, stripped);
        }
        return candidates;
    }

    private void addJsonCandidate(LinkedHashSet<String> candidates, String candidate) {
        if (candidates == null) {
            return;
        }
        String text = String.valueOf(candidate == null ? "" : candidate).trim();
        if (!StringUtils.hasText(text)) {
            return;
        }
        boolean objectLike = text.startsWith("{") && text.endsWith("}");
        boolean arrayLike = text.startsWith("[") && text.endsWith("]");
        if (objectLike || arrayLike) {
            candidates.add(text);
        }
    }

    private void collectBalancedJsonCandidates(LinkedHashSet<String> candidates, String text) {
        if (candidates == null || !StringUtils.hasText(text)) {
            return;
        }
        String source = String.valueOf(text);
        ArrayDeque<Character> stack = new ArrayDeque<>();
        boolean inString = false;
        boolean escaped = false;
        int start = -1;
        for (int index = 0; index < source.length(); index += 1) {
            char ch = source.charAt(index);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                inString = !inString;
                continue;
            }
            if (inString) {
                continue;
            }
            if (ch == '{' || ch == '[') {
                if (stack.isEmpty()) {
                    start = index;
                }
                stack.push(ch);
                continue;
            }
            if (ch == '}' || ch == ']') {
                if (stack.isEmpty()) {
                    start = -1;
                    continue;
                }
                char open = stack.pop();
                boolean matched = (open == '{' && ch == '}') || (open == '[' && ch == ']');
                if (!matched) {
                    stack.clear();
                    start = -1;
                    continue;
                }
                if (stack.isEmpty() && start >= 0) {
                    String candidate = source.substring(start, index + 1).trim();
                    addJsonCandidate(candidates, candidate);
                    start = -1;
                }
            }
        }
    }

    private String stripJsonFence(String text) {
        String value = String.valueOf(text == null ? "" : text).trim();
        if (!StringUtils.hasText(value)) {
            return "";
        }
        if (value.startsWith("```")) {
            int firstBreak = value.indexOf('\n');
            if (firstBreak >= 0) {
                value = value.substring(firstBreak + 1).trim();
            }
        }
        if (value.endsWith("```")) {
            value = value.substring(0, value.length() - 3).trim();
        }
        return value;
    }

    private List<String> parseLooseArrayByKey(String text, String... keys) {
        if (!StringUtils.hasText(text) || keys == null || keys.length == 0) {
            return List.of();
        }
        for (String key : keys) {
            if (!StringUtils.hasText(key)) {
                continue;
            }
            String quotedKey = "\"" + key + "\"";
            int keyStart = text.indexOf(quotedKey);
            if (keyStart < 0) {
                continue;
            }
            int bracketStart = text.indexOf('[', keyStart + quotedKey.length());
            if (bracketStart < 0) {
                continue;
            }
            String arrayContent = extractArrayContentLoose(text, bracketStart);
            List<String> lines = extractQuotedStrings(arrayContent);
            if (!lines.isEmpty()) {
                return lines;
            }
        }
        return List.of();
    }

    private String extractArrayContentLoose(String text, int bracketStart) {
        if (!StringUtils.hasText(text) || bracketStart < 0 || bracketStart >= text.length()) {
            return "";
        }
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        StringBuilder builder = new StringBuilder();
        for (int index = bracketStart; index < text.length(); index += 1) {
            char ch = text.charAt(index);
            builder.append(ch);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (ch == '\\') {
                escaped = true;
                continue;
            }
            if (ch == '"') {
                inString = !inString;
                continue;
            }
            if (inString) {
                continue;
            }
            if (ch == '[') {
                depth += 1;
            } else if (ch == ']') {
                depth -= 1;
                if (depth == 0) {
                    return builder.toString();
                }
            }
        }
        return builder.toString();
    }

    private List<String> extractQuotedStrings(String text) {
        if (!StringUtils.hasText(text)) {
            return List.of();
        }
        List<String> output = new ArrayList<>();
        int index = 0;
        while (index < text.length() && output.size() < 3) {
            int start = text.indexOf('"', index);
            if (start < 0) {
                break;
            }
            StringBuilder builder = new StringBuilder();
            boolean escaped = false;
            int cursor = start + 1;
            while (cursor < text.length()) {
                char ch = text.charAt(cursor);
                if (escaped) {
                    if (ch == 'n' || ch == 'r' || ch == 't') {
                        builder.append(' ');
                    } else {
                        builder.append(ch);
                    }
                    escaped = false;
                    cursor += 1;
                    continue;
                }
                if (ch == '\\') {
                    escaped = true;
                    cursor += 1;
                    continue;
                }
                if (ch == '"') {
                    break;
                }
                builder.append(ch);
                cursor += 1;
            }
            if (cursor >= text.length()) {
                break;
            }
            String line = trimContext(builder.toString());
            if (StringUtils.hasText(line)) {
                output.add(line);
            }
            index = cursor + 1;
        }
        return output;
    }

    private boolean isWrapperKey(String key) {
        String normalized = normalizeTerm(key).toLowerCase(Locale.ROOT);
        if (!StringUtils.hasText(normalized)) {
            return true;
        }
        for (String wrapper : WRAPPER_KEYS) {
            if (normalized.equals(wrapper.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    private String loadPromptTemplate(String cacheKey, Resource resource, String defaultTemplate) {
        return promptTemplateCache.computeIfAbsent(cacheKey, key -> readPromptTemplate(resource, defaultTemplate, key));
    }

    private String readPromptTemplate(Resource resource, String defaultTemplate, String templateName) {
        if (resource == null || !resource.exists()) {
            logger.warn("DeepSeek advisor prompt template missing ({}), fallback to default", templateName);
            return defaultTemplate;
        }
        try (InputStream input = resource.getInputStream()) {
            String template = StreamUtils.copyToString(input, StandardCharsets.UTF_8).trim();
            if (StringUtils.hasText(template)) {
                return template;
            }
            logger.warn("DeepSeek advisor prompt template empty ({}), fallback to default", templateName);
        } catch (IOException ex) {
            logger.warn("DeepSeek advisor prompt template load failed ({}): {}", templateName, ex.getMessage());
        }
        return defaultTemplate;
    }

    private String applyTemplate(String template, Map<String, String> values) {
        String resolved = String.valueOf(template == null ? "" : template);
        for (Map.Entry<String, String> entry : values.entrySet()) {
            String key = "{" + entry.getKey() + "}";
            String value = String.valueOf(entry.getValue() == null ? "" : entry.getValue());
            resolved = resolved.replace(key, value);
        }
        return resolved;
    }
    private String trimContext(String context) {
        return String.valueOf(context == null ? "" : context)
                .replace("\r\n", "\n")
                .replace('\r', '\n')
                .trim();
    }

    public static class AdviceResult {
        public final String advice;
        public final String source;

        private AdviceResult(String advice, String source) {
            this.advice = advice;
            this.source = source;
        }

        public static AdviceResult deepseek(String advice) {
            return new AdviceResult(advice, "deepseek");
        }

        public static AdviceResult empty(String source) {
            return new AdviceResult("", String.valueOf(source == null ? "" : source));
        }
    }

    public static class StructuredAdviceResult {
        public final List<String> background;
        public final List<String> contextualExplanations;
        public final List<String> depth;
        public final List<String> breadth;
        public final String source;
        public final String raw;
        public final String requestPayloadJson;
        public final String responseBodyJson;

        private StructuredAdviceResult(
                List<String> background,
                List<String> contextualExplanations,
                List<String> depth,
                List<String> breadth,
                String source,
                String raw,
                String requestPayloadJson,
                String responseBodyJson
        ) {
            this.background = background == null ? List.of() : List.copyOf(background);
            this.contextualExplanations = contextualExplanations == null ? List.of() : List.copyOf(contextualExplanations);
            this.depth = depth == null ? List.of() : List.copyOf(depth);
            this.breadth = breadth == null ? List.of() : List.copyOf(breadth);
            this.source = String.valueOf(source == null ? "" : source).trim();
            this.raw = String.valueOf(raw == null ? "" : raw).trim();
            this.requestPayloadJson = String.valueOf(requestPayloadJson == null ? "" : requestPayloadJson).trim();
            this.responseBodyJson = String.valueOf(responseBodyJson == null ? "" : responseBodyJson).trim();
        }

        public boolean hasContent() {
            return !background.isEmpty() || !contextualExplanations.isEmpty() || !depth.isEmpty() || !breadth.isEmpty();
        }

        public static StructuredAdviceResult deepseek(
                List<String> background,
                List<String> contextualExplanations,
                List<String> depth,
                List<String> breadth,
                String raw
        ) {
            return new StructuredAdviceResult(background, contextualExplanations, depth, breadth, "deepseek", raw, "", "");
        }

        public static StructuredAdviceResult deepseek(
                List<String> contextualExplanations,
                List<String> depth,
                List<String> breadth,
                String raw
        ) {
            return deepseek(List.of(), contextualExplanations, depth, breadth, raw);
        }

        public static StructuredAdviceResult deepseek(
                List<String> background,
                List<String> contextualExplanations,
                List<String> depth,
                List<String> breadth,
                String raw,
                String requestPayloadJson,
                String responseBodyJson
        ) {
            return new StructuredAdviceResult(
                    background,
                    contextualExplanations,
                    depth,
                    breadth,
                    "deepseek",
                    raw,
                    requestPayloadJson,
                    responseBodyJson
            );
        }

        public static StructuredAdviceResult deepseek(
                List<String> contextualExplanations,
                List<String> depth,
                List<String> breadth,
                String raw,
                String requestPayloadJson,
                String responseBodyJson
        ) {
            return deepseek(List.of(), contextualExplanations, depth, breadth, raw, requestPayloadJson, responseBodyJson);
        }

        public static StructuredAdviceResult empty(String source) {
            return new StructuredAdviceResult(List.of(), List.of(), List.of(), List.of(), source, "", "", "");
        }

        public static StructuredAdviceResult empty(String source, String raw) {
            return new StructuredAdviceResult(List.of(), List.of(), List.of(), List.of(), source, raw, "", "");
        }

        public static StructuredAdviceResult empty(
                String source,
                String raw,
                String requestPayloadJson,
                String responseBodyJson
        ) {
            return new StructuredAdviceResult(
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    source,
                    raw,
                    requestPayloadJson,
                    responseBodyJson
            );
        }
    }

    public static class Phase2bMarkdownResult {
        public final String markdown;
        public final String source;
        public final String provider;
        public final boolean degraded;

        private Phase2bMarkdownResult(String markdown, String source, String provider, boolean degraded) {
            this.markdown = String.valueOf(markdown == null ? "" : markdown).trim();
            this.source = String.valueOf(source == null ? "" : source).trim();
            this.provider = String.valueOf(provider == null ? "" : provider).trim();
            this.degraded = degraded;
        }
    }

}
