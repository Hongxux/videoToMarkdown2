package com.mvp.module2.fusion.service.llm;

import java.util.function.Consumer;

public interface LlmClient {
    LlmResponse complete(LlmProviderConfig provider, LlmPromptRequest request) throws Exception;

    LlmResponse stream(LlmProviderConfig provider, LlmPromptRequest request, Consumer<String> onDelta) throws Exception;
}
