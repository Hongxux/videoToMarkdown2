package com.mvp.module2.fusion.service.llm;

public final class LlmResponse {
    public final String content;
    public final String requestPayloadJson;
    public final String responseBodyJson;
    public final String finishReason;

    public LlmResponse(
            String content,
            String requestPayloadJson,
            String responseBodyJson,
            String finishReason
    ) {
        this.content = String.valueOf(content == null ? "" : content).trim();
        this.requestPayloadJson = String.valueOf(requestPayloadJson == null ? "" : requestPayloadJson).trim();
        this.responseBodyJson = String.valueOf(responseBodyJson == null ? "" : responseBodyJson).trim();
        this.finishReason = String.valueOf(finishReason == null ? "" : finishReason).trim();
    }
}
