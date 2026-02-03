package com.mvp.module2.fusion.service;

import com.mvp.module2.fusion.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class TextGeneratorService {

    private final com.mvp.module2.fusion.grpc.client.PythonComputeClient pythonClient;

    public TextGeneratorService(com.mvp.module2.fusion.grpc.client.PythonComputeClient pythonClient) {
        this.pythonClient = pythonClient;
    }

    /**
     * Generates enhancement text via Python Worker (LLM). 
     */
    public String generateEnhancementText(VisualFeatures features, com.mvp.module2.fusion.grpc.SemanticFeatures semantic) {
        // Delegate to Python
        var response = pythonClient.generateEnhancementText(features, semantic, "general", "", "", "");
        if (response.getSuccess()) {
             return response.getGeneratedText();
        }
        return "Note: Visual details require attention (Generation Failed).";
    }
    
    private String truncate(String input, int length) {
        if (input == null) return "";
        if (input.length() <= length) return input;
        return input.substring(0, length) + "...";
    }
}
