package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.service.FusionDecisionService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
public class FusionController {

    private final FusionDecisionService fusionService;
    private final com.mvp.module2.fusion.service.MaterialOptimizerService optimizerService;

    public FusionController(FusionDecisionService fusionService, 
                            com.mvp.module2.fusion.service.MaterialOptimizerService optimizerService) {
        this.fusionService = fusionService;
        this.optimizerService = optimizerService;
    }

    @GetMapping("/api/fusion/process")
    public CompletableFuture<String> processVideo(
            @RequestParam String path, 
            @RequestParam(required = false) String text,
            @RequestParam(required = false, defaultValue = "0.0") double start,
            @RequestParam(required = false, defaultValue = "-1.0") double end,
            @RequestParam(required = false) String sentences_path,
            @RequestParam(required = false) String subtitles_path,
            @RequestParam(required = false) String merge_data_path,
            @RequestParam(required = false) String main_topic) {
        return fusionService.processVideo(path, text, start, end, sentences_path, subtitles_path, merge_data_path, main_topic);
    }
    
    @GetMapping("/api/fusion/batch")
    public java.util.List<String> batchProcess(
            @RequestParam String path,
            @RequestParam String sentences_path,
            @RequestParam String subtitles_path,
            @RequestParam String merge_data_path,
            @RequestParam(required = false) String main_topic) {
        return fusionService.processFullVideo(path, sentences_path, subtitles_path, merge_data_path, main_topic);
    }
}
