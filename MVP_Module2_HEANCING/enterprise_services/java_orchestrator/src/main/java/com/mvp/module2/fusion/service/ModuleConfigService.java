package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

@Service
public class ModuleConfigService {
    private static final Logger logger = LoggerFactory.getLogger(ModuleConfigService.class);
    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    
    private boolean vlEnabled = false;
    private long lastCheckTime = 0;
    private static final long CACHE_DURATION_MS = 60000; // 1 minute cache

    public boolean isVLEnabled() {
        long now = System.currentTimeMillis();
        // Force refresh if never checked or cache expired
        if (lastCheckTime == 0 || now - lastCheckTime > CACHE_DURATION_MS) {
            refreshConfig();
        }
        return vlEnabled;
    }

    private synchronized void refreshConfig() {
        try {
            File configFile = locateConfigFile();
            if (configFile != null && configFile.exists()) {
                JsonNode root = yamlMapper.readTree(configFile);
                JsonNode vlNode = root.path("vl_material_generation");
                if (!vlNode.isMissingNode()) {
                    this.vlEnabled = vlNode.path("enabled").asBoolean(false);
                    logger.debug("Refreshed VL Config: enabled={}", this.vlEnabled);
                } else {
                    this.vlEnabled = false;
                }
            } else {
                logger.warn("module2_config.yaml not found, defaulting VL to false");
                this.vlEnabled = false;
            }
        } catch (IOException e) {
            logger.error("Failed to read module2_config.yaml: {}", e.getMessage());
            this.vlEnabled = false;
        } finally {
            this.lastCheckTime = System.currentTimeMillis();
        }
    }

    private File locateConfigFile() {
        // Try multiple possible paths relative to execution directory
        // 1. Dev environment: d:\videoToMarkdownTest2\MVP_Module2_HEANCING\enterprise_services\java_orchestrator
        // Target: ../../config/module2_config.yaml
        
        // 2. Production/Root: d:\videoToMarkdownTest2
        // Target: MVP_Module2_HEANCING/config/module2_config.yaml
        
        String[] candidates = {
            "../../config/module2_config.yaml",
            "MVP_Module2_HEANCING/config/module2_config.yaml",
            "config/module2_config.yaml" // Fallback
        };

        String userDir = System.getProperty("user.dir");
        for (String relativePath : candidates) {
            File f = Paths.get(userDir, relativePath).toFile();
            if (f.exists()) {
                try {
                    return f.getCanonicalFile();
                } catch (IOException e) {
                    return f;
                }
            }
        }
        return null;
    }
}
