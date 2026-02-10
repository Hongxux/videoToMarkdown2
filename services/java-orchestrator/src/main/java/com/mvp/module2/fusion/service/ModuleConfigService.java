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
    private double ffmpegTimeoutMultiplier = 1.0;
    private int ffmpegTimeoutMinSec = 0;
    private int ffmpegTimeoutMaxSec = 0;
    private long lastCheckTime = 0;
    private static final long CACHE_DURATION_MS = 60000; // 1 minute cache

    public boolean isVLEnabled() {
        refreshIfNeeded();
        return vlEnabled;
    }

    public double getFfmpegTimeoutMultiplier() {
        refreshIfNeeded();
        return ffmpegTimeoutMultiplier;
    }

    public int getFfmpegTimeoutMinSec() {
        refreshIfNeeded();
        return ffmpegTimeoutMinSec;
    }

    public int getFfmpegTimeoutMaxSec() {
        refreshIfNeeded();
        return ffmpegTimeoutMaxSec;
    }

    private void refreshIfNeeded() {
        long now = System.currentTimeMillis();
        if (lastCheckTime == 0 || now - lastCheckTime > CACHE_DURATION_MS) {
            refreshConfig();
        }
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

                JsonNode ffmpegNode = root.path("ffmpeg_extraction");
                if (!ffmpegNode.isMissingNode()) {
                    this.ffmpegTimeoutMultiplier = ffmpegNode.path("timeout_multiplier").asDouble(1.0);
                    this.ffmpegTimeoutMinSec = ffmpegNode.path("min_timeout_sec").asInt(0);
                    this.ffmpegTimeoutMaxSec = ffmpegNode.path("max_timeout_sec").asInt(0);
                } else {
                    this.ffmpegTimeoutMultiplier = 1.0;
                    this.ffmpegTimeoutMinSec = 0;
                    this.ffmpegTimeoutMaxSec = 0;
                }
            } else {
                logger.warn("module2_config.yaml not found, defaulting VL to false");
                this.vlEnabled = false;
                this.ffmpegTimeoutMultiplier = 1.0;
                this.ffmpegTimeoutMinSec = 0;
                this.ffmpegTimeoutMaxSec = 0;
            }
        } catch (IOException e) {
            logger.error("Failed to read module2_config.yaml: {}", e.getMessage());
            this.vlEnabled = false;
            this.ffmpegTimeoutMultiplier = 1.0;
            this.ffmpegTimeoutMinSec = 0;
            this.ffmpegTimeoutMaxSec = 0;
        } finally {
            this.lastCheckTime = System.currentTimeMillis();
        }
    }

    private File locateConfigFile() {
        // Try multiple possible paths relative to execution directory
        // Single source of truth: config/module2_config.yaml
        // Keep one local fallback only for java-orchestrator standalone working dir.
        
        String[] candidates = {
            "config/module2_config.yaml",
            "../../config/module2_config.yaml"
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
