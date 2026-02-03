package com.videotomd;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 视频转文字稿系统 - 主应用类
 * 
 * @author HongXU
 * @since 2026-01-22
 */
@SpringBootApplication
@EnableScheduling  // 启用定时任务(用于文件清理)
public class VideoToMarkdownApplication {

    public static void main(String[] args) {
        SpringApplication.run(VideoToMarkdownApplication.class, args);
        System.out.println("""
            
            ╔═══════════════════════════════════════════════════════╗
            ║     视频转文字稿系统 - 后端服务已启动                  ║
            ║     VideoToMarkdown Backend Service Started          ║
            ╠═══════════════════════════════════════════════════════╣
            ║     Swagger UI: http://localhost:8080/swagger-ui.html║
            ║     Health Check: http://localhost:8080/actuator/health
            ╚═══════════════════════════════════════════════════════╝
            """);
    }
}
