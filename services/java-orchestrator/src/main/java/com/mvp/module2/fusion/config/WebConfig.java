package com.mvp.module2.fusion.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Web配置类
 * - 配置CORS跨域
 * - 配置静态资源服务 (前端)
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        // 允许前端跨域访问。
        // 使用 allowedOriginPatterns("*") 兼容 cpolar/ngrok 等动态隧道域名，
        // 避免移动端通过隧道访问时因 Origin 不在白名单而返回 403。
        registry.addMapping("/**")
                .allowedOriginPatterns("*")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*")
                .allowCredentials(true)
                .maxAge(3600);
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // 配置前端静态资源 - 使用绝对路径
        // 访问 /frontend/** 会映射到 frontend 目录
        String frontendPath = System.getProperty("user.dir").replace("java_orchestrator", "frontend/");
        registry.addResourceHandler("/frontend/**")
                .addResourceLocations("file:" + frontendPath);
    }
}
