package com.mvp.module2.fusion.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.ResourceRegionHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

/**
 * Web 配置：
 * 1. 配置 CORS，兼容移动端经隧道域名访问。
 * 2. 配置前端静态资源目录映射。
 * 3. 为 MVC 异步请求绑定线程池，避免默认执行器在负载下退化。
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {
    private final AsyncTaskExecutor mvcTaskExecutor;

    public WebConfig(@Qualifier("taskExecutor") AsyncTaskExecutor mvcTaskExecutor) {
        this.mvcTaskExecutor = mvcTaskExecutor;
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOriginPatterns("*")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*")
                .allowCredentials(true)
                .maxAge(3600);
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        String frontendPath = System.getProperty("user.dir").replace("java_orchestrator", "frontend/");
        registry.addResourceHandler("/frontend/**")
                .addResourceLocations("file:" + frontendPath);
    }

    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        configurer.setTaskExecutor(mvcTaskExecutor);
    }

    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
        boolean hasResourceRegionConverter = converters.stream()
                .anyMatch(ResourceRegionHttpMessageConverter.class::isInstance);
        if (!hasResourceRegionConverter) {
            converters.add(new ResourceRegionHttpMessageConverter());
        }
    }
}
