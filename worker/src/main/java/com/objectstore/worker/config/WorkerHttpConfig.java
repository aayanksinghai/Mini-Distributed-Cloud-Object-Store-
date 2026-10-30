package com.objectstore.worker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.http.InternalHeaders;
import com.objectstore.common.security.InternalTokenFilter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;

/**
 * Worker infrastructure beans: internal token filter and {@link RestClient} for controller communication.
 */
@Configuration
public class WorkerHttpConfig {

    @Bean
    public FilterRegistrationBean<InternalTokenFilter> internalTokenFilter(
            WorkerProperties props, ObjectMapper objectMapper) {
        FilterRegistrationBean<InternalTokenFilter> bean = new FilterRegistrationBean<>();
        bean.setFilter(new InternalTokenFilter(props.internalApiToken(), objectMapper));
        bean.addUrlPatterns("/internal/*");
        bean.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return bean;
    }

    @Bean
    public RestClient controllerRestClient(WorkerProperties props) {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(Duration.ofSeconds(5));
        return RestClient.builder()
                .baseUrl(props.controllerUrl())
                .requestFactory(requestFactory)
                .defaultHeader(InternalHeaders.INTERNAL_TOKEN, props.internalApiToken())
                .build();
    }

    @Configuration
    @EnableScheduling
    @ConditionalOnProperty(name = "worker.scheduling.enabled", havingValue = "true", matchIfMissing = true)
    static class SchedulingConfig {
    }
}

