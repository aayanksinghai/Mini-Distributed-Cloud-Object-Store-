package com.objectstore.controller.config;

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

/**
 * Controller infrastructure beans: internal token filter, worker {@link RestClient}, and scheduling.
 */
@Configuration
public class ControllerHttpConfig {

    @Bean
    public FilterRegistrationBean<InternalTokenFilter> internalTokenFilter(
            ControllerProperties props, ObjectMapper objectMapper) {
        FilterRegistrationBean<InternalTokenFilter> bean = new FilterRegistrationBean<>();
        bean.setFilter(new InternalTokenFilter(props.internal().apiToken(), objectMapper));
        bean.addUrlPatterns("/internal/*");
        bean.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return bean;
    }

    @Bean
    public RestClient workerRestClient(ControllerProperties props) {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(props.cluster().workerRequestTimeout())
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(props.cluster().workerRequestTimeout());
        return RestClient.builder()
                .requestFactory(requestFactory)
                .defaultHeader(InternalHeaders.INTERNAL_TOKEN, props.internal().apiToken())
                .build();
    }

    @Configuration
    @EnableScheduling
    @ConditionalOnProperty(name = "controller.scheduling.enabled", havingValue = "true", matchIfMissing = true)
    static class SchedulingConfig {
    }
}

