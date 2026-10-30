package com.objectstore.worker.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerPropertiesTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
                    ValidationAutoConfiguration.class))
            .withUserConfiguration(Config.class)
            .withPropertyValues(
                    "worker.advertised-url=http://worker1:8081",
                    "worker.controller-url=http://controller:8080",
                    "worker.storage-dir=/tmp/chunks",
                    "worker.internal-api-token=secret");

    @Test
    void validConfigurationBindsWithDefaults() {
        runner.withPropertyValues("worker.id=worker1").run(ctx -> {
            assertThat(ctx).hasNotFailed();
            WorkerProperties p = ctx.getBean(WorkerProperties.class);
            assertThat(p.id()).isEqualTo("worker1");
            assertThat(p.heartbeatInterval()).isEqualTo(Duration.ofSeconds(3));
            assertThat(p.debugEndpoints()).isFalse();
        });
    }

    @Test
    void missingWorkerIdFailsStartup() {
        runner.withPropertyValues("worker.id=").run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void workerIdWithPathCharactersFailsStartup() {
        runner.withPropertyValues("worker.id=../evil").run(ctx -> assertThat(ctx).hasFailed());
    }

    @Configuration
    @EnableConfigurationProperties(WorkerProperties.class)
    static class Config {
    }
}

