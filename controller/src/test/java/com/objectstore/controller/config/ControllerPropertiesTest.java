package com.objectstore.controller.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ControllerPropertiesTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class,
                    ValidationAutoConfiguration.class))
            .withUserConfiguration(Config.class);

    @Test
    void defaultsMatchSpecification() {
        runner.withPropertyValues("objectstore.internal.api-token=secret")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    ControllerProperties p = ctx.getBean(ControllerProperties.class);
                    assertThat(p.storage().chunkSize().toBytes()).isEqualTo(1_048_576L);
                    assertThat(p.storage().replicationFactor()).isEqualTo(2);
                    assertThat(p.cluster().heartbeatTimeout()).isEqualTo(Duration.ofSeconds(10));
                    assertThat(p.cluster().reReplicationGracePeriod()).isEqualTo(Duration.ofSeconds(30));
                });
    }

    @Test
    void environmentStyleValuesBind() {
        runner.withPropertyValues(
                        "objectstore.internal.api-token=secret",
                        "objectstore.storage.chunk-size=1048576B",
                        "objectstore.cluster.heartbeat-timeout=7000ms")
                .run(ctx -> {
                    ControllerProperties p = ctx.getBean(ControllerProperties.class);
                    assertThat(p.storage().chunkSize().toBytes()).isEqualTo(1_048_576L);
                    assertThat(p.cluster().heartbeatTimeout()).isEqualTo(Duration.ofMillis(7000));
                });
    }

    @Test
    void blankInternalTokenFailsStartup() {
        runner.withPropertyValues("objectstore.internal.api-token=")
                .run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void replicationFactorBelowOneFailsStartup() {
        runner.withPropertyValues("objectstore.internal.api-token=secret",
                        "objectstore.storage.replication-factor=0")
                .run(ctx -> assertThat(ctx).hasFailed());
    }

    @Configuration
    @EnableConfigurationProperties(ControllerProperties.class)
    static class Config {
    }
}

