package com.github.xingjianluo.telemetryaggregatorjava.config;

import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/** 启用配置属性绑定并提供可替换的时钟（测试可注入固定时钟）。 */
@Configuration
@ConfigurationPropertiesScan
public class TelemetryConfig {

    @Bean
    public Clock systemClock() {
        return Clock.systemUTC();
    }
}
