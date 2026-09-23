package com.github.xingjianluo.telemetryaggregatorjava.config;

import com.github.xingjianluo.telemetryaggregatorjava.core.AggregationEngine;
import jakarta.annotation.PreDestroy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 把聚合引擎接入 Spring 生命周期（骨架）。 */
@Configuration
public class TelemetryEngineConfiguration {

    private AggregationEngine engine;

    @Bean
    public AggregationEngine aggregationEngine(TelemetryProperties props) {
        engine = new AggregationEngine(props);
        engine.start();
        return engine;
    }

    @PreDestroy
    public void shutdown() {
        if (engine != null) {
            engine.stop();
        }
    }
}
