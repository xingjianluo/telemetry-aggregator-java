package com.github.xingjianluo.telemetryaggregatorjava.config;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEngine;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleRegistry;
import com.github.xingjianluo.telemetryaggregatorjava.dedup.EventDeduper;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.EventProcessor;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionEngine;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionMetrics;
import com.github.xingjianluo.telemetryaggregatorjava.replay.EventJournal;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayService;
import com.github.xingjianluo.telemetryaggregatorjava.query.QueryService;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceRegistry;
import com.github.xingjianluo.telemetryaggregatorjava.window.WatermarkGenerator;
import com.github.xingjianluo.telemetryaggregatorjava.window.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 组件装配：与 Spring Boot 的属性配置自然衔接。 */
@Configuration
public class TelemetryBeans {

    @Bean
    public IngestionMetrics ingestionMetrics() {
        return new IngestionMetrics();
    }

    @Bean
    public EventDeduper eventDeduper(TelemetryProperties props) {
        return new EventDeduper(props.getCardinality().getMaxEventIds());
    }

    @Bean
    public WatermarkGenerator watermarkGenerator(TelemetryProperties props, Clock clock) {
        return new WatermarkGenerator(
                props.getWindow().getSizeMillis(),
                props.getWindow().getAllowedLatenessMillis(),
                props.getWindow().isAdvanceOnIdle(),
                clock);
    }

    @Bean
    public WindowStore windowStore(TelemetryProperties props) {
        return new WindowStore(props.getCardinality().getMaxSeriesPerWindow(),
                props.getCardinality().getMaxSealedWindows());
    }

    @Bean
    public TraceRegistry traceRegistry(TelemetryProperties props) {
        return new TraceRegistry(props.getCardinality().getMaxTraces(),
                props.getCardinality().getMaxSpansPerTrace());
    }

    @Bean
    public RuleRegistry ruleRegistry(TelemetryProperties props,
                                     @Value("${telemetry.alert.rules:}") String rulesSpec) {
        List<AlertRule> rules = parseRules(rulesSpec);
        if (rules.isEmpty()) {
            // 开箱即用的默认规则：任意 service/metric 的窗口均值 > 100 即进入告警判定。
            rules = List.of(new AlertRule("default-avg-gt-100", AlertRule.Scope.TAGS,
                    null, null, Map.of(), 100.0, AlertRule.Operator.GT));
        }
        return new RuleRegistry(rules, props.getRules().getMaxVersions());
    }

    @Bean
    public AlertEngine alertEngine(TelemetryProperties props) {
        return new AlertEngine(
                props.getAlert().getBreachStreak(),
                props.getAlert().getRecoverStreak(),
                props.getAlert().getCooldown().toMillis());
    }

    @Bean
    public EventJournal eventJournal(TelemetryProperties props) {
        return new EventJournal(props.getJournal().getMaxEvents());
    }

    @Bean
    public EventProcessor eventProcessor(TelemetryProperties props,
                                         EventDeduper deduper, WatermarkGenerator watermark,
                                         WindowStore windowStore, TraceRegistry traceRegistry,
                                         AlertEngine alertEngine, RuleRegistry ruleRegistry,
                                         IngestionMetrics metrics, EventJournal journal) {
        return new EventProcessor(props, watermark, windowStore, traceRegistry,
                alertEngine, ruleRegistry, deduper, metrics, journal);
    }

    @Bean
    public IngestionEngine ingestionEngine(TelemetryProperties props,
                                           EventProcessor processor,
                                           IngestionMetrics metrics) {
        return new IngestionEngine(props, processor, metrics);
    }

    @Bean
    public ReplayService replayService(TelemetryProperties props, EventJournal journal,
                                       RuleRegistry ruleRegistry) {
        return new ReplayService(props, journal, ruleRegistry);
    }

    @Bean
    public QueryService queryService(IngestionEngine engine, WindowStore windowStore,
                                     WatermarkGenerator watermark, TraceRegistry traceRegistry,
                                     AlertEngine alertEngine, EventDeduper deduper,
                                     RuleRegistry ruleRegistry, ReplayService replayService) {
        return new QueryService(engine, windowStore, watermark, traceRegistry,
                alertEngine, deduper, ruleRegistry, replayService);
    }

    /**
     * 规则以分号分隔，字段以逗号分隔：
     * id,scope,service,metric,threshold,operator,tagK=tagV,...
     * 例：r1,TAGS,svc-a,cpu,80,GT,host=h1,region=cn
     */
    static List<AlertRule> parseRules(String spec) {
        List<AlertRule> rules = new ArrayList<>();
        if (spec == null || spec.isBlank()) {
            return rules;
        }
        for (String item : spec.split(";")) {
            if (item.isBlank()) {
                continue;
            }
            String[] f = Arrays.stream(item.split(",")).map(String::trim).toArray(String[]::new);
            if (f.length < 6) {
                throw new IllegalArgumentException("告警规则字段不足: " + item);
            }
            AlertRule.Scope scope = AlertRule.Scope.valueOf(f[1].toUpperCase());
            Map<String, String> selector = new LinkedHashMap<>();
            for (int i = 6; i < f.length; i++) {
                String token = f[i];
                if (token == null || token.isBlank()) {
                    continue;
                }
                int eq = token.indexOf('=');
                if (eq <= 0) {
                    throw new IllegalArgumentException("标签选择器格式应为 k=v: " + token);
                }
                selector.put(token.substring(0, eq), token.substring(eq + 1));
            }
            String service = emptyToNull(f[2]);
            String metric = emptyToNull(f[3]);
            rules.add(new AlertRule(f[0], scope, service, metric, selector,
                    Double.parseDouble(f[4]), AlertRule.Operator.valueOf(f[5].toUpperCase())));
        }
        return rules;
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }
}
