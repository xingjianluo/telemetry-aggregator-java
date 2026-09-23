package com.github.xingjianluo.telemetryaggregatorjava.web;

import com.github.xingjianluo.telemetryaggregatorjava.core.AggregationEngine;
import com.github.xingjianluo.telemetryaggregatorjava.model.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** 遥测摄取 / 聚合 / 链路 / 告警 / 查询的 HTTP 入口（骨架）。 */
@RestController
@RequestMapping("/telemetry")
public class TelemetryController {

    private final AggregationEngine engine;

    public TelemetryController(AggregationEngine engine) {
        this.engine = engine;
    }

    @PostMapping("/events")
    public IngestResult ingest(@RequestBody TelemetryEvent event) throws Exception {
        return engine.ingest(event).get(10, TimeUnit.SECONDS);
    }

    @PostMapping("/events/batch")
    public List<IngestResult> ingestBatch(@RequestBody List<TelemetryEvent> events)
            throws Exception {
        if (events == null) {
            return List.of();
        }
        // 先全部提交（保持入队顺序），再统一等待结论
        List<CompletableFuture<IngestResult>> futures = new ArrayList<>(events.size());
        for (TelemetryEvent e : events) {
            futures.add(engine.ingest(e));
        }
        List<IngestResult> out = new ArrayList<>(futures.size());
        for (CompletableFuture<IngestResult> f : futures) {
            out.add(f.get(10, TimeUnit.SECONDS));
        }
        return out;
    }

    @GetMapping("/windows")
    public Object windows(@RequestParam(required = false) Long from,
                          @RequestParam(required = false) Long to) {
        return engine.queryWindows(from, to);
    }

    @GetMapping("/chains/{traceId}")
    public Object chain(@PathVariable String traceId) {
        return engine.queryChain(traceId);
    }

    @GetMapping("/chains")
    public Object chains() {
        return engine.queryChains();
    }

    @PostMapping("/alerts/rules")
    public String addRule(@RequestBody AlertRule rule) {
        engine.addAlertRule(rule);
        return rule.getRuleId();
    }

    @GetMapping("/alerts/events")
    public Object alertEvents(@RequestParam(defaultValue = "100") int limit) {
        return engine.queryAlertEvents(limit);
    }

    @GetMapping("/alerts/states")
    public Object alertStates() {
        return engine.alertStates();
    }

    @PostMapping("/watermark/advance")
    public Map<String, Object> advanceWatermark(@RequestParam long eventTimeMillis,
                                                @RequestParam(defaultValue = "5000") long awaitMillis) {
        engine.advanceWatermarkTo(eventTimeMillis);
        engine.awaitDrained(awaitMillis);
        return engine.getStats().snapshot();
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return engine.getStats().snapshot();
    }
}
