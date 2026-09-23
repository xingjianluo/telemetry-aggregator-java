package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;

import java.util.Map;

/** 测试辅助：构造小容量引擎配置与事件。 */
final class EngineTestSupport {

    private EngineTestSupport() {
    }

    static TelemetryProperties props(long windowMillis, long lateness,
                                     int queueCapacity, int dedupCapacity,
                                     int maxTagValues, int maxSeries,
                                     int maxWindows, int maxSpans) {
        TelemetryProperties p = new TelemetryProperties();
        p.setWindowMillis(windowMillis);
        p.setAllowedLatenessMillis(lateness);
        p.setIngestQueueCapacity(queueCapacity);
        p.setDedupCapacity(dedupCapacity);
        p.setMaxTagValuesPerKey(maxTagValues);
        p.setMaxSeriesPerWindow(maxSeries);
        p.setMaxActiveWindows(maxWindows);
        p.setMaxSpans(maxSpans);
        p.setRetainedWindows(100);
        p.setRetainedAlertEvents(100);
        return p;
    }

    static TelemetryEvent event(String id, long ts, String metric, double value,
                                Map<String, String> tags) {
        TelemetryEvent e = new TelemetryEvent();
        e.setEventId(id);
        e.setTimestampMillis(ts);
        e.setMetric(metric);
        e.setValue(value);
        e.setTags(tags);
        return e;
    }

    static TelemetryEvent span(String id, long ts, String traceId, String spanId,
                               String parentSpanId, long durationMillis) {
        TelemetryEvent e = event(id, ts, "latency", durationMillis,
                Map.of("service", "svc-" + traceId));
        e.setTraceId(traceId);
        e.setSpanId(spanId);
        e.setParentSpanId(parentSpanId);
        e.setDurationMillis(durationMillis);
        return e;
    }
}
