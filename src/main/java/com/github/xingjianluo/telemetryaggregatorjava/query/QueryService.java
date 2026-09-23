package com.github.xingjianluo.telemetryaggregatorjava.query;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEngine;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.dedup.EventDeduper;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionEngine;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceRegistry;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.window.WatermarkGenerator;
import com.github.xingjianluo.telemetryaggregatorjava.window.WindowStore;

import java.util.List;

/**
 * 统一对外查询门面。聚合查询读到的是定稿窗口数据（与告警评估同源），
 * 指标查询暴露当前堆积量、按原因区分的丢弃计数与各类基数占用，保证语义对齐。
 */
public class QueryService {

    private final IngestionEngine engine;
    private final WindowStore windowStore;
    private final WatermarkGenerator watermark;
    private final TraceRegistry traceRegistry;
    private final AlertEngine alertEngine;
    private final EventDeduper deduper;

    public QueryService(IngestionEngine engine, WindowStore windowStore, WatermarkGenerator watermark,
                        TraceRegistry traceRegistry, AlertEngine alertEngine, EventDeduper deduper) {
        this.engine = engine;
        this.windowStore = windowStore;
        this.watermark = watermark;
        this.traceRegistry = traceRegistry;
        this.alertEngine = alertEngine;
        this.deduper = deduper;
    }

    public List<WindowAggregate> queryAggregates(String service, String metric) {
        return windowStore.query(service, metric);
    }

    public List<AlertSnapshot> queryAlerts() {
        return alertEngine.snapshots();
    }

    public TraceSnapshot queryTrace(String traceId) {
        return traceRegistry.snapshot(traceId);
    }

    public MetricsSnapshot metrics() {
        return new MetricsSnapshot(
                engine.metrics().acceptedCount(),
                engine.metrics().totalRejected(),
                engine.metrics().rejectedCounts(),
                engine.queueDepth(),
                engine.metrics().queuePeak(),
                engine.metrics().duplicateReplayCount(),
                watermark.currentWatermark(),
                deduper.size(),
                windowStore.liveWindowCount(),
                windowStore.liveSeriesCount(),
                traceRegistry.traceCount());
    }
}
