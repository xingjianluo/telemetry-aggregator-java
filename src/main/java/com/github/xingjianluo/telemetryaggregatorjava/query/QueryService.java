package com.github.xingjianluo.telemetryaggregatorjava.query;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEngine;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.ResolvedRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleRegistry;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayService;
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
    private final RuleRegistry ruleRegistry;
    private final ReplayService replayService;

    public QueryService(IngestionEngine engine, WindowStore windowStore, WatermarkGenerator watermark,
                        TraceRegistry traceRegistry, AlertEngine alertEngine, EventDeduper deduper,
                        RuleRegistry ruleRegistry, ReplayService replayService) {
        this.engine = engine;
        this.windowStore = windowStore;
        this.watermark = watermark;
        this.traceRegistry = traceRegistry;
        this.alertEngine = alertEngine;
        this.deduper = deduper;
        this.ruleRegistry = ruleRegistry;
        this.replayService = replayService;
    }

    /** 当前规则集（含版本与免打扰边界）。 */
    public List<ResolvedRule> currentRules() {
        return ruleRegistry.currentRules();
    }

    /** 全部规则变更历史（版本升序）。 */
    public List<RuleChange> ruleHistory() {
        return ruleRegistry.history();
    }

    /** 按事件时间边界复盘。 */
    public ReplayReport replay(long asOfMillis) {
        return replayService.replay(asOfMillis);
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
