package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEngine;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.dedup.EventDeduper;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionEngine;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionMetrics;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import com.github.xingjianluo.telemetryaggregatorjava.replay.EventJournal;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayService;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleManager;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersion;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersionStore;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceRegistry;
import com.github.xingjianluo.telemetryaggregatorjava.window.WatermarkGenerator;
import com.github.xingjianluo.telemetryaggregatorjava.window.WindowStore;
import org.slf4j.Logger;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

/** 确定性测试夹具：关闭空闲推进，水位线仅由事件时间推进。 */
final class TestHarness {

    private TestHarness() {
    }

    static TelemetryProperties defaultProps() {
        TelemetryProperties p = new TelemetryProperties();
        p.getWindow().setAdvanceOnIdle(false);
        return p;
    }

    static final class Engine implements AutoCloseable {
        final TelemetryProperties props;
        final EventDeduper deduper;
        final WatermarkGenerator watermark;
        final WindowStore windowStore;
        final TraceRegistry traceRegistry;
        final AlertEngine alertEngine;
        final IngestionMetrics metrics;
        final IngestionEngine engine;
        final EventJournal journal;
        final RuleVersionStore ruleStore;
        final RuleManager ruleManager;
        final boolean versioned;

        /** 旧用法：固定规则、无日志（回归测试沿用）。 */
        Engine(TelemetryProperties props, List<AlertRule> rules, long cooldownMillis) {
            this.props = props;
            this.deduper = new EventDeduper(props.getCardinality().getMaxEventIds());
            this.watermark = new WatermarkGenerator(
                    props.getWindow().getSizeMillis(),
                    props.getWindow().getAllowedLatenessMillis(),
                    false,
                    Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
            this.windowStore = new WindowStore(
                    props.getCardinality().getMaxSeriesPerWindow(),
                    props.getCardinality().getMaxSealedWindows());
            this.traceRegistry = new TraceRegistry(
                    props.getCardinality().getMaxTraces(),
                    props.getCardinality().getMaxSpansPerTrace());
            this.alertEngine = new AlertEngine(rules,
                    props.getAlert().getBreachStreak(),
                    props.getAlert().getRecoverStreak(),
                    cooldownMillis);
            this.metrics = new IngestionMetrics();
            this.engine = new IngestionEngine(props, watermark, windowStore, traceRegistry,
                    alertEngine, deduper, metrics);
            this.journal = null;
            this.ruleStore = null;
            this.ruleManager = null;
            this.versioned = false;
        }

        /** 新用法：版本化规则 + 复盘日志 + 复盘服务。 */
        Engine(TelemetryProperties props, List<RuleVersion> seedRules, long cooldownMillis,
               int journalCapacity, int factCapacity) {
            this.props = props;
            this.deduper = new EventDeduper(props.getCardinality().getMaxEventIds());
            this.watermark = new WatermarkGenerator(
                    props.getWindow().getSizeMillis(),
                    props.getWindow().getAllowedLatenessMillis(),
                    false,
                    Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
            this.windowStore = new WindowStore(
                    props.getCardinality().getMaxSeriesPerWindow(),
                    props.getCardinality().getMaxSealedWindows());
            this.traceRegistry = new TraceRegistry(
                    props.getCardinality().getMaxTraces(),
                    props.getCardinality().getMaxSpansPerTrace());
            this.journal = new EventJournal(journalCapacity, factCapacity);
            this.ruleStore = new RuleVersionStore();
            this.ruleStore.seed(seedRules);
            this.ruleManager = new RuleManager();
            this.alertEngine = new AlertEngine(ruleStore, journal,
                    props.getAlert().getBreachStreak(),
                    props.getAlert().getRecoverStreak(),
                    cooldownMillis);
            this.metrics = new IngestionMetrics();
            this.engine = new IngestionEngine(props, watermark, windowStore, traceRegistry,
                    alertEngine, deduper, metrics, journal, ruleStore, ruleManager);
            this.versioned = true;
        }

        ReplayService replayService() {
            return new ReplayService(journal, ruleStore, props);
        }

        IngestResult send(Logger log, TelemetryEvent e) {
            IngestResult r = engine.ingest(e);
            log.info("INGEST eventId={} eventTime={} traceId={} spanId={} parent={} value={} tags={} => {} {}{}",
                    e.eventId(), e.eventTimeMillis(), e.traceId(), e.spanId(),
                    e.parentSpanId(), e.value(), e.tags(),
                    r.status(),
                    r.rejectReason() == null ? "" : r.rejectReason(),
                    r.detail() == null ? "" : " | " + r.detail());
            return r;
        }

        void dumpState(Logger log) {
            log.info("STATE watermark={} accepted={} rejected={} byReason={} queueDepth={} dedupSize={} liveWindows={} liveSeries={} traces={}",
                    watermark.currentWatermark(),
                    metrics.acceptedCount(), metrics.totalRejected(), metrics.rejectedCounts(),
                    engine.queueDepth(), deduper.size(),
                    windowStore.liveWindowCount(), windowStore.liveSeriesCount(),
                    traceRegistry.traceCount());
            windowStore.query(null, null).forEach(a -> log.info(
                    "AGGREGATE window=[{},{}) service={} metric={} tags={} count={} sum={} min={} max={} avg={}",
                    a.windowStartMillis(), a.windowEndMillis(), a.service(), a.metric(),
                    a.tags(), a.count(), a.sum(), a.min(), a.max(), a.avg()));
            alertEngine.snapshots().forEach(s -> log.info(
                    "ALERT rule={}@v{} scope={} subject={} state={} breachStreak={} recoverStreak={} value={} window={} quiet={} suppressed={} marker={} basis={}",
                    s.ruleId(), s.ruleVersion(), s.scope(), s.subjectKey(), s.state(),
                    s.consecutiveBreaches(), s.consecutiveRecoveries(),
                    s.lastValue(), s.lastEvaluatedWindowStart(),
                    s.quietActive(), s.suppressedCount(), s.traceMarker(), s.basis()));
            if (versioned) {
                ruleStore.history().forEach(v -> log.info(
                        "RULE-VERSION id={} v={} enabled={} scope={} service={} metric={} threshold={} op={} quiet=[{},{}) effectiveFrom={} note={}",
                        v.id(), v.version(), v.enabled(), v.scope(), v.service(), v.metric(),
                        v.threshold(), v.operator(), v.quietFromMillis(), v.quietUntilMillis(),
                        v.effectiveFromMillis(), v.note()));
            }
        }

        @Override
        public void close() {
            engine.shutdown();
        }
    }

    static TelemetryEvent event(String id, long t, String traceId, String spanId, String parent,
                                String service, String metric, double value, Map<String, String> tags) {
        return new TelemetryEvent(id, t, traceId, spanId, parent, service, metric, value, tags);
    }

    /** 构造一条启动版本规则。 */
    static RuleVersion seed(String id, AlertRule.Scope scope, String service, String metric,
                            double threshold, AlertRule.Operator op) {
        return new RuleVersion(0, id, true, scope, service, metric,
                Map.of(), threshold, op, -1L, -1L, Long.MIN_VALUE, 0L, "启动配置");
    }
}
