package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEngine;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.dedup.EventDeduper;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionEngine;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionMetrics;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
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
                    "ALERT rule={} scope={} subject={} state={} breachStreak={} recoverStreak={} value={} window={} marker={}",
                    s.ruleId(), s.scope(), s.subjectKey(), s.state(),
                    s.consecutiveBreaches(), s.consecutiveRecoveries(),
                    s.lastValue(), s.lastEvaluatedWindowStart(), s.traceMarker()));
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
}
