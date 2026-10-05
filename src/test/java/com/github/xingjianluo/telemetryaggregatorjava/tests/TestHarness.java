package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEngine;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleRegistry;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayService;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.dedup.EventDeduper;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.EventProcessor;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionEngine;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionMetrics;
import com.github.xingjianluo.telemetryaggregatorjava.replay.EventJournal;
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
        final RuleRegistry ruleRegistry;
        final IngestionMetrics metrics;
        final EventJournal journal;
        final EventProcessor processor;
        final IngestionEngine engine;
        final ReplayService replayService;

        Engine(TelemetryProperties props, List<AlertRule> rules, long cooldownMillis) {
            this.props = props;
            // 冷却参数统一写回配置，保证实时与复盘（ReplayService 从 props 构造）使用同一值。
            props.getAlert().setCooldown(java.time.Duration.ofMillis(cooldownMillis));
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
            this.alertEngine = new AlertEngine(
                    props.getAlert().getBreachStreak(),
                    props.getAlert().getRecoverStreak(),
                    cooldownMillis,
                    props.getAlert().getEvidenceTrailSize());
            this.ruleRegistry = new RuleRegistry(rules, props.getRules().getMaxVersions());
            this.metrics = new IngestionMetrics();
            this.journal = new EventJournal(props.getJournal().getMaxEvents());
            this.processor = new EventProcessor(props, watermark, windowStore, traceRegistry,
                    alertEngine, ruleRegistry, deduper, metrics, journal);
            this.engine = new IngestionEngine(props, processor, metrics);
            this.replayService = new ReplayService(props, journal, ruleRegistry);
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

        RuleChange change(Logger log, RuleChangeRequest req) {
            com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeOutcome outcome =
                    submitChange(log, req);
            return outcome.change();
        }

        com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeOutcome submitChange(
                Logger log, RuleChangeRequest req) {
            com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeOutcome outcome =
                    engine.submitRuleChange(req);
            RuleChange c = outcome.change();
            log.info("RULE_CHANGE requestId={} replayed={} version={} rule={} action={} effectiveFrom={} muteUntil={} snapshot={}",
                    c.requestId(), outcome.replayed(), c.version(), c.ruleId(), c.action(),
                    c.effectiveFromMillis(), c.muteUntilMillis(), c.ruleSnapshot());
            return outcome;
        }

        ReplayReport replay(Logger log, long asOfMillis) {
            ReplayReport r = replayService.replay(asOfMillis);
            log.info("REPLAY asOf={} => status={} watermark={} accepted={} sealedWindows={} detail={}",
                    asOfMillis, r.status(), r.replayWatermarkMillis(), r.acceptedCount(),
                    r.sealedWindowCount(), r.detail());
            r.alerts().forEach(a -> {
                log.info(
                        "REPLAY_ALERT rule={} subject={} state={} ruleVersion={} muted={} value={} evidenceSteps={}",
                        a.ruleId(), a.subjectKey(), a.state(), a.ruleVersion(), a.muted(),
                        a.lastValue(), a.evidence().size());
                a.evidence().forEach(v -> log.info(
                        "REPLAY_BASIS rule={} subject={} v{} window=[{},{}) avg={}/n={} {} => {}->{} muted={} breach={} streak={}/{} transition={} suppressed={} | {}",
                        a.ruleId(), a.subjectKey(), v.ruleVersion(),
                        v.windowStartMillis(), v.windowEndMillis(), v.avg(), v.count(),
                        v.operator() + " " + v.threshold(), v.stateBefore(), v.stateAfter(),
                        v.muted(), v.breach(), v.consecutiveBreaches(),
                        v.consecutiveRecoveries(), v.transition(), v.suppressed(),
                        v.rationale()));
            });
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
            alertEngine.snapshots().forEach(s -> {
                log.info(
                        "ALERT rule={} scope={} subject={} state={} breachStreak={} recoverStreak={} value={} window={} marker={} ruleVersion={} muted={} evidenceSteps={}",
                        s.ruleId(), s.scope(), s.subjectKey(), s.state(),
                        s.consecutiveBreaches(), s.consecutiveRecoveries(),
                        s.lastValue(), s.lastEvaluatedWindowStart(), s.traceMarker(),
                        s.ruleVersion(), s.muted(), s.evidence().size());
                s.evidence().forEach(v -> log.info(
                        "BASIS rule={} subject={} v{} window=[{},{}) avg={}/n={} {} => {}->{} muted={} breach={} streak={}/{} transition={} suppressed={} | {}",
                        s.ruleId(), s.subjectKey(), v.ruleVersion(),
                        v.windowStartMillis(), v.windowEndMillis(), v.avg(), v.count(),
                        v.operator() + " " + v.threshold(), v.stateBefore(), v.stateAfter(),
                        v.muted(), v.breach(), v.consecutiveBreaches(),
                        v.consecutiveRecoveries(), v.transition(), v.suppressed(),
                        v.rationale()));
            });
        }

        @Override
        public void close() {
            engine.shutdown();
            replayService.shutdown();
        }
    }

    static TelemetryEvent event(String id, long t, String traceId, String spanId, String parent,
                                String service, String metric, double value, Map<String, String> tags) {
        return new TelemetryEvent(id, t, traceId, spanId, parent, service, metric, value, tags);
    }

    private static final java.util.concurrent.atomic.AtomicLong REQUEST_SEQ =
            new java.util.concurrent.atomic.AtomicLong();

    /** 构造带显式幂等键的变更请求（同键重试请复用同一个 requestId）。 */
    static RuleChangeRequest changeReq(String requestId, String ruleId, RuleChange.Action action,
                                       AlertRule rule, Long muteUntilMillis) {
        return new RuleChangeRequest(requestId, ruleId, action, rule, muteUntilMillis);
    }

    /** 构造带自动幂等键的变更请求（每个不同的逻辑变更调用一次）。 */
    static RuleChangeRequest changeReq(String ruleId, RuleChange.Action action,
                                       AlertRule rule, Long muteUntilMillis) {
        return new RuleChangeRequest(
                "req-" + REQUEST_SEQ.incrementAndGet(), ruleId, action, rule, muteUntilMillis);
    }
}
