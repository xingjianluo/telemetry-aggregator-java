package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 调用链还原：孤立、乱序到达、环路、重复 span 均被明确标记且不丢数据。
 */
class TraceTopologyTest {

    private static final Logger log = LoggerFactory.getLogger(TraceTopologyTest.class);

    private TelemetryProperties props() {
        return TestHarness.defaultProps();
    }

    @Test
    void orphanThenOutOfOrderCompletionTransitionsToComplete() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(), 0L)) {
            log.info("[CASE] 子 span 先到（parent 缺失=ORPHAN），根 span 乱序后到 => COMPLETE");
            h.send(log, TestHarness.event("e1", 10, "tr", "child", "root",
                    "svc", "m", 1, Map.of()));
            TraceSnapshot s1 = h.traceRegistry.snapshot("tr");
            log.info("[STEP1] status={} reasons={}", s1.status(), s1.reasons());
            assertEquals(TraceStatus.ORPHAN, s1.status());

            h.send(log, TestHarness.event("e2", 8, "tr", "root", null,
                    "svc", "m", 1, Map.of()));
            TraceSnapshot s2 = h.traceRegistry.snapshot("tr");
            log.info("[STEP2] status={} reasons={} spans={}", s2.status(), s2.reasons(), s2.spans().size());
            assertEquals(TraceStatus.COMPLETE, s2.status());
            assertEquals(2, s2.spans().size(), "孤立期间的数据被保留，未丢弃整条链路");
            h.dumpState(log);
        }
    }

    @Test
    void cycleIsMarkedAndDataIsKept() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(), 0L)) {
            log.info("[CASE] a->b->c->a 构成有向环 => CYCLE，且 span 全部保留、判定不中断");
            h.send(log, TestHarness.event("a", 1, "trc", "a", "c", "svc", "m", 1, Map.of()));
            h.send(log, TestHarness.event("b", 2, "trc", "b", "a", "svc", "m", 1, Map.of()));
            h.send(log, TestHarness.event("c", 3, "trc", "c", "b", "svc", "m", 1, Map.of()));
            TraceSnapshot s = h.traceRegistry.snapshot("trc");
            log.info("[ASSERT] status={} reasons={} spans={}", s.status(), s.reasons(), s.spans().size());
            assertEquals(TraceStatus.CYCLE, s.status());
            assertEquals(3, s.spans().size());
            assertNotNull(s.reasons());
            h.dumpState(log);
        }
    }

    @Test
    void duplicateSpanIsMarked() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(), 0L)) {
            log.info("[CASE] 相同 spanId 再次出现 => DUPLICATE_SPAN");
            h.send(log, TestHarness.event("e1", 1, "trd", "root", null, "svc", "m", 1, Map.of()));
            // 不同 eventId、相同 spanId（重复投递 span 标识冲突）
            h.send(log, TestHarness.event("e2", 2, "trd", "root", null, "svc", "m", 2, Map.of()));
            TraceSnapshot s = h.traceRegistry.snapshot("trd");
            log.info("[ASSERT] status={} reasons={} spans={}", s.status(), s.reasons(), s.spans().size());
            assertEquals(TraceStatus.DUPLICATE_SPAN, s.status());
            h.dumpState(log);
        }
    }

    @Test
    void multipleRootsAreOrphan() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(), 0L)) {
            log.info("[CASE] 两个无根 span => ORPHAN（多根），数据保留");
            h.send(log, TestHarness.event("e1", 1, "trm", "r1", null, "svc", "m", 1, Map.of()));
            h.send(log, TestHarness.event("e2", 2, "trm", "r2", null, "svc", "m", 1, Map.of()));
            TraceSnapshot s = h.traceRegistry.snapshot("trm");
            log.info("[ASSERT] status={} reasons={}", s.status(), s.reasons());
            assertEquals(TraceStatus.ORPHAN, s.status());
            assertEquals(2, s.spans().size());
            h.dumpState(log);
        }
    }

    @Test
    void traceCardinalityLimitsAreObservable() {
        TelemetryProperties p = props();
        p.getCardinality().setMaxTraces(2);
        p.getCardinality().setMaxSeriesPerWindow(1000);
        p.getCardinality().setMaxTraceSeriesPerWindow(1000);
        try (TestHarness.Engine h = new TestHarness.Engine(p, List.of(), 0L)) {
            log.info("[CASE] 调用链数量上限=2，第 3 条 trace 被 TOO_MANY_TRACES 拒绝");
            h.send(log, TestHarness.event("e1", 1, "t1", "s", null, "svc", "m", 1, Map.of()));
            h.send(log, TestHarness.event("e2", 1, "t2", "s", null, "svc", "m", 1, Map.of()));
            var third = h.send(log, TestHarness.event("e3", 1, "t3", "s", null,
                    "svc", "m", 1, Map.of()));
            assertEquals(com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason.TOO_MANY_TRACES,
                    third.rejectReason());
            log.info("[ASSERT] 拒绝原因={} traceCount={}", third.rejectReason(), h.traceRegistry.traceCount());
            assertEquals(2, h.traceRegistry.traceCount());
            h.dumpState(log);
        }
    }
}
