package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 调用链作用域告警：按 traceId 评估；孤立/乱序/环路链路的判定不中断，结论带可解释标记。
 */
class TraceAlertTest {

    private static final Logger log = LoggerFactory.getLogger(TraceAlertTest.class);

    private TelemetryProperties props() {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(0);
        p.getAlert().setBreachStreak(1);
        p.getAlert().setRecoverStreak(1);
        return p;
    }

    private AlertRule traceRule() {
        return new AlertRule("trace-hot", AlertRule.Scope.TRACE, null, "latency",
                Map.of(), 10.0, AlertRule.Operator.GT);
    }

    @Test
    void orphanTraceStillEvaluatesAndCarriesMarker() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(traceRule()), 0L)) {
            log.info("[CASE] ORPHAN 链路（parent 缺失）窗口均值超阈值：仍出告警，marker 标明拓扑异常");
            // 一个孤立 span：parent=ghost 不存在，value=20 超阈值
            h.send(log, TestHarness.event("o1", 1, "tr-orphan", "child", "ghost",
                    "svc", "latency", 20, Map.of()));
            // 关闭窗口 0（推进事件落到窗口1，不参与 latency 指标）
            h.send(log, TestHarness.event("seal", 11, "tr-seal", "s", null,
                    "svc", "other", 0, Map.of()));

            AlertSnapshot snap = h.alertEngine.snapshots().stream()
                    .filter(s -> s.ruleId().equals("trace-hot")
                            && s.subjectKey().equals("tr-orphan"))
                    .findFirst().orElse(null);
            assertNotNull(snap, "孤立链路也必须产生判定，不能因拓扑异常中断");
            assertEquals(AlertState.FIRING, snap.state());
            assertNotNull(snap.traceMarker());
            assertTrue(snap.traceMarker().contains("ORPHAN"),
                    "marker 必须标明 ORPHAN: " + snap.traceMarker());
            log.info("[ASSERT] state={} value={} marker={}", snap.state(), snap.lastValue(), snap.traceMarker());
            h.dumpState(log);
        }
    }

    @Test
    void cyclicTraceStillEvaluatesAndCarriesCycleMarker() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(traceRule()), 0L)) {
            log.info("[CASE] CYCLE 链路超阈值：判定继续，marker=CYCLE");
            h.send(log, TestHarness.event("a", 1, "trc", "a", "c", "svc", "latency", 12, Map.of()));
            h.send(log, TestHarness.event("b", 2, "trc", "b", "a", "svc", "latency", 14, Map.of()));
            h.send(log, TestHarness.event("c", 3, "trc", "c", "b", "svc", "latency", 16, Map.of()));
            h.send(log, TestHarness.event("seal", 11, "trs", "s", null, "svc", "other", 0, Map.of()));

            AlertSnapshot snap = h.alertEngine.snapshots().stream()
                    .filter(s -> s.subjectKey().equals("trc")).findFirst().orElseThrow();
            assertEquals(AlertState.FIRING, snap.state());
            assertTrue(snap.traceMarker().contains("CYCLE"));
            assertEquals(14.0, snap.lastValue(), 1e-9, "三 span 均值=(12+14+16)/3=14");
            log.info("[ASSERT] CYCLE 链路 state={} avg={} marker={}",
                    snap.state(), snap.lastValue(), snap.traceMarker());
            h.dumpState(log);
        }
    }
}
