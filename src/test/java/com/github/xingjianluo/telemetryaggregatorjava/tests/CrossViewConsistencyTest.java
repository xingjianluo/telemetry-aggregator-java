package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayStatus;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 跨视图一致性：同一事件 / 窗口 / 调用链在聚合、链路、告警三视图中的
 * 计入与排除结论一致，异常标记与拒绝原因对得上；
 * 孤立链路与环路在任何视图都不会“既是正常数据又不存在”。
 */
class CrossViewConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(CrossViewConsistencyTest.class);

    private TelemetryProperties props() {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(0);
        p.getAlert().setBreachStreak(1);
        p.getAlert().setRecoverStreak(1);
        return p;
    }

    private AlertSnapshot alert(TestHarness.Engine h, String ruleId, String subject) {
        return h.alertEngine.snapshots().stream()
                .filter(s -> s.ruleId().equals(ruleId) && s.subjectKey().equals(subject))
                .findFirst().orElse(null);
    }

    private WindowAggregate aggregate(TestHarness.Engine h, String metric) {
        return h.windowStore.query("svc", metric).stream()
                .filter(a -> a.windowStartMillis() == 0).findFirst().orElse(null);
    }

    @Test
    void sameEventIsConsistentAcrossAggregateTraceAndAlertViews() {
        List<AlertRule> rules = List.of(
                new AlertRule("r", AlertRule.Scope.TAGS, "svc", "m", Map.of(),
                        10.0, AlertRule.Operator.GT),
                new AlertRule("tr", AlertRule.Scope.TRACE, null, "latency", Map.of(),
                        10.0, AlertRule.Operator.GT));
        try (TestHarness.Engine h = new TestHarness.Engine(props(), rules, 0L)) {
            log.info("[CASE] 正常事件 + 孤立链路 + 环路 + 冲突重投 + 迟到：三视图结论互相对得上");

            IngestResult a1 = h.send(log, TestHarness.event("e1", 1, "tr-1", "root", null,
                    "svc", "m", 20, Map.of()));
            assertTrue(a1.accepted());
            h.send(log, TestHarness.event("e3", 2, "tr-o", "child", "ghost",
                    "svc", "latency", 20, Map.of()));
            h.send(log, TestHarness.event("e4", 3, "tr-c", "a", "c",
                    "svc", "latency", 15, Map.of()));
            h.send(log, TestHarness.event("e5", 4, "tr-c", "c", "a",
                    "svc", "latency", 15, Map.of()));
            h.send(log, TestHarness.event("seal", 11, "tr-seal", "s", null,
                    "svc", "other", 0, Map.of()));

            // 聚合视图：e1 计入 svc/m；e3/e4/e5 计入 svc/latency（异常链路也计入，不静默排除）
            WindowAggregate m = aggregate(h, "m");
            assertEquals(1, m.count());
            assertEquals(20.0, m.sum(), 1e-9);
            WindowAggregate lat = aggregate(h, "latency");
            assertEquals(3, lat.count(), "孤立/环路链路的 span 在聚合视图中照常计入");
            log.info("[STEP] 聚合视图 svc/m count=1 avg=20；svc/latency count={}", lat.count());

            // 链路视图：三条链各自结论
            assertEquals(TraceStatus.COMPLETE, h.traceRegistry.snapshot("tr-1").status());
            assertEquals(TraceStatus.ORPHAN, h.traceRegistry.snapshot("tr-o").status());
            assertEquals(TraceStatus.CYCLE, h.traceRegistry.snapshot("tr-c").status());

            // 告警视图：同一窗口、同一均值；异常链路的标记与链路视图一致
            AlertSnapshot tagAlert = alert(h, "r", "{}");
            assertEquals(AlertState.FIRING, tagAlert.state());
            assertEquals(20.0, tagAlert.lastValue(), 1e-9);
            assertEquals(0L, tagAlert.lastEvaluatedWindowStart(),
                    "告警评估的窗口与聚合视图窗口一致");
            AlertSnapshot orphanAlert = alert(h, "tr", "tr-o");
            assertEquals(AlertState.FIRING, orphanAlert.state());
            assertTrue(orphanAlert.traceMarker().contains("ORPHAN"),
                    "告警标记必须与链路视图结论一致");
            AlertSnapshot cycleAlert = alert(h, "tr", "tr-c");
            assertTrue(cycleAlert.traceMarker().contains("CYCLE"));
            log.info("[STEP] 告警视图 tr-o marker=ORPHAN、tr-c marker=CYCLE，与链路视图一致");

            // 冲突重投：拒绝原因可区分，且任何视图都不重复计入
            IngestResult conflict = h.send(log, TestHarness.event("e1", 1, "tr-1", "root", null,
                    "svc", "m", 999, Map.of()));
            assertFalse(conflict.accepted());
            assertEquals(RejectReason.CONFLICTING_EVENT_ID, conflict.rejectReason());
            assertEquals(1, aggregate(h, "m").count(), "冲突重投不得在聚合视图重复计数");
            assertEquals(1, h.traceRegistry.snapshot("tr-1").spans().size(),
                    "冲突重投不得在链路视图重复登记");

            // 迟到事件：确定拒绝，三视图均不计入
            IngestResult late = h.send(log, TestHarness.event("e6", 2, "tr-1", "late-span", "root",
                    "svc", "m", 50, Map.of()));
            assertFalse(late.accepted());
            assertEquals(RejectReason.LATE_BEHIND_WATERMARK, late.rejectReason());
            assertEquals(1, aggregate(h, "m").count(), "迟到事件不得计入已定稿窗口");
            assertEquals(1, h.traceRegistry.snapshot("tr-1").spans().size(),
                    "迟到事件不得进入链路视图");
            log.info("[STEP] 冲突与迟到均被拒绝且三视图零计入，原因={}/{}",
                    conflict.rejectReason(), late.rejectReason());

            // 复盘视图与实时三视图一致，且拒绝原因进入审计计数
            ReplayReport replay = h.replay(log, 11);
            assertEquals(ReplayStatus.OK, replay.status());
            assertEquals(5, replay.acceptedCount());
            assertEquals(1L, replay.rejectedByReason().get(RejectReason.LATE_BEHIND_WATERMARK),
                    "复盘审计必须列出边界内的确定拒绝");
            assertEquals(TraceStatus.ORPHAN, replay.traces().stream()
                    .filter(t -> t.traceId().equals("tr-o")).findFirst().orElseThrow().status());
            assertEquals(TraceStatus.CYCLE, replay.traces().stream()
                    .filter(t -> t.traceId().equals("tr-c")).findFirst().orElseThrow().status());
            AlertSnapshot replayOrphan = replay.alerts().stream()
                    .filter(s -> s.ruleId().equals("tr") && s.subjectKey().equals("tr-o"))
                    .findFirst().orElseThrow();
            assertTrue(replayOrphan.traceMarker().contains("ORPHAN"),
                    "复盘告警标记必须与实时一致");
            log.info("[ASSERT] 聚合/链路/告警/复盘四处的计入、排除与异常标记全部对齐");
            h.dumpState(log);
        }
    }
}
