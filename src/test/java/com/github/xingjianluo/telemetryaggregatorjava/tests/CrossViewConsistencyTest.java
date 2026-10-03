package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayAlert;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayStatus;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.trace.TraceStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 跨视图一致性：同一事件 / 窗口 / 调用链在聚合视图、链路视图、告警视图中的计入与排除
 * 结论一致；标识冲突、孤立链路、环路在所有视图里都不会“一个视图当正常、另一个说不存在”。
 * 复盘视角必须与实时视角看到同样的结论与异常原因。
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

    private long flush(TestHarness.Engine h, long at) {
        IngestResult r = h.send(log, TestHarness.event("flush-" + at, at,
                "tr-flush-" + at, "sf", null, "svc", "zz-flush", 0, Map.of()));
        return r.accepted() ? 1 : 0;
    }

    @Test
    void sameEventCountedConsistentlyAcrossAggregateTraceAndAlertViews() {
        // TRACE 作用域规则：trace-slow 对 latency GTE 50
        var tagsSeed = TestHarness.seed("cpu-hot", AlertRule.Scope.TAGS, "svc", "cpu",
                100.0, AlertRule.Operator.GT);
        var traceSeed = new com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersion(
                0, "trace-slow", true, AlertRule.Scope.TRACE, "svc", "latency",
                Map.of(), 50.0, AlertRule.Operator.GTE, -1L, -1L, Long.MIN_VALUE, 0L, "启动配置");
        try (TestHarness.Engine h = new TestHarness.Engine(
                props(), List.of(tagsSeed, traceSeed), 0L, 5000, 5000)) {

            // 完整链路 tr1：root->a->b；三个 span 落在同一窗口，两条 metric（cpu/latency）
            h.send(log, TestHarness.event("e-root", 1, "tr1", "root", null,
                    "svc", "cpu", 120, Map.of("host", "h1")));
            h.send(log, TestHarness.event("e-a", 2, "tr1", "a", "root",
                    "svc", "latency", 60, Map.of("host", "h1")));
            h.send(log, TestHarness.event("e-b", 3, "tr1", "b", "a",
                    "svc", "cpu", 5, Map.of("host", "h1")));
            flush(h, 11);

            // 聚合视图：窗口0 内 host=h1 的 cpu 序列 count=2（root+b），latency 序列 count=1。
            List<WindowAggregate> cpuAgg = h.windowStore.query("svc", "cpu");
            List<WindowAggregate> latAgg = h.windowStore.query("svc", "latency");
            WindowAggregate cpuW0 = cpuAgg.stream().filter(a -> a.windowStartMillis() == 0).findFirst().orElseThrow();
            WindowAggregate latW0 = latAgg.stream().filter(a -> a.windowStartMillis() == 0).findFirst().orElseThrow();
            assertEquals(2, cpuW0.count());
            assertEquals(1, latW0.count());
            assertEquals(60.0, latW0.avg(), 0.0001);

            // 链路视图：tr1 COMPLETE，3 span。
            TraceSnapshot liveTrace = h.traceRegistry.snapshot("tr1");
            assertEquals(TraceStatus.COMPLETE, liveTrace.status());
            assertEquals(3, liveTrace.spans().size());

            // 告警视图：cpu 序列 avg=(120+5)/2=62.5 不越界；latency trace 聚合 avg=60 GTE 50 FIRING。
            var cpuAlert = h.alertEngine.snapshots().stream()
                    .filter(s -> s.ruleId().equals("cpu-hot")).findFirst().orElseThrow();
            var traceAlert = h.alertEngine.snapshots().stream()
                    .filter(s -> s.ruleId().equals("trace-slow")).findFirst().orElseThrow();
            assertEquals(AlertState.OK, cpuAlert.state(), "cpu avg=62.5 未越界 => OK");
            assertEquals(AlertState.FIRING, traceAlert.state(), "latency avg=60 GTE 50 => FIRING");
            log.info("[ASSERT-LIVE] agg(cpu n=2 avg={}) agg(lat n=1 avg=60) trace=COMPLETE "
                            + "alert(cpu=OK,trace=FIRING)", cpuW0.avg());

            // 复盘视图：T=11 时所有结论必须与实时一致。
            ReplayReport rep = h.replayService().replayAt(11, "tr1");
            assertEquals(ReplayStatus.COMPLETE, rep.status());
            ReplayAlert rcpu = rep.alerts().stream()
                    .filter(a -> a.ruleId().equals("cpu-hot")).findFirst().orElseThrow();
            ReplayAlert rtr = rep.alerts().stream()
                    .filter(a -> a.ruleId().equals("trace-slow")).findFirst().orElseThrow();
            assertEquals(AlertState.OK, rcpu.state());
            assertEquals(AlertState.FIRING, rtr.state());

            WindowAggregate rCpuW0 = rep.aggregates().stream()
                    .filter(a -> a.windowStartMillis() == 0 && a.metric().equals("cpu")).findFirst().orElseThrow();
            assertEquals(2, rCpuW0.count(), "复盘 cpu 计数必须与实时一致");
            WindowAggregate rLatW0 = rep.aggregates().stream()
                    .filter(a -> a.windowStartMillis() == 0 && a.metric().equals("latency")).findFirst().orElseThrow();
            assertEquals(1, rLatW0.count(), "复盘 latency 计数必须与实时一致");

            TraceSnapshot rTrace = rep.traces().stream()
                    .filter(t -> t.traceId().equals("tr1")).findFirst().orElseThrow();
            assertEquals(TraceStatus.COMPLETE, rTrace.status());
            assertEquals(3, rTrace.spans().size());
            log.info("[ASSERT-REPLAY] 复盘 T=11 的聚合/告警/链路与实时完全一致");
            h.dumpState(log);
        }
    }

    @Test
    void orphanAndCycleAreMarkedInEveryViewAndReplayAgrees() {
        var traceSeed = new com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersion(
                0, "trace-slow", true, AlertRule.Scope.TRACE, "svc", "latency",
                Map.of(), 50.0, AlertRule.Operator.GTE, -1L, -1L, Long.MIN_VALUE, 0L, "启动配置");
        try (TestHarness.Engine h = new TestHarness.Engine(
                props(), List.of(traceSeed), 0L, 5000, 5000)) {

            // 1) 孤立链路：child 的 parent 缺失，但事件仍计入聚合与告警（带 marker）。
            h.send(log, TestHarness.event("o1", 1, "orph", "child", "missing-parent",
                    "svc", "latency", 80, Map.of()));
            flush(h, 11);
            TraceSnapshot orphanLive = h.traceRegistry.snapshot("orph");
            assertEquals(TraceStatus.ORPHAN, orphanLive.status());
            assertTrue(orphanLive.reasons().get(0).contains("孤立"));
            var orphanAlert = h.alertEngine.snapshots().stream()
                    .filter(s -> s.ruleId().equals("trace-slow") && s.subjectKey().equals("orph"))
                    .findFirst().orElseThrow();
            assertEquals(AlertState.FIRING, orphanAlert.state(), "ORPHAN 不阻断判定：avg=80 仍 FIRING");
            assertNotNull(orphanAlert.traceMarker(), "告警必须携带 traceMarker 标注 ORPHAN");
            assertTrue(orphanAlert.traceMarker().contains("ORPHAN"));
            // 聚合视图也必须包含该事件（不能在聚合里当正常数据、在链路里说不存在）。
            WindowAggregate orphanAgg = h.windowStore.query("svc", "latency").stream()
                    .filter(a -> a.windowStartMillis() == 0).findFirst().orElseThrow();
            assertEquals(1, orphanAgg.count());
            log.info("[ASSERT] ORPHAN：聚合 count=1、告警 FIRING+marker、链路 ORPHAN，三视图一致");

            // 2) 环路：a->b->c->a，数据保留、状态 CYCLE、判定继续、复盘一致。
            h.send(log, TestHarness.event("c-a", 21, "cyc", "a", "c",
                    "svc", "latency", 90, Map.of()));
            h.send(log, TestHarness.event("c-b", 22, "cyc", "b", "a",
                    "svc", "latency", 90, Map.of()));
            h.send(log, TestHarness.event("c-c", 23, "cyc", "c", "b",
                    "svc", "latency", 90, Map.of()));
            flush(h, 31);
            TraceSnapshot cycLive = h.traceRegistry.snapshot("cyc");
            assertEquals(TraceStatus.CYCLE, cycLive.status());
            assertTrue(cycLive.reasons().get(0).contains("环"));
            assertEquals(3, cycLive.spans().size(), "环路上的 span 全部保留");
            var cycAlert = h.alertEngine.snapshots().stream()
                    .filter(s -> s.ruleId().equals("trace-slow") && s.subjectKey().equals("cyc"))
                    .findFirst().orElseThrow();
            assertEquals(AlertState.FIRING, cycAlert.state());
            assertTrue(cycAlert.traceMarker().contains("CYCLE"));
            log.info("[ASSERT] CYCLE：3 span 保留、告警 FIRING+CYCLE marker、聚合照常计入");

            // 复盘 T=31：两条异常链路状态、原因、计数全部对得上。
            ReplayReport rep = h.replayService().replayAt(31, null);
            assertEquals(ReplayStatus.COMPLETE, rep.status());
            TraceSnapshot rOrph = rep.traces().stream().filter(t -> t.traceId().equals("orph")).findFirst().orElseThrow();
            TraceSnapshot rCyc = rep.traces().stream().filter(t -> t.traceId().equals("cyc")).findFirst().orElseThrow();
            assertEquals(TraceStatus.ORPHAN, rOrph.status());
            assertEquals(TraceStatus.CYCLE, rCyc.status());
            assertFalse(rCyc.reasons().isEmpty());
            ReplayAlert rOrphAlert = rep.alerts().stream()
                    .filter(a -> a.ruleId().equals("trace-slow") && a.subjectKey().equals("orph")).findFirst().orElseThrow();
            ReplayAlert rCycAlert = rep.alerts().stream()
                    .filter(a -> a.ruleId().equals("trace-slow") && a.subjectKey().equals("cyc")).findFirst().orElseThrow();
            assertEquals(AlertState.FIRING, rOrphAlert.state());
            assertEquals(AlertState.FIRING, rCycAlert.state());
            assertTrue(rOrphAlert.traceMarker().contains("ORPHAN"));
            assertTrue(rCycAlert.traceMarker().contains("CYCLE"));
            // 复盘聚合里两个窗口的 latency 都各有计数。
            assertEquals(2, rep.aggregates().stream()
                    .filter(a -> a.metric().equals("latency")).count());
            log.info("[ASSERT-REPLAY] ORPHAN/CYCLE 在复盘里的状态、原因、marker、计数与实时一致");
            h.dumpState(log);
        }
    }

    @Test
    void rejectedEventIsAbsentFromEveryViewButReasonIsConsistent() {
        var seed = TestHarness.seed("cpu-hot", AlertRule.Scope.TAGS, "svc", "cpu",
                100.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(
                props(), List.of(seed), 0L, 5000, 5000)) {
            // 先关闭窗口0。
            h.send(log, TestHarness.event("good", 1, "trg", "s", null, "svc", "cpu", 1, Map.of()));
            flush(h, 11);
            // 迟到事件（L=0，wm=11，t=2 < 11）必须拒绝。
            IngestResult late = h.send(log, TestHarness.event("late", 2, "trl", "s", null,
                    "svc", "cpu", 999, Map.of()));
            assertFalse(late.accepted());
            assertEquals(RejectReason.LATE_BEHIND_WATERMARK, late.rejectReason());

            // 三个视图都不得包含该事件：窗口0 cpu count 仍为1；trace trl 不存在；无对应告警。
            WindowAggregate w0 = h.windowStore.query("svc", "cpu").stream()
                    .filter(a -> a.windowStartMillis() == 0).findFirst().orElseThrow();
            assertEquals(1, w0.count());
            assertEquals(null, h.traceRegistry.snapshot("trl"));
            boolean alertForLate = h.alertEngine.snapshots().stream()
                    .anyMatch(s -> s.lastValue() == 999.0);
            assertFalse(alertForLate, "被拒绝事件不得驱动任何告警");

            // 复盘同样：trl 链路不存在，窗口0 计数仍为 1。
            ReplayReport rep = h.replayService().replayAt(11, "trl");
            assertEquals(ReplayStatus.COMPLETE, rep.status());
            assertTrue(rep.traces().stream().noneMatch(t -> t.traceId().equals("trl")),
                    "被拒绝事件在复盘链路视图中也必须不存在");
            WindowAggregate rw0 = rep.aggregates().stream()
                    .filter(a -> a.windowStartMillis() == 0 && a.metric().equals("cpu")).findFirst().orElseThrow();
            assertEquals(1, rw0.count());
            log.info("[ASSERT] 迟到拒绝事件在实时/复盘的聚合、链路、告警视图中均被一致排除");
            h.dumpState(log);
        }
    }

    @Test
    void duplicateSpanIdConflictIsVisibleAndCountedOnce() {
        var seed = TestHarness.seed("cpu-hot", AlertRule.Scope.TAGS, "svc", "cpu",
                1000.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(
                props(), List.of(seed), 0L, 5000, 5000)) {
            h.send(log, TestHarness.event("d1", 1, "dup", "root", null, "svc", "cpu", 10, Map.of()));
            h.send(log, TestHarness.event("d2", 2, "dup", "root", null, "svc", "cpu", 20, Map.of()));
            flush(h, 11);
            TraceSnapshot snap = h.traceRegistry.snapshot("dup");
            assertEquals(TraceStatus.DUPLICATE_SPAN, snap.status());
            // 两条事件 eventId 不同：聚合视图计 2；但链路视图只有 1 个唯一 span 并明确标冲突。
            assertEquals(1, snap.spans().size(), "重复 spanId 不得在拓扑里出现两次");
            WindowAggregate w0 = h.windowStore.query("svc", "cpu").stream()
                    .filter(a -> a.windowStartMillis() == 0).findFirst().orElseThrow();
            assertEquals(2, w0.count(), "不同 eventId 的事件仍各自计入聚合");

            ReplayReport rep = h.replayService().replayAt(11, "dup");
            TraceSnapshot rs = rep.traces().get(0);
            assertEquals(TraceStatus.DUPLICATE_SPAN, rs.status());
            assertEquals(1, rs.spans().size());
            log.info("[ASSERT] span 标识冲突：聚合 n=2、链路 1 span+DUPLICATE_SPAN，复盘一致");
            h.dumpState(log);
        }
    }
}
