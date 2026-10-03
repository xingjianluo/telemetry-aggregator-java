package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayAlert;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayService;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayStatus;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeRequest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 按事件时间复盘的可复现性与隔离性：
 * 同一批输入多次执行、不同实例执行结论一致；与实时结果对得上；
 * 复盘不改写实时状态；超出保留范围明确 NOT_REPLAYABLE；资源超限 RESOURCE_LIMITED。
 */
class ReplayDeterminismTest {

    private static final Logger log = LoggerFactory.getLogger(ReplayDeterminismTest.class);

    private TelemetryProperties props() {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(0);
        p.getAlert().setBreachStreak(1);
        p.getAlert().setRecoverStreak(1);
        return p;
    }

    /** 构造一段确定性输入：窗口 0/20/40/60 各一条 m 事件，阈值在边界 21 从 100 改为 10。 */
    private void buildScenario(TestHarness.Engine h) {
        h.send(log, TestHarness.event("a0", 1, "t0", "s0", null, "svc", "m", 50, Map.of()));
        h.send(log, TestHarness.event("f0", 11, "tf0", "sf", null, "svc", "z", 0, Map.of()));
        // 水位线现在 11；在此变更阈值 => effectiveFrom=11
        RuleChangeResultLike(h);
        h.send(log, TestHarness.event("a20", 21, "t20", "s20", null, "svc", "m", 50, Map.of()));
        h.send(log, TestHarness.event("f20", 31, "tf20", "sf", null, "svc", "z", 0, Map.of()));
        h.send(log, TestHarness.event("a40", 41, "t40", "s40", null, "svc", "m", 5, Map.of()));
        h.send(log, TestHarness.event("f40", 51, "tf40", "sf", null, "svc", "z", 0, Map.of()));
        h.send(log, TestHarness.event("a60", 61, "t60", "s60", null, "svc", "m", 50, Map.of()));
        h.send(log, TestHarness.event("f60", 71, "tf60", "sf", null, "svc", "z", 0, Map.of()));
    }

    private static void RuleChangeResultLike(TestHarness.Engine h) {
        h.engine.changeRule(new RuleChangeRequest(
                "lower", "r1", null, null, null, null, null,
                10.0, null, null, null, "阈值100->10"));
    }

    private ReplayAlert r1Alert(ReplayReport r) {
        return r.alerts().stream().filter(a -> a.ruleId().equals("r1")).findFirst().orElseThrow();
    }

    @Test
    void replayIsReproducibleAcrossRunsAndInstancesAndMatchesLive() {
        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                100.0, AlertRule.Operator.GT);
        TelemetryProperties p1 = props();
        ReplayReport first;
        ReplayReport second;
        try (TestHarness.Engine h = new TestHarness.Engine(p1, List.of(seed), 0L, 5000, 5000)) {
            buildScenario(h);
            ReplayService svc1 = h.replayService();
            // T=71：窗口 0/20/40/60 均已定稿。
            first = svc1.replayAt(71, null);
            second = svc1.replayAt(71, null);

            assertEquals(ReplayStatus.COMPLETE, first.status());
            // 1) 同一实例两次重放结果一致（状态、计数、水位线、版本）。
            assertEquals(canonical(first), canonical(second),
                    "同一批输入多次复盘必须逐字节一致");
            log.info("[ASSERT] 同一实例两次复盘结果一致：aggs={} alerts={} wm={} cutoffSeq={}",
                    first.aggregates().size(), first.alerts().size(),
                    first.watermarkMillis(), first.cutoffSeq());
        }

        // 2) 另起一个全新实例，喂完全相同的输入（相同 seq 交错顺序），结论一致。
        var seed2 = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                100.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h2 = new TestHarness.Engine(props(), List.of(seed2), 0L, 5000, 5000)) {
            buildScenario(h2);
            ReplayReport otherInstance = h2.replayService().replayAt(71, null);
            assertEquals(canonical(first), canonical(otherInstance),
                    "不同实例、不同时刻执行，同一批输入结论必须一致");
            log.info("[ASSERT] 跨实例复盘结果一致");

            // 3) 与实时结果对得上：w0 用旧阈值 100 不报警；w20 用新阈值 10 已 FIRING；
            //    w40=5 恢复 OK（recoverStreak=1）；w60=50 再次 FIRING。最终实时=FIRING。
            AlertState live = h2.alertEngine.snapshots().stream()
                    .filter(s -> s.ruleId().equals("r1")).findFirst().orElseThrow().state();
            assertEquals(AlertState.FIRING, live);
            assertEquals(live, r1Alert(otherInstance).state(),
                    "同一时间边界下复盘结论必须与实时已产生的结果对得上");
            log.info("[ASSERT] T=71 复盘告警状态={} 与实时一致", live);

            // 4) 边界之前定稿窗口维持原判：在 T=11 复盘时还没有窗口按新阈值判定。
            ReplayReport atBoundary = h2.replayService().replayAt(11, null);
            assertEquals(ReplayStatus.COMPLETE, atBoundary.status());
            // 此时只有窗口0定稿；avg=50 对旧阈值100 => OK；且使用 v1。
            ReplayAlert aAt11 = r1Alert(atBoundary);
            assertEquals(AlertState.OK, aAt11.state());
            assertEquals(1L, aAt11.ruleVersion(), "边界 11 处窗口0必须按 v1 判定");
            log.info("[ASSERT] T=11 边界处 w0 按 v1(阈值100) 判定为 OK，规则版本={}", aAt11.ruleVersion());

            // 5) 规则版本表在 T=31 的复盘里应包含 v2（effectiveFrom=11<=31）。
            ReplayReport at31 = h2.replayService().replayAt(31, null);
            assertEquals(2, at31.rulesEffective().stream()
                    .filter(v -> v.id().equals("r1")).findFirst().orElseThrow().version());
            log.info("[ASSERT] T=31 复盘看到 r1 已生效 v2");

            // 6) 每个复盘告警都带判定依据。
            otherInstance.alerts().forEach(a ->
                    assertFalse(a.basis().isEmpty(), "每个复盘告警必须有判定依据"));
            otherInstance.alerts().forEach(a -> log.info(
                    "REPLAY-ALERT rule={}@v{} state={} quiet={} basis={}",
                    a.ruleId(), a.ruleVersion(), a.state(), a.quietAtCutoff(), a.basis()));
            h2.dumpState(log);
        }
    }

    @Test
    void replayDoesNotMutateLiveState() {
        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                100.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(seed), 0L, 5000, 5000)) {
            buildScenario(h);
            long wmBefore = h.watermark.currentWatermark();
            long acceptedBefore = h.metrics.acceptedCount();
            int journalBefore = h.journal.eventCount();

            ReplayReport r1 = h.replayService().replayAt(41, null);
            ReplayReport r2 = h.replayService().replayAt(11, null);
            assertNotEquals(r1.cutoffSeq(), r2.cutoffSeq());

            assertEquals(wmBefore, h.watermark.currentWatermark(), "复盘不得推进实时水位线");
            assertEquals(acceptedBefore, h.metrics.acceptedCount(), "复盘不得改变实时接受计数");
            assertEquals(journalBefore, h.journal.eventCount(), "复盘不得写入实时日志");
            log.info("[ASSERT] 两次不同切点复盘后实时水位线/接受数/日志均未变化");
        }
    }

    @Test
    void replayBeyondRetentionIsClearlyNotReplayable() {
        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                1000.0, AlertRule.Operator.GT);
        TelemetryProperties p = props();
        p.getReplay().setMaxJournalEvents(3);
        try (TestHarness.Engine h = new TestHarness.Engine(p, List.of(seed), 0L, 3, 1000)) {
            // 9 条事件跨 9 个窗口，日志只留最近 3 条；中间穿插高事件时间推进。
            for (int i = 0; i < 9; i++) {
                h.send(log, TestHarness.event("e" + i, 1L + i * 10, "t" + i, "s", null,
                        "svc", "m", i % 2 == 0 ? 50 : 5, Map.of()));
            }
            ReplayService svc = h.replayService();
            ReplayReport old = svc.replayAt(20, null);
            assertEquals(ReplayStatus.NOT_REPLAYABLE, old.status());
            assertTrue(old.unavailableReason().contains("可复盘下界"));
            log.info("[ASSERT] 早于保留下界的复盘 => NOT_REPLAYABLE: {}", old.unavailableReason());

            ReplayReport recent = svc.replayAt(81, null);
            assertTrue(recent.replayable(), "下界之后的时间点仍可复盘");
            log.info("[ASSERT] 下界之后 T=81 => {}，aggs={} alerts={}",
                    recent.status(), recent.aggregates().size(), recent.alerts().size());
        }
    }

    @Test
    void replayResourceLimitsAreEnforced() {
        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                1000.0, AlertRule.Operator.GT);
        TelemetryProperties p = props();
        p.getReplay().setMaxScanEventsPerReplay(2);
        try (TestHarness.Engine h = new TestHarness.Engine(p, List.of(seed), 0L, 1000, 1000)) {
            for (int i = 0; i < 6; i++) {
                h.send(log, TestHarness.event("e" + i, 1L + i * 10, "t" + i, "s", null,
                        "svc", "m", 1, Map.of()));
            }
            ReplayReport r = h.replayService().replayAt(61, null);
            assertEquals(ReplayStatus.RESOURCE_LIMITED, r.status(),
                    "扫描事件数超上限必须快速返回 RESOURCE_LIMITED，不拖垮摄取");
            log.info("[ASSERT] 超扫描上限 => RESOURCE_LIMITED: {}", r.unavailableReason());

            // 复盘期间实时摄取仍可正常工作。
            var live = h.send(log, TestHarness.event("after", 101, "ta", "sa", null,
                    "svc", "m", 1, Map.of()));
            assertTrue(live.accepted(), "复盘资源受限不得影响实时摄取");
        }
    }

    /** 去掉易变字段后的规范化文本，用于跨运行比较。 */
    private String canonical(ReplayReport r) {
        StringBuilder sb = new StringBuilder();
        sb.append("wm=").append(r.watermarkMillis())
                .append(" cutoff=").append(r.cutoffSeq())
                .append(" status=").append(r.status());
        for (WindowAggregate a : r.aggregates()) {
            sb.append("\nA[").append(a.windowStartMillis()).append(',').append(a.windowEndMillis())
                    .append(")").append(a.service()).append('/').append(a.metric())
                    .append(a.tags()).append(" n=").append(a.count())
                    .append(" sum=").append(a.sum()).append(" avg=").append(a.avg());
        }
        r.alerts().stream().map(a -> a.ruleId() + "@v" + a.ruleVersion() + ":" + a.state()
                        + ":b" + a.consecutiveBreaches() + ":r" + a.consecutiveRecoveries()
                        + ":v" + a.lastValue() + ":w" + a.lastEvaluatedWindowStart()
                        + ":q" + a.quietAtCutoff() + ":s" + a.suppressedCount()
                        + ":" + a.basis())
                .sorted().forEach(s -> sb.append("\nL").append(s));
        r.traces().stream().map(t -> t.traceId() + ":" + t.status() + ":" + t.reasons()
                        + ":" + t.spans().size())
                .sorted().forEach(s -> sb.append("\nT").append(s));
        r.rulesEffective().stream()
                .map(v -> v.id() + "@v" + v.version() + ":th" + v.threshold() + ":en" + v.enabled())
                .sorted().forEach(s -> sb.append("\nR").append(s));
        return sb.toString();
    }
}
