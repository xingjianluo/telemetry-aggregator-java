package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 复盘：只由输入事件与规则变更历史决定；同一批输入重复复盘结论逐位一致；
 * 与当时实时结论对得上；复盘不改写实时状态；超出保留范围明确拒绝。
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

    private AlertRule rule(double threshold) {
        return new AlertRule("r", AlertRule.Scope.TAGS, "svc", "m",
                Map.of(), threshold, AlertRule.Operator.GT);
    }

    private void windowValue(TestHarness.Engine h, int windowStart, double value) {
        String id = "w" + windowStart + "-" + (int) value;
        h.send(log, TestHarness.event(id, windowStart + 1, "tr-" + id, "s", null,
                "svc", "m", value, Map.of()));
        h.send(log, TestHarness.event("seal-" + windowStart, windowStart + 11,
                "tr-seal-" + windowStart, "ss", null, "svc", "other", 0, Map.of()));
    }

    private AlertSnapshot alertOf(ReplayReport report) {
        return report.alerts().stream()
                .filter(a -> a.ruleId().equals("r")).findFirst().orElse(null);
    }

    @Test
    void replayIsDeterministicAndMatchesRealtimeAtBoundary() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 含一次规则变更的输入流：复盘与实时结论对齐，重复复盘逐位一致");

            windowValue(h, 0, 20);   // 窗口[0,10) => FIRING（v0）
            h.change(log, TestHarness.changeReq("r", RuleChange.Action.UPSERT, rule(100), null));
            windowValue(h, 20, 20);  // 窗口[20,30) => 新阈值不越界 => OK（v1）

            // 实时当前结论
            AlertSnapshot live = h.alertEngine.snapshots().stream()
                    .filter(s -> s.ruleId().equals("r")).findFirst().orElseThrow();
            assertEquals(AlertState.OK, live.state());
            assertEquals(1L, live.ruleVersion());

            // 边界 asOf=11：只重放到第一个窗口定稿 => 应当与当时实时结论一致（FIRING/v0）
            ReplayReport early = h.replay(log, 11);
            assertEquals(ReplayStatus.OK, early.status());
            assertEquals(AlertState.FIRING, alertOf(early).state());
            assertEquals(0L, alertOf(early).ruleVersion());
            assertEquals(1, early.sealedWindowCount());

            // 边界 asOf=31：两个窗口定稿 => 与当前实时结论一致（OK/v1）
            ReplayReport r1 = h.replay(log, 31);
            assertEquals(ReplayStatus.OK, r1.status());
            assertEquals(AlertState.OK, alertOf(r1).state());
            assertEquals(1L, alertOf(r1).ruleVersion());
            assertEquals(3, r1.sealedWindowCount(),
                    "窗口[0,10)、[10,20)（推进事件）、[20,30) 均已定稿");
            assertEquals(2, r1.aggregates().stream()
                    .filter(a -> a.metric().equals("m")).count());

            // 规则版本历史进入报告，可说明每个结论的依据
            assertEquals(1, r1.ruleChanges().size());
            assertEquals(11L, r1.ruleChanges().get(0).effectiveFromMillis());

            // 同一批输入重复复盘：结论逐位一致（与何时执行无关）
            ReplayReport r2 = h.replay(log, 31);
            assertEquals(r1, r2, "同一批输入的两次复盘必须逐位一致");
            log.info("[ASSERT] 两次复盘 asOf=31 结论逐位一致（{} 个聚合、{} 条告警、{} 条链路）",
                    r1.aggregates().size(), r1.alerts().size(), r1.traces().size());

            // 复盘不改写实时状态
            AlertSnapshot liveAfter = h.alertEngine.snapshots().stream()
                    .filter(s -> s.ruleId().equals("r")).findFirst().orElseThrow();
            assertEquals(live, liveAfter, "复盘不得改写实时告警状态");
            assertEquals(31L, h.watermark.currentWatermark(), "复盘不得推进实时水位线");
            log.info("[ASSERT] 复盘后实时状态未变：state={} ruleVersion={} watermark={}",
                    liveAfter.state(), liveAfter.ruleVersion(), h.watermark.currentWatermark());
            h.dumpState(log);
        }
    }

    @Test
    void replayBeyondJournalRetentionIsExplicitlyRejected() {
        TelemetryProperties p = props();
        p.getJournal().setMaxEvents(4);
        try (TestHarness.Engine h = new TestHarness.Engine(p, List.of(rule(10)), 0L)) {
            log.info("[CASE] 事件日志容量=4，写入 6 条后最老 2 条被淘汰：触及淘汰区间的复盘明确拒绝");
            windowValue(h, 0, 20);
            windowValue(h, 20, 20);
            windowValue(h, 40, 20); // 共 6 条，淘汰 eventTime=1,11 两条
            assertTrue(h.journal.evictedCount() >= 2);
            log.info("[STEP] journal evicted={} oldestEvicted={}",
                    h.journal.evictedCount(), h.journal.oldestEvictedEventTimeMillis());

            ReplayReport report = h.replay(log, 51);
            assertEquals(ReplayStatus.OUT_OF_RETENTION, report.status());
            assertNotNull(report.detail());
            assertTrue(report.aggregates().isEmpty(), "不可复盘时不得给出看似完整的部分数据");
            log.info("[ASSERT] status={} detail={}", report.status(), report.detail());
        }
    }

    @Test
    void replayBeyondRuleHistoryRetentionIsExplicitlyRejected() {
        TelemetryProperties p = props();
        p.getRules().setMaxVersions(1);
        try (TestHarness.Engine h = new TestHarness.Engine(p, List.of(rule(10)), 0L)) {
            log.info("[CASE] 规则版本历史上限=1，两次变更后最早版本被折叠：依赖旧版本的窗口不可复盘");
            windowValue(h, 0, 20);
            h.change(log, TestHarness.changeReq("r", RuleChange.Action.UPSERT, rule(100), null));
            windowValue(h, 20, 20);
            h.change(log, TestHarness.changeReq("r", RuleChange.Action.UPSERT, rule(1000), null));
            windowValue(h, 40, 5);
            assertNotNull(h.ruleRegistry.oldestEvictedEffectiveFromMillis());
            log.info("[STEP] 规则历史已折叠，oldestEvictedEffectiveFrom={}",
                    h.ruleRegistry.oldestEvictedEffectiveFromMillis());

            ReplayReport report = h.replay(log, 51);
            assertEquals(ReplayStatus.OUT_OF_RETENTION, report.status());
            assertTrue(report.detail().contains("规则版本"), "应说明是规则历史超出保留范围: "
                    + report.detail());
            log.info("[ASSERT] status={} detail={}", report.status(), report.detail());
        }
    }
}
