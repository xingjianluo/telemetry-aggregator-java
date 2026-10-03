package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayStatus;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeRequest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 新链路上的历史语义回归：迟到可区分拒绝、重复不双计数、标识冲突明确拒绝、
 * 阈值附近抖动不反复翻转；并验证这些历史场景在复盘视图里同样成立。
 */
class FeatureRegressionTest {

    private static final Logger log = LoggerFactory.getLogger(FeatureRegressionTest.class);

    @Test
    void lateDuplicateConflictAndThresholdJitterOnVersionedPipeline() {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(0);
        p.getAlert().setBreachStreak(2);
        p.getAlert().setRecoverStreak(2);

        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                10.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(p, List.of(seed), 0L, 5000, 5000)) {

            // ---- 重复：同 ID 同载荷，任何重试返回首次结论且不双计数 ----
            var e1 = TestHarness.event("dup", 1, "t1", "s", null, "svc", "m", 1, Map.of());
            assertTrue(h.send(log, e1).accepted());
            assertTrue(h.send(log, e1).accepted(), "重复投递仍返回首次 ACCEPTED 结论");
            assertEquals(1, h.metrics.acceptedCount());
            assertEquals(1, h.metrics.duplicateReplayCount());
            h.send(log, TestHarness.event("f0", 11, "tf", "s", null, "svc", "z", 0, Map.of()));

            // ---- 迟到：wm=11 后，t=2 属于已定稿窗口 0 ----
            IngestResult late = h.send(log, TestHarness.event("late", 2, "t2", "s", null,
                    "svc", "m", 1, Map.of()));
            assertFalse(late.accepted());
            assertEquals(RejectReason.LATE_BEHIND_WATERMARK, late.rejectReason());
            assertTrue(late.detail().contains("迟到边界") || late.detail().contains("定稿关闭"),
                    "迟到拒绝必须携带可解释的边界依据");
            log.info("[ASSERT] 迟到拒绝可区分：{}", late.detail());
            // 迟到事件的重试仍然幂等地得到同一拒绝结论。
            IngestResult lateRetry = h.engine.ingest(TestHarness.event("late", 2, "t2", "s", null,
                    "svc", "m", 1, Map.of()));
            assertEquals(late.rejectReason(), lateRetry.rejectReason());
            log.info("[ASSERT] 迟到拒绝可区分且重试幂等：{}", late.detail());

            // 窗口0 的 m 序列只有 e1 一条，重复与迟到都没被计入。
            WindowAggregate w0 = h.windowStore.query("svc", "m").stream()
                    .filter(a -> a.windowStartMillis() == 0).findFirst().orElseThrow();
            assertEquals(1, w0.count());

            // ---- 标识冲突：相同 eventId 不同载荷 ----
            IngestResult conflict = h.engine.ingest(TestHarness.event("dup", 1, "t1", "s", null,
                    "svc", "m", 999, Map.of("host", "other")));
            assertEquals(RejectReason.CONFLICTING_EVENT_ID, conflict.rejectReason());
            log.info("[ASSERT] 同 ID 不同载荷 => CONFLICTING_EVENT_ID");

            // ---- 阈值抖动：单点毛刺不报警，连续确认才 FIRING，瞬时回落不恢复 ----
            // 当前水位线 11，后续窗口 20/40/60/80...
            sealedValue(h, 20, 20); // PENDING
            assertEquals(AlertState.PENDING, state(h));
            sealedValue(h, 40, 5);  // OK（毛刺清零）
            assertEquals(AlertState.OK, state(h));
            sealedValue(h, 60, 20); // PENDING
            sealedValue(h, 80, 20); // FIRING
            assertEquals(AlertState.FIRING, state(h));
            sealedValue(h, 100, 5); // 瞬时回落 => 仍 FIRING
            assertEquals(AlertState.FIRING, state(h));
            log.info("[ASSERT] 阈值抖动：毛刺不报警、连续2次才 FIRING、单次回落不恢复");

            // ---- 复盘对齐：T=111（w100 的 flush 已把水位线推到111）时，
            //      窗口100 已定稿，看到与实时相同的最终状态与计数 ----
            ReplayReport rep = h.replayService().replayAt(111, null);
            assertEquals(ReplayStatus.COMPLETE, rep.status());
            var replayAlert = rep.alerts().stream()
                    .filter(a -> a.ruleId().equals("r1")).findFirst().orElseThrow();
            assertEquals(AlertState.FIRING, replayAlert.state());
            long mCount = rep.aggregates().stream()
                    .filter(a -> a.metric().equals("m")).mapToLong(WindowAggregate::count).sum();
            // 被计入的 m 事件：dup + w20/40/60/80/100 共 6 条
            assertEquals(6, mCount, "复盘 m 计数=6（重复/迟到/冲突均未计入）");
            // 依据里能看出规则版本与每步判定。
            assertTrue(replayAlert.basis().stream().anyMatch(b -> b.contains("FIRING")));
            log.info("[ASSERT] 复盘 T=111：状态 FIRING、m 计数=6、依据含翻转过程，与实时一致");
            h.dumpState(log);
        }
    }

    /** 向窗口 wStart 放一条 m 事件，再用高 11ms 的 z 事件关闭它。 */
    private void sealedValue(TestHarness.Engine h, int wStart, double value) {
        h.send(log, TestHarness.event("w-" + wStart, wStart + 1,
                "tr-" + wStart, "s", null, "svc", "m", value, Map.of()));
        h.send(log, TestHarness.event("wf-" + wStart, wStart + 11,
                "trf-" + wStart, "s", null, "svc", "z", 0, Map.of()));
    }

    private AlertState state(TestHarness.Engine h) {
        return h.alertEngine.snapshots().stream()
                .filter(s -> s.ruleId().equals("r1")).findFirst().orElseThrow().state();
    }
}
