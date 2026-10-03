package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayAlert;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayStatus;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeRequest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 免打扰结束后的告警状态恢复：
 * 区间内证据照常累计但不对外翻转；结束后按当前证据平稳恢复（每个窗口最多一次翻转），
 * 不会一次性补发一串告警；FIRE 中进入免打扰不会因恢复窗口而抖动回 OK。
 */
class QuietRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(QuietRecoveryTest.class);

    private TelemetryProperties props(int breach, int recover) {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(0);
        p.getAlert().setBreachStreak(breach);
        p.getAlert().setRecoverStreak(recover);
        return p;
    }

    private void windowValue(TestHarness.Engine h, int wStart, double value) {
        h.send(log, TestHarness.event("e-" + wStart + "-" + (int) value, wStart + 1,
                "tr-" + wStart, "s", null, "svc", "m", value, Map.of()));
        h.send(log, TestHarness.event("f-" + wStart, wStart + 11,
                "trf-" + wStart, "s", null, "svc", "other", 0, Map.of()));
    }

    private AlertSnapshot alert(TestHarness.Engine h) {
        return h.alertEngine.snapshots().stream()
                .filter(s -> s.ruleId().equals("r1")).findFirst().orElseThrow();
    }

    @Test
    void breachEvidenceAccumulatesDuringQuietAndFiresOnceAfterQuietEnds() {
        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                10.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(
                props(2, 2), List.of(seed), 0L, 5000, 5000)) {
            log.info("[CASE] breachStreak=2：免打扰[20,60)内连续两个越界窗口也不报警，"
                    + "区间结束后首个越界窗口只翻转一次");
            windowValue(h, 0, 5); // OK
            var q = h.engine.changeRule(new RuleChangeRequest(
                    "q", "r1", null, null, null, null, null, null, null,
                    20L, 60L, "免打扰20-60"));
            assertTrue(q.applied());

            windowValue(h, 20, 20); // 越界证据1，抑制，状态仍 OK
            assertEquals(AlertState.OK, alert(h).state());
            windowValue(h, 40, 20); // 越界证据已够2次，但仍在免打扰 => 仍 OK
            assertEquals(AlertState.OK, alert(h).state());
            assertEquals(2, alert(h).suppressedCount());
            log.info("[ASSERT] 免打扰内 breach 证据已累计到2但不报警，suppressed=2");

            windowValue(h, 60, 20); // 区间结束：证据连续达标 => FIRING，仅一次
            assertEquals(AlertState.FIRING, alert(h).state());
            log.info("[ASSERT] 免打扰结束 w@60 => 单次 FIRING，无补发");

            // 复盘视角：在免打扰区间内（T=51）状态仍为 OK；结束后（T=71）为 FIRING。
            ReplayReport during = h.replayService().replayAt(51, null);
            ReplayReport after = h.replayService().replayAt(71, null);
            assertEquals(ReplayStatus.COMPLETE, during.status());
            ReplayAlert dAlert = during.alerts().stream()
                    .filter(a -> a.ruleId().equals("r1")).findFirst().orElseThrow();
            assertEquals(AlertState.OK, dAlert.state(), "复盘 T=51（免打扰内）必须还是 OK");
            assertTrue(dAlert.quietAtCutoff(), "复盘应标注切点处于免打扰");
            assertEquals(AlertState.FIRING, after.alerts().stream()
                    .filter(a -> a.ruleId().equals("r1")).findFirst().orElseThrow().state());
            log.info("[ASSERT] 复盘 T=51 quiet=OK；T=71 FIRING，与实时一致，无补发");
            h.dumpState(log);
        }
    }

    @Test
    void firingStaysFiringThroughQuietRecoveryWindowsAndRecoversAfter() {
        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                10.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(
                props(1, 2), List.of(seed), 0L, 5000, 5000)) {
            log.info("[CASE] recoverStreak=2：FIRE 中进入免打扰，区间内两个恢复窗口不得导致恢复；"
                    + "区间结束后再连续两个恢复窗口才回 OK");
            windowValue(h, 0, 20); // FIRING
            assertEquals(AlertState.FIRING, alert(h).state());
            h.engine.changeRule(new RuleChangeRequest(
                    "q2", "r1", null, null, null, null, null, null, null,
                    20L, 60L, "免打扰20-60"));

            windowValue(h, 20, 5); // 恢复证据1，抑制
            windowValue(h, 40, 5); // 恢复证据2，但免打扰 => 维持 FIRING
            assertEquals(AlertState.FIRING, alert(h).state(),
                    "免打扰期间恢复证据不产生对外状态翻转");
            assertEquals(2, alert(h).suppressedCount());
            log.info("[ASSERT] 免打扰内两次恢复窗口仍维持 FIRING");

            windowValue(h, 60, 5); // 区间结束后的第一个恢复窗口：恢复计数延续 => 回 OK
            assertEquals(AlertState.OK, alert(h).state(),
                    "免打扰结束后按已累计证据平稳恢复，恰好一次翻转");
            log.info("[ASSERT] w@60=5 免打扰结束后恢复 OK（无补发、无抖动）");
            h.dumpState(log);
        }
    }

    @Test
    void cancellingQuietRestoresNormalEvaluationImmediately() {
        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                10.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(
                props(1, 1), List.of(seed), 0L, 5000, 5000)) {
            log.info("[CASE] 免打扰可被后续版本取消（0,0 表示无区间）");
            windowValue(h, 0, 5);
            h.engine.changeRule(new RuleChangeRequest(
                    "q", "r1", null, null, null, null, null, null, null,
                    20L, 60L, "设置免打扰"));
            windowValue(h, 20, 20); // 被抑制 => OK
            assertEquals(AlertState.OK, alert(h).state());
            // 取消免打扰：显式 from=until=0（RuleManager 中 <0 才视为无区间）
            h.engine.changeRule(new RuleChangeRequest(
                    "cancel", "r1", null, null, null, null, null, null, null,
                    0L, 0L, "取消免打扰"));
            windowValue(h, 40, 20); // 正常评估 => FIRING
            assertEquals(AlertState.FIRING, alert(h).state());
            log.info("[ASSERT] 取消免打扰后 w@40=20 立即正常 FIRING");
            h.dumpState(log);
        }
    }
}
