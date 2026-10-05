package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 免打扰语义：区间内只记录证据、不推进状态机；结束后按当前证据平稳恢复，
 * 需要重新累计确认次数，绝不一次性补发告警。
 */
class MuteRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(MuteRecoveryTest.class);

    private TelemetryProperties props(int breach, int recover) {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(0);
        p.getAlert().setBreachStreak(breach);
        p.getAlert().setRecoverStreak(recover);
        return p;
    }

    private AlertRule rule() {
        return new AlertRule("r", AlertRule.Scope.TAGS, "svc", "m",
                Map.of(), 10.0, AlertRule.Operator.GT);
    }

    private void windowValue(TestHarness.Engine h, int windowStart, double value) {
        String id = "w" + windowStart + "-" + (int) value;
        h.send(log, TestHarness.event(id, windowStart + 1, "tr-" + id, "s", null,
                "svc", "m", value, Map.of()));
        h.send(log, TestHarness.event("seal-" + windowStart, windowStart + 11,
                "tr-seal-" + windowStart, "ss", null, "svc", "other", 0, Map.of()));
    }

    private AlertSnapshot snapshot(TestHarness.Engine h) {
        return h.alertEngine.snapshots().stream()
                .filter(s -> s.ruleId().equals("r")).findFirst().orElseThrow();
    }

    @Test
    void muteSuppressesAndRecoveryIsGradualWithoutBurst() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(2, 2), List.of(rule()), 0L)) {
            log.info("[CASE] breachStreak=2：免打扰期间证据不累计，结束后需重新连续确认，不补发");

            windowValue(h, 0, 20); // 越界 1/2 => PENDING
            assertEquals(AlertState.PENDING, snapshot(h).state());

            RuleChange mute = h.change(log, TestHarness.changeReq(
                    "r", RuleChange.Action.MUTE, null, 35L));
            log.info("[STEP] 免打扰至事件时间 35（窗口 end<=35 抑制判定）version={}", mute.version());

            windowValue(h, 10, 20); // end=20 <= 35：抑制
            AlertSnapshot m1 = snapshot(h);
            assertTrue(m1.muted());
            assertEquals(AlertState.PENDING, m1.state(), "免打扰期间状态机冻结");
            assertEquals(0, m1.consecutiveBreaches(), "免打扰期间越界证据不累计");
            log.info("[STEP] 窗口[10,20) avg=20 免打扰中 => state={} muted={} streak={}",
                    m1.state(), m1.muted(), m1.consecutiveBreaches());

            windowValue(h, 20, 20); // end=30 <= 35：仍抑制
            assertTrue(snapshot(h).muted());

            windowValue(h, 30, 20); // end=40 > 35：免打扰结束，重新累计 1/2
            AlertSnapshot r1 = snapshot(h);
            assertFalse(r1.muted());
            assertEquals(AlertState.PENDING, r1.state(),
                    "免打扰结束后只累计一次，不得直接补发 FIRING");
            log.info("[STEP] 窗口[30,40) avg=20 免打扰结束 => {} streak={}（不补发）",
                    r1.state(), r1.consecutiveBreaches());

            windowValue(h, 40, 20); // 连续第二次 => FIRING
            assertEquals(AlertState.FIRING, snapshot(h).state());
            log.info("[ASSERT] 免打扰后需重新连续确认 {} 次才 FIRING，平稳恢复", 2);
            h.dumpState(log);
        }
    }

    @Test
    void firingSurvivesMuteAndRecoversGradually() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(1, 2), List.of(rule()), 0L)) {
            log.info("[CASE] FIRING 进入免打扰：状态保留但标记 muted；结束后恢复也需连续确认");

            windowValue(h, 0, 20); // FIRING
            assertEquals(AlertState.FIRING, snapshot(h).state());

            h.change(log, TestHarness.changeReq("r", RuleChange.Action.MUTE, null, 35L));

            windowValue(h, 10, 5); // end=20：抑制，恢复证据不累计
            AlertSnapshot m = snapshot(h);
            assertTrue(m.muted());
            assertEquals(AlertState.FIRING, m.state(), "免打扰不抹掉既有 FIRING 状态");
            assertEquals(0, m.consecutiveRecoveries());
            log.info("[STEP] 窗口[10,20) avg=5 免打扰中 => state={} muted={} recoverStreak={}",
                    m.state(), m.muted(), m.consecutiveRecoveries());

            windowValue(h, 20, 5); // end=30：仍抑制
            windowValue(h, 30, 5); // end=40：恢复累计 1/2，维持 FIRING
            AlertSnapshot r1 = snapshot(h);
            assertFalse(r1.muted());
            assertEquals(AlertState.FIRING, r1.state());
            windowValue(h, 40, 5); // end=50：恢复累计 2/2 => OK
            assertEquals(AlertState.OK, snapshot(h).state());
            log.info("[ASSERT] 免打扰结束后连续 {} 个恢复窗口才回 OK", 2);
            h.dumpState(log);
        }
    }

    @Test
    void unmuteRestoresEvaluationImmediately() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(1, 1), List.of(rule()), 0L)) {
            log.info("[CASE] UNMUTE 提前解除免打扰：下一窗口立即恢复正常判定");
            h.change(log, TestHarness.changeReq("r", RuleChange.Action.MUTE, null, 1000L));
            windowValue(h, 0, 20); // end=10 <= 1000：抑制
            assertTrue(snapshot(h).muted());
            assertEquals(AlertState.OK, snapshot(h).state());

            h.change(log, TestHarness.changeReq("r", RuleChange.Action.UNMUTE, null, null));
            windowValue(h, 20, 20); // 已解除：正常判定 => FIRING
            AlertSnapshot s = snapshot(h);
            assertFalse(s.muted());
            assertEquals(AlertState.FIRING, s.state());
            log.info("[ASSERT] UNMUTE 后窗口[20,30) 正常判定 => {}", s.state());
            h.dumpState(log);
        }
    }
}
