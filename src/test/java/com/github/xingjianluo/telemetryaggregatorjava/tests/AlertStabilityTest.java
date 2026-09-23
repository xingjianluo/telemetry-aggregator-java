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
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 告警稳定性：阈值附近抖动不反复触发/恢复；连续确认 + 冷却；调用链异常不阻断判定。
 */
class AlertStabilityTest {

    private static final Logger log = LoggerFactory.getLogger(AlertStabilityTest.class);

    private TelemetryProperties props(int breach, int recover) {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(0);
        p.getAlert().setBreachStreak(breach);
        p.getAlert().setRecoverStreak(recover);
        return p;
    }

    private AlertRule rule() {
        return new AlertRule("high", AlertRule.Scope.TAGS, "svc", "m",
                Map.of(), 10.0, AlertRule.Operator.GT);
    }

    /** 每个窗口放一个事件，随后用一个“推进事件”关闭上一窗口。 */
    private void windowValue(TestHarness.Engine h, int windowStart, double value) {
        String id = "w" + windowStart + "-" + (int) value;
        h.send(log, TestHarness.event(id, windowStart + 1, "tr-" + id, "s", null,
                "svc", "m", value, Map.of()));
        seal(h, windowStart);
    }

    /** 在目标窗口结束后推进水位线（L=0 时 wm=t），不污染本规则指标。 */
    private void seal(TestHarness.Engine h, int windowStart) {
        h.send(log, TestHarness.event("seal-" + windowStart, windowStart + 11,
                "tr-seal-" + windowStart, "ss", null, "svc", "other", 0, Map.of()));
    }

    private AlertSnapshot snapshot(TestHarness.Engine h) {
        return snapshot(h, "high");
    }

    private AlertSnapshot snapshot(TestHarness.Engine h, String ruleId) {
        return h.alertEngine.snapshots().stream()
                .filter(s -> s.ruleId().equals(ruleId)).findFirst().orElse(null);
    }

    @Test
    void singleBlipDoesNotFireAndRecoverStreakSuppressesFlapping() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(2, 2), List.of(rule()), 0L)) {
            log.info("[CASE] breachStreak=2：单次越界只到 PENDING；恢复也需连续2次才回 OK");

            windowValue(h, 0, 20); // 窗口0 越界 => PENDING
            assertEquals(AlertState.PENDING, snapshot(h).state());
            log.info("[STEP] w0=20 => {}", snapshot(h).state());

            windowValue(h, 20, 5); // 恢复 => 计数清零回 OK（未真正报警）
            assertEquals(AlertState.OK, snapshot(h).state());
            log.info("[STEP] w@20=5 => {}（单点毛刺未触发报警）", snapshot(h).state());

            windowValue(h, 40, 20); // PENDING
            windowValue(h, 60, 20); // 连续两次 => FIRING
            assertEquals(AlertState.FIRING, snapshot(h).state());
            log.info("[STEP] w@40,@60=20 => {}", snapshot(h).state());

            windowValue(h, 80, 5); // 仅一次恢复 => 必须维持 FIRING，不抖动恢复
            assertEquals(AlertState.FIRING, snapshot(h).state());
            log.info("[STEP] w@80=5 => {}（瞬时回落不恢复）", snapshot(h).state());

            windowValue(h, 100, 20); // 再次越界 => 恢复计数清零，继续 FIRING
            assertEquals(AlertState.FIRING, snapshot(h).state());
            log.info("[STEP] w@100=20 => {}（阈值附近波动不翻转）", snapshot(h).state());

            windowValue(h, 120, 5);
            windowValue(h, 140, 5); // 连续两次恢复 => OK
            assertEquals(AlertState.OK, snapshot(h).state());
            log.info("[STEP] w6,w7=5 => {}（连续确认后恢复）", snapshot(h).state());
            h.dumpState(log);
        }
    }

    @Test
    void cooldownPreventsRapidRefire() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(1, 1), List.of(rule()), 60_000L)) {
            log.info("[CASE] cooldown=60s：即使每窗口都翻转，冷却期内也保持 FIRING/不重新计时");
            windowValue(h, 0, 20); // streak=1 => FIRING（首次 lastChanged=now）
            assertEquals(AlertState.FIRING, snapshot(h).state());
            windowValue(h, 20, 5); // recover=1 满足，但处于冷却 => 维持 FIRING
            assertEquals(AlertState.FIRING, snapshot(h).state());
            windowValue(h, 40, 20); // 持续越界
            assertEquals(AlertState.FIRING, snapshot(h).state());
            log.info("[ASSERT] 冷却期内回落窗口未导致 FIRING->OK 抖动");
            h.dumpState(log);
        }
    }

    @Test
    void tagSelectorScopesTheRule() {
        AlertRule selectorRule = new AlertRule("host-a", AlertRule.Scope.TAGS, "svc", "m",
                Map.of("host", "a"), 10.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(props(1, 1), List.of(selectorRule), 0L)) {
            log.info("[CASE] 规则只匹配 host=a：host=b 越界不报警，host=a 报警");
            windowValueTagged(h, 0, 20, "b");
            assertNull(snapshot(h), "host=b 不应产生任何告警主体");
            windowValueTagged(h, 20, 20, "a");
            AlertSnapshot snap = snapshot(h, "host-a");
            assertNotNull(snap);
            assertEquals(AlertState.FIRING, snap.state());
            assertEquals("a", snap.tags().get("host"));
            log.info("[ASSERT] 仅 host=a 进入 FIRING，tags={}", snap.tags());
            h.dumpState(log);
        }
    }

    private void windowValueTagged(TestHarness.Engine h, int windowStart, double value, String host) {
        String id = "w" + windowStart + "-" + host;
        h.send(log, TestHarness.event(id, windowStart + 1, "tr-" + id, "s", null,
                "svc", "m", value, Map.of("host", host)));
        h.send(log, TestHarness.event("seal-" + id, windowStart + 11,
                "tr-seal-" + id, "ss", null, "svc", "other", 0, Map.of()));
    }
}
