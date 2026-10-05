package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEvidence;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayStatus;
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
 * 告警判定依据（AlertEvidence）：
 * 每次窗口定稿都留下可读、可审计的依据；正常->告警的翻转可完整解释；
 * 免打扰期间标记抑制且不累计；同一时间边界下实时与复盘的依据链逐位一致；
 * 同一批输入重复复盘依据完全相同。
 */
class AlertEvidenceTest {

    private static final Logger log = LoggerFactory.getLogger(AlertEvidenceTest.class);

    private TelemetryProperties props(int breach, int recover) {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(0);
        p.getAlert().setBreachStreak(breach);
        p.getAlert().setRecoverStreak(recover);
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

    private AlertSnapshot snapshot(TestHarness.Engine h) {
        return h.alertEngine.snapshots().stream()
                .filter(s -> s.ruleId().equals("r")).findFirst().orElseThrow();
    }

    private AlertSnapshot of(ReplayReport r) {
        return r.alerts().stream().filter(a -> a.ruleId().equals("r")).findFirst().orElseThrow();
    }

    @Test
    void evidenceExplainsOkToFiringTransitionWithWindowAndVersion() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(2, 2), List.of(rule(10)), 0L)) {
            log.info("[CASE] breachStreak=2：依据链完整记录毛刺 PENDING、确认翻转 FIRING 的每一步");
            windowValue(h, 0, 5);   // 未越界 OK
            windowValue(h, 20, 20); // 越界 1/2 => PENDING
            windowValue(h, 40, 20); // 越界 2/2 => FIRING

            AlertSnapshot s = snapshot(h);
            assertEquals(AlertState.FIRING, s.state());
            List<AlertEvidence> ev = s.evidence();
            assertEquals(3, ev.size(), "每个定稿窗口（匹配规则）都应留一条依据");

            // 依据按事件时间升序，且字段完整（窗口、聚合、版本、阈值、计数、前后状态）
            assertEquals(10L, ev.get(0).windowEndMillis());
            assertEquals(30L, ev.get(1).windowEndMillis());
            assertEquals(50L, ev.get(2).windowEndMillis());
            for (AlertEvidence e : ev) {
                assertEquals(0L, e.ruleVersion());
                assertEquals(10.0, e.threshold());
                assertNotNull(e.rationale());
                assertFalse(e.rationale().isBlank());
                assertEquals(1L, e.count(), "每个 m 窗口恰有一个业务事件");
            }
            assertFalse(ev.get(0).breach());
            assertEquals(AlertState.OK, ev.get(0).stateAfter());

            assertTrue(ev.get(1).breach());
            assertEquals(AlertState.PENDING, ev.get(1).stateAfter());
            assertEquals(1, ev.get(1).consecutiveBreaches());
            assertTrue(ev.get(1).rationale().contains("PENDING"),
                    "依据应说明为何只到 PENDING: " + ev.get(1).rationale());

            assertTrue(ev.get(2).breach());
            assertEquals(AlertState.FIRING, ev.get(2).stateAfter());
            assertTrue(ev.get(2).transition());
            assertEquals(2, ev.get(2).consecutiveBreaches());
            assertTrue(ev.get(2).rationale().contains("FIRING"),
                    "依据应说明为什么从 PENDING 翻成 FIRING: " + ev.get(2).rationale());
            log.info("[ASSERT] 正常->PENDING->FIRING 每一步依据完整可读");
            h.dumpState(log);
        }
    }

    @Test
    void evidenceMarksMutedWindowsAndNoAccumulation() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(2, 2), List.of(rule(10)), 0L)) {
            log.info("[CASE] 免打扰窗口的依据必须标记 muted 且说明越界不累计");
            windowValue(h, 0, 20); // PENDING 1/2
            h.change(log, new RuleChangeRequest(
                    "mute-1", "r", com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange.Action.MUTE,
                    null, 35L));
            windowValue(h, 10, 20); // end=20 <= 35 抑制
            windowValue(h, 20, 20); // end=30 <= 35 抑制

            List<AlertEvidence> ev = snapshot(h).evidence();
            AlertEvidence e1 = ev.get(1);
            AlertEvidence e2 = ev.get(2);
            assertTrue(e1.muted() && e2.muted(), "免打扰窗口必须标记 muted");
            assertEquals(0, e1.consecutiveBreaches(), "抑制期间越界不得累计");
            assertEquals(0, e2.consecutiveBreaches());
            assertFalse(e1.transition() && e2.transition(), "抑制窗口不得翻转状态");
            assertTrue(e1.rationale().contains("免打扰"));
            assertTrue(e1.rationale().contains("不累计"));
            assertEquals(35L, e1.muteUntilMillis());
            log.info("[ASSERT] 免打扰依据：muted=true streak=0，且文案说明不累计不补发");
            h.dumpState(log);
        }
    }

    @Test
    void evidenceUsesRuleVersionAfterBoundaryAndMatchesReplayExactly() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(1, 1), List.of(rule(10)), 0L)) {
            log.info("[CASE] 依据中记录的规则版本随生效边界切换；实时与复盘依据逐位一致");
            windowValue(h, 0, 20); // v0 越界 => FIRING
            h.change(log, new RuleChangeRequest(
                    "up-1", "r", com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange.Action.UPSERT,
                    rule(100), null));
            windowValue(h, 20, 20); // v1 不越界 => OK
            windowValue(h, 40, 20); // v1 仍不越界 => OK

            List<AlertEvidence> live = snapshot(h).evidence();
            assertEquals(3, live.size());
            assertEquals(0L, live.get(0).ruleVersion());
            assertEquals(10.0, live.get(0).threshold());
            assertTrue(live.get(0).transition());
            assertEquals(1L, live.get(1).ruleVersion());
            assertEquals(100.0, live.get(1).threshold());
            assertTrue(live.get(1).transition(), "v1 下 FIRING->OK 的恢复必须有翻转依据");
            assertEquals(1L, live.get(2).ruleVersion());

            // 同一时间边界重复复盘：报告（含依据链）逐位相等，且与实时同一条结论对得上
            ReplayReport r1 = h.replay(log, 51);
            assertEquals(ReplayStatus.OK, r1.status());
            ReplayReport r2 = h.replay(log, 51);
            assertEquals(r1, r2, "同一批输入重复复盘（含依据）必须逐位一致");

            List<AlertEvidence> replayEv = of(r1).evidence();
            assertEquals(live, replayEv,
                    "同一时间边界下实时结论的依据链必须与复盘逐位一致");
            assertEquals(snapshot(h).evidence(), replayEv);
            log.info("[ASSERT] 依据链：版本/阈值随边界切换、实时=复盘、重复复盘逐位一致");
            h.dumpState(log);
        }
    }

    @Test
    void everyAlertInReplayCarriesReadableRationale() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(1, 1), List.of(rule(10)), 0L)) {
            log.info("[CASE] 复盘报告中每条告警结论都必须带非空可读依据");
            windowValue(h, 0, 20);
            windowValue(h, 20, 5);
            ReplayReport r = h.replay(log, 31);
            assertEquals(ReplayStatus.OK, r.status());
            assertFalse(r.alerts().isEmpty());
            for (AlertSnapshot a : r.alerts()) {
                assertFalse(a.evidence().isEmpty(), "每条告警都必须至少有一条依据");
                for (AlertEvidence e : a.evidence()) {
                    assertNotNull(e.rationale());
                    assertTrue(e.rationale().contains("窗口["),
                            "依据必须指明来自哪个窗口: " + e.rationale());
                    assertTrue(e.rationale().contains(String.valueOf(e.ruleVersion()))
                                    || e.rationale().contains("avg="),
                            "依据必须可读（版本或均值）: " + e.rationale());
                    // 状态前后、窗口边界自洽
                    assertTrue(e.windowEndMillis() > e.windowStartMillis());
                }
            }
            log.info("[ASSERT] 复盘 {} 条告警全部带可读依据链", r.alerts().size());
        }
    }
}
