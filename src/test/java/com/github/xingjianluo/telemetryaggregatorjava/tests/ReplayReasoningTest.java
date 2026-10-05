package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 判定依据（decisionBasis）完整性与一致性：
 * 每条告警结论都能说清依据哪个窗口的哪份证据、命中哪个阈值、
 * streak 累计进度、状态翻转或未翻转的原因、是否在免打扰区间；
 * 同一时间边界下实时结论与复盘结论逐条对得上；
 * 同一批输入重复复盘，依据逐位一致。
 */
class ReplayReasoningTest {

    private static final Logger log = LoggerFactory.getLogger(ReplayReasoningTest.class);

    private TelemetryProperties props() {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(0);
        p.getAlert().setBreachStreak(2);
        p.getAlert().setRecoverStreak(2);
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

    @Test
    void everyConclusionCarriesReadableDecisionBasis() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 覆盖 PENDING/FIRING/免打扰/恢复 全路径，每条结论都带可读判定依据");

            windowValue(h, 0, 20); // 越界 1/2 => PENDING
            AlertSnapshot s1 = snapshot(h);
            assertEquals(AlertState.PENDING, s1.state());
            assertBasis(s1, "窗口[0,10)", "avg=20.0", "命中阈值 GT 10.0", "1/2", "PENDING", "规则版本 0");
            log.info("[STEP] PENDING 依据: {}", s1.decisionBasis());

            windowValue(h, 10, 20); // 越界 2/2 => FIRING
            AlertSnapshot s2 = snapshot(h);
            assertEquals(AlertState.FIRING, s2.state());
            assertBasis(s2, "窗口[10,20)", "2/2", "FIRING", "规则版本 0");
            log.info("[STEP] FIRING 依据: {}", s2.decisionBasis());

            // 免打扰到事件时间 45：抑制期间证据不累计
            RuleChange mute = h.change(log, new RuleChangeRequest(
                    "r", RuleChange.Action.MUTE, null, 45L, "mute-1"));
            log.info("[STEP] 免打扰至 45，version={}", mute.version());

            windowValue(h, 20, 20); // end=30 <= 45：抑制
            AlertSnapshot s3 = snapshot(h);
            assertTrue(s3.muted());
            assertEquals(AlertState.FIRING, s3.state(), "免打扰期间状态机冻结");
            assertEquals(0, s3.consecutiveBreaches(), "抑制期间越界不累计");
            assertBasis(s3, "窗口[20,30)", "免打扰", "不累计", "规则版本 1");
            log.info("[STEP] 免打扰依据: {}", s3.decisionBasis());

            windowValue(h, 30, 20); // end=40 <= 45：仍抑制
            assertTrue(snapshot(h).muted());

            windowValue(h, 40, 20); // end=50 > 45：免打扰结束，越界持续
            AlertSnapshot s4 = snapshot(h);
            assertFalse(s4.muted());
            assertEquals(AlertState.FIRING, s4.state());
            assertBasis(s4, "窗口[40,50)", "越界持续", "规则版本 1");
            log.info("[STEP] 免打扰结束依据: {}", s4.decisionBasis());

            windowValue(h, 50, 5); // 恢复 1/2：确认不足，维持 FIRING
            AlertSnapshot s5 = snapshot(h);
            assertEquals(AlertState.FIRING, s5.state());
            assertBasis(s5, "窗口[50,60)", "未命中阈值 GT 10.0", "连续恢复 1/2", "确认次数不足");
            log.info("[STEP] 恢复确认不足依据: {}", s5.decisionBasis());

            windowValue(h, 60, 5); // 恢复 2/2 => OK
            AlertSnapshot s6 = snapshot(h);
            assertEquals(AlertState.OK, s6.state());
            assertBasis(s6, "窗口[60,70)", "连续恢复 2/2", "FIRING->OK");
            log.info("[STEP] 恢复依据: {}", s6.decisionBasis());

            // 在线调整阈值后，依据中的规则版本随之更新
            h.change(log, new RuleChangeRequest("r", RuleChange.Action.UPSERT,
                    rule(100), null, "up-1"));
            windowValue(h, 70, 50); // end=80 > 边界：新规则，50<=100 未命中
            AlertSnapshot s7 = snapshot(h);
            assertEquals(2L, s7.ruleVersion());
            assertBasis(s7, "窗口[70,80)", "未命中阈值 GT 100.0", "规则版本 2");
            log.info("[ASSERT] 全路径判定依据齐备，最新依据: {}", s7.decisionBasis());
            h.dumpState(log);
        }
    }

    @Test
    void replayMatchesLiveConclusionsAndRepeatsIdentically() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 同一时间边界：实时结论与复盘结论逐条对得上，重复复盘逐位一致");

            windowValue(h, 0, 20);
            windowValue(h, 10, 20); // FIRING
            h.change(log, new RuleChangeRequest("r", RuleChange.Action.MUTE, null, 45L, "m-1"));
            windowValue(h, 20, 20); // 抑制
            windowValue(h, 30, 5);  // 抑制
            h.change(log, new RuleChangeRequest("r", RuleChange.Action.UNMUTE, null, null, "u-1"));
            windowValue(h, 40, 5);  // 恢复 1/2
            windowValue(h, 50, 5);  // 恢复 2/2 => OK

            long asOf = 61; // 当前水位线
            List<AlertSnapshot> live = h.alertEngine.snapshots();
            assertFalse(live.isEmpty());
            live.forEach(s -> log.info("[LIVE] rule={} subject={} state={} v={} basis={}",
                    s.ruleId(), s.subjectKey(), s.state(), s.ruleVersion(), s.decisionBasis()));

            ReplayReport replay1 = h.replay(log, asOf);
            assertEquals(ReplayStatus.OK, replay1.status());
            ReplayReport replay2 = h.replay(log, asOf);
            assertEquals(ReplayStatus.OK, replay2.status());

            Map<String, AlertSnapshot> liveByKey = byKey(live);
            Map<String, AlertSnapshot> replayByKey = byKey(replay1.alerts());
            assertEquals(liveByKey.keySet(), replayByKey.keySet(),
                    "实时与复盘的结论集合必须一致");
            liveByKey.forEach((key, liveSnap) -> {
                AlertSnapshot replaySnap = replayByKey.get(key);
                assertEquals(liveSnap, replaySnap,
                        "同一结论的实时与复盘必须逐位一致（含判定依据）: " + key);
                log.info("[MATCH] {} => state={} v={} basis={}",
                        key, replaySnap.state(), replaySnap.ruleVersion(), replaySnap.decisionBasis());
            });

            assertEquals(replay1.alerts(), replay2.alerts(),
                    "同一批输入、同一时间边界重复复盘，结论与依据必须逐位一致");
            replay1.alerts().forEach(a -> {
                assertFalse(a.decisionBasis() == null || a.decisionBasis().isBlank(),
                        "每条复盘结论都必须带判定依据");
                assertTrue(a.decisionBasis().contains("窗口["),
                        "判定依据必须指认证据窗口: " + a.decisionBasis());
            });
            log.info("[ASSERT] 实时/复盘 {} 条结论逐条对得上；重复复盘逐位一致", live.size());
        }
    }

    private static Map<String, AlertSnapshot> byKey(List<AlertSnapshot> snapshots) {
        return snapshots.stream().collect(Collectors.toMap(
                s -> s.ruleId() + "|" + s.subjectKey(), Function.identity()));
    }

    private static void assertBasis(AlertSnapshot s, String... fragments) {
        String basis = s.decisionBasis();
        assertFalse(basis == null || basis.isBlank(), "判定依据不能为空");
        for (String fragment : fragments) {
            assertTrue(basis.contains(fragment),
                    "判定依据缺少片段 [" + fragment + "]: " + basis);
        }
    }
}
