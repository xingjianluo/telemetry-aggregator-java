package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertEvidence;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 同一时间边界下，实时已经产生的每一条告警结论（状态、版本、计数、<b>完整判定依据</b>）
 * 与复盘报告中的同一条结论逐条对得上；含规则变更与免打扰的场景同样成立。
 */
class RealtimeReplayAlignmentTest {

    private static final Logger log = LoggerFactory.getLogger(RealtimeReplayAlignmentTest.class);

    private TelemetryProperties props() {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(0);
        p.getAlert().setBreachStreak(2);
        p.getAlert().setRecoverStreak(2);
        return p;
    }

    private AlertRule rule(String id, String service, double threshold) {
        return new AlertRule(id, AlertRule.Scope.TAGS, service, "m",
                Map.of(), threshold, AlertRule.Operator.GT);
    }

    private void value(TestHarness.Engine h, String id, int t, String service, double v) {
        h.send(log, TestHarness.event(id, t, "tr-" + id, "s", null, service, "m", v, Map.of()));
    }

    private void sealAfter(TestHarness.Engine h, int t, int at) {
        h.send(log, TestHarness.event("seal-" + at, at, "tr-seal-" + at, "ss", null,
                "svc-seal", "other", 0, Map.of()));
    }

    private Comparator<AlertSnapshot> byKey() {
        return Comparator.comparing(AlertSnapshot::ruleId)
                .thenComparing(AlertSnapshot::subjectKey);
    }

    @Test
    void everyRealtimeAlertMatchesReplayAtSameBoundaryIncludingEvidence() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(),
                List.of(rule("rA", "svc-a", 10), rule("rB", "svc-b", 100)), 0L)) {
            log.info("[CASE] 多规则多对象 + 阈值变更 + 免打扰：同一时间边界实时/复盘逐条对齐（含依据）");

            // 窗口 [0,10)：svc-a 越界(1/2 PENDING)；svc-b 正常
            value(h, "a1", 1, "svc-a", 20);
            value(h, "b1", 2, "svc-b", 5);
            sealAfter(h, 0, 11);
            // 窗口 [20,30)：svc-a 再次越界(2/2 FIRING)；svc-b 越界(1/2 PENDING)
            value(h, "a2", 21, "svc-a", 20);
            value(h, "b2", 22, "svc-b", 200);
            sealAfter(h, 20, 31);

            // 在线变更：svc-a 阈值升到 100（v1），svc-b 免打扰到 end<=55（v2）
            h.change(log, new RuleChangeRequest(
                    "align-up", "rA",
                    com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange.Action.UPSERT,
                    rule("rA", "svc-a", 100), null));
            h.change(log, new RuleChangeRequest(
                    "align-mute", "rB",
                    com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange.Action.MUTE,
                    null, 55L));

            // 窗口 [40,50)：svc-a 按 v1 不越界（恢复 1/2 仍 FIRING）；svc-b 免打扰抑制
            value(h, "a3", 41, "svc-a", 5);
            value(h, "b3", 42, "svc-b", 200);
            sealAfter(h, 40, 51);
            // 窗口 [60,70)：svc-a 恢复 2/2 => OK（v1）；svc-b 免打扰结束，重新越界 1/2（v2）
            value(h, "a4", 61, "svc-a", 5);
            value(h, "b4", 62, "svc-b", 200);
            sealAfter(h, 60, 71);

            long boundary = 71;
            ReplayReport replay = h.replay(log, boundary);
            assertEquals(ReplayStatus.OK, replay.status());

            List<AlertSnapshot> live = new ArrayList<>(h.alertEngine.snapshots());
            List<AlertSnapshot> replayed = new ArrayList<>(replay.alerts());
            live.sort(byKey());
            replayed.sort(byKey());
            assertEquals(live.size(), replayed.size(), "告警条数必须一致");

            for (int i = 0; i < live.size(); i++) {
                AlertSnapshot l = live.get(i);
                AlertSnapshot r = replayed.get(i);
                assertEquals(l.ruleId(), r.ruleId());
                assertEquals(l.subjectKey(), r.subjectKey());
                assertEquals(l.state(), r.state());
                assertEquals(l.ruleVersion(), r.ruleVersion());
                assertEquals(l.muted(), r.muted());
                assertEquals(l.consecutiveBreaches(), r.consecutiveBreaches());
                assertEquals(l.consecutiveRecoveries(), r.consecutiveRecoveries());
                assertEquals(l.lastValue(), r.lastValue());
                // 完整判定依据逐条一致
                List<AlertEvidence> le = l.evidence();
                List<AlertEvidence> re = r.evidence();
                assertEquals(le.size(), re.size(),
                        "规则 " + l.ruleId() + " 的依据步数必须一致");
                for (int j = 0; j < le.size(); j++) {
                    assertEquals(le.get(j), re.get(j),
                            "规则 " + l.ruleId() + " 第 " + j + " 步依据必须逐位一致");
                }
                log.info("[ASSERT] rule={} subject={} state={} v{} muted={} evidence={} 步实时=复盘",
                        l.ruleId(), l.subjectKey(), l.state(), l.ruleVersion(), l.muted(), le.size());
            }

            // 关键字断言：svc-b 免打扰窗口在两侧都被标 muted 且 streak 清零；
            // svc-a 的恢复翻转依据在两侧完全相同
            AlertSnapshot bLive = live.stream().filter(s -> s.ruleId().equals("rB")).findFirst().orElseThrow();
            assertTrue(bLive.evidence().stream().anyMatch(
                    e -> e.muted() && e.windowEndMillis() == 50L && e.consecutiveBreaches() == 0));
            AlertSnapshot aLive = live.stream().filter(s -> s.ruleId().equals("rA")).findFirst().orElseThrow();
            AlertEvidence recover = aLive.evidence().stream()
                    .filter(e -> e.windowEndMillis() == 70L).findFirst().orElseThrow();
            assertTrue(recover.transition());
            assertEquals(com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState.OK,
                    recover.stateAfter());

            // 再复盘一次：逐位相等（与何时跑、跑几次无关）
            ReplayReport again = h.replay(log, boundary);
            assertEquals(replay, again, "同一边界重复复盘必须逐位一致");
            h.dumpState(log);
        }
    }

    @Test
    void aggregateAndAlertCountsLineUpAtBoundary() {
        // 附加守护：对齐所依据的定稿聚合在两侧也一致（计数不丢不重）。
        TelemetryProperties p = props();
        try (TestHarness.Engine h = new TestHarness.Engine(p,
                List.of(rule("rA", "svc-a", 10)), 0L)) {
            value(h, "a1", 1, "svc-a", 20);
            sealAfter(h, 0, 11);
            value(h, "a2", 21, "svc-a", 20);
            sealAfter(h, 20, 31);

            ReplayReport r = h.replay(log, 31);
            Comparator<WindowAggregate> cmp = Comparator
                    .comparingLong(WindowAggregate::windowStartMillis)
                    .thenComparing(WindowAggregate::service)
                    .thenComparing(WindowAggregate::metric);
            List<WindowAggregate> liveAgg = new ArrayList<>(h.windowStore.query(null, null));
            List<WindowAggregate> replayAgg = new ArrayList<>(r.aggregates());
            liveAgg.sort(cmp);
            replayAgg.sort(cmp);
            assertEquals(liveAgg, replayAgg);
            assertTrue(h.alertEngine.snapshots().stream()
                    .anyMatch(s -> s.ruleId().equals("rA") && !s.evidence().isEmpty()));
            log.info("[ASSERT] 定稿聚合两侧一致，且告警携带依据");
        }
    }
}
