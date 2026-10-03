package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersion;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayAlert;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayStatus;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多条规则交错在线变更时，复盘重建的版本号与版本历史、告警事实严格对齐；
 * 不同规则各自的版本演进互不串号，生效边界仍按各自 effectiveFrom 解析。
 */
class InterleavedRuleVersionReplayTest {

    private static final Logger log = LoggerFactory.getLogger(InterleavedRuleVersionReplayTest.class);

    @Test
    void interleavedChangesKeepVersionNumbersConsistentInReplay() {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(0);
        p.getAlert().setBreachStreak(1);
        p.getAlert().setRecoverStreak(1);

        var r1 = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m1",
                1000.0, AlertRule.Operator.GT);
        var r2 = TestHarness.seed("r2", AlertRule.Scope.TAGS, "svc", "m2",
                1000.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(p, List.of(r1, r2), 0L, 5000, 5000)) {
            // 推进水位线到 11
            h.send(log, TestHarness.event("f0", 11, "tf", "s", null, "svc", "z", 0, Map.of()));
            // 交错变更：全局版本号 v3(r1), v4(r2), v5(r1)（种子 r1=v1、r2=v2）。
            var c1 = h.engine.changeRule(new RuleChangeRequest("c1", "r1", null, null,
                    null, null, null, 10.0, null, null, null, "r1第一次"));
            var c2 = h.engine.changeRule(new RuleChangeRequest("c2", "r2", null, null,
                    null, null, null, 20.0, null, null, null, "r2第一次"));
            var c3 = h.engine.changeRule(new RuleChangeRequest("c3", "r1", null, null,
                    null, null, null, 30.0, null, null, null, "r1第二次"));
            assertEquals(List.of(3L, 4L, 5L), List.of(c1.version(), c2.version(), c3.version()));
            // 按规则看各自历史：r1 有 2 个版本（v1,v3,v5 共3条），r2 有 2 个版本（v2,v4）。
            assertEquals(3, h.ruleStore.historyOf("r1").size());
            assertEquals(2, h.ruleStore.historyOf("r2").size());
            log.info("[CHANGE] 交错全局版本 r1->v3, r2->v4, r1->v5，边界均=11");

            // 产生窗口20：m1=50（r1 v4 阈值30 越界），m2=50（r2 v3 阈值20 越界）
            h.send(log, TestHarness.event("a", 21, "ta", "s", null, "svc", "m1", 50, Map.of()));
            h.send(log, TestHarness.event("b", 22, "tb", "s", null, "svc", "m2", 50, Map.of()));
            h.send(log, TestHarness.event("f20", 31, "tf2", "s", null, "svc", "z", 0, Map.of()));

            ReplayReport rep = h.replayService().replayAt(31, null);
            assertEquals(ReplayStatus.COMPLETE, rep.status());

            ReplayAlert a1 = rep.alerts().stream().filter(a -> a.ruleId().equals("r1")).findFirst().orElseThrow();
            ReplayAlert a2 = rep.alerts().stream().filter(a -> a.ruleId().equals("r2")).findFirst().orElseThrow();
            assertEquals(5L, a1.ruleVersion(), "r1 必须解析到交错后的 v5");
            assertEquals(4L, a2.ruleVersion(), "r2 必须解析到 v4，不被 r1 的版本号串扰");
            assertEquals("FIRING", a1.state().name());
            assertEquals("FIRING", a2.state().name());

            // 复盘生效规则表里版本号与实时历史一致。
            Map<String, Long> versionById = new java.util.HashMap<>();
            for (RuleVersion v : rep.rulesEffective()) {
                versionById.put(v.id(), v.version());
            }
            assertEquals(5L, versionById.get("r1"));
            assertEquals(4L, versionById.get("r2"));

            // T=11 时切点事件 f0 先于三个变更线性化：该线性化点只看到 v1/v2，
            // 与实时在 f0 处理完那一刻所见一致（边界之后定稿的窗口才用新版本）。
            ReplayReport at11 = h.replayService().replayAt(11, null);
            RuleVersion r1at11 = at11.rulesEffective().stream()
                    .filter(v -> v.id().equals("r1")).findFirst().orElseThrow();
            assertEquals(1L, r1at11.version(), "切点 f0 先于变更，T=11 只可见种子 v1");
            assertTrue(at11.alerts().stream().noneMatch(a -> a.state().name().equals("FIRING")));
            // 越过变更后的下一个事件时间（21）复盘，即能看到交错后的 v5。
            ReplayReport at21 = h.replayService().replayAt(21, null);
            RuleVersion r1at21 = at21.rulesEffective().stream()
                    .filter(v -> v.id().equals("r1")).findFirst().orElseThrow();
            assertEquals(5L, r1at21.version(), "effectiveFrom=11<=21，T=21 已可见 v5");
            log.info("[ASSERT] 复盘版本号 r1=v5 r2=v4 与历史一致，T=11 只见种子版本，T=31 双双 FIRING");
            h.dumpState(log);
        }
    }
}
