package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.ingest.IngestionEngine;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayReport;
import com.github.xingjianluo.telemetryaggregatorjava.replay.ReplayStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 规则在线变更：不重启调整阈值 / 启停；每次变更有可查询版本与生效边界；
 * 边界之前已定稿窗口维持原判，边界之后按新规则判定。
 */
class RuleHotUpdateTest {

    private static final Logger log = LoggerFactory.getLogger(RuleHotUpdateTest.class);

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

    /** 在指定窗口放一个事件并推进水位线定稿该窗口。 */
    private void windowValue(TestHarness.Engine h, int windowStart, double value) {
        String id = "w" + windowStart + "-" + (int) value;
        h.send(log, TestHarness.event(id, windowStart + 1, "tr-" + id, "s", null,
                "svc", "m", value, Map.of()));
        h.send(log, TestHarness.event("seal-" + windowStart, windowStart + 11,
                "tr-seal-" + windowStart, "ss", null, "svc", "other", 0, Map.of()));
    }

    private AlertSnapshot snapshot(TestHarness.Engine h) {
        return h.alertEngine.snapshots().stream()
                .filter(s -> s.ruleId().equals("r")).findFirst().orElse(null);
    }

    @Test
    void thresholdChangeAppliesOnlyAfterEffectiveBoundary() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 阈值 10 -> 100 在线调整：边界前定稿窗口维持原判，边界后按新规则");

            windowValue(h, 0, 20); // 窗口[0,10)：20 > 10 => FIRING（初始规则 v0）
            AlertSnapshot s0 = snapshot(h);
            assertEquals(AlertState.FIRING, s0.state());
            assertEquals(0L, s0.ruleVersion());
            log.info("[STEP] 窗口[0,10) avg=20 阈值=10 => {} ruleVersion={}", s0.state(), s0.ruleVersion());

            // 在线把阈值调到 100：返回版本与生效边界（当前水位线=11）
            RuleChange change = h.change(log, TestHarness.changeReq(
                    "r", RuleChange.Action.UPSERT, rule(100), null));
            assertEquals(1L, change.version());
            assertEquals(11L, change.effectiveFromMillis(),
                    "生效边界应等于变更应用时刻的水位线");
            log.info("[STEP] 变更版本={} 生效边界={}（end<=11 的窗口维持原判）",
                    change.version(), change.effectiveFromMillis());

            // 边界之后的新窗口按新阈值判定：20 <= 100 => 恢复 OK
            windowValue(h, 20, 20); // 窗口[20,30)，end=30 > 11 => 新规则
            AlertSnapshot s1 = snapshot(h);
            assertEquals(AlertState.OK, s1.state());
            assertEquals(1L, s1.ruleVersion(), "边界后的窗口必须使用新规则版本");
            log.info("[STEP] 窗口[20,30) avg=20 阈值=100 => {} ruleVersion={}", s1.state(), s1.ruleVersion());

            // 边界之前的定论可复盘且维持原判：asOf=11 时仍是 FIRING + v0
            ReplayReport replay = h.replay(log, 11);
            assertEquals(ReplayStatus.OK, replay.status());
            AlertSnapshot rs = replay.alerts().stream()
                    .filter(a -> a.ruleId().equals("r")).findFirst().orElse(null);
            assertNotNull(rs);
            assertEquals(AlertState.FIRING, rs.state(), "边界前已定稿窗口的判定不得被新规则改写");
            assertEquals(0L, rs.ruleVersion());
            log.info("[ASSERT] 复盘 asOf=11 => state={} ruleVersion={}（与当时实时结论一致）",
                    rs.state(), rs.ruleVersion());

            // 版本历史可查询
            assertEquals(1, h.ruleRegistry.history().size());
            h.dumpState(log);
        }
    }

    @Test
    void disableStopsEvaluationAndEnableResumes() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] DISABLE 后新窗口不再评估，ENABLE 后续接评估（状态不丢失）");

            windowValue(h, 0, 20); // FIRING
            assertEquals(AlertState.FIRING, snapshot(h).state());

            h.change(log, TestHarness.changeReq("r", RuleChange.Action.DISABLE, null, null));
            windowValue(h, 20, 5); // 停用期间：不评估，状态保持 FIRING，lastWindowStart 不前进
            AlertSnapshot disabled = snapshot(h);
            assertEquals(AlertState.FIRING, disabled.state());
            assertEquals(0L, disabled.lastEvaluatedWindowStart(),
                    "停用期间不得评估新窗口");
            log.info("[STEP] DISABLE 期间窗口[20,30) 未评估，state={} lastWindow={}",
                    disabled.state(), disabled.lastEvaluatedWindowStart());

            h.change(log, TestHarness.changeReq("r", RuleChange.Action.ENABLE, null, null));
            windowValue(h, 40, 5); // 重新启用：恢复评估，连续恢复计数满足 => OK
            AlertSnapshot enabled = snapshot(h);
            assertEquals(AlertState.OK, enabled.state());
            assertEquals(40L, enabled.lastEvaluatedWindowStart());
            assertEquals(2L, enabled.ruleVersion(), "ENABLE 也是一次版本变更");
            log.info("[ASSERT] ENABLE 后窗口[40,50) 评估 => {} ruleVersion={}",
                    enabled.state(), enabled.ruleVersion());
            h.dumpState(log);
        }
    }

    @Test
    void invalidChangeIsRejectedWithDistinguishableError() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 非法变更（未知规则 / 缺定义 / id 不一致）快速失败，不影响现有规则");
            assertThrows(IngestionEngine.RuleChangeException.class, () ->
                    h.change(log, TestHarness.changeReq("ghost", RuleChange.Action.DISABLE, null, null)));
            assertThrows(IngestionEngine.RuleChangeException.class, () ->
                    h.change(log, TestHarness.changeReq("r", RuleChange.Action.UPSERT, null, null)));
            assertThrows(IngestionEngine.RuleChangeException.class, () ->
                    h.change(log, TestHarness.changeReq("r", RuleChange.Action.UPSERT,
                            new AlertRule("other", AlertRule.Scope.TAGS, null, null,
                                    Map.of(), 1, AlertRule.Operator.GT), null)));
            assertTrue(h.ruleRegistry.history().isEmpty(), "非法变更不得产生版本");
            assertEquals(1, h.ruleRegistry.currentRules().size());
            log.info("[ASSERT] 三次非法变更均被拒绝，规则集未受影响");
        }
    }
}
