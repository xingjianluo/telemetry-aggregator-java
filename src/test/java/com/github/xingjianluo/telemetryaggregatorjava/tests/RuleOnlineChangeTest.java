package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeResult;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 规则在线变更：不重启调整阈值/匹配范围/启停/免打扰；版本与生效边界可查询；
 * 边界之前定稿的窗口维持原判，之后按新规则判定。
 */
class RuleOnlineChangeTest {

    private static final Logger log = LoggerFactory.getLogger(RuleOnlineChangeTest.class);

    private TelemetryProperties props() {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(0);
        p.getAlert().setBreachStreak(1);
        p.getAlert().setRecoverStreak(1);
        return p;
    }

    private AlertSnapshot alert(TestHarness.Engine h) {
        return h.alertEngine.snapshots().stream()
                .filter(s -> s.ruleId().equals("r1")).findFirst().orElse(null);
    }

    /** 向窗口 w 放一条 svc/m 的事件，再用其它 metric 的推进事件关闭该窗口。 */
    private void windowValue(TestHarness.Engine h, int wStart, double value) {
        h.send(log, TestHarness.event("e-" + wStart + "-" + (int) value, wStart + 1,
                "tr-" + wStart, "s", null, "svc", "m", value, Map.of()));
        h.send(log, TestHarness.event("seal-" + wStart, wStart + 11,
                "trs-" + wStart, "ss", null, "svc", "other", 0, Map.of()));
    }

    @Test
    void thresholdChangeAppliesOnlyToWindowsSealedAfterBoundary() {
        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                100.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(seed), 0L, 1000, 1000)) {
            log.info("[CASE] 阈值 100->10：边界(11)之前定稿的窗口维持原判，之后按新阈值");

            windowValue(h, 0, 50); // 50 <= 100：旧规则不报警（主体存在但状态 OK）
            assertEquals(AlertState.OK, alert(h).state(), "旧阈值 100 下 avg=50 应保持 OK");
            log.info("[STEP] w0=50 旧阈值100 => OK（无报警）");

            // 在水位线=11（窗口1 结束后定稿）时变更阈值；effectiveFrom 应为 11。
            RuleChangeResult ch = h.engine.changeRule(new RuleChangeRequest(
                    "req-1", "r1", null, null, null, null, null,
                    10.0, null, null, null, "阈值下调到10"));
            assertEquals(RuleChangeStatus.APPLIED, ch.status());
            assertEquals(11L, ch.effectiveFromMillis(), "生效边界=变更时水位线 11");
            assertEquals(2L, ch.version());
            log.info("[CHANGE] {} v={} effectiveFrom={}", ch.status(), ch.version(), ch.effectiveFromMillis());

            windowValue(h, 20, 50); // 50 > 10：新规则 FIRING
            AlertSnapshot snap = alert(h);
            assertNotNull(snap);
            assertEquals(AlertState.FIRING, snap.state());
            assertEquals(2L, snap.ruleVersion());
            log.info("[STEP] w@20=50 新阈值10 v{} => FIRING", snap.ruleVersion());

            // 版本历史可查询，且定稿旧窗口没有被追溯改写。
            assertEquals(2, h.ruleStore.historyOf("r1").size());
            assertEquals(100.0, h.ruleStore.historyOf("r1").get(0).threshold());
            assertEquals(10.0, h.ruleStore.historyOf("r1").get(1).threshold());
            log.info("[ASSERT] 版本历史可查询；w0 维持旧判（无告警），w@20 按新阈值 FIRING");
            h.dumpState(log);
        }
    }

    @Test
    void sameRequestIdIsIdempotentAndDoesNotCreateExtraVersion() {
        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                100.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(seed), 0L, 1000, 1000)) {
            log.info("[CASE] 相同 requestId 的任意次重试只产生一个版本");
            // 先推进一点水位线，证明“重试”不会用新水位线再造一个不同边界的版本。
            h.send(log, TestHarness.event("p", 5, "trp", "s", null, "svc", "x", 1, Map.of()));
            RuleChangeRequest req = new RuleChangeRequest("dup-1", "r1", null, null,
                    null, null, null, 20.0, null, null, null, "调到20");
            RuleChangeResult first = h.engine.changeRule(req);
            assertEquals(RuleChangeStatus.APPLIED, first.status());
            h.send(log, TestHarness.event("p2", 25, "trp2", "s", null, "svc", "x", 1, Map.of()));
            RuleChangeResult second = h.engine.changeRule(req);
            RuleChangeResult third = h.engine.changeRule(req);
            assertEquals(first.version(), second.version());
            assertEquals(first.version(), third.version());
            assertEquals(first.effectiveFromMillis(), second.effectiveFromMillis());
            assertEquals(2, h.ruleStore.historyOf("r1").size(),
                    "相同 requestId 的重试不得产生额外版本");
            log.info("[ASSERT] 三次提交（requestId=dup-1）只产生 v{}，边界固定={}",
                    second.version(), second.effectiveFromMillis());
            h.dumpState(log);
        }
    }

    @Test
    void disableStopsEvaluationAndEnableResumesWithoutResettingEvidence() {
        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                100.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(seed), 0L, 1000, 1000)) {
            log.info("[CASE] 停用后窗口不再评估；重新启用后继续工作");
            windowValue(h, 0, 200);
            assertEquals(AlertState.FIRING, alert(h).state());

            RuleChangeResult off = h.engine.changeRule(new RuleChangeRequest(
                    "off", "r1", false, null, null, null, null, null, null, null, null, "停用"));
            assertTrue(off.applied());
            windowValue(h, 20, 5);   // 停用期间：不评估、不翻转
            windowValue(h, 40, 200);
            AlertSnapshot during = alert(h);
            assertEquals(AlertState.FIRING, during.state(), "停用期间状态维持，不被更新");
            log.info("[ASSERT] 停用期间窗口不评估，状态维持 FIRING");

            h.engine.changeRule(new RuleChangeRequest(
                    "on", "r1", true, null, null, null, null, null, null, null, null, "启用"));
            windowValue(h, 60, 5); // 恢复=1 => recoverStreak1 回 OK
            assertEquals(AlertState.OK, alert(h).state());
            log.info("[ASSERT] 重新启用后评估恢复，w@60=5 => OK");
            h.dumpState(log);
        }
    }

    @Test
    void quietWindowSuppressesTransitionsButKeepsEvidenceAndDoesNotBurstOnExit() {
        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                100.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(seed), 0L, 1000, 1000)) {
            log.info("[CASE] 免打扰[20,60)：窗口20/40越界只累计证据不 FIRING；"
                    + "结束后按当前证据平稳处理，不补发");
            // 水位线先到 11，便于在边界 11 处设置免打扰。
            windowValue(h, 0, 5); // OK
            RuleChangeResult q = h.engine.changeRule(new RuleChangeRequest(
                    "q", "r1", null, null, null, null, null, null, null,
                    20L, 60L, "免打扰20-60"));
            assertTrue(q.applied());
            log.info("[CHANGE] quiet[20,60) effectiveFrom={}", q.effectiveFromMillis());

            windowValue(h, 20, 200); // 落在免打扰 => 抑制，保持 OK
            AlertSnapshot s1 = alert(h);
            assertEquals(AlertState.OK, s1.state(), "免打扰期间不得 FIRING");
            assertEquals(1, s1.suppressedCount());
            windowValue(h, 40, 200); // 仍在免打扰 => 继续抑制，不补发
            AlertSnapshot s2 = alert(h);
            assertEquals(AlertState.OK, s2.state());
            assertEquals(2, s2.suppressedCount());
            log.info("[ASSERT] w@20,w@40 越界但被免打扰抑制，suppressedCount={}", s2.suppressedCount());

            windowValue(h, 60, 200); // 区间结束后的第一个越界窗口 => 平稳进入 FIRING（一次，非补发两条）
            AlertSnapshot s3 = alert(h);
            assertEquals(AlertState.FIRING, s3.state(), "免打扰结束后按证据判定，单次 FIRING");
            log.info("[ASSERT] w@60=200 免打扰结束 => FIRING（仅一次状态翻转，无补发）");
            h.dumpState(log);
        }
    }

    @Test
    void invalidChangeIsRejectedWithoutVersion() {
        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                100.0, AlertRule.Operator.GT);
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(seed), 0L, 1000, 1000)) {
            RuleChangeResult bad1 = h.engine.changeRule(new RuleChangeRequest(
                    "b1", "  ", null, null, null, null, null, 1.0, null, null, null, null));
            assertEquals(RuleChangeStatus.INVALID_REQUEST, bad1.status());
            RuleChangeResult bad2 = h.engine.changeRule(new RuleChangeRequest(
                    "b2", "r1", null, null, null, null, null, 1.0, null, 100L, 50L, null));
            assertEquals(RuleChangeStatus.INVALID_REQUEST, bad2.status(), "倒置免打扰区间必须拒绝");
            assertEquals(1, h.ruleStore.historyOf("r1").size(), "非法请求不产生版本");
            log.info("[ASSERT] 缺ID/免打扰区间倒置均 INVALID_REQUEST，无新版本产生");
        }
    }
}
