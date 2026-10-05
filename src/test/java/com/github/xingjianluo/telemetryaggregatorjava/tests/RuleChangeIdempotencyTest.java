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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 规则变更幂等：同一变更请求（相同 requestId）无论重试多少次、是否并发，
 * 最终只落一个版本，且每次返回同一版本号与同一生效边界；
 * 已生效后的重试不改变规则状态，也不影响边界两侧窗口的既有结论；
 * 非法变更快速拒绝、不产生版本；超时后的重试收敛到唯一版本。
 */
class RuleChangeIdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(RuleChangeIdempotencyTest.class);

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

    private RuleChangeRequest upsert(double threshold, String requestId) {
        return new RuleChangeRequest("r", RuleChange.Action.UPSERT, rule(threshold),
                null, requestId);
    }

    /** 在指定窗口放一个事件并推进水位线定稿该窗口。 */
    private void windowValue(TestHarness.Engine h, int windowStart, double value) {
        String id = "w" + windowStart + "-" + (int) value;
        h.send(log, TestHarness.event(id, windowStart + 1, "tr-" + id, "s", null,
                "svc", "m", value, Map.of()));
        h.send(log, TestHarness.event("seal-" + windowStart, windowStart + 11,
                "tr-seal-" + windowStart, "ss", null, "svc", "other", 0, Map.of()));
    }

    @Test
    void duplicateSubmissionReturnsSameVersionAndBoundary() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 同一变更请求（requestId=req-1）顺序重复提交 3 次");
            RuleChange first = h.change(log, upsert(100, "req-1"));
            RuleChange second = h.change(log, upsert(100, "req-1"));
            RuleChange third = h.change(log, upsert(100, "req-1"));

            assertEquals(1L, first.version());
            assertEquals(first, second, "重试必须返回与首次完全一致的变更记录");
            assertEquals(first, third);
            assertEquals(1, h.ruleRegistry.history().size(),
                    "同一请求的任意次重试只落一个版本");
            assertEquals(100.0, h.ruleRegistry.currentRules().get(0).rule().threshold());
            log.info("[ASSERT] 3 次提交 => 版本号均为 {}，生效边界均为 {}，历史仅 1 条",
                    first.version(), first.effectiveFromMillis());
        }
    }

    @Test
    void concurrentRetriesLandExactlyOneVersionAndVersionsStayContiguous() throws Exception {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            int threads = 16;
            log.info("[CASE] {} 个线程并发提交同一请求（requestId=req-conc）", threads);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<RuleChange>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return h.engine.submitRuleChange(upsert(100, "req-conc"));
                }));
            }
            start.countDown();
            List<RuleChange> results = new ArrayList<>();
            for (Future<RuleChange> f : futures) {
                results.add(f.get(30, TimeUnit.SECONDS));
            }
            pool.shutdown();

            RuleChange first = results.get(0);
            for (RuleChange r : results) {
                assertEquals(first, r, "并发重试的每一次返回都必须一致");
            }
            assertEquals(1L, first.version());
            assertEquals(1, h.ruleRegistry.history().size(),
                    "并发重试不得产生第二个版本");
            log.info("[ASSERT] {} 路并发重试 => 全部返回 version={} effectiveFrom={}，历史仅 1 条",
                    threads, first.version(), first.effectiveFromMillis());

            // 后续不同请求的版本必须连续（不丢失、不互相覆盖）
            RuleChange next = h.change(log, upsert(200, "req-next"));
            assertEquals(2L, next.version(), "幂等重试不得消耗版本号，后续版本必须连续");
            assertEquals(2, h.ruleRegistry.history().size());
            log.info("[ASSERT] 后续变更 version={}，版本序列连续无空洞", next.version());
        }
    }

    @Test
    void retryAfterAppliedKeepsBoundaryAndFinalizedWindowsVerdicts() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 变更生效并推进水位线后重试同一请求：边界不回移、状态不变、既有结论不变");

            windowValue(h, 0, 20); // 窗口[0,10)：20 > 10 => FIRING（v0）
            AlertSnapshot before = h.alertEngine.snapshots().stream()
                    .filter(s -> s.ruleId().equals("r")).findFirst().orElseThrow();
            assertEquals(AlertState.FIRING, before.state());

            RuleChange change = h.change(log, upsert(100, "req-1"));
            assertEquals(11L, change.effectiveFromMillis());

            // 边界之后的新窗口按新规则判定，水位线继续推进到 31
            windowValue(h, 20, 20); // 窗口[20,30) end=30 > 11 => 新规则：20<=100 => OK
            AlertSnapshot afterNewRule = h.alertEngine.snapshots().stream()
                    .filter(s -> s.ruleId().equals("r")).findFirst().orElseThrow();
            assertEquals(AlertState.OK, afterNewRule.state());
            assertEquals(1L, afterNewRule.ruleVersion());

            // 首次变更已经生效之后重试同一请求：不得再次改动规则状态、不得回移生效边界
            RuleChange retry = h.change(log, upsert(100, "req-1"));
            assertEquals(change, retry, "生效后的重试必须返回首次登记的同一条记录");
            assertEquals(11L, retry.effectiveFromMillis(),
                    "重试不得把生效边界回移到当前水位线（31）");
            assertEquals(1, h.ruleRegistry.history().size());
            assertEquals(100.0, h.ruleRegistry.currentRules().get(0).rule().threshold(),
                    "重试不得改变已生效的规则状态");
            log.info("[ASSERT] 生效后重试 => version={} effectiveFrom={} 与首次一致，历史仍 1 条",
                    retry.version(), retry.effectiveFromMillis());

            // 边界之前已定稿窗口的结论不被改写：asOf=11 复盘仍是 FIRING + v0
            ReplayReport replayBefore = h.replay(log, 11);
            assertEquals(ReplayStatus.OK, replayBefore.status());
            AlertSnapshot rs = replayBefore.alerts().stream()
                    .filter(a -> a.ruleId().equals("r")).findFirst().orElseThrow();
            assertEquals(AlertState.FIRING, rs.state(), "边界前定稿窗口的结论不得被重试改写");
            assertEquals(0L, rs.ruleVersion());

            // 边界之后的判定也不受重试影响：asOf=31 复盘与实时一致
            ReplayReport replayAfter = h.replay(log, 31);
            AlertSnapshot rs2 = replayAfter.alerts().stream()
                    .filter(a -> a.ruleId().equals("r")).findFirst().orElseThrow();
            assertEquals(afterNewRule, rs2, "边界后窗口的实时结论与复盘结论必须逐位一致");
            log.info("[ASSERT] 边界前复盘 FIRING/v0 维持原判；边界后实时与复盘一致：{}",
                    rs2.decisionBasis());
        }
    }

    @Test
    void sameRequestIdWithDifferentPayloadIsRejectedWithoutNewVersion() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 相同 requestId 搭配不同载荷 => 冲突拒绝，不产生版本");
            RuleChange first = h.change(log, upsert(100, "req-1"));

            IngestionEngine.RuleChangeInvalidException ex = assertThrows(
                    IngestionEngine.RuleChangeInvalidException.class,
                    () -> h.engine.submitRuleChange(upsert(200, "req-1")));
            log.info("[STEP] 冲突拒绝: {}", ex.getMessage());

            assertEquals(1, h.ruleRegistry.history().size(), "冲突不得产生新版本");
            assertEquals(100.0, h.ruleRegistry.currentRules().get(0).rule().threshold(),
                    "冲突不得改变已生效的规则状态");
            assertEquals(1L, h.ruleRegistry.history().get(0).version());
            assertEquals(first.version(), h.ruleRegistry.history().get(0).version());
            log.info("[ASSERT] 历史仍 1 条（version=1），当前阈值仍为 100");
        }
    }

    @Test
    void invalidChangesFailFastWithoutVersionOrStateChange() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 各类非法变更：快速拒绝、不产生版本、不影响已有规则状态");

            assertThrows(IngestionEngine.RuleChangeInvalidException.class, () ->
                    h.engine.submitRuleChange(new RuleChangeRequest(
                            "ghost", RuleChange.Action.DISABLE, null, null, "bad-1")),
                    "未知规则的 DISABLE 必须拒绝");
            assertThrows(IngestionEngine.RuleChangeInvalidException.class, () ->
                    h.engine.submitRuleChange(new RuleChangeRequest(
                            "r", RuleChange.Action.UPSERT, null, null, "bad-2")),
                    "缺少规则定义的 UPSERT 必须拒绝");
            assertThrows(IngestionEngine.RuleChangeInvalidException.class, () ->
                    h.engine.submitRuleChange(new RuleChangeRequest(
                            "r", RuleChange.Action.MUTE, null, null, "bad-3")),
                    "缺少 muteUntilMillis 的 MUTE 必须拒绝");
            assertThrows(IngestionEngine.RuleChangeInvalidException.class, () ->
                    h.engine.submitRuleChange(new RuleChangeRequest(
                            "r", RuleChange.Action.UPSERT,
                            new AlertRule("other", AlertRule.Scope.TAGS, "svc", "m",
                                    Map.of(), 50.0, AlertRule.Operator.GT),
                            null, "bad-4")),
                    "ruleId 与规则定义 id 不一致必须拒绝");
            // 非法请求的 requestId 不得被登记：修正载荷后同一 requestId 可正常使用
            RuleChange fixed = h.change(log, new RuleChangeRequest(
                    "r2", RuleChange.Action.UPSERT,
                    new AlertRule("r2", AlertRule.Scope.TAGS, "svc", "m",
                            Map.of(), 50.0, AlertRule.Operator.GT),
                    null, "bad-4"));
            assertEquals(1L, fixed.version(), "非法尝试不得消耗版本号");

            assertEquals(1, h.ruleRegistry.history().size(), "非法变更不得产生任何版本");
            assertEquals(10.0, h.ruleRegistry.currentRules().stream()
                    .filter(r -> r.rule().id().equals("r")).findFirst().orElseThrow()
                    .rule().threshold(), "已有规则状态不得被非法变更影响");
            log.info("[ASSERT] 4 次非法变更全部 422 语义拒绝，历史仅含修正后的 version=1");
        }
    }

    @Test
    void timeoutThenRetryConvergesToSingleVersion() throws Exception {
        TelemetryProperties p = props();
        // 回执等待超时压到 1ms：worker 繁忙时调用方先拿到“结果未知”
        p.getBuffer().setResultTimeout(Duration.ofMillis(1));
        try (TestHarness.Engine h = new TestHarness.Engine(p, List.of(rule(10)), 0L)) {
            log.info("[CASE] 回执超时（结果未知）后以相同 requestId 重试 => 收敛到唯一版本");

            // 让 worker 保持繁忙，提高首次提交超时的确定性（不依赖则断言语义仍成立）
            AtomicInteger timeouts = new AtomicInteger();
            RuleChange settled = null;
            for (int attempt = 1; attempt <= 200 && settled == null; attempt++) {
                try {
                    settled = h.engine.submitRuleChange(upsert(100, "req-timeout"));
                    log.info("[STEP] 第 {} 次提交成功: version={} effectiveFrom={}",
                            attempt, settled.version(), settled.effectiveFromMillis());
                } catch (IngestionEngine.RuleChangeTimeoutException e) {
                    timeouts.incrementAndGet();
                    log.info("[STEP] 第 {} 次提交回执超时（结果未知）: {}", attempt, e.getMessage());
                    Thread.sleep(5);
                }
            }
            assertNotNull(settled, "有限次重试内必须收敛到确定结论");
            log.info("[STEP] 共发生 {} 次回执超时后收敛", timeouts.get());

            // 收敛后再重试：仍返回同一条记录
            RuleChange again = h.change(log, upsert(100, "req-timeout"));
            assertEquals(settled, again, "收敛后的重试必须返回同一条变更记录");

            // 无论超时期间首次请求是否已生效，最终只落一个版本
            assertEquals(1, h.ruleRegistry.history().size(),
                    "超时 + 重试全程最多落一个版本");
            assertEquals(settled.version(), h.ruleRegistry.history().get(0).version());
            assertEquals(settled.effectiveFromMillis(),
                    h.ruleRegistry.history().get(0).effectiveFromMillis());

            // 后续变更版本连续
            RuleChange next = h.change(log, upsert(200, "req-after-timeout"));
            assertEquals(settled.version() + 1, next.version(), "版本号必须连续无空洞");
            log.info("[ASSERT] 超时重试收敛到 version={}，后续变更 version={}，历史 {} 条",
                    settled.version(), next.version(), h.ruleRegistry.history().size());
        }
    }

    @Test
    void backpressureMeansDefinitelyNotAppliedAndRetrySucceeds() throws Exception {
        TelemetryProperties p = props();
        p.getBuffer().setQueueCapacity(1);
        p.getBuffer().setOfferTimeout(Duration.ZERO);
        try (TestHarness.Engine h = new TestHarness.Engine(p, List.of(rule(10)), 0L)) {
            log.info("[CASE] 队列打满时变更被背压拒绝（确定未生效），重试后正常落一个版本");

            // 后台线程持续打满队列
            AtomicInteger accepted = new AtomicInteger();
            AtomicInteger bufferFull = new AtomicInteger();
            ExecutorService flood = Executors.newFixedThreadPool(4);
            CountDownLatch stop = new CountDownLatch(1);
            for (int t = 0; t < 4; t++) {
                final int tid = t;
                flood.submit(() -> {
                    int i = 0;
                    while (stop.getCount() > 0) {
                        var r = h.engine.ingest(TestHarness.event(
                                "f-" + tid + "-" + (i++), 5, "tr-f" + tid, "s", null,
                                "svc", "m", 1, Map.of()));
                        if (r.accepted()) {
                            accepted.incrementAndGet();
                        } else {
                            bufferFull.incrementAndGet();
                        }
                    }
                });
            }

            // 在队列被持续打满期间提交变更：必然遇到背压（确定未生效）
            IngestionEngine.RuleChangeBackpressureException backpressure = null;
            for (int attempt = 0; attempt < 5000 && backpressure == null; attempt++) {
                try {
                    h.engine.submitRuleChange(upsert(100, "req-bp"));
                } catch (IngestionEngine.RuleChangeBackpressureException e) {
                    backpressure = e;
                }
            }
            stop.countDown();
            flood.shutdown();
            assertTrue(flood.awaitTermination(30, TimeUnit.SECONDS));
            assertNotNull(backpressure, "队列打满期间必须出现背压拒绝");
            log.info("[STEP] 背压拒绝（确定未生效）: {}（期间 accepted={} bufferFull={}）",
                    backpressure.getMessage(), accepted.get(), bufferFull.get());
            assertEquals(0, h.ruleRegistry.history().size(),
                    "背压拒绝的变更不得产生任何版本");

            // 确定未生效 => 重试拿到确定结论：正常落一个版本
            RuleChange retry = h.change(log, upsert(100, "req-bp"));
            assertEquals(1L, retry.version());
            RuleChange retry2 = h.change(log, upsert(100, "req-bp"));
            assertEquals(retry, retry2, "重试的再重试仍返回同一条记录");
            assertEquals(1, h.ruleRegistry.history().size());
            log.info("[ASSERT] 背压后重试 => version={} effectiveFrom={}，历史仅 1 条",
                    retry.version(), retry.effectiveFromMillis());
        }
    }
}
