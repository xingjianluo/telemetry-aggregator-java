package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertState;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.alert.RuleChangeOutcome;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 规则变更的幂等与失败语义：
 * <ol>
 *   <li>同一 requestId 顺序 / 并发重试只产生一个版本，返回相同版本号与生效边界，
 *       且重试不改变已定稿窗口结论与后续窗口判定；</li>
 *   <li>变更与并发摄取交错时版本号连续、不丢失、不互相覆盖；</li>
 *   <li>背压（确定未生效）与等待超时（结论不确定）可区分，用同键重试后拿到确定结论；</li>
 *   <li>同键不同载荷按非法变更拒绝，不产生版本；真正非法变更快速失败无副作用。</li>
 * </ol>
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
    void repeatedSubmissionProducesSingleVersionAndStableBoundary() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 同一变更请求顺序重试 5 次：只落一个版本、同一边界，仅首次生效");
            windowValue(h, 0, 20); // FIRING v0，水位线推进到 11
            assertEquals(AlertState.FIRING, snapshot(h).state());

            RuleChangeRequest req = TestHarness.changeReq(
                    "idem-1", "r", RuleChange.Action.UPSERT, rule(100), null);

            RuleChangeOutcome first = h.submitChange(log, req);
            assertFalse(first.replayed());
            assertEquals(1L, first.change().version());
            assertEquals(11L, first.change().effectiveFromMillis());

            for (int i = 0; i < 4; i++) {
                RuleChangeOutcome again = h.submitChange(log, req);
                assertTrue(again.replayed(), "第 " + (i + 2) + " 次提交必须是幂等重放");
                assertEquals(first.change(), again.change(),
                        "重试必须返回完全相同的版本记录（版本号、生效边界、快照）");
            }
            assertEquals(1, h.ruleRegistry.history().size(), "历史中只能有一条版本");
            log.info("[ASSERT] 5 次提交只产生 version=1，4 次重放无副作用");

            // 重试不改变任何判定：边界前窗口仍 FIRING/v0，边界后窗口按 v1 判定
            windowValue(h, 20, 20); // end=30 > 11 => 新阈值 100 => OK
            AlertSnapshot now = snapshot(h);
            assertEquals(AlertState.OK, now.state());
            assertEquals(1L, now.ruleVersion());

            ReplayReport replay = h.replay(log, 11);
            assertEquals(ReplayStatus.OK, replay.status());
            AlertSnapshot atBoundary = replay.alerts().stream()
                    .filter(a -> a.ruleId().equals("r")).findFirst().orElseThrow();
            assertEquals(AlertState.FIRING, atBoundary.state());
            assertEquals(0L, atBoundary.ruleVersion(),
                    "重试不得改变边界之前已定稿窗口的结论");
            log.info("[ASSERT] 边界前结论维持 FIRING/v0，边界后 OK/v1，未因重试漂移");
            h.dumpState(log);
        }
    }

    @Test
    void retryAfterChangeAlreadyEffectiveDoesNotReSealOrMutate() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 首次生效后，边界两侧都已有定稿窗口，再重试不得重放定稿副作用");
            windowValue(h, 0, 20);  // [0,10) FIRING v0
            RuleChangeRequest req = TestHarness.changeReq(
                    "idem-2", "r", RuleChange.Action.UPSERT, rule(100), null);
            RuleChange c1 = h.change(log, req);
            windowValue(h, 20, 20); // [20,30) OK v1
            windowValue(h, 40, 20); // [40,50) OK v1
            long sealedBefore = h.windowStore.query(null, null).stream()
                    .map(a -> a.windowStartMillis()).distinct().count();

            // 此时重试：不得再次定稿（没有可定稿窗口）也不得推进状态
            RuleChangeOutcome again = h.submitChange(log, req);
            assertTrue(again.replayed());
            assertEquals(c1, again.change());
            long sealedAfter = h.windowStore.query(null, null).stream()
                    .map(a -> a.windowStartMillis()).distinct().count();
            assertEquals(sealedBefore, sealedAfter, "重试不得触发额外的窗口定稿");
            assertEquals(AlertState.OK, snapshot(h).state());
            assertEquals(1, h.ruleRegistry.history().size());
            log.info("[ASSERT] 生效后重试：定稿窗口数不变 {}，状态不回退", sealedAfter);
            h.dumpState(log);
        }
    }

    @Test
    void concurrentRetriesOfSameRequestResolveToOneVersion() throws Exception {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 同一 requestId 被 16 个线程并发重发：只产生一个版本，全部拿到同一结果");
            windowValue(h, 0, 20);

            RuleChangeRequest req = TestHarness.changeReq(
                    "idem-3", "r", RuleChange.Action.UPSERT, rule(100), null);
            int threads = 16;
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<java.util.concurrent.Future<RuleChangeOutcome>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return h.engine.submitRuleChange(req);
                }));
            }
            start.countDown();
            int firstCount = 0;
            RuleChange reference = null;
            for (var f : futures) {
                RuleChangeOutcome o = f.get(10, TimeUnit.SECONDS);
                if (reference == null) {
                    reference = o.change();
                }
                assertEquals(reference, o.change(), "并发重发必须拿到同一条版本记录");
                if (!o.replayed()) {
                    firstCount++;
                }
            }
            pool.shutdown();
            assertEquals(1, firstCount, "恰好一次首次生效");
            assertEquals(1, h.ruleRegistry.history().size());
            assertNotNull(reference);
            assertEquals(1L, reference.version());
            log.info("[ASSERT] 16 个并发提交 => 唯一 version=1，1 次首次 + 15 次重放");
            h.dumpState(log);
        }
    }

    @Test
    void distinctChangesConcurrentWithIngestionStayContinuous() throws Exception {
        TelemetryProperties p = props();
        try (TestHarness.Engine h = new TestHarness.Engine(p, List.of(rule(10)), 0L)) {
            log.info("[CASE] 20 个不同变更与并发摄取交错：版本 1..20 连续，且同键重试不占新版本");
            int changeCount = 20;
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(4);
            ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

            // 20 个变更，每个都由 2 个线程并发提交（同一 requestId）=> 仍只有 20 个版本
            List<java.util.concurrent.Future<RuleChange>> futures = new ArrayList<>();
            for (int c = 0; c < changeCount; c++) {
                final int idx = c;
                double threshold = (idx % 2 == 0) ? 50.0 : 150.0;
                for (int copy = 0; copy < 2; copy++) {
                    futures.add(pool.submit(() -> {
                        try {
                            start.await();
                            return h.engine.submitRuleChange(TestHarness.changeReq(
                                    "distinct-" + idx, "r", RuleChange.Action.UPSERT,
                                    rule(threshold), null)).change();
                        } catch (Throwable t) {
                            errors.add(t);
                            throw t;
                        }
                    }));
                }
                // 同时穿插事件摄取，推动窗口与水位线
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                        h.engine.ingest(TestHarness.event("ing-" + idx,
                                1 + idx, "tr-" + idx, "s", null, "svc", "m", 20, Map.of()));
                        return null;
                    } catch (Throwable e) {
                        errors.add(e);
                        throw e;
                    }
                }));
            }
            start.countDown();
            for (var f : futures) {
                f.get(15, TimeUnit.SECONDS);
            }
            pool.shutdown();
            assertTrue(errors.isEmpty(), "并发提交不应抛错: " + errors);

            List<RuleChange> history = h.ruleRegistry.history();
            assertEquals(changeCount, history.size(), "只有不同 requestId 才产生新版本");
            for (int i = 0; i < changeCount; i++) {
                assertEquals(i + 1, history.get(i).version(), "版本号必须连续无空洞");
            }
            log.info("[ASSERT] 40 次同键并发提交 + {} 事件摄取 => 版本 1..{} 连续无覆盖",
                    changeCount, changeCount);
            h.dumpState(log);
        }
    }

    @Test
    void backpressureIsReportedAsNotAppliedAndRetrySucceeds() throws Exception {
        TelemetryProperties p = props();
        // 容量 1 + 零等待：worker 取出第一个阻塞任务后在其中 sleep，
        // 第二个阻塞任务填满唯一队列槽位，第三个（含规则变更）确定入不了队 => 背压。
        p.getBuffer().setQueueCapacity(1);
        p.getBuffer().setOfferTimeout(Duration.ZERO);
        try (TestHarness.Engine h = new TestHarness.Engine(p, List.of(rule(10)), 0L)) {
            log.info("[CASE] worker 被占住且队列已满：变更被背压（确定未生效），腾空后同键重试成功");
            assertTrue(h.engine.offerWorkerBlockForTesting(2_000), "阻塞任务应能入空队列");
            Thread.sleep(100); // 确保 worker 已取出阻塞任务并在 sleep
            // worker 被占住期间把队列填满（容量 2）；阻塞任务不会被 worker 取走。
            assertTrue(h.engine.offerWorkerBlockForTesting(2_000), "第二个任务入队");
            assertFalse(h.engine.offerWorkerBlockForTesting(2_000), "第三个任务必须背压，队列已满");

            RuleChangeRequest req = TestHarness.changeReq(
                    "bp-1", "r", RuleChange.Action.UPSERT, rule(100), null);
            assertThrows(IngestionEngine.RuleChangeBackpressureException.class,
                    () -> h.engine.submitRuleChange(req));
            assertTrue(h.ruleRegistry.history().stream().noneMatch(c -> "bp-1".equals(c.requestId())),
                    "被背压的请求确定未生效，不得留下版本");
            log.info("[STEP] 背压已观察到，bp-1 未产生版本");

            // worker 完成阻塞任务、队列排空后，用同一 requestId 重试：确定成功且只有一个版本
            Thread.sleep(2_100);
            RuleChangeOutcome ok = h.submitChange(log, req);
            assertFalse(ok.replayed());
            assertEquals(1L, ok.change().version());
            RuleChangeOutcome retry = h.submitChange(log, req);
            assertTrue(retry.replayed());
            assertEquals(ok.change(), retry.change());
            assertEquals(1L, h.ruleRegistry.history().stream()
                    .filter(c -> "bp-1".equals(c.requestId())).count());
            log.info("[ASSERT] 背压后同键重试成功且幂等：version=1");
            h.dumpState(log);
        }
    }

    @Test
    void timeoutOutcomeIsAmbiguousButSameKeyRetryIsConclusive() throws Exception {
        TelemetryProperties p = props();
        // resultTimeout=50ms：让变更排在一个 800ms 阻塞任务之后，调用方必然先超时
        // （回执丢失、结果未知），但任务随后确实生效。同键重试必须重放同一版本。
        p.getBuffer().setResultTimeout(Duration.ofMillis(50));
        p.getBuffer().setQueueCapacity(3);
        try (TestHarness.Engine h = new TestHarness.Engine(p, List.of(rule(10)), 0L)) {
            log.info("[CASE] 回执超时但变更稍后真实生效：同键重试只重放，绝不产生第二版本");
            windowValue(h, 0, 20);
            assertTrue(h.engine.offerWorkerBlockForTesting(800));
            RuleChangeRequest req = TestHarness.changeReq(
                    "to-1", "r", RuleChange.Action.UPSERT, rule(100), null);
            assertTrue(h.engine.offerRuleChangeForTesting(req),
                    "变更已入队（排在阻塞任务之后），但调用方拿不到回执");

            // 调用方视角：用带超时的提交会得到“结论不确定”
            assertThrows(IngestionEngine.RuleChangeTimeoutException.class,
                    () -> h.engine.submitRuleChange(
                            TestHarness.changeReq("to-other", "r",
                                    RuleChange.Action.UPSERT, rule(50), null)),
                    "此刻队列拥塞，新提交等待超时 => 结论不确定");

            // 等阻塞结束，原 to-1 已在 worker 内生效；同键重试必须重放同一条记录
            Thread.sleep(900);
            RuleChangeOutcome conclusive = h.submitChange(log, req);
            assertTrue(conclusive.replayed(), "原任务已生效，重试只能是幂等重放");
            assertEquals(1L, conclusive.change().version());
            assertEquals(11L, conclusive.change().effectiveFromMillis());
            long toVersions = h.ruleRegistry.history().stream()
                    .filter(c -> "to-1".equals(c.requestId())).count();
            assertEquals(1L, toVersions, "超时 + 重试后只能有一个 to-1 版本");

            RuleChangeOutcome again = h.submitChange(log, req);
            assertEquals(conclusive.change(), again.change());
            log.info("[ASSERT] 超时场景下同键重试收敛到唯一版本，边界仍为首次生效时的水位线");
            h.dumpState(log);
        }
    }

    @Test
    void sameKeyDifferentPayloadAndInvalidChangesAreRejectedWithoutSideEffect() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(rule(10)), 0L)) {
            log.info("[CASE] 同键不同载荷=冲突拒绝；未知规则/缺定义=非法快速拒绝，均无版本无状态改动");
            RuleChangeRequest a = TestHarness.changeReq(
                    "dup-key", "r", RuleChange.Action.UPSERT, rule(100), null);
            h.change(log, a);

            RuleChangeRequest different = TestHarness.changeReq(
                    "dup-key", "r", RuleChange.Action.UPSERT, rule(200), null);
            assertThrows(IngestionEngine.RuleChangeInvalidException.class,
                    () -> h.engine.submitRuleChange(different));
            log.info("[STEP] 同键不同载荷被拒绝，未产生第二版本");

            assertThrows(IngestionEngine.RuleChangeInvalidException.class,
                    () -> h.engine.submitRuleChange(TestHarness.changeReq(
                            "x1", "ghost", RuleChange.Action.DISABLE, null, null)));
            assertThrows(IngestionEngine.RuleChangeInvalidException.class,
                    () -> h.engine.submitRuleChange(TestHarness.changeReq(
                            "x2", "r", RuleChange.Action.UPSERT, null, null)));
            assertThrows(IngestionEngine.RuleChangeInvalidException.class,
                    () -> h.engine.submitRuleChange(TestHarness.changeReq(
                            "x3", "r", RuleChange.Action.MUTE, null, -5L)));

            // 唯一版本仍是第一次合法变更，规则当前阈值 100
            assertEquals(1, h.ruleRegistry.history().size());
            assertEquals(100.0, h.ruleRegistry.currentRules().get(0).rule().threshold());
            // 原键的一致重试仍返回首次记录
            RuleChangeOutcome replay = h.submitChange(log, a);
            assertTrue(replay.replayed());
            assertEquals(100.0, replay.change().ruleSnapshot().threshold());
            log.info("[ASSERT] 冲突/非法变更零版本零状态改动，合法键重放正常");
            h.dumpState(log);
        }
    }
}
