package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertSnapshot;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import com.github.xingjianluo.telemetryaggregatorjava.replay.JournaledEvent;
import com.github.xingjianluo.telemetryaggregatorjava.replay.JournaledRuleChange;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeRequest;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleChangeResult;
import com.github.xingjianluo.telemetryaggregatorjava.rule.RuleVersion;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 规则变更与并发摄取同时发生：
 * 事件计数精确无丢失/重复；规则版本严格连续；事件与变更在日志里严格线性化、
 * 互不覆盖；所有定稿窗口都只使用当时生效的规则版本。
 */
class RuleChangeConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(RuleChangeConcurrencyTest.class);

    @Test
    void concurrentIngestionAndRuleChangesAreLinearizedSafely() throws Exception {
        TelemetryProperties props = TestHarness.defaultProps();
        props.getWindow().setSizeMillis(10);
        props.getWindow().setAllowedLatenessMillis(0);
        props.getAlert().setBreachStreak(1);
        props.getAlert().setRecoverStreak(1);
        props.getBuffer().setOfferTimeout(Duration.ofSeconds(2));
        props.getBuffer().setResultTimeout(Duration.ofSeconds(5));

        var seed = TestHarness.seed("r1", AlertRule.Scope.TAGS, "svc", "m",
                1_000_000.0, AlertRule.Operator.GT); // 初始阈值极高，默认不报警
        try (TestHarness.Engine h = new TestHarness.Engine(
                props, List.of(seed), 0L, 100_000, 100_000)) {

            int ingestThreads = 8;
            int perThread = 300;
            int totalEvents = ingestThreads * perThread;

            ExecutorService pool = Executors.newFixedThreadPool(ingestThreads + 1);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger accepted = new AtomicInteger();
            Map<String, RejectReason> rejects = new ConcurrentHashMap<>();

            // 8 个摄取线程：事件时间在 [1, 3000]，事件时间越晚越会关闭早期窗口。
            for (int t = 0; t < ingestThreads; t++) {
                final int base = t * perThread;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            int idx = base + i;
                            // 每条事件事件时间 1..3000，用 idx 打散，保证唯一
                            long et = 1L + ((idx * 7L) % 3000L);
                            double value = 50 + (idx % 100); // 50..149
                            var e = TestHarness.event("ev-" + idx, et,
                                    "tr-" + idx, "s" + idx, null, "svc", "m", value, Map.of());
                            IngestResult r = h.engine.ingest(e);
                            if (r.accepted()) {
                                accepted.incrementAndGet();
                            } else {
                                rejects.put(e.eventId(), r.rejectReason());
                            }
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            // 1 个规则变更线程：持续在不同阈值间切换（都 > 149 或 <= 149 两种），
            // 每次都带独立 requestId。
            List<RuleChangeResult> changeResults = new ArrayList<>();
            AtomicInteger changesDone = new AtomicInteger();
            pool.submit(() -> {
                try {
                    start.await();
                    double[] thresholds = {90, 1_000_000, 80, 1_000_000, 100};                    for (int i = 0; i < thresholds.length; i++) {
                        RuleChangeResult r = h.engine.changeRule(new RuleChangeRequest(
                                "chg-" + i, "r1", null, null, null, null, null,
                                thresholds[i], null, null, null, "并发调阈值 " + thresholds[i]));
                        if (r.applied()) {
                            changesDone.incrementAndGet();
                        }
                        synchronized (changeResults) {
                            changeResults.add(r);
                        }
                        Thread.yield();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    // 不允许变更线程异常中断
                    throw e;
                }
            });

            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "并发任务应在超时前结束");

            // 1) 事件计数精确：全部事件要么接受、要么可区分拒绝；不允许出现处理超时丢失。
            int accounted = accepted.get() + rejects.size();
            assertEquals(totalEvents, accounted,
                    "所有事件必须有明确结论（接受或可区分拒绝），不能丢失");
            assertFalse(rejects.containsValue(RejectReason.PROCESSING_TIMEOUT),
                    "不应出现处理超时");
            log.info("[ASSERT] 事件 {} 条全部有结论：accepted={} rejected={} byReason={}",
                    totalEvents, accepted.get(), rejects.size(), rejects.values());

            // 2) 版本严格连续（v2..v(n+1)），生效边界单调不减。
            List<RuleVersion> history = h.ruleStore.historyOf("r1");
            assertEquals(changesDone.get() + 1, history.size(), "每次成功变更恰好产生一个版本");
            for (int i = 1; i < history.size(); i++) {
                assertEquals(history.get(i - 1).version() + 1, history.get(i).version());
                assertTrue(history.get(i).effectiveFromMillis()
                                >= history.get(i - 1).effectiveFromMillis(),
                        "生效边界必须单调不减");
            }
            log.info("[ASSERT] 规则版本连续 {} 个，边界单调: {}", history.size(),
                    history.stream().map(v -> "v" + v.version() + "@" + v.effectiveFromMillis())
                            .toList());

            // 3) 日志：事件数=接受数；seq 严格递增；变更与事件的 seq 互不重复且交错有序。
            List<JournaledEvent> je = h.journal.events();
            List<JournaledRuleChange> jc = h.journal.ruleChanges();
            assertEquals(accepted.get(), je.size(), "每条被接受事件恰好入日志一次，无重复计数");
            long prev = -1;
            for (JournaledEvent e : je) {
                assertTrue(e.seq() > prev, "事件 seq 必须严格递增");
                prev = e.seq();
            }
            List<Long> allSeqs = new ArrayList<>();
            je.forEach(e -> allSeqs.add(e.seq()));
            jc.forEach(c -> allSeqs.add(c.seq()));
            assertEquals(allSeqs.size(), allSeqs.stream().distinct().count(),
                    "事件 seq 与变更 seq 不得重复（无状态覆盖）");
            log.info("[ASSERT] 日志线性化：events={} changes={}，全部 seq 唯一", je.size(), jc.size());

            // 4) 每个已产生的告警快照使用的规则版本，其生效边界必须严格早于被评估窗口的结束。
            for (AlertSnapshot s : h.alertEngine.snapshots()) {
                RuleVersion v = h.ruleStore.historyOf(s.ruleId()).stream()
                        .filter(x -> x.version() == s.ruleVersion()).findFirst().orElseThrow();
                assertTrue(v.effectiveFromMillis() < s.lastEvaluatedWindowStart()
                                + props.getWindow().getSizeMillis(),
                        "快照所用版本的生效边界必须早于窗口结束");
            }

            // 5) 跨视图一致：投一条高水位线 flush 事件关闭全部窗口，然后
            //    metric=m 的定稿聚合 count 之和必须精确等于此前接受的事件数（每条恰好计入一次）。
            long beforeFlush = accepted.get();
            IngestResult flush = h.engine.ingest(TestHarness.event(
                    "flush", 10_000, "tr-flush", "sf", null, "svc", "flush-metric", 1, Map.of()));
            assertTrue(flush.accepted());
            List<WindowAggregate> aggs = h.windowStore.query("svc", "m");
            long aggCount = aggs.stream().mapToLong(WindowAggregate::count).sum();
            assertEquals(beforeFlush, aggCount,
                    "跨视图一致：metric=m 的定稿聚合计数之和必须精确等于接受事件数");
            log.info("[ASSERT] 跨视图计数一致：sealedAggCount={} == accepted={}", aggCount, beforeFlush);
            h.dumpState(log);
        }
    }
}
