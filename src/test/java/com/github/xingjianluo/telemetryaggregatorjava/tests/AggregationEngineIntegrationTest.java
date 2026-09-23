package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.core.AggregationEngine;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestStatus;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 聚合引擎集成测试：去重/冲突、迟到拒绝、并发摄取一致性、背压、标签基数保护。
 */
class AggregationEngineIntegrationTest {

    private static final Logger log = Logger.getLogger(
            AggregationEngineIntegrationTest.class.getName());

    private AggregationEngine engine;

    private AggregationEngine newEngine(TelemetryProperties p) {
        engine = new AggregationEngine(p);
        engine.start();
        return engine;
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    private IngestResult send(TelemetryEvent e) {
        try {
            return engine.ingest(e).get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    void duplicateRetriesNeverDoubleCountedAndConflictRejected() {
        newEngine(EngineTestSupport.props(1000, 500, 100, 100, 100, 100, 10, 100));

        TelemetryEvent e = EngineTestSupport.event("dup-1", 100, "cpu", 5,
                Map.of("host", "a"));
        IngestResult first = send(e);
        IngestResult retry1 = send(e);
        TelemetryEvent sameIdDifferentBody = EngineTestSupport.event("dup-1", 100,
                "cpu", 6, Map.of("host", "a")); // value 不同
        IngestResult conflict = send(sameIdDifferentBody);
        IngestResult retry2 = send(e);

        log.info("[输入] 首次=" + first.getStatus() + ", 重试1=" + retry1.getStatus()
                + ", 冲突=" + conflict.getStatus() + "("
                + conflict.getReason() + ")" + ", 重试2=" + retry2.getStatus());

        assertEquals(IngestStatus.ACCEPTED, first.getStatus());
        assertEquals(IngestStatus.ACCEPTED, retry1.getStatus(),
                "重复投递回放到首次的已接受结论（不重复计数）");
        assertEquals(IngestStatus.ACCEPTED, retry2.getStatus());
        assertEquals(IngestStatus.ID_CONFLICT, conflict.getStatus(),
                "相同 id 不同负载必须以 ID_CONFLICT 拒绝");
        assertTrue(first == retry1 || first.getStatus().equals(retry1.getStatus()));

        engine.advanceWatermarkTo(2000);
        engine.awaitDrained(5000);
        List<WindowResult> windows = engine.queryWindows(null, null);
        long totalCount = windows.stream()
                .flatMap(w -> w.getSeries().stream())
                .mapToLong(s -> s.getCount()).sum();
        log.info("[判定] 关闭窗口数=" + windows.size() + ", 窗口内总计数=" + totalCount
                + "（同 id 投递4次只计1次）");
        assertEquals(1, totalCount, "重复与冲突均不得造成重复计数");
        long replays = (long) engine.getStats().snapshot().get("dedupReplays");
        assertEquals(2, replays, "两次重试（其中一次为冲突，不计回放）应为2次幂等回放");
    }

    @Test
    void lateEventAfterWatermarkRejectedWithReason() {
        newEngine(EngineTestSupport.props(1000, 0, 100, 100, 100, 100, 10, 100));
        send(EngineTestSupport.event("late-1", 5000, "cpu", 1, Map.of("host", "a")));
        engine.awaitDrained(5000);
        // lateness=0：水位线=最大事件时间，旧事件必迟到
        IngestResult late = send(EngineTestSupport.event(
                "late-2", 100, "cpu", 1, Map.of("host", "a")));
        log.info("[输入] eventTime=100（水位线已达5000）-> [判定] "
                + late.getStatus() + " : " + late.getReason()
                + ", watermark=" + late.getWatermark());
        assertEquals(IngestStatus.LATE_BEHIND_WATERMARK, late.getStatus());
        assertTrue(late.getReason().contains("watermark"));
    }

    @Test
    void concurrentIngestionCountsExactlyOnce() throws Exception {
        int threads = 16;
        int perThread = 500;
        // 所有事件同一事件时间（同窗口），避免并发提交顺序交错造成的乱序迟到，
        // 本测试专注验证“同 id 并发去重恰好计一次”
        newEngine(EngineTestSupport.props(1000, 0, 100_000, 1_000_000,
                1_000_000, 1_000_000, 1000, 1_000_000));

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        // 每个 eventId 额外被两个线程重投，制造并发重复
        ConcurrentHashMap<String, Integer> uniqueIds = new ConcurrentHashMap<>();

        for (int t = 0; t < threads; t++) {
            final int base = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        String id = "ev-" + (base % 8) + "-" + i; // 8 个线程组共享 id
                        // 同 id 的负载必须逐字节一致（含时间戳与标签），否则会判 ID_CONFLICT
                        TelemetryEvent e = EngineTestSupport.event(id, 1500,
                                "cpu", 1, Map.of("host", "shared"));
                        engine.ingest(e);
                        uniqueIds.put(id, 1);
                    }
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(60, TimeUnit.SECONDS));
        pool.shutdown();

        engine.advanceWatermarkTo(100_000);
        engine.awaitDrained(30_000);

        long accepted = (long) ((Map<?, ?>) engine.getStats().snapshot()
                .get("byStatus")).get(IngestStatus.ACCEPTED.name());
        List<WindowResult> windows = engine.queryWindows(null, null);
        long counted = windows.stream().flatMap(w -> w.getSeries().stream())
                .mapToLong(s -> s.getCount()).sum();
        @SuppressWarnings("unchecked")
        Map<String, Long> allStatuses = (Map<String, Long>)
                engine.getStats().snapshot().get("byStatus");
        log.info("[输入] " + threads + " 线程 x " + perThread
                + " 次提交, 唯一 eventId=" + uniqueIds.size()
                + " -> [判定] ACCEPTED=" + accepted + ", 窗口内总计数=" + counted
                + ", 全状态=" + allStatuses + ", dedupReplays="
                + engine.getStats().snapshot().get("dedupReplays"));
        assertEquals(uniqueIds.size(), accepted,
                "并发重复投递下每个 eventId 必须恰好计一次; statuses=" + allStatuses);
        assertEquals(uniqueIds.size(), counted, "窗口内计数必须与 ACCEPTED 一致");
    }

    @Test
    void boundedQueueAppliesBackpressureWithoutBlocking() throws Exception {
        // 极小队列 + 工作线程被一个控制任务占住 -> 队列迅速填满 -> QUEUE_FULL 立即返回
        newEngine(EngineTestSupport.props(1000, 500, 4, 10_000, 100, 100, 5000, 100));

        CountDownLatch blockEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // 在独立线程提交一个会挂起工作线程的控制任务
        Thread blocker = new Thread(() -> {
            try {
                engine.runOnWorker(() -> {
                    blockEntered.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, 15_000L);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        blocker.start();
        assertTrue(blockEntered.await(5, TimeUnit.SECONDS), "阻塞控制任务应已进入工作线程");

        // 工作线程被占住，队列容量4，前4个入队，之后全部立即 QUEUE_FULL
        int queueFull = 0;
        int enqueued = 0;
        long t0 = System.nanoTime();
        for (int i = 0; i < 500; i++) {
            TelemetryEvent e = EngineTestSupport.event("q" + i, 1000L + i,
                    "cpu", i, Map.of("host", "a"));
            // 工作线程被挂起期间不阻塞等待结论：立即兑现的 future 只有拒绝类
            CompletableFuture<IngestResult> future = engine.ingest(e);
            if (!future.isDone()) {
                enqueued++;
                future.cancel(false); // 仅用于背压观察，取消本地等待，不影响入队任务
            } else if (!future.isCompletedExceptionally()) {
                IngestStatus st = future.getNow(null).getStatus();
                if (st == IngestStatus.QUEUE_FULL) {
                    queueFull++;
                }
            }
        }
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        release.countDown();
        blocker.join(5000);
        engine.awaitDrained(10_000);

        log.info("[输入] 工作线程被占住期间提交500（队列容量4）-> [判定] QUEUE_FULL="
                + queueFull + ", 已入队=" + enqueued + ", 提交耗时="
                + elapsedMs + "ms（写入线程不阻塞）");
        assertTrue(queueFull > 0, "队列满必须出现可观察的 QUEUE_FULL 拒绝");
        assertTrue(elapsedMs < 5000, "非阻塞背压：提交阶段不得长时间等待");
        long rejectedCount = (long) ((Map<?, ?>) engine.getStats().snapshot()
                .get("byStatus")).get(IngestStatus.QUEUE_FULL.name());
        assertEquals(queueFull, rejectedCount, "拒绝数必须在 stats 中可观察");
        log.info("[判定] QUEUE_FULL 计数=" + rejectedCount
                + "；被拒事件未计数，去重新占位已回滚，解除阻塞后入队事件正常处理");
    }

    @Test
    void tagCardinalityIsBoundedWithDistinctReason() {
        // 单 tag key 仅允许 3 个不同 value
        newEngine(EngineTestSupport.props(1000, 500, 100, 10_000, 3, 100, 10, 100));
        List<IngestResult> rs = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            rs.add(send(EngineTestSupport.event("card" + i, 100, "cpu", i,
                    Map.of("host", "h" + i))));
        }
        long accepted = rs.stream().filter(r -> r.getStatus() == IngestStatus.ACCEPTED).count();
        long rejected = rs.stream().filter(r -> r.getStatus() == IngestStatus.TAG_CARDINALITY).count();
        log.info("[输入] 同窗口6个不同 host（上限3）-> [判定] ACCEPTED=" + accepted
                + ", TAG_CARDINALITY=" + rejected
                + ", 拒绝原因示例=" + rs.get(3).getReason());
        assertEquals(3, accepted);
        assertEquals(3, rejected);
        assertFalse(rs.get(3).getReason().isBlank());
        // 被拒事件重试结果仍一致且不会补计数（同 id -> 回放到拒绝结论）
        IngestResult retry = send(EngineTestSupport.event("card3", 100, "cpu", 3,
                Map.of("host", "h3")));
        assertEquals(IngestStatus.TAG_CARDINALITY, retry.getStatus(),
                "被拒事件的重试必须得到一致的拒绝结论");
        log.info("[判定] 被拒 id 重放结论一致: " + retry.getStatus());
    }
}
