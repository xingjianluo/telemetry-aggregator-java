package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发摄取：计数不丢失/不重复；缓冲与标签基数接近上限时按确定策略拒绝且可观测。
 */
class ConcurrencyBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyBackpressureTest.class);

    @Test
    void concurrentIngestionProducesExactCountWithNoLossOrDoubleCount() throws Exception {
        TelemetryProperties props = TestHarness.defaultProps();
        props.getWindow().setSizeMillis(10_000);
        props.getWindow().setAllowedLatenessMillis(0);
        props.getBuffer().setOfferTimeout(Duration.ofSeconds(2));
        props.getBuffer().setResultTimeout(Duration.ofSeconds(5));

        try (TestHarness.Engine h = new TestHarness.Engine(props, List.of(), 0L)) {
            int threads = 16;
            int perThread = 500;
            int total = threads * perThread;
            log.info("[CASE] {} 线程 × {} = {} 个唯一事件并发写入同一序列", threads, perThread, total);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger accepted = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();
            Set<String> failures = ConcurrentHashMap.newKeySet();

            for (int t = 0; t < threads; t++) {
                final int base = t * perThread;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            String id = "ev-" + (base + i);
                            var e = TestHarness.event(id, 5, "trace-" + id, "span-" + id, null,
                                    "svc", "m", 1, Map.of());
                            IngestResult r = h.engine.ingest(e);
                            if (r.accepted()) {
                                accepted.incrementAndGet();
                            } else {
                                rejected.incrementAndGet();
                                failures.add(r.rejectReason() + ":" + r.detail());
                            }
                        }
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

            log.info("[ASSERT] accepted={} rejected={}（预期 accepted={} rejected=0）",
                    accepted.get(), rejected.get(), total);
            assertEquals(total, accepted.get());
            assertEquals(0, rejected.get(), "失败原因: " + failures);

            // 关闭窗口定稿后，序列总计数必须精确等于唯一事件数
            h.engine.ingest(TestHarness.event("seal", 30_000, "trace-seal", "seal-span", null,
                    "svc", "m", 0, Map.of()));
            List<WindowAggregate> agg = h.windowStore.query("svc", "m");
            long count = agg.stream().filter(a -> a.windowStartMillis() == 0)
                    .mapToLong(WindowAggregate::count).sum();
            long distinct = agg.stream().filter(a -> a.windowStartMillis() == 0)
                    .mapToLong(WindowAggregate::distinctEventCount).sum();
            log.info("[ASSERT] 定稿聚合 count={} distinct={}（均预期 {}）", count, distinct, total);
            assertEquals(total, count);
            assertEquals(total, distinct);
            h.dumpState(log);
        }
    }

    @Test
    void fullBufferRejectsFastWithObservableReason() throws Exception {
        TelemetryProperties props = TestHarness.defaultProps();
        props.getBuffer().setQueueCapacity(8);
        props.getBuffer().setOfferTimeout(Duration.ZERO);
        props.getBuffer().setResultTimeout(Duration.ofSeconds(5));
        props.getWindow().setSizeMillis(10);

        try (TestHarness.Engine h = new TestHarness.Engine(props, List.of(), 0L)) {
            log.info("[CASE] 队列容量=4，从多线程突发提交，超出部分应快速 BUFFER_FULL");
            int n = 2000;
            ExecutorService pool = Executors.newFixedThreadPool(16);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger accepted = new AtomicInteger();
            AtomicInteger bufferFull = new AtomicInteger();
            for (int i = 0; i < n; i++) {
                final String id = "burst-" + i;
                pool.submit(() -> {
                    try {
                        start.await();
                        IngestResult r = h.engine.ingest(TestHarness.event(id, 1, "tr-" + id, id, null,
                                "svc", "m", 1, Map.of()));
                        if (r.accepted()) {
                            accepted.incrementAndGet();
                        } else if (r.rejectReason() == RejectReason.BUFFER_FULL) {
                            bufferFull.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

            log.info("[ASSERT] accepted={} bufferFull={}（无计数丢失：二者之和={}）",
                    accepted.get(), bufferFull.get(), accepted.get() + bufferFull.get());
            assertEquals(n, accepted.get() + bufferFull.get());
            assertTrue(bufferFull.get() > 0, "小队列下必须观察到背压拒绝");
            assertEquals(bufferFull.get(),
                    h.metrics.rejectedCounts().get(RejectReason.BUFFER_FULL).longValue());
            h.dumpState(log);
        }
    }

    @Test
    void seriesCardinalityIsBoundedAndRejectionsAreCounted() {
        TelemetryProperties props = TestHarness.defaultProps();
        props.getWindow().setSizeMillis(10_000);
        props.getCardinality().setMaxSeriesPerWindow(10);

        try (TestHarness.Engine h = new TestHarness.Engine(props, List.of(), 0L)) {
            log.info("[CASE] 单窗口标签序列上限=10，写入 25 个不同标签");
            AtomicInteger accepted = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();
            for (int i = 0; i < 25; i++) {
                IngestResult r = h.engine.ingest(TestHarness.event(
                        "card-" + i, 5, "t", "s" + i, null, "svc", "m", 1,
                        Map.of("host", "h" + i)));
                if (r.accepted()) {
                    accepted.incrementAndGet();
                } else {
                    rejected.incrementAndGet();
                    assertEquals(RejectReason.TOO_MANY_SERIES, r.rejectReason());
                }
            }
            log.info("[ASSERT] accepted={} rejected={} liveSeries={}（预期 10/15/10）",
                    accepted.get(), rejected.get(), h.windowStore.liveSeriesCount());
            assertEquals(10, accepted.get());
            assertEquals(15, rejected.get());
            assertEquals(10, h.windowStore.liveSeriesCount());
            assertEquals(15,
                    h.metrics.rejectedCounts().get(RejectReason.TOO_MANY_SERIES).longValue());
            h.dumpState(log);
        }
    }

    @Test
    void tagShapeAndEventIdPoolAreBounded() {
        TelemetryProperties props = TestHarness.defaultProps();
        props.getWindow().setSizeMillis(10_000);
        props.getCardinality().setMaxTagsPerEvent(2);
        props.getCardinality().setMaxTagValueLength(5);
        props.getCardinality().setMaxEventIds(3);
        props.getCardinality().setMaxTraces(100);
        props.getCardinality().setMaxSpansPerTrace(100);

        try (TestHarness.Engine h = new TestHarness.Engine(props, List.of(), 0L)) {
            log.info("[CASE] 标签数量/长度与事件 ID 池的有界拒绝");
            IngestResult tooManyTags = h.engine.ingest(TestHarness.event("a", 5, "t", "1", null,
                    "svc", "m", 1, Map.of("k1", "v1", "k2", "v2", "k3", "v3")));
            assertEquals(RejectReason.TAG_CARDINALITY_LIMIT, tooManyTags.rejectReason());
            log.info("[ASSERT] 标签数超限 => {}", tooManyTags.rejectReason());

            IngestResult tooLong = h.engine.ingest(TestHarness.event("b", 5, "t", "2", null,
                    "svc", "m", 1, Map.of("k", "toolong")));
            assertEquals(RejectReason.TAG_CARDINALITY_LIMIT, tooLong.rejectReason());
            log.info("[ASSERT] 标签值过长 => {}", tooLong.rejectReason());

            // ID 池容量为 3：前 3 个唯一合法事件占满，第 4 个被明确拒绝
            for (int i = 0; i < 3; i++) {
                assertTrue(h.engine.ingest(TestHarness.event("id-" + i, 5, "t", "x" + i, null,
                        "svc", "m", 1, Map.of())).accepted());
            }
            IngestResult poolFull = h.engine.ingest(TestHarness.event("id-3", 5, "t", "x3", null,
                    "svc", "m", 1, Map.of()));
            assertEquals(RejectReason.TOO_MANY_EVENT_IDS, poolFull.rejectReason());
            log.info("[ASSERT] ID 池满 => {} 依据={}", poolFull.rejectReason(), poolFull.detail());
            h.dumpState(log);
        }
    }
}
