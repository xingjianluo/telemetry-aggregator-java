package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.alert.AlertRule;
import com.github.xingjianluo.telemetryaggregatorjava.config.TelemetryProperties;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestResult;
import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowAggregate;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重复投递、失败重试、标识冲突：结论确定、可区分，且永不重复计数。
 */
class DedupRetryTest {

    private static final Logger log = LoggerFactory.getLogger(DedupRetryTest.class);

    private TelemetryProperties props() {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(5);
        return p;
    }

    @Test
    void repeatedDeliveryNeverDoubleCountsAcrossArbitraryRetries() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(), 0L)) {
            log.info("[CASE] 同一事件任意次数重试：只计一次，每次结论一致");
            var e = TestHarness.event("dup-1", 5, "t", "s1", null, "svc", "m", 42, Map.of());
            for (int i = 1; i <= 5; i++) {
                IngestResult r = h.send(log, e);
                assertTrue(r.accepted(), "第 " + i + " 次投递应返回首次 ACCEPTED 结论");
                assertEquals(0L, r.windowStartMillis());
            }
            h.send(log, TestHarness.event("seal", 40, "t", "s9", null, "svc", "m", 0, Map.of()));
            WindowAggregate agg = h.windowStore.query("svc", "m").stream()
                    .filter(a -> a.windowStartMillis() == 0).findFirst().orElseThrow();
            log.info("[ASSERT] 重试5次后窗口[0,10) count={} distinctEventCount={}（预期均为1）",
                    agg.count(), agg.distinctEventCount());
            assertEquals(1, agg.count());
            assertEquals(1, agg.distinctEventCount());
            // dup-1 首次 + seal 事件 = 2 个真正进入管道的事件；4 次重放不计数
            log.info("[ASSERT] metrics.accepted={} duplicateReplays={}（预期 accepted=2, replays=4）",
                    h.metrics.acceptedCount(), h.metrics.duplicateReplayCount());
            assertEquals(2, h.metrics.acceptedCount());
            assertEquals(4, h.metrics.duplicateReplayCount());
            h.dumpState(log);
        }
    }

    @Test
    void sameEventIdWithDifferentPayloadIsMarkedConflicting() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(), 0L)) {
            log.info("[CASE] 相同 eventId 但载荷不一致 => CONFLICTING_EVENT_ID（区别于重复投递）");
            h.send(log, TestHarness.event("id-x", 5, "t", "s1", null, "svc", "m", 1, Map.of()));
            // value 不同 => 载荷冲突
            IngestResult conflict = h.send(log,
                    TestHarness.event("id-x", 5, "t", "s1", null, "svc", "m", 999, Map.of()));
            assertEquals(RejectReason.CONFLICTING_EVENT_ID, conflict.rejectReason());
            log.info("[ASSERT] 冲突拒绝原因={} 依据={}", conflict.rejectReason(), conflict.detail());

            // 之后无论按哪个版本重试，结论保持稳定：
            IngestResult retryOriginal = h.send(log,
                    TestHarness.event("id-x", 5, "t", "s1", null, "svc", "m", 1, Map.of()));
            assertTrue(retryOriginal.accepted(), "按首次载荷重放仍返回首次 ACCEPTED 结论");
            IngestResult retryConflict = h.send(log,
                    TestHarness.event("id-x", 5, "t", "s1", null, "svc", "m", 999, Map.of()));
            assertEquals(RejectReason.CONFLICTING_EVENT_ID, retryConflict.rejectReason());

            h.send(log, TestHarness.event("seal", 40, "t", "s9", null, "svc", "m", 0, Map.of()));
            WindowAggregate agg = h.windowStore.query("svc", "m").stream()
                    .filter(a -> a.windowStartMillis() == 0).findFirst().orElseThrow();
            log.info("[ASSERT] 冲突载荷未被计入：count={}（预期1，仅首次）sum={}", agg.count(), agg.sum());
            assertEquals(1, agg.count());
            assertEquals(1.0, agg.sum(), 1e-9);
            h.dumpState(log);
        }
    }

    @Test
    void deterministicRejectionIsIdempotentOnRetry() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(), 0L)) {
            log.info("[CASE] 确定性拒绝（迟到）在重试时返回同一结论，且不会因水位线变化而改变");
            h.send(log, TestHarness.event("adv", 100, "t", "s0", null, "svc", "m", 1, Map.of()));
            var late = TestHarness.event("late-1", 80, "t", "s1", null, "svc", "m", 7, Map.of());
            IngestResult first = h.send(log, late);
            IngestResult second = h.send(log, late);
            assertEquals(RejectReason.LATE_BEHIND_WATERMARK, first.rejectReason());
            assertEquals(RejectReason.LATE_BEHIND_WATERMARK, second.rejectReason());
            log.info("[ASSERT] 两次迟到重放结论一致：{} / {}", first.rejectReason(), second.rejectReason());
            assertEquals(1, h.metrics.acceptedCount());
            h.dumpState(log);
        }
    }
}
