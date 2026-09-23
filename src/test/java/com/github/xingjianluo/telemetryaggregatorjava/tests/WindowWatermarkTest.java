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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 窗口归属、水位线推进、乱序与迟到事件的确定性行为。
 */
class WindowWatermarkTest {

    private static final Logger log = LoggerFactory.getLogger(WindowWatermarkTest.class);

    private TelemetryProperties props() {
        TelemetryProperties p = TestHarness.defaultProps();
        p.getWindow().setSizeMillis(10);
        p.getWindow().setAllowedLatenessMillis(5);
        return p;
    }

    @Test
    void outOfOrderEventsWithinLatenessAreAggregatedInSameWindow() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(),
                List.of(new AlertRule("r", AlertRule.Scope.TAGS, null, null,
                        Map.of(), 100, AlertRule.Operator.GT)), 0L)) {

            log.info("[CASE] 乱序但在容忍期内：事件按事件时间归属同一窗口");
            // 先到一个较晚的事件推进水位线，再到较早事件（乱序但未越界）
            h.send(log, TestHarness.event("e-late-arrive", 18, "t", "s2", null,
                    "svc", "m", 3, Map.of()));
            // 此时 maxEventTime=18, wm=13；窗口[10,20)未关闭，t=12 可进入
            IngestResult early = h.send(log, TestHarness.event("e-early-arrive", 12, "t", "s1", null,
                    "svc", "m", 7, Map.of()));
            assertTrue(early.accepted());

            // 推进时间到 25：wm=20，窗口[10,20)关闭定稿
            h.send(log, TestHarness.event("e-seal", 25, "t", "s3", null,
                    "svc", "m", 0, Map.of()));

            List<WindowAggregate> agg = h.windowStore.query("svc", "m");
            WindowAggregate first = agg.stream()
                    .filter(a -> a.windowStartMillis() == 10).findFirst().orElseThrow();
            log.info("[ASSERT] 窗口[10,20) count={} sum={} avg={}（预期 count=2 sum=10）",
                    first.count(), first.sum(), first.avg());
            assertEquals(2, first.count());
            assertEquals(10.0, first.sum(), 1e-9);
            h.dumpState(log);
        }
    }

    @Test
    void eventsBeyondWatermarkBoundAreRejectedWithDistinguishableReason() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(),
                List.of(), 0L)) {

            log.info("[CASE] 超出水位线迟到边界 => LATE_BEHIND_WATERMARK");
            // maxEventTime=100 => wm=95；迟到边界=90
            h.send(log, TestHarness.event("e1", 100, "t", "s1", null, "svc", "m", 1, Map.of()));
            IngestResult r1 = h.send(log, TestHarness.event("e2", 89, "t", "s2", null,
                    "svc", "m", 1, Map.of()));
            assertEquals(RejectReason.LATE_BEHIND_WATERMARK, r1.rejectReason());
            assertNotNull(r1.detail());
            log.info("[ASSERT] e2 拒绝原因={} 依据={}", r1.rejectReason(), r1.detail());

            log.info("[CASE] 窗口已关闭但仍在容忍期内：同样明确拒绝，不静默计入");
            // maxEventTime=100 wm=95：窗口[90,100) end=100 尚未 seal(100<=95 false)。
            // 推进到 106 => wm=101：窗口[90,100) 已 seal；t=96 在容忍边界 96? wm-L=96，t=96 不 <96
            h.send(log, TestHarness.event("e3", 106, "t", "s3", null, "svc", "m", 1, Map.of()));
            IngestResult r2 = h.send(log, TestHarness.event("e4", 97, "t", "s4", null,
                    "svc", "m", 1, Map.of()));
            // wm=101, bound=96；t=97>=96（不是越界迟到），但窗口[90,100)已关闭 => 明确拒绝
            assertEquals(RejectReason.LATE_BEHIND_WATERMARK, r2.rejectReason());
            assertTrue(r2.detail().contains("定稿关闭"), "detail 应说明窗口已关闭: " + r2.detail());
            log.info("[ASSERT] e4 拒绝原因={} 依据={}", r2.rejectReason(), r2.detail());

            // 被拒绝事件绝不能出现在聚合中
            long accepted = h.metrics.acceptedCount();
            log.info("[ASSERT] 总接受数={}（预期 2：e1,e3）", accepted);
            assertEquals(2, accepted);
            h.dumpState(log);
        }
    }

    @Test
    void windowAssignmentIsFloorOfEventTime() {
        try (TestHarness.Engine h = new TestHarness.Engine(props(), List.of(), 0L)) {
            log.info("[CASE] 窗口归属 = floor(eventTime/10)*10，边界点归属右闭窗口");
            h.send(log, TestHarness.event("a", 9, "t", "1", null, "svc", "m", 1, Map.of()));
            h.send(log, TestHarness.event("b", 10, "t", "2", null, "svc", "m", 2, Map.of()));
            h.send(log, TestHarness.event("c", 19, "t", "3", null, "svc", "m", 4, Map.of()));
            // 关闭前两个窗口
            h.send(log, TestHarness.event("seal", 40, "t", "4", null, "svc", "m", 0, Map.of()));

            Map<Long, WindowAggregate> byStart = new java.util.HashMap<>();
            h.windowStore.query("svc", "m").forEach(a -> byStart.put(a.windowStartMillis(), a));
            assertEquals(1, byStart.get(0L).count());
            assertEquals(2, byStart.get(10L).count());
            log.info("[ASSERT] window0.count=1, window10.count=2（t=10 属于[10,20)）");
            h.dumpState(log);
        }
    }
}
