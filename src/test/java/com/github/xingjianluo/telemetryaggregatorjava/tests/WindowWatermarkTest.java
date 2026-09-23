package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.core.WatermarkManager;
import com.github.xingjianluo.telemetryaggregatorjava.core.WindowTable;
import com.github.xingjianluo.telemetryaggregatorjava.model.IngestStatus;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import com.github.xingjianluo.telemetryaggregatorjava.model.WindowResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 窗口归属与水位线规则单测：floor 对齐、单调推进、迟到拒绝、到点关闭。
 */
class WindowWatermarkTest {

    private static final Logger log = Logger.getLogger(WindowWatermarkTest.class.getName());

    private final WatermarkManager wm = new WatermarkManager(500);
    private final WindowTable table = new WindowTable(1000, 100, 100, 10, 50);

    @Test
    void windowAssignmentIsFloorAligned() {
        // 输入：事件时间 100/999/1000/1001 -> 窗口起点 0/0/1000/1000
        long[] times = {100, 999, 1000, 1001};
        long[] expected = {0, 0, 1000, 1000};
        for (int i = 0; i < times.length; i++) {
            long start = table.windowStartOf(times[i]);
            log.info("[输入] eventTime=" + times[i] + " -> [判定] windowStart=" + start);
            assertEquals(expected[i], start);
        }
        // floorDiv 对负时间也是确定的（向负无穷取整）
        assertEquals(-1000, table.windowStartOf(-1));
    }

    @Test
    void lateEventsRejectedAndWindowsCloseAtWatermark() {
        long w0 = wm.currentWatermark();
        log.info("[输入] 初始水位线=" + (w0 == Long.MIN_VALUE ? "<未初始化>" : w0));

        accept(ev("e1", 200, 10)); // 将 maxEventTime 推到 200
        long w1 = wm.observe(200);
        log.info("[判定] 观察 eventTime=200 后 watermark=" + w1 + " (=200-500，未超过0)");
        // watermark 尚未越过任何窗口边界，无窗口关闭
        assertTrue(table.closeUpTo(w1).isEmpty());

        accept(ev("e2", 1500, 30));
        long w2 = wm.observe(1500);
        log.info("[判定] 观察 eventTime=1500 后 watermark=" + w2);
        assertEquals(1000, w2);
        // [0,1000) 的 end=1000 <= watermark=1000 -> 关闭（边界含等号，确定可复现）
        List<WindowResult> closed = table.closeUpTo(w2);
        assertEquals(1, closed.size());
        WindowResult r = closed.get(0);
        log.info("[判定] 窗口 [" + r.getWindowStart() + "," + r.getWindowEnd()
                + ") 已关闭, closedAtWatermark=" + r.getClosedAtWatermark()
                + ", series=" + r.getSeries().size());
        assertEquals(0, r.getWindowStart());

        // 迟到边界：eventTime 1000/999 相对水位线 1000
        assertFalse(wm.isLate(1000), "eventTime == watermark 必须接受");
        assertTrue(wm.isLate(999), "eventTime < watermark 必须判迟到");
        log.info("[输入] 迟到事件 eventTime=999 -> [判定] isLate="
                + wm.isLate(999) + "（窗口已关闭，拒绝原因 LATE_BEHIND_WATERMARK）");
    }

    private void accept(TelemetryEvent e) {
        WindowTable.ApplyOutcome o = table.applyEvent(e, wm.currentWatermark());
        assertEquals(IngestStatus.ACCEPTED, o.getStatus(),
                () -> "unexpected: " + o.getReason());
    }

    private TelemetryEvent ev(String id, long ts, double v) {
        return EngineTestSupport.event(id, ts, "cpu", v, Map.of("host", "a"));
    }
}
