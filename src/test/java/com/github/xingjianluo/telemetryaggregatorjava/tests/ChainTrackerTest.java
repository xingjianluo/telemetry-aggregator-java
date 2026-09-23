package com.github.xingjianluo.telemetryaggregatorjava.tests;

import com.github.xingjianluo.telemetryaggregatorjava.core.ChainTracker;
import com.github.xingjianluo.telemetryaggregatorjava.model.ChainStatus;
import com.github.xingjianluo.telemetryaggregatorjava.model.ChainView;
import com.github.xingjianluo.telemetryaggregatorjava.model.TelemetryEvent;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 调用链还原单测：乱序到达（父后到）、孤立、自环/多节点环、spanId 冲突。
 */
class ChainTrackerTest {

    private static final Logger log = Logger.getLogger(ChainTrackerTest.class.getName());
    private final ChainTracker tracker = new ChainTracker(1000);

    @Test
    void outOfOrderArrivalCompletesChain() {
        // 子先到（父缺失 -> AWAITING_PARENT），父根后到 -> COMPLETE
        TelemetryEvent child = EngineTestSupport.span("c1", 100, "T", "child", "root", 30);
        ChainTracker.AddOutcome o1 = tracker.addSpan(child);
        ChainView v1 = o1.getView();
        log.info("[输入] 子 span 先到 spanId=child parent=root -> [判定] status="
                + v1.getStatus());
        assertEquals(ChainStatus.AWAITING_PARENT, v1.getStatus());
        assertEquals(1, v1.getDepth()); // 父缺失的节点以自身为根，深度为 1

        TelemetryEvent root = EngineTestSupport.span("c2", 90, "T", "root", null, 100);
        ChainView v2 = tracker.addSpan(root).getView();
        log.info("[输入] 根 span 后到 spanId=root -> [判定] status=" + v2.getStatus()
                + ", depth=" + v2.getDepth() + ", totalDuration="
                + v2.getTotalDurationMillis());
        assertEquals(ChainStatus.COMPLETE, v2.getStatus());
        assertEquals(2, v2.getDepth());
        assertEquals(130, v2.getTotalDurationMillis());
    }

    @Test
    void selfAndMultiNodeCyclesAreMarkedNotDropped() {
        // 自环：a.parent = a
        tracker.addSpan(EngineTestSupport.span("s1", 1, "C1", "a", "a", 10));
        ChainView self = tracker.getChain("C1");
        log.info("[输入] 自环 a->a -> [判定] status=" + self.getStatus()
                + ", cycle=" + self.getCycle());
        assertEquals(ChainStatus.CYCLE, self.getStatus());
        assertFalse(self.getCycle().isEmpty(), "必须给出环路径");
        assertEquals(1, tracker.getChain("C1").getSpans().size(), "环链路不被丢弃");

        // 多节点环：x->y->z->x（无根）
        tracker.addSpan(EngineTestSupport.span("s2", 1, "C2", "x", "z", 10));
        tracker.addSpan(EngineTestSupport.span("s3", 2, "C2", "y", "x", 10));
        ChainView multi = tracker.addSpan(
                EngineTestSupport.span("s4", 3, "C2", "z", "y", 10)).getView();
        log.info("[输入] 环 x->y->z->x -> [判定] status=" + multi.getStatus()
                + ", cycle=" + multi.getCycle());
        assertEquals(ChainStatus.CYCLE, multi.getStatus());
        assertEquals(4, multi.getCycle().size(), "环路径首尾重复，共4个元素");
    }

    @Test
    void conflictingSpanIdIsRejectedAndChainUntouched() {
        tracker.addSpan(EngineTestSupport.span("s1", 1, "D", "a", null, 10));
        // 相同 spanId 但不同 duration -> 标识冲突
        ChainTracker.AddOutcome conflict = tracker.addSpan(
                EngineTestSupport.span("s2", 1, "D", "a", null, 99));
        log.info("[输入] 重复 spanId=a 但 duration 不同 -> [判定] conflict="
                + conflict.isConflict() + ", reason=" + conflict.getReason());
        assertTrue(conflict.isConflict());
        assertEquals(10, tracker.getChain("D").getSpans().get(0).getDurationMillis(),
                "冲突事件不得覆盖原节点");

        // 完全相同的 span 重放 -> 幂等接受
        ChainTracker.AddOutcome replay = tracker.addSpan(
                EngineTestSupport.span("s1", 1, "D", "a", null, 10));
        log.info("[输入] 相同 span 重放 -> [判定] accepted=" + replay.isAccepted()
                + " (幂等)");
        assertTrue(replay.isAccepted());
        assertEquals(1, tracker.getChain("D").getSpans().size());
    }

    @Test
    void multipleRootsMarkedDisconnected() {
        tracker.addSpan(EngineTestSupport.span("r1", 1, "M", "r1", null, 5));
        ChainView v = tracker.addSpan(
                EngineTestSupport.span("r2", 2, "M", "r2", null, 6)).getView();
        log.info("[输入] 同一 trace 出现两个根 r1,r2 -> [判定] status=" + v.getStatus());
        assertEquals(ChainStatus.DISCONNECTED, v.getStatus());
        assertEquals(2, v.getSpans().size(), "多根链路同样保留，不丢弃");
    }

    @Test
    void pruneMarksStrandedChainsDisconnected() {
        // 父永远缺失的子 span；水位线越过它之后被标记 DISCONNECTED 而非删除
        tracker.addSpan(EngineTestSupport.span("p1", 100, "P", "child", "ghost", 10));
        int removed = tracker.prune(500, 1000);
        ChainView v = tracker.getChain("P");
        log.info("[输入] 孤立链 child->(缺失 ghost), prune@watermark=500 -> [判定] status="
                + v.getStatus() + ", removed=" + removed);
        assertEquals(ChainStatus.DISCONNECTED, v.getStatus());
        assertEquals(1, v.getSpans().size(), "断链保留可查");
        assertEquals(0, removed);
    }
}
