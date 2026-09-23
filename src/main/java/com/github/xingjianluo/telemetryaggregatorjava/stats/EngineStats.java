package com.github.xingjianluo.telemetryaggregatorjava.stats;

import com.github.xingjianluo.telemetryaggregatorjava.model.IngestStatus;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 引擎运行指标：所有接受/拒绝计数、当前堆积量、表规模与水位线。
 * 任何拒绝都计入对应计数器，不会静默。线程安全。
 */
public final class EngineStats {

    private final LongAdder received = new LongAdder();
    /** 命中幂等去重的重复投递次数（无论首次结论是接受还是拒绝，回放均计入）。 */
    private final LongAdder dedupReplays = new LongAdder();
    private final Map<IngestStatus, LongAdder> byStatus = new LinkedHashMap<>();

    private final AtomicLong queueDepth = new AtomicLong();
    private final AtomicLong dedupSize = new AtomicLong();
    private final AtomicLong spanCount = new AtomicLong();
    private final AtomicLong activeWindows = new AtomicLong();
    private final AtomicLong closedWindows = new AtomicLong();
    private final AtomicLong watermark = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong maxObservedEventTime = new AtomicLong(Long.MIN_VALUE);

    public EngineStats() {
        for (IngestStatus s : IngestStatus.values()) {
            byStatus.put(s, new LongAdder());
        }
    }

    public void recordReceived() {
        received.increment();
    }

    public void recordDedupReplay() {
        dedupReplays.increment();
    }

    public void recordStatus(IngestStatus status) {
        byStatus.get(status).increment();
    }

    public void setQueueDepth(long v) {
        queueDepth.set(v);
    }

    public void setDedupSize(long v) {
        dedupSize.set(v);
    }

    public void setSpanCount(long v) {
        spanCount.set(v);
    }

    public void setActiveWindows(long v) {
        activeWindows.set(v);
    }

    public void incrementClosedWindows() {
        closedWindows.incrementAndGet();
    }

    public void updateWatermark(long w) {
        watermark.set(w);
    }

    public void observeEventTime(long t) {
        maxObservedEventTime.updateAndGet(cur -> Math.max(cur, t));
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("received", received.sum());
        m.put("dedupReplays", dedupReplays.sum());
        Map<String, Long> statuses = new LinkedHashMap<>();
        byStatus.forEach((s, a) -> statuses.put(s.name(), a.sum()));
        m.put("byStatus", statuses);
        m.put("queueDepth", queueDepth.get());
        m.put("dedupSize", dedupSize.get());
        m.put("spanCount", spanCount.get());
        m.put("activeWindows", activeWindows.get());
        m.put("closedWindows", closedWindows.get());
        m.put("watermark", watermark.get() == Long.MIN_VALUE ? null : watermark.get());
        m.put("maxObservedEventTime",
                maxObservedEventTime.get() == Long.MIN_VALUE ? null : maxObservedEventTime.get());
        return m;
    }
}
