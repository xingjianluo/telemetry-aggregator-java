package com.github.xingjianluo.telemetryaggregatorjava.ingest;

import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** 摄取侧可观测计数：接受量、各拒绝原因的拒绝量、当前队列峰值等。 */
public class IngestionMetrics {

    private final AtomicLong accepted = new AtomicLong();
    private final Map<RejectReason, AtomicLong> rejected = new EnumMap<>(RejectReason.class);
    private final AtomicLong queuePeak = new AtomicLong();
    private final AtomicLong duplicateReplays = new AtomicLong();

    public IngestionMetrics() {
        for (RejectReason reason : RejectReason.values()) {
            rejected.put(reason, new AtomicLong());
        }
    }

    public void incrementAccepted() {
        accepted.incrementAndGet();
    }

    public void incrementRejected(RejectReason reason) {
        rejected.get(reason).incrementAndGet();
    }

    public void observeQueueDepth(int depth) {
        queuePeak.accumulateAndGet(depth, Math::max);
    }

    public long acceptedCount() {
        return accepted.get();
    }

    public Map<RejectReason, Long> rejectedCounts() {
        Map<RejectReason, Long> snapshot = new EnumMap<>(RejectReason.class);
        rejected.forEach((k, v) -> snapshot.put(k, v.get()));
        return snapshot;
    }

    public long queuePeak() {
        return queuePeak.get();
    }

    public void incrementDuplicateReplay() {
        duplicateReplays.incrementAndGet();
    }

    public long duplicateReplayCount() {
        return duplicateReplays.get();
    }

    public long totalRejected() {
        return rejected.values().stream().mapToLong(AtomicLong::get).sum();
    }
}
