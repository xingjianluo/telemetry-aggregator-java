package com.github.xingjianluo.telemetryaggregatorjava.query;

import com.github.xingjianluo.telemetryaggregatorjava.model.RejectReason;

import java.util.Map;

/** 对外可观测的运行态快照：堆积量、接受/拒绝量（按原因区分）、基数占用与水位线。 */
public record MetricsSnapshot(
        long acceptedCount,
        long rejectedCount,
        Map<RejectReason, Long> rejectedByReason,
        int queueDepth,
        long queuePeak,
        long duplicateReplayCount,
        long watermarkMillis,
        int dedupSize,
        int liveWindowCount,
        int liveSeriesCount,
        int traceCount
) {
}
