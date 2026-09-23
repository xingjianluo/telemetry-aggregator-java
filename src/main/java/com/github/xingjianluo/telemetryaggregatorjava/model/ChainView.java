package com.github.xingjianluo.telemetryaggregatorjava.model;

import java.util.List;

/**
 * 一条调用链当前的判定视图（不可变快照）。
 */
public final class ChainView {

    private final String traceId;
    private final ChainStatus status;
    private final List<String> cycle;
    private final List<SpanNode> spans;
    private final long totalDurationMillis;
    private final int depth;
    private final long updatedAtEventTime;

    public ChainView(String traceId, ChainStatus status, List<String> cycle,
                     List<SpanNode> spans, long totalDurationMillis, int depth,
                     long updatedAtEventTime) {
        this.traceId = traceId;
        this.status = status;
        this.cycle = List.copyOf(cycle);
        this.spans = List.copyOf(spans);
        this.totalDurationMillis = totalDurationMillis;
        this.depth = depth;
        this.updatedAtEventTime = updatedAtEventTime;
    }

    public String getTraceId() {
        return traceId;
    }

    public ChainStatus getStatus() {
        return status;
    }

    public List<String> getCycle() {
        return cycle;
    }

    public List<SpanNode> getSpans() {
        return spans;
    }

    public long getTotalDurationMillis() {
        return totalDurationMillis;
    }

    public int getDepth() {
        return depth;
    }

    public long getUpdatedAtEventTime() {
        return updatedAtEventTime;
    }
}
