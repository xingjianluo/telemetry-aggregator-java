package com.github.xingjianluo.telemetryaggregatorjava.model;

/**
 * 调用链中的一个 span 节点（保留首达事件的全部判定所需字段）。
 */
public final class SpanNode {

    private final String spanId;
    private final String parentSpanId;
    private final long durationMillis;
    private final long timestampMillis;
    private final String eventId;

    public SpanNode(String spanId, String parentSpanId, long durationMillis,
                    long timestampMillis, String eventId) {
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.durationMillis = durationMillis;
        this.timestampMillis = timestampMillis;
        this.eventId = eventId;
    }

    public String getSpanId() {
        return spanId;
    }

    public String getParentSpanId() {
        return parentSpanId;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    public String getEventId() {
        return eventId;
    }
}
