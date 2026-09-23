package com.github.xingjianluo.telemetryaggregatorjava.model;

import java.util.Map;

/**
 * 一条遥测事件（也可携带调用链 span 字段）。
 *
 * <p>{@code eventId} 为投递幂等键：相同 id 的重复投递永远只计一次；
 * 若相同 id 携带不同负载，则按 {@link IngestStatus#ID_CONFLICT} 拒绝。</p>
 *
 * <p>{@code timestampMillis} 为事件时间（由生产者给出，单调推进用于水位线）。</p>
 */
public final class TelemetryEvent {

    private String eventId;
    private long timestampMillis;
    private String metric;
    private double value;
    private Map<String, String> tags;

    // ---- 可选的调用链（span）字段 ----
    /** 调用链 id；为空表示该事件不参与调用链还原。 */
    private String traceId;
    /** span id；traceId 非空时必填。 */
    private String spanId;
    /** 父 span id；根 span 为 null/空。 */
    private String parentSpanId;
    /** span 耗时（毫秒），用于按链聚合的告警判定。 */
    private long durationMillis;

    public TelemetryEvent() {
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    public void setTimestampMillis(long timestampMillis) {
        this.timestampMillis = timestampMillis;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getSpanId() {
        return spanId;
    }

    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    public String getParentSpanId() {
        return parentSpanId;
    }

    public void setParentSpanId(String parentSpanId) {
        this.parentSpanId = parentSpanId;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    public void setDurationMillis(long durationMillis) {
        this.durationMillis = durationMillis;
    }
}
