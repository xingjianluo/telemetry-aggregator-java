package com.github.xingjianluo.telemetryaggregatorjava.model;

/**
 * 告警状态变迁事件（触发 / 恢复），保留判定时的观测值与依据。
 */
public final class AlertEvent {

    private final String ruleId;
    private final String traceId;
    private final AlertState state;
    private final double observedValue;
    private final String reason;
    private final long eventTimeMillis;

    public AlertEvent(String ruleId, String traceId, AlertState state,
                      double observedValue, String reason, long eventTimeMillis) {
        this.ruleId = ruleId;
        this.traceId = traceId;
        this.state = state;
        this.observedValue = observedValue;
        this.reason = reason;
        this.eventTimeMillis = eventTimeMillis;
    }

    public String getRuleId() {
        return ruleId;
    }

    public String getTraceId() {
        return traceId;
    }

    public AlertState getState() {
        return state;
    }

    public double getObservedValue() {
        return observedValue;
    }

    public String getReason() {
        return reason;
    }

    public long getEventTimeMillis() {
        return eventTimeMillis;
    }
}
