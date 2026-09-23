package com.github.xingjianluo.telemetryaggregatorjava.model;

/**
 * 单条事件的摄取结论。{@code reason} 给出可解释的判定依据，
 * 拒绝类结果（非 ACCEPTED/DUPLICATE）同样携带原因且不会被静默。
 */
public final class IngestResult {

    private final IngestStatus status;
    private final String reason;
    private final long eventTimestamp;
    private final Long windowStart;
    private final Long watermark;

    public IngestResult(IngestStatus status, String reason, long eventTimestamp,
                        Long windowStart, Long watermark) {
        this.status = status;
        this.reason = reason;
        this.eventTimestamp = eventTimestamp;
        this.windowStart = windowStart;
        this.watermark = watermark;
    }

    public IngestStatus getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public Long getWatermark() {
        return watermark;
    }

    public boolean isCounted() {
        return status == IngestStatus.ACCEPTED;
    }
}
