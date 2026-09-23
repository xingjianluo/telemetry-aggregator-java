package com.github.xingjianluo.telemetryaggregatorjava.model;

/**
 * 单条事件的摄取结论，可解释：被拒绝时带确定原因，且对重复投递返回首次结论。
 */
public record IngestResult(
        String eventId,
        IngestStatus status,
        RejectReason rejectReason,
        String detail,
        long windowStartMillis
) {
    public static IngestResult accepted(String eventId, long windowStart) {
        return new IngestResult(eventId, IngestStatus.ACCEPTED, null, null, windowStart);
    }

    public static IngestResult rejected(String eventId, RejectReason reason, String detail) {
        return new IngestResult(eventId, IngestStatus.REJECTED, reason, detail, -1L);
    }

    public boolean accepted() {
        return status == IngestStatus.ACCEPTED;
    }
}
