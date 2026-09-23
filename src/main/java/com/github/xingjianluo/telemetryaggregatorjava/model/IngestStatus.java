package com.github.xingjianluo.telemetryaggregatorjava.model;

/** 单条事件摄取结论。 */
public enum IngestStatus {
    /** 已计入聚合。 */
    ACCEPTED,
    /** 已拒绝，原因见 rejectReason。 */
    REJECTED
}
