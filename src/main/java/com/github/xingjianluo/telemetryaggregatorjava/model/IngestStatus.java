package com.github.xingjianluo.telemetryaggregatorjava.model;

/**
 * 摄取结果状态。ACCEPTED 表示计入聚合；其余均为可区分的拒绝原因，
 * 拒绝不会静默发生：计数与原因可通过 GET /stats 观察。
 */
public enum IngestStatus {
    ACCEPTED,
    INVALID,
    QUEUE_FULL,
    DEDUP_CAPACITY,
    LATE_BEHIND_WATERMARK,
    TAG_CARDINALITY,
    TOO_MANY_ACTIVE_WINDOWS,
    /**
     * 保留状态：重复投递当前统一回放到首次结论（即也可能返回 ACCEPTED），
     * 重复次数由 stats.dedupReplays 单独统计；该枚举用于显式区分“重复”这一语义类别。
     */
    DUPLICATE,
    ID_CONFLICT,
    SHUTDOWN
}
