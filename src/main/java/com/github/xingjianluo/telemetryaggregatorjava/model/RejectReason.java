package com.github.xingjianluo.telemetryaggregatorjava.model;

/**
 * 事件未被计入聚合时的可区分原因。任何拒绝都必须显式携带其中之一，禁止静默丢弃。
 */
public enum RejectReason {
    /** 事件时间早于 watermark - allowedLateness，超出迟到容忍。 */
    LATE_BEHIND_WATERMARK,
    /** 相同事件 ID 此前已被接受或已被拒绝（幂等：重试返回首次结论，不重复计数）。 */
    DUPLICATE_EVENT_ID,
    /** 相同事件 ID 但载荷与首次投递不一致，存在标识冲突。 */
    CONFLICTING_EVENT_ID,
    /** 事件本身字段不合法（缺 ID、时间为负等）。 */
    INVALID_EVENT,
    /** 摄取缓冲队列已满，背压拒绝。 */
    BUFFER_FULL,
    /** 事件 ID 去重池已达基数上限。 */
    TOO_MANY_EVENT_IDS,
    /** 单窗口标签序列数已达基数上限。 */
    TOO_MANY_SERIES,
    /** 标签数量或标签值长度超限。 */
    TAG_CARDINALITY_LIMIT,
    /** 调用链数量已达基数上限。 */
    TOO_MANY_TRACES,
    /** 单条调用链内 span 数已达上限。 */
    TOO_MANY_SPANS,
    /** 入队后等待处理结果超时（事件可能已被处理，重试将由幂等层给出确定结论）。 */
    PROCESSING_TIMEOUT
}
