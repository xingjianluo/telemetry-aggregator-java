package com.github.xingjianluo.telemetryaggregatorjava.rule;

/** 规则变更结论状态。 */
public enum RuleChangeStatus {
    /** 已产生新版本并入版本历史。 */
    APPLIED,
    /** 相同 requestId 的重试：直接返回首次结论，未重复产生版本。 */
    DUPLICATE_REQUEST,
    /** 请求本身不合法（缺规则 ID、阈值不可解析、免打扰区间倒置等）。 */
    INVALID_REQUEST,
    /** 入队 / 等待处理超时（瞬态，可携带 requestId 重试）。 */
    BUFFER_FULL,
    PROCESSING_TIMEOUT
}
