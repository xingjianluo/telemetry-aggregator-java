package com.github.xingjianluo.telemetryaggregatorjava.replay;

/** 复盘结论状态：任何不可完整复盘的情形都显式标注，绝不给出看似完整的缺数据答案。 */
public enum ReplayStatus {
    /** 复盘完成，结论完整。 */
    OK,
    /** 请求的边界超出可复盘保留范围（事件日志或规则历史已淘汰）。 */
    OUT_OF_RETENTION,
    /** 边界内事件量超过单次复盘上限。 */
    TOO_LARGE,
    /** 已有复盘在执行，为避免拖慢实时摄取而快速失败。 */
    BUSY
}
