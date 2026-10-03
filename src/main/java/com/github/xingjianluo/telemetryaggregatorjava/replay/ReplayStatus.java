package com.github.xingjianluo.telemetryaggregatorjava.replay;

/**
 * 复盘结论状态。超出可复盘保留范围时必须明确返回
 * {@link #NOT_REPLAYABLE}，不能给出看似完整、实际缺数据的答案。
 */
public enum ReplayStatus {
    /** 时间点已被水位线越过（存在已定稿数据），结论完整。 */
    COMPLETE,
    /** 时间点尚未被任何输入越过：只有部分窗口定稿，结论为当时真实状态。 */
    PARTIAL,
    /** 超出可复盘保留范围（早期日志已老化），拒绝给出不完整答案。 */
    NOT_REPLAYABLE,
    /** 复盘资源超限（扫描事件数 / 并发槽位），不拖垮实时摄取。 */
    RESOURCE_LIMITED
}
