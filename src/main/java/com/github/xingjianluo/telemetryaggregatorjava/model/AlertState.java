package com.github.xingjianluo.telemetryaggregatorjava.model;

/** 告警规则的当前状态机状态。 */
public enum AlertState {
    /** 正常。 */
    OK,
    /** 超过阈值但尚在连续触发计数窗口内（预触发，避免抖动）。 */
    PENDING_FIRING,
    /** 告警触发中。 */
    FIRING,
    /** 告警已触发，当前低于阈值但尚在连续恢复计数窗口内。 */
    PENDING_RESOLVED
}
