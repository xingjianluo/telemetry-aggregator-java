package com.github.xingjianluo.telemetryaggregatorjava.alert;

/** 告警状态机状态：使用滞回（连续计数 + 冷却）抑制阈值附近抖动。 */
public enum AlertState {
    /** 未超阈值。 */
    OK,
    /** 已超阈值但连续确认次数不足，不对外报警。 */
    PENDING,
    /** 已对外触发。 */
    FIRING
}
