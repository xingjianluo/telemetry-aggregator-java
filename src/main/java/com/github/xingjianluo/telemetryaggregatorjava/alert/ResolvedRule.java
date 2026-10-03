package com.github.xingjianluo.telemetryaggregatorjava.alert;

/**
 * 针对某个定稿窗口解析出的可执行规则：规则定义 + 版本 + 免打扰边界。
 * 告警引擎只面向该解析结果评估，不直接感知规则管理。
 */
public record ResolvedRule(
        AlertRule rule,
        /** 该定义对应的规则版本（0 表示初始规则集）。 */
        long version,
        /** 免打扰截止（事件时间）；窗口 end <= 该值时判定被抑制。0 表示未设置。 */
        long muteUntilMillis
) {
}
