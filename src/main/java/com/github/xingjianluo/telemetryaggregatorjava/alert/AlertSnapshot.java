package com.github.xingjianluo.telemetryaggregatorjava.alert;

import java.util.Map;

/**
 * 单个规则 × 单个评估对象（标签指纹或 traceId）的当前告警状态与判定依据。
 * ruleVersion 标明最近一次判定使用的规则版本；muted 标明该对象当前处于免打扰区间。
 */
public record AlertSnapshot(
        String ruleId,
        AlertRule.Scope scope,
        String subjectKey,
        Map<String, String> tags,
        AlertState state,
        int consecutiveBreaches,
        int consecutiveRecoveries,
        double lastValue,
        long lastEvaluatedWindowStart,
        /** 最近一次状态翻转发生的窗口 end（事件时间）。 */
        long lastChangedMillis,
        /** 调用链作用域下链路拓扑异常时的标记；TAGS 作用域为空。 */
        String traceMarker,
        /** 最近一次判定使用的规则版本（0 为初始规则集）。 */
        long ruleVersion,
        /** 最近一次判定时该对象是否处于免打扰区间。 */
        boolean muted
) {
}
