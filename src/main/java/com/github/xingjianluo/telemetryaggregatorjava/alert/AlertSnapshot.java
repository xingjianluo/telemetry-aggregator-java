package com.github.xingjianluo.telemetryaggregatorjava.alert;

import java.util.Map;

/**
 * 单个规则 + 单个评估对象（标签指纹或 traceId）的当前告警状态与判定依据。
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
        long lastChangedMillis,
        /** 调用链作用域下链路拓扑异常时的标记；TAGS 作用域为空。 */
        String traceMarker
) {
}
